"""Operator-controlled PAPER session maintenance operations.

This runtime coordinates the stateful reset/cleanup/resume workflow. HTTP route
registration lives in :mod:\`interface.paper_session_routes\`.
"""
from __future__ import annotations

import asyncio
import json
import time
from datetime import datetime
from typing import Any, Callable

from fastapi import HTTPException


PAPER_CLEAR_CONFIRMATION = "ОЧИСТИТЬ PAPER-СЕССИЮ"
_PAPER_MAINTENANCE_KEY = "piluitus:paper:maintenance"


class PaperSessionMaintenanceRuntime:
    """Own guarded PAPER reset, stale cleanup and explicit resume operations."""

    def __init__(
        self,
        *,
        db_manager: Any,
        execution_engine: Any,
        risk_manager: Any,
        paper_session_lock: Any,
        anomaly_queue: Any,
        ai_candidate_deduplicator: Any,
        paper_performance_tracker: Any,
        shadow_tracker: Any,
        paper_shadow_labels: list[dict[str, Any]],
        last_ai_verdicts_cache: list[dict[str, Any]],
        trade_flow_counters: Any,
        pending_bbo_candidates: dict[str, dict[str, Any]],
        pending_bbo_lock: Any,
        position_order_book_diagnostics: Any,
        require_paper_mode: Callable[[], None],
        advance_session_generation: Callable[[], Any],
        reset_balance_guard_signature: Callable[[], Any],
        rotate_bbo_replay_session: Callable[[], Any],
        refresh_dashboard_balance_snapshot: Callable[[list[dict[str, Any]] | None], Any],
        broadcast_paper_tables: Callable[[], Any],
        broadcast_paper_session_reset: Callable[[dict[str, Any]], Any],
        get_bbo_replay_session_id: Callable[[], str],
        deposit: float,
        logger: Any,
        paper_clear_confirmation: str = PAPER_CLEAR_CONFIRMATION,
        monotonic: Callable[[], float] = time.monotonic,
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.db_manager = db_manager
        self.execution_engine = execution_engine
        self.risk_manager = risk_manager
        self.paper_session_lock = paper_session_lock
        self.anomaly_queue = anomaly_queue
        self.ai_candidate_deduplicator = ai_candidate_deduplicator
        self.paper_performance_tracker = paper_performance_tracker
        self.shadow_tracker = shadow_tracker
        self.paper_shadow_labels = paper_shadow_labels
        self.last_ai_verdicts_cache = last_ai_verdicts_cache
        self.trade_flow_counters = trade_flow_counters
        self.pending_bbo_candidates = pending_bbo_candidates
        self.pending_bbo_lock = pending_bbo_lock
        self.position_order_book_diagnostics = position_order_book_diagnostics
        self.require_paper_mode = require_paper_mode
        self.advance_session_generation = advance_session_generation
        self.reset_balance_guard_signature = reset_balance_guard_signature
        self.rotate_bbo_replay_session = rotate_bbo_replay_session
        self.refresh_dashboard_balance_snapshot = refresh_dashboard_balance_snapshot
        self.broadcast_paper_tables = broadcast_paper_tables
        self.broadcast_paper_session_reset = broadcast_paper_session_reset
        self.get_bbo_replay_session_id = get_bbo_replay_session_id
        self.deposit = float(deposit)
        self.logger = logger
        self.paper_clear_confirmation = str(paper_clear_confirmation)
        self.monotonic = monotonic
        self.now = now or (lambda: datetime.now().astimezone())

    async def _persist_maintenance(self, *, active: bool, reason: str) -> None:
        """Persist the maintenance gate before changing the corresponding phase."""
        await self.db_manager.set_json(
            _PAPER_MAINTENANCE_KEY,
            {
                "active": bool(active),
                "reason": str(reason),
                "updated_at": self.now().isoformat(timespec="seconds"),
            },
        )

    def _purge_queued_market_events(self) -> None:
        """Drop stale market work while preserving priority-zero emergency commands."""
        emergency_items = []
        while True:
            try:
                queued_item = self.anomaly_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                if (
                    isinstance(queued_item, tuple)
                    and queued_item
                    and queued_item[0] == 0
                ):
                    emergency_items.append(queued_item)
            finally:
                self.anomaly_queue.task_done()
        for item in emergency_items:
            self.anomaly_queue.put_nowait(item)

    async def clear_paper_session(
        self, payload: dict[str, object]
    ) -> dict[str, Any]:
        """Clear the current PAPER session and leave trading paused for review."""
        self.require_paper_mode()
        if (
            str(payload.get("confirmation", "")).strip()
            != self.paper_clear_confirmation
        ):
            raise HTTPException(
                status_code=400,
                detail=(
                    "Для подтверждения введите точную фразу: "
                    f"{self.paper_clear_confirmation}"
                ),
            )

        async with self.paper_session_lock:
            engine = self.execution_engine
            engine._maintenance_active = True
            engine._maintenance_reason = (
                "PAPER-сессия приостановлена для полной очистки. "
                "Проверьте результат и вручную возобновите PAPER."
            )
            self.advance_session_generation()
            # Persist the pause first. Any later failure must leave the bot paused
            # across process restarts rather than silently resuming a partial reset.
            await self._persist_maintenance(
                active=True, reason=engine._maintenance_reason
            )

            self._purge_queued_market_events()
            self.ai_candidate_deduplicator.clear()

            if engine._opening_tickers or engine._lock_closing:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "PAPER-операция ещё выполняется. Торговля оставлена на паузе; "
                        "обновите страницу и повторите очистку."
                    ),
                )

            monitor_task = engine._monitor_worker_task
            if monitor_task is not None and not monitor_task.done():
                monitor_task.cancel()
                await asyncio.gather(monitor_task, return_exceptions=True)
            engine._monitor_worker_task = None
            engine._pending_monitor_prices.clear()

            try:
                storage_result = await self.db_manager.clear_paper_session()
            except RuntimeError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc

            removed_memory_positions = len(engine.paper_positions)
            removed_memory_closed = len(engine.closed_trades)
            removed_memory_performance = self.paper_performance_tracker.clear()
            removed_memory_shadow_pending = self.shadow_tracker.clear()
            removed_memory_shadow_labels = len(self.paper_shadow_labels)
            self.paper_shadow_labels.clear()
            removed_memory_stale = len(engine._unreconciled_paper_positions)
            engine.paper_positions.clear()
            engine.closed_trades.clear()
            engine._unreconciled_paper_positions.clear()
            engine._ticker_cooldowns.clear()
            engine._recent_stop_losses.clear()
            engine._hydrated_stop_loss_keys.clear()
            engine._reserved_notional.clear()
            engine._reserved_stop_risk.clear()

            risk = self.risk_manager
            risk._balance_initialized = False
            risk.block_trading = False
            risk._blocked_date = None
            risk.total_session_pnl = 0.0
            risk.realized_pnl = 0.0
            risk.open_pnl = 0.0
            risk.current_equity = self.deposit
            risk.free_margin = self.deposit
            risk.high_water_mark = self.deposit
            risk.current_drawdown_pct = 0.0
            risk._drawdown_warning_active = False
            risk._loss_streak_count = 0
            risk._loss_pause_signature = None
            risk._cached_trading_allowed = None
            risk._last_risk_snapshot_monotonic = 0.0

            self.last_ai_verdicts_cache.clear()
            self.trade_flow_counters.reset(self.monotonic())
            with self.pending_bbo_lock:
                self.pending_bbo_candidates.clear()
            self.position_order_book_diagnostics.clear()
            try:
                await self.rotate_bbo_replay_session()
            except Exception as replay_exc:
                engine._maintenance_active = True
                engine._maintenance_reason = (
                    "PAPER-сессия очищена, но новый BBO replay-сеанс не запустился; "
                    "торговля оставлена на паузе."
                )
                self.logger.critical(
                    "[PAPER BBO REPLAY SESSION RESET ERROR] trading remains paused: %s",
                    replay_exc,
                    exc_info=True,
                )
                raise HTTPException(
                    status_code=500, detail=engine._maintenance_reason
                ) from replay_exc

            self.reset_balance_guard_signature()
            balance = await self.refresh_dashboard_balance_snapshot([])
            await self.broadcast_paper_session_reset(
                {
                    "type": "PAPER_SESSION_RESET",
                    "balance": balance,
                    "active_positions": [],
                    "closed_trades": [],
                }
            )

            result = {
                "status": "ok",
                "maintenance_active": True,
                "message": (
                    "PAPER-сессия очищена. Для защиты от старых сигналов "
                    "торговый конвейер оставлен на паузе."
                ),
                "shadow_pending_discarded": removed_memory_shadow_pending,
                "shadow_labels_discarded": removed_memory_shadow_labels,
                "bbo_replay_session_id": self.get_bbo_replay_session_id(),
                "storage": storage_result,
                "memory": {
                    "removed_open_positions": removed_memory_positions,
                    "removed_closed_trades": removed_memory_closed,
                    "removed_performance_records": removed_memory_performance,
                    "removed_stale_positions": removed_memory_stale,
                },
                "balance": balance,
            }
            self.logger.warning(
                "🧹 Полная очистка PAPER-сессии завершена; торговля "
                "приостановлена до ручного возобновления."
            )
            return result

    async def cleanup_stale_and_resume(self) -> dict[str, Any]:
        """Delete only quarantined STALE_REVIEW rows, then resume PAPER atomically."""
        self.require_paper_mode()

        async with self.paper_session_lock:
            engine = self.execution_engine
            if engine._opening_tickers or engine._lock_closing:
                raise HTTPException(
                    status_code=409,
                    detail="Есть незавершённая операция исполнения. Очистка и запуск отменены.",
                )

            snapshot_before = await self.db_manager.get_paper_session_snapshot()
            if (
                snapshot_before.get("live_trade_count", 0)
                or snapshot_before.get("live_history_count", 0)
                or snapshot_before.get("live_active_order_count", 0)
            ):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "В хранилище обнаружены LIVE-записи. Автоматический запуск "
                        "PAPER заблокирован; данные не изменены."
                    ),
                )

            engine._maintenance_active = True
            engine._maintenance_reason = (
                "PAPER приостановлен на время удаления только STALE_REVIEW-записей."
            )
            self.advance_session_generation()
            await self._persist_maintenance(
                active=True, reason=engine._maintenance_reason
            )

            # Drop queued market events so old candidates cannot run immediately
            # after resume. Priority-zero emergency commands are retained.
            self._purge_queued_market_events()
            self.ai_candidate_deduplicator.clear()

            try:
                storage_result = (
                    await self.db_manager.clear_unreconciled_paper_positions()
                )
            except RuntimeError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc

            removed_ids = {
                str(order_id) for order_id in storage_result.get("position_ids", [])
            }
            removed_memory_stale = sum(
                1
                for order_id in removed_ids
                if order_id in engine._unreconciled_paper_positions
            )
            for order_id in removed_ids:
                engine._unreconciled_paper_positions.pop(order_id, None)

            snapshot_after = await self.db_manager.get_paper_session_snapshot()
            if snapshot_after.get("paper_stale_count", 0) > 0:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Осталось STALE_REVIEW-позиций: "
                        f"{snapshot_after['paper_stale_count']}. "
                        "PAPER оставлен на паузе; проверьте журнал."
                    ),
                )
            if (
                snapshot_after.get("live_trade_count", 0)
                or snapshot_after.get("live_history_count", 0)
                or snapshot_after.get("live_active_order_count", 0)
            ):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "После очистки обнаружены LIVE-записи. "
                        "PAPER оставлен на паузе."
                    ),
                )

            # No unresolved PAPER records remain in persistent storage. Clear any
            # matching in-memory quarantine remnants but preserve current open
            # positions, closed history, verdicts and risk state.
            removed_memory_stale += len(engine._unreconciled_paper_positions)
            engine._unreconciled_paper_positions.clear()
            # Persist the resume before lifting the in-memory maintenance lock.
            # If storage fails, this process must remain paused.
            await self._persist_maintenance(active=False, reason="")
            engine._maintenance_active = False
            engine._maintenance_reason = ""

            self.reset_balance_guard_signature()
            await self.broadcast_paper_tables()
            result = {
                "status": "ok",
                "maintenance_active": False,
                "message": (
                    "STALE_REVIEW-записи удалены. PAPER-конвейер автоматически "
                    "возобновлён; закрытая история, вердикты, риск-состояние и "
                    "текущие позиции в памяти сохранены."
                ),
                "storage": storage_result,
                "memory": {"removed_stale_positions": removed_memory_stale},
                "preserved": {
                    "paper_history_rows": snapshot_after.get("paper_history_count", 0),
                    "verdict_history_rows": snapshot_after.get("verdict_history_count", 0),
                    "verdict_execution_keys": snapshot_after.get("verdict_execution_count", 0),
                    "active_memory_positions": len(engine.paper_positions),
                },
            }
            self.logger.warning(
                "[PAPER STALE CLEANUP + RESUME] deleted=%d; memory_stale=%d; PAPER resumed.",
                storage_result.get("deleted_stale_positions", 0),
                removed_memory_stale,
            )
            return result

    async def resume_paper_session(self) -> dict[str, Any]:
        """Manually resume PAPER after verifying persistent state and execution locks."""
        self.require_paper_mode()
        async with self.paper_session_lock:
            engine = self.execution_engine
            if engine._opening_tickers or engine._lock_closing:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Выполняется операция исполнения; PAPER пока нельзя возобновить."
                    ),
                )
            snapshot = await self.db_manager.get_paper_session_snapshot()
            if snapshot.get("paper_stale_count", 0) > 0:
                raise HTTPException(
                    status_code=409,
                    detail="Сначала завершите сверку всех STALE_REVIEW-позиций.",
                )
            if (
                snapshot.get("live_trade_count", 0)
                or snapshot.get("live_history_count", 0)
                or snapshot.get("live_active_order_count", 0)
            ):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "В хранилище есть LIVE-записи. Возобновление PAPER "
                        "заблокировано до проверки хранилища."
                    ),
                )
            engine._maintenance_active = False
            engine._maintenance_reason = ""
            await self._persist_maintenance(active=False, reason="")
            self.logger.warning("▶️ PAPER-сессия вручную возобновлена оператором.")
            await self.broadcast_paper_tables()
            return {
                "status": "ok",
                "maintenance_active": False,
                "message": "PAPER-торговля возобновлена. Новые сигналы будут приниматься.",
            }
