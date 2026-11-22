"""Full PAPER session clear operation; trading remains paused afterwards."""
from __future__ import annotations

import asyncio
from typing import Any

from fastapi import HTTPException


class PaperSessionClearMixin:
    """Execute the full operator-confirmed PAPER reset."""

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
