"""Read-only PAPER research endpoints and operator reconciliation services."""
from __future__ import annotations

import math
import os
from collections.abc import Mapping
from datetime import datetime
from typing import Any, Callable

from fastapi import HTTPException


class PaperResearchRuntime:
    """Own PAPER analytics/replay APIs and explicit stale-position reconciliation."""

    def __init__(
        self,
        *,
        db_manager: Any,
        is_paper_trade: Callable[[Mapping[str, Any], str], bool],
        execution_engine: Any,
        screener: Any,
        paper_performance_tracker: Any,
        paper_shadow_labels: list[dict[str, Any]],
        paper_session_lock: Any,
        broadcast_paper_tables: Callable[[], Any],
        bbo_replay_storage_key: str,
        bbo_replay_storage_limit: int,
        paper_closed_trades_storage_limit: int,
        shadow_outcomes_storage_limit: int,
        lot_sizes: Mapping[str, int],
        parse_finite_float: Callable[[object], float | None],
        replay_exit_batch: Callable[..., dict[str, Any]],
        replay_exit_scenarios: Callable[..., dict[str, Any]],
        build_shadow_reconciliation: Callable[..., dict[str, Any]],
        build_actual_vs_replay_reconciliation: Callable[..., dict[str, Any]],
        build_shadow_cost_sensitivity: Callable[..., dict[str, Any]],
        build_shadow_walk_forward_validation: Callable[..., dict[str, Any]],
        now: Callable[[], datetime] | None = None,
    ) -> None:
        self.db_manager = db_manager
        self.is_paper_trade = is_paper_trade
        self.execution_engine = execution_engine
        self.screener = screener
        self.paper_performance_tracker = paper_performance_tracker
        self.paper_shadow_labels = paper_shadow_labels
        self.paper_session_lock = paper_session_lock
        self.broadcast_paper_tables = broadcast_paper_tables
        self.bbo_replay_storage_key = bbo_replay_storage_key
        self.bbo_replay_storage_limit = max(1, int(bbo_replay_storage_limit))
        self.paper_closed_trades_storage_limit = max(
            1, int(paper_closed_trades_storage_limit)
        )
        self.shadow_outcomes_storage_limit = max(
            1, int(shadow_outcomes_storage_limit)
        )
        self.lot_sizes = lot_sizes
        self.parse_finite_float = parse_finite_float
        self.replay_exit_batch_fn = replay_exit_batch
        self.replay_exit_scenarios_fn = replay_exit_scenarios
        self.build_shadow_reconciliation_fn = build_shadow_reconciliation
        self.build_actual_vs_replay_reconciliation_fn = (
            build_actual_vs_replay_reconciliation
        )
        self.build_shadow_cost_sensitivity_fn = build_shadow_cost_sensitivity
        self.build_shadow_walk_forward_validation_fn = (
            build_shadow_walk_forward_validation
        )
        self.now = now or (lambda: datetime.now().astimezone())

    def require_paper_mode(self) -> None:
        if not getattr(self.execution_engine, "paper_mode", True):
            raise HTTPException(
                status_code=403,
                detail="Операция доступна только в PAPER_TRADING=True.",
            )

    @staticmethod
    def _paper_trade_rows(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
        rows = snapshot.get("paper_trades", [])
        return (
            [dict(row) for row in rows if isinstance(row, dict)]
            if isinstance(rows, list) else []
        )

    def _max_bbo_age_sec(self) -> float:
        configured_age = getattr(self.screener, "bbo_max_age_sec", None)
        if configured_age is None:
            configured_age = os.getenv(
                "PILUITUS_BBO_MAX_AGE_SEC",
                os.getenv("PILUITUS_BBO_EXIT_MAX_AGE_SEC", "2.0"),
            )
        try:
            max_age = float(configured_age)
        except (TypeError, ValueError, OverflowError):
            max_age = 2.0
        if not math.isfinite(max_age) or max_age <= 0.0:
            max_age = 2.0
        return min(5.0, max(0.25, max_age))

    def get_performance(self) -> dict[str, Any]:
        self.require_paper_mode()
        return {
            "paper_mode": True,
            "retained_trade_limit": self.paper_closed_trades_storage_limit,
            "scorecard": self.paper_performance_tracker.scorecard(
                self.paper_shadow_labels
            ),
        }

    async def replay_batch(
        self,
        *,
        limit: int = 30,
        ticker: str | None = None,
        session_id: str | None = None,
        portfolio_capital_rub: float | None = None,
        max_allocation_pct: float = 15.0,
    ) -> dict[str, Any]:
        """Read-only batch replay of recent closed PAPER positions."""
        self.require_paper_mode()
        if limit < 1 or limit > 100:
            raise HTTPException(status_code=422, detail="limit должен быть от 1 до 100.")
        if portfolio_capital_rub is not None and (
            not math.isfinite(portfolio_capital_rub) or portfolio_capital_rub <= 0.0
        ):
            raise HTTPException(
                status_code=422,
                detail="portfolio_capital_rub должен быть положительным числом.",
            )
        if (
            not math.isfinite(max_allocation_pct)
            or not 0.0 < max_allocation_pct <= 100.0
        ):
            raise HTTPException(
                status_code=422,
                detail="max_allocation_pct должен быть в диапазоне (0, 100].",
            )

        all_trades = await self.db_manager.get_all_daily_trades()
        if not isinstance(all_trades, Mapping):
            all_trades = {}
        normalized_ticker = str(ticker or "").strip().upper().split("@")[0]
        normalized_session = str(session_id or "").strip()
        candidates: list[dict[str, Any]] = []
        skipped = {
            "NOT_CLOSED_PAPER": 0,
            "NO_REPLAY_SESSION": 0,
            "TICKER_FILTERED": 0,
            "SESSION_FILTERED": 0,
        }
        for order_id, raw in all_trades.items():
            if (
                not isinstance(raw, Mapping)
                or not self.is_paper_trade(raw, str(order_id))
            ):
                skipped["NOT_CLOSED_PAPER"] += 1
                continue
            if str(raw.get("status") or "").upper() != "CLOSED":
                skipped["NOT_CLOSED_PAPER"] += 1
                continue
            trade_ticker = str(raw.get("ticker") or "").strip().upper().split("@")[0]
            trade_session = str(raw.get("bbo_replay_session_id") or "").strip()
            if normalized_ticker and trade_ticker != normalized_ticker:
                skipped["TICKER_FILTERED"] += 1
                continue
            if normalized_session and trade_session != normalized_session:
                skipped["SESSION_FILTERED"] += 1
                continue
            if not trade_session:
                skipped["NO_REPLAY_SESSION"] += 1
                continue
            candidates.append(dict(raw, id=str(raw.get("id") or order_id)))

        candidates.sort(
            key=lambda row: str(
                row.get("exit_timestamp") or row.get("exit_time") or ""
            ),
            reverse=True,
        )
        selected = candidates[:limit]
        if not selected:
            return {
                "paper_mode": True,
                "selected_positions": 0,
                "skipped": skipped,
                "replay": {
                    "schema_version": 1,
                    "engine": "PAPER_EXIT_REPLAY_V1",
                    "positions": 0,
                    "trades": [],
                    "summary_by_scenario": {},
                    "matched_cohort_comparison": {
                        "available": False,
                        "reason": "NO_REPLAYED_POSITIONS",
                        "comparisons_vs_baseline": {},
                        "all_scenarios_common_cohort": None,
                    },
                },
                "execution_reconciliation": (
                    self.build_actual_vs_replay_reconciliation_fn(
                        [], {"trades": []}, limit=limit
                    )
                ),
                "note": (
                    "Нет закрытых PAPER-сделок с bbo_replay_session_id, "
                    "подходящих под фильтры."
                ),
            }

        required_sessions = {
            str(row["bbo_replay_session_id"]) for row in selected
        }
        stored_events = await self.db_manager.get_list(
            self.bbo_replay_storage_key,
            start=0,
            stop=self.bbo_replay_storage_limit - 1,
        )
        replay_events = [
            row for row in stored_events
            if isinstance(row, Mapping)
            and str(row.get("session_id") or "") in required_sessions
        ]
        if not replay_events:
            raise HTTPException(
                status_code=409,
                detail="В журнале BBO нет событий для выбранных replay-сессий.",
            )

        max_age = self._max_bbo_age_sec()
        try:
            replay = self.replay_exit_batch_fn(
                replay_events,
                selected,
                max_bbo_age_sec=max_age,
                portfolio_capital_rub=portfolio_capital_rub,
                max_allocation_pct=max_allocation_pct,
            )
        except ValueError as replay_error:
            raise HTTPException(
                status_code=422, detail=str(replay_error)
            ) from replay_error

        return {
            "paper_mode": True,
            "selected_positions": len(selected),
            "candidate_positions": len(candidates),
            "limit": limit,
            "skipped": skipped,
            "journal_events_loaded": len(replay_events),
            "retained_event_limit": self.bbo_replay_storage_limit,
            "replay": replay,
            "execution_reconciliation": (
                self.build_actual_vs_replay_reconciliation_fn(
                    selected, replay, scenario_name="BASELINE_SL_TP", limit=limit
                )
            ),
            "note": (
                "Replay является исследовательской симуляцией; открытые в конце "
                "сценария позиции не включены в реализованный PnL."
            ),
        }

    def get_shadow_reconciliation(self, *, limit: int = 100) -> dict[str, Any]:
        self.require_paper_mode()
        if limit < 1 or limit > 500:
            raise HTTPException(
                status_code=422, detail="limit должен быть от 1 до 500."
            )
        result = self.build_shadow_reconciliation_fn(
            self.paper_performance_tracker.records,
            self.paper_shadow_labels,
            limit=limit,
        )
        return {"paper_mode": True, **result}

    def get_shadow_walk_forward(
        self,
        *,
        folds: int = 5,
        embargo_seconds: float = 30.0,
        limit: int = 20000,
    ) -> dict[str, Any]:
        self.require_paper_mode()
        if folds < 3 or folds > 10:
            raise HTTPException(
                status_code=422, detail="folds должен быть от 3 до 10."
            )
        if (
            not math.isfinite(embargo_seconds)
            or embargo_seconds < 0.0
            or embargo_seconds > 3600.0
        ):
            raise HTTPException(
                status_code=422,
                detail="embargo_seconds должен быть от 0 до 3600.",
            )
        if limit < 1 or limit > self.shadow_outcomes_storage_limit:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"limit должен быть от 1 до "
                    f"{self.shadow_outcomes_storage_limit}."
                ),
            )
        return {
            "paper_mode": True,
            **self.build_shadow_walk_forward_validation_fn(
                self.paper_shadow_labels,
                folds=folds,
                embargo_seconds=embargo_seconds,
                max_horizon_seconds=600,
                limit=limit,
            ),
        }

    def get_shadow_cost_sensitivity(self, *, limit: int = 20000) -> dict[str, Any]:
        self.require_paper_mode()
        if limit < 1 or limit > self.shadow_outcomes_storage_limit:
            raise HTTPException(
                status_code=422,
                detail=(
                    f"limit должен быть от 1 до "
                    f"{self.shadow_outcomes_storage_limit}."
                ),
            )
        return {
            "paper_mode": True,
            **self.build_shadow_cost_sensitivity_fn(
                self.paper_shadow_labels,
                scenarios_bps=(0.0, 1.0, 2.0, 5.0),
                limit=limit,
            ),
        }

    async def get_research_report(self, *, limit: int = 30) -> dict[str, Any]:
        self.require_paper_mode()
        if limit < 1 or limit > 100:
            raise HTTPException(
                status_code=422, detail="limit должен быть от 1 до 100."
            )
        scorecard = self.paper_performance_tracker.scorecard(
            self.paper_shadow_labels
        )
        reconciliation = self.build_shadow_reconciliation_fn(
            self.paper_performance_tracker.records,
            self.paper_shadow_labels,
            limit=limit,
        )
        try:
            exit_replay = await self.replay_batch(limit=limit)
        except HTTPException as replay_exc:
            if replay_exc.status_code != 409:
                raise
            exit_replay = {
                "available": False,
                "status_code": replay_exc.status_code,
                "detail": replay_exc.detail,
            }
        return {
            "paper_mode": True,
            "generated_at": self.now().isoformat(timespec="seconds"),
            "limit": limit,
            "realized_scorecard": scorecard,
            "shadow_reconciliation": reconciliation,
            "shadow_walk_forward": (
                self.build_shadow_walk_forward_validation_fn(
                    self.paper_shadow_labels,
                    folds=5,
                    embargo_seconds=30.0,
                    max_horizon_seconds=600,
                    limit=self.shadow_outcomes_storage_limit,
                )
            ),
            "shadow_cost_sensitivity": (
                self.build_shadow_cost_sensitivity_fn(
                    self.paper_shadow_labels,
                    scenarios_bps=(0.0, 1.0, 2.0, 5.0),
                    limit=self.shadow_outcomes_storage_limit,
                )
            ),
            "exit_replay": exit_replay,
            "interpretation": (
                "Read-only research snapshot. Realized PAPER PnL, hypothetical "
                "Shadow labels and replay scenarios are separate evidence sets; "
                "replay is not portfolio-level and none of these diagnostics "
                "changes active trading rules."
            ),
        }

    async def replay_trade(self, order_id: str) -> dict[str, Any]:
        self.require_paper_mode()
        all_trades = await self.db_manager.get_all_daily_trades()
        trade = (
            all_trades.get(order_id)
            if isinstance(all_trades, Mapping) else None
        )
        if (
            not isinstance(trade, Mapping)
            or not self.is_paper_trade(trade, order_id)
        ):
            raise HTTPException(
                status_code=404,
                detail="PAPER-сделка не найдена в постоянном журнале.",
            )
        replay_session_id = str(
            trade.get("bbo_replay_session_id") or ""
        ).strip()
        if not replay_session_id:
            raise HTTPException(
                status_code=409,
                detail=(
                    "У сделки нет bbo_replay_session_id. Старые сделки до "
                    "включения replay связать нельзя."
                ),
            )

        stored_events = await self.db_manager.get_list(
            self.bbo_replay_storage_key,
            start=0,
            stop=self.bbo_replay_storage_limit - 1,
        )
        session_events = [
            row for row in stored_events
            if isinstance(row, Mapping)
            and str(row.get("session_id") or "") == replay_session_id
        ]
        if not session_events:
            raise HTTPException(
                status_code=409,
                detail="В журнале BBO нет событий для replay-сессии этой сделки.",
            )

        try:
            replay = self.replay_exit_scenarios_fn(
                session_events,
                trade,
                max_bbo_age_sec=self._max_bbo_age_sec(),
            )
        except ValueError as replay_error:
            raise HTTPException(
                status_code=422, detail=str(replay_error)
            ) from replay_error

        execution_reconciliation = (
            self.build_actual_vs_replay_reconciliation_fn(
                [trade] if str(trade.get("status") or "").upper() == "CLOSED" else [],
                {"trades": [replay]},
                scenario_name="BASELINE_SL_TP",
                limit=1,
            )
        )
        return {
            "paper_mode": True,
            "trade_id": order_id,
            "journal_events_for_session": len(session_events),
            "retained_event_limit": self.bbo_replay_storage_limit,
            "replay": replay,
            "execution_reconciliation": execution_reconciliation,
        }

    async def get_reconciliation(self) -> dict[str, Any]:
        self.require_paper_mode()
        snapshot = await self.db_manager.get_paper_session_snapshot()
        paper_rows = self._paper_trade_rows(snapshot)
        stale_rows = [
            row for row in paper_rows
            if str(row.get("status", "")).upper() == "STALE_REVIEW"
            and str(row.get("reconciliation_status", "")).upper()
            == "REVIEW_REQUIRED"
        ]
        stale_rows.sort(
            key=lambda row: str(
                row.get("entry_timestamp") or row.get("entry_time") or ""
            )
        )
        summary = {
            key: value for key, value in snapshot.items()
            if key != "paper_trades"
        }
        memory_open_positions = getattr(
            self.execution_engine, "paper_positions", {}
        )
        return {
            "paper_mode": True,
            "maintenance_active": bool(
                getattr(self.execution_engine, "_maintenance_active", False)
            ),
            "summary": summary,
            "stale_positions": stale_rows,
            "memory_open_positions": [
                dict(row) for row in memory_open_positions.values()
                if isinstance(row, dict)
            ],
        }

    async def reconcile_position(
        self,
        order_id: str,
        payload: dict[str, object],
    ) -> dict[str, Any]:
        """Apply an explicitly confirmed restore/close/void action to one stale PAPER row."""
        self.require_paper_mode()
        action = str(payload.get("action", "")).strip().lower()
        if action not in {"restore_open", "close", "void"}:
            raise HTTPException(
                status_code=400, detail="Неизвестное действие сверки."
            )

        async with self.paper_session_lock:
            if (
                self.execution_engine._opening_tickers
                or self.execution_engine._lock_closing
            ):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "Есть незавершённая операция исполнения. "
                        "Повторите сверку после её завершения."
                    ),
                )

            all_trades = await self.db_manager.get_all_daily_trades()
            trade = all_trades.get(order_id)
            if (
                not isinstance(trade, dict)
                or not self.is_paper_trade(trade, order_id)
            ):
                raise HTTPException(
                    status_code=404,
                    detail="PAPER-запись не найдена в постоянном журнале.",
                )
            trade = dict(trade)
            if (
                str(trade.get("status", "")).upper() != "STALE_REVIEW"
                or str(trade.get("reconciliation_status", "")).upper()
                != "REVIEW_REQUIRED"
            ):
                raise HTTPException(
                    status_code=409,
                    detail="Запись уже не находится в статусе STALE_REVIEW.",
                )

            ticker = str(trade.get("ticker", "")).strip().upper().split("@")[0]
            side = str(trade.get("side", "")).strip().upper()
            try:
                entry_price = float(trade.get("entry_price", 0.0))
                volume = int(float(trade.get("volume") or 0))
                if volume <= 0:
                    lots = int(float(trade.get("lots") or 0))
                    lot_size = int(
                        float(trade.get("lot_size") or self.lot_sizes.get(ticker, 1) or 1)
                    )
                    volume = lots * lot_size
            except (TypeError, ValueError, OverflowError):
                entry_price = 0.0
                volume = 0
            if (
                not ticker
                or side not in {"BUY", "SELL"}
                or not math.isfinite(entry_price)
                or entry_price <= 0
                or volume <= 0
            ):
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "В журнале недостаточно корректных данных "
                        "для безопасной сверки."
                    ),
                )

            now = self.now()
            note = str(payload.get("note", "")).strip()
            if action in {"close", "void"} and len(note) < 3:
                raise HTTPException(
                    status_code=422,
                    detail="Добавьте короткое пояснение к ручной операции.",
                )

            if action == "restore_open":
                if ticker in self.execution_engine.paper_positions:
                    raise HTTPException(
                        status_code=409,
                        detail=f"По {ticker} уже есть активная PAPER-позиция.",
                    )
                trade.update({
                    "status": "OPEN",
                    "mode": "PAPER",
                    "current_price": float(
                        trade.get("current_price") or entry_price
                    ),
                    "volume": volume,
                    "reconciliation_status": "CONFIRMED_OPEN",
                    "reconciled_at": now.isoformat(timespec="seconds"),
                    "reconciliation_note": (
                        note or "Оператор подтвердил, что PAPER-позиция остаётся открытой."
                    ),
                })
                await self.db_manager.set_json(
                    f"piluitus:trade:{order_id}", trade
                )
                self.execution_engine.paper_positions[ticker] = trade
                self.execution_engine._unreconciled_paper_positions.pop(
                    order_id, None
                )
                message = (
                    f"Позиция {ticker} возвращена в активный PAPER-мониторинг. "
                    "После следующего перезапуска потребуется повторная сверка."
                )
            elif action == "close":
                exit_price = self.parse_finite_float(payload.get("exit_price"))
                if exit_price is None or exit_price <= 0.0:
                    raise HTTPException(
                        status_code=422,
                        detail="Укажите корректную положительную цену выхода.",
                    )
                sign = 1 if side == "BUY" else -1
                pnl = (exit_price - entry_price) * volume * sign
                pnl_pct = ((exit_price - entry_price) / entry_price) * 100.0 * sign
                trade.update({
                    "status": "CLOSED",
                    "mode": "PAPER",
                    "volume": volume,
                    "current_price": exit_price,
                    "exit_price": exit_price,
                    "exit_time": now.strftime("%H:%M:%S"),
                    "exit_timestamp": now.isoformat(timespec="milliseconds"),
                    "exit_reason": "MANUAL_RECONCILIATION",
                    "pnl": round(pnl, 2),
                    "pnl_percent": round(pnl_pct, 4),
                    "final_outcome": "MANUALLY_RECONCILED",
                    "reconciliation_status": "CLOSED_CONFIRMED",
                    "reconciled_at": now.isoformat(timespec="seconds"),
                    "reconciliation_note": note,
                })
                await self.db_manager.set_json(
                    f"piluitus:trade:{order_id}", trade
                )
                await self.db_manager.push_to_list(
                    "piluitus:trade_history", trade, max_len=100
                )
                self.execution_engine._unreconciled_paper_positions.pop(
                    order_id, None
                )
                self.execution_engine.closed_trades.append(dict(trade))
                message = (
                    f"Закрытие {ticker} зафиксировано по {exit_price:.4f} ₽; "
                    f"расчётный PnL {pnl:+.2f} ₽."
                )
            else:  # void
                trade.update({
                    "status": "VOIDED",
                    "mode": "PAPER",
                    "final_outcome": "VOIDED",
                    "reconciliation_status": "VOIDED_CONFIRMED",
                    "reconciled_at": now.isoformat(timespec="seconds"),
                    "reconciliation_note": note,
                })
                await self.db_manager.set_json(
                    f"piluitus:trade:{order_id}", trade
                )
                self.execution_engine._unreconciled_paper_positions.pop(
                    order_id, None
                )
                message = (
                    f"Запись {order_id} помечена VOIDED. Она сохранена в журнале "
                    "для аудита, но больше не блокирует торговлю и капитал."
                )

        await self.broadcast_paper_tables()
        return {"status": "ok", "message": message, "trade": trade}
