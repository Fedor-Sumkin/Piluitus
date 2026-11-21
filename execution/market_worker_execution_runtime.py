"""Order execution and outcome publication for the market worker."""
from __future__ import annotations

import time
from datetime import datetime
from typing import Any, Callable


class MarketWorkerExecutionRuntime:
    """Own the final guarded execution and publication lifecycle for one verdict."""

    def __init__(
        self,
        *,
        db: Any,
        client_id: str,
        execution_engine: Any,
        risk_manager: Any,
        session_lock: Any,
        session_generation_provider: Callable[[], int],
        candidate_is_expired: Callable[..., bool],
        candidate_ttl_sec: float,
        trade_flow_increment: Callable[[str], None],
        logger: Any,
        shadow_tracker: Any,
        normalise_execution_status: Callable[[str, str], str],
        nonnegative_int: Callable[[Any], int | None],
        normalize_execution_fill: Callable[..., dict[str, Any]],
        lot_sizes: dict[str, int],
        publish_execution_outcome: Callable[..., Any],
        verdict_cache: list[dict[str, Any]],
        get_open_positions: Callable[..., Any],
        refresh_balance_snapshot: Callable[..., Any],
        manager: Any,
        monotonic: Callable[[], float] = time.monotonic,
        now_local: Callable[[], datetime] | None = None,
    ) -> None:
        self.db = db
        self.client_id = client_id
        self.execution_engine = execution_engine
        self.risk_manager = risk_manager
        self.session_lock = session_lock
        self.session_generation_provider = session_generation_provider
        self.candidate_is_expired = candidate_is_expired
        self.candidate_ttl_sec = float(candidate_ttl_sec)
        self.trade_flow_increment = trade_flow_increment
        self.logger = logger
        self.shadow_tracker = shadow_tracker
        self.normalise_execution_status = normalise_execution_status
        self.nonnegative_int = nonnegative_int
        self.normalize_execution_fill = normalize_execution_fill
        self.lot_sizes = lot_sizes
        self.publish_execution_outcome = publish_execution_outcome
        self.verdict_cache = verdict_cache
        self.get_open_positions = get_open_positions
        self.refresh_balance_snapshot = refresh_balance_snapshot
        self.manager = manager
        self.monotonic = monotonic
        self.now_local = now_local or (lambda: datetime.now().astimezone())

    async def execute_and_publish(
        self,
        *,
        verdict_id: str,
        ticker: str,
        lots: int,
        side: str,
        price: float,
        ai_analysis: dict,
        session_generation: int,
        candidate_created_monotonic: object | None = None,
        shadow_candidate_id: str | None = None,
    ) -> None:
        """Serialize execution with PAPER reset and reject stale session signals."""
        async with self.session_lock:
            if (
                getattr(self.execution_engine, "_maintenance_active", False)
                or session_generation != self.session_generation_provider()
            ):
                self.logger.info(
                    "⏭️ Старый сигнал %s по %s отброшен: PAPER-сессия изменилась/на обслуживании.",
                    verdict_id,
                    ticker,
                )
                return
            await self.execute_and_publish_locked(
                verdict_id=verdict_id,
                ticker=ticker,
                lots=lots,
                side=side,
                price=price,
                ai_analysis=ai_analysis,
                candidate_created_monotonic=candidate_created_monotonic,
                shadow_candidate_id=shadow_candidate_id,
            )

    async def execute_and_publish_locked(
        self,
        *,
        verdict_id: str,
        ticker: str,
        lots: int,
        side: str,
        price: float,
        ai_analysis: dict,
        candidate_created_monotonic: object | None = None,
        shadow_candidate_id: str | None = None,
    ) -> None:
        """Execute only a fresh, risk-approved candidate and publish its final result."""
        try:
            # Recheck the original evidence age immediately before execution.
            if self.candidate_is_expired(
                candidate_created_monotonic,
                ttl_sec=self.candidate_ttl_sec,
            ):
                try:
                    candidate_age_sec = max(
                        0.0,
                        self.monotonic() - float(candidate_created_monotonic),
                    )
                except (TypeError, ValueError, OverflowError):
                    candidate_age_sec = -1.0
                result = {
                    "status": "BLOCKED",
                    "message": (
                        "SIGNAL_EXPIRED: рыночные данные кандидата устарели во время LLM/очереди; "
                        "вход отменён без исполнения."
                    ),
                }
                self.trade_flow_increment("execution_stale_signal_blocked")
                self.logger.warning(
                    "[EXECUTION STALE SIGNAL BLOCK] %s %s: age=%.3fs ttl=%.3fs; "
                    "устаревший сигнал не передан исполнителю.",
                    side,
                    ticker,
                    candidate_age_sec,
                    self.candidate_ttl_sec,
                )
            # Refresh the risk state immediately before actual execution.
            elif not await self.risk_manager.is_trading_allowed(
                self.db, force_refresh=True
            ):
                result = {
                    "status": "BLOCKED",
                    "message": (
                        "RISK_FINAL_GATE: RiskManager запретил исполнение после "
                        "повторной проверки; ранее одобренный сигнал отброшен."
                    ),
                }
                self.trade_flow_increment("risk_final_gate_blocked")
                self.logger.warning(
                    "🛑 [RISK FINAL GATE] %s %s: отложенный сигнал %s заблокирован перед исполнением.",
                    side,
                    ticker,
                    verdict_id,
                )
            else:
                result = await self.execution_engine.execute_order(
                    ticker=ticker,
                    lots=lots,
                    side=side,
                    price=price,
                    client_id=self.client_id,
                    db=self.db,
                    ai_analysis=ai_analysis,
                )
            if not isinstance(result, dict):
                result = {
                    "status": "FAILED",
                    "message": "Исполнитель вернул некорректный ответ.",
                }
        except Exception as exc:
            self.logger.error(
                "Ошибка исполнения %s %s: %s", side, ticker, exc, exc_info=True
            )
            result = {"status": "FAILED", "message": str(exc)}

        raw_status = str(result.get("status", "FAILED")).upper()
        result_mode = str(
            result.get("mode")
            or ("PAPER" if getattr(self.execution_engine, "paper_mode", True) else "LIVE")
        ).upper()
        execution_status = self.normalise_execution_status(raw_status, result_mode)
        if shadow_candidate_id:
            self.shadow_tracker.update_execution_result(
                shadow_candidate_id, execution_status=execution_status
            )
        result_message = str(result.get("message", ""))
        outcome_requested_lots = self.nonnegative_int(result.get("requested_lots"))
        if outcome_requested_lots is None or outcome_requested_lots <= 0:
            outcome_requested_lots = max(0, int(lots))
        outcome_filled_lots = self.nonnegative_int(result.get("lots"))
        if outcome_filled_lots is None:
            outcome_filled_lots = (
                outcome_requested_lots
                if execution_status in {"OPENED", "ACCEPTED"}
                else 0
            )
        self.logger.info(
            "[EXECUTION OUTCOME] %s %s: status=%s mode=%s requested_lots=%d filled_lots=%d message=%s",
            side,
            ticker,
            execution_status,
            result_mode,
            outcome_requested_lots,
            outcome_filled_lots,
            result_message.replace(chr(10), " ").replace(chr(13), " ")[:240] or "-",
        )
        if "BBO_ENTRY_BLOCK" in result_message:
            self.trade_flow_increment("execution_bbo_blocked")
        if "SPREAD_ATR_ENTRY_BLOCK" in result_message:
            self.trade_flow_increment("execution_spread_atr_blocked")
        if execution_status in {"OPENED", "ACCEPTED"}:
            # Count actual executor outcomes, not merely queued AI decisions.
            self.trade_flow_increment("orders_scheduled")
            self.trade_flow_increment(
                "orders_opened_buy" if side == "BUY" else "orders_opened_sell"
            )
        execution_fill = self.normalize_execution_fill(
            result,
            requested_lots=lots,
            ticker=ticker,
            price=price,
            lot_sizes=self.lot_sizes,
            allow_fallback=execution_status in {"OPENED", "ACCEPTED"},
        )
        order_id = execution_fill["order_id"]
        actual_lots = execution_fill["actual_lots"]
        actual_shares = execution_fill["actual_shares"]
        actual_notional = execution_fill["actual_notional"]
        requested_lots_from_executor = execution_fill["requested_lots"]
        if execution_status in {"SKIPPED", "BLOCKED", "FAILED"}:
            if "Лимиты портфеля:" in result_message:
                self.trade_flow_increment("portfolio_cap_blocked")
            elif "ENTRY_SESSION_BLOCK" in result_message:
                self.trade_flow_increment("entry_session_blocked")
            elif (
                "BBO_ENTRY_BLOCK" not in result_message
                and "SPREAD_ATR_ENTRY_BLOCK" not in result_message
                and "AI Decision is HOLD" not in result_message
            ):
                self.trade_flow_increment("execution_other_blocked")
        if (
            execution_status in {"OPENED", "ACCEPTED"}
            and requested_lots_from_executor is not None
            and actual_lots is not None
            and actual_lots < requested_lots_from_executor
        ):
            self.trade_flow_increment("portfolio_size_reduced")
        if execution_status in {"OPENED", "ACCEPTED"}:
            self.logger.info(
                "📌 [EXECUTION RESULT] %s %s: requested=%s lots; actual=%s lots/%s shares; "
                "notional=%.2f ₽; status=%s",
                side,
                ticker,
                requested_lots_from_executor
                if requested_lots_from_executor is not None
                else lots,
                actual_lots if actual_lots is not None else lots,
                actual_shares if actual_shares is not None else 0,
                actual_notional if actual_notional is not None else 0.0,
                execution_status,
            )
        update = {
            "verdict_id": verdict_id,
            "ticker": ticker,
            "execution_status": execution_status,
            "execution_message": str(
                result.get("message")
                or (
                    "PAPER-позиция зарегистрирована."
                    if execution_status == "OPENED"
                    else "Брокер принял заявку; факт исполнения должен быть подтверждён отдельно."
                    if execution_status == "ACCEPTED"
                    else "Заявка не открыта."
                )
            ),
            "order_id": order_id or None,
            "executed_lots": actual_lots,
            "executed_shares": actual_shares,
            "actual_notional_rub": actual_notional,
            "execution_timestamp": self.now_local().isoformat(timespec="milliseconds"),
        }

        await self.publish_execution_outcome(
            update,
            db=self.db,
            verdict_cache=self.verdict_cache,
            get_open_positions=self.get_open_positions,
            refresh_balance_snapshot=self.refresh_balance_snapshot,
            manager=self.manager,
            execution_engine=self.execution_engine,
            logger=self.logger,
            side=side,
            ticker=ticker,
            status=execution_status,
            actual_lots=actual_lots,
            actual_notional=actual_notional,
            order_id=order_id,
        )
