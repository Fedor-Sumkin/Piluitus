"""Composition boundary for market-flow and PAPER observability.

The runtime coordinates existing monitoring adapters and counters. It is
strictly observational: it does not approve candidates, alter filter ordering,
size orders, or execute trades.
"""
from __future__ import annotations

from collections.abc import Callable
import logging
from typing import Any

from monitoring.position_order_book_diagnostics import PositionOrderBookDiagnostics
from monitoring.prefilter_diagnostics import PrefilterBlockLogger
from monitoring.shadow_scorecard_logger import StrategyShadowScorecardLogger
from monitoring.strategy_shadow_diagnostics import StrategyShadowDiagnostics
from monitoring.trade_flow_diagnostics import TradeFlowCounters
from monitoring.trade_flow_report import TradeFlowReportLogger


class MarketObservabilityRuntime:
    """Own the monitoring collaborators and their established callback API."""

    def __init__(
        self,
        *,
        logger: logging.Logger,
        shadow_tracker: Any,
        execution_engine: Any,
        screener: Any,
        parse_finite_float: Callable[[object], float | None],
        guard_reason_bucket: Callable[[str], str],
        evaluate_candidate: Callable[[str, dict[str, Any]], dict[str, Any]],
        pending_bbo_count_provider: Callable[[], int],
        monotonic: Callable[[], float],
    ) -> None:
        self.logger = logger
        self.shadow_tracker = shadow_tracker
        self.execution_engine = execution_engine
        self.screener = screener
        self.parse_finite_float = parse_finite_float
        self.pending_bbo_count_provider = pending_bbo_count_provider
        self.monotonic = monotonic

        self.trade_flow_counters = TradeFlowCounters(monotonic())
        self.position_order_book_diagnostics = PositionOrderBookDiagnostics(
            logger, monotonic=monotonic
        )
        self.shadow_scorecard_logger = StrategyShadowScorecardLogger(logger)
        self.trade_flow_report_logger = TradeFlowReportLogger(logger)
        self.strategy_shadow_diagnostics = StrategyShadowDiagnostics(
            logger,
            shadow_tracker,
            self.increment_trade_flow,
            guard_reason_bucket,
            evaluate_candidate,
        )
        self.prefilter_block_logger = PrefilterBlockLogger(
            logger, monotonic=monotonic
        )

    def increment_trade_flow(self, key: str, amount: int = 1) -> None:
        self.trade_flow_counters.increment(key, amount)
        self.maybe_log_trade_flow()

    def maybe_log_trade_flow(self, force: bool = False) -> None:
        counts = self.trade_flow_counters.snapshot_if_due(
            now=self.monotonic(), interval_sec=60.0, force=force
        )
        if counts is None:
            return

        pending_bbo = self.pending_bbo_count_provider()
        self.trade_flow_report_logger.log(counts, pending_bbo=pending_bbo)
        self.shadow_scorecard_logger.log(self.shadow_tracker)
        self.log_open_position_bbo_diagnostics()

    def guard_block(self, reason: str) -> None:
        self.increment_trade_flow("guard_blocked")
        lowered = str(reason or "").lower()
        if "bid/ask" in lowered or "котировк" in lowered or "актуальн" in lowered:
            bucket = "guard_quotes"
        elif "спред" in lowered:
            bucket = "guard_spread"
        elif "размер лучшей заявки" in lowered or "сокращение" in lowered or "истории размера" in lowered:
            bucket = "guard_size"
        elif "всплеск" in lowered or "объём" in lowered or "объем" in lowered:
            bucket = "guard_volume"
        elif "импульс" in lowered or "vwap" in lowered or "цена не" in lowered:
            bucket = "guard_direction"
        elif "объяснение" in lowered or "текст" in lowered or "поврежд" in lowered or "неподтверждён" in lowered:
            bucket = "guard_text"
        else:
            bucket = "guard_other"
        self.increment_trade_flow(bucket)

    def record_position_order_book_event(
        self, ticker: str, result: dict[str, Any]
    ) -> None:
        self.position_order_book_diagnostics.record(
            ticker, result, self.execution_engine
        )

    def log_open_position_bbo_diagnostics(self) -> None:
        self.position_order_book_diagnostics.log_open_positions(
            self.execution_engine,
            self.screener,
            self.parse_finite_float,
        )

    def mark_shadow_llm_not_called(
        self, candidate_id: object, reason_code: str
    ) -> None:
        self.strategy_shadow_diagnostics.mark_llm_not_called(
            candidate_id, reason_code
        )

    def log_shadow_candidate(
        self, ticker: str, direction: str, metrics: dict[str, Any]
    ) -> dict[str, Any]:
        return self.strategy_shadow_diagnostics.log_candidate(
            ticker, direction, metrics
        )

    def log_prefilter_blocked(
        self, ticker: str, reason: str, *, interval_sec: float = 15.0
    ) -> None:
        """Rate-limit identical prefilter warnings per ticker and reason category."""
        self.prefilter_block_logger.log(
            ticker, reason, interval_sec=interval_sec
        )
