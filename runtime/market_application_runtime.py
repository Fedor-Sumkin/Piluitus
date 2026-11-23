"""Composition root for the complete market runtime graph.

This module wires ingestion, candidate admission, MarketWorker processing and
background Finam services. Trading policy stays in the existing strategy,
execution, risk and service runtimes.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ai.news_parser import MarketNewsParser
from risk.risk_manager import RiskManager
from runtime.market_application_background_composition import create_market_background_services
from runtime.market_application_ingestion_composition import create_market_ingestion_components
from runtime.market_application_worker_composition import create_market_worker_factory
from runtime.market_background_services import MarketBackgroundServicesRuntime
from runtime.market_ingestion_runtime import MarketIngestionRuntimeFactory
from runtime.market_worker import MarketWorkerFactory
from strategy.market_data_runtime import paper_queue_flow_conflict_reason
from strategy.scalping_strategy import has_queue_trade_flow_conflict


class MarketApplicationRuntime:
    """Build and expose the shared market-ingestion/worker/service graph."""

    def __init__(
        self,
        *,
        dependencies: Mapping[str, Any],
        callbacks: Mapping[str, Any],
    ) -> None:
        self.dependencies = dict(dependencies)
        self.callbacks = dict(callbacks)
        self.ingestion_components: Any | None = None
        self.market_worker_factory: Any | None = None
        self.market_background_services: Any | None = None
        # Keep the news parser injection seam on the established application facade.
        self.market_news_parser = MarketNewsParser

    def _require_ingestion(self) -> Any:
        if self.ingestion_components is None:
            raise RuntimeError("MarketApplicationRuntime.create() must run first.")
        return self.ingestion_components

    def paper_queue_flow_conflict_reason(
        self, direction: str, metrics: dict[str, Any]
    ) -> str:
        dependencies = self.dependencies
        callbacks = self.callbacks
        return paper_queue_flow_conflict_reason(
            direction,
            metrics,
            paper_mode=bool(
                getattr(dependencies["settings"], "PAPER_TRADING", False)
            ),
            enabled=dependencies["environment"].get(
                "PILUITUS_PAPER_QUEUE_FLOW_CONFLICT_GUARD", "1"
            ),
            conflict_detector=callbacks.get(
                "queue_flow_conflict_detector", has_queue_trade_flow_conflict
            ),
            finite_float=callbacks["finite_float"],
        )

    def reserve_ai_ticker(
        self,
        ticker: str,
        direction: str = "NONE",
        metrics: dict[str, Any] | None = None,
    ) -> bool:
        pattern = (
            str((metrics or {}).get("bar_pattern_name") or "FLOW")
            if isinstance(metrics, dict) else "FLOW"
        )
        return self._require_ingestion().ai_candidate_deduplicator.reserve(
            ticker, direction, pattern
        )

    def release_ai_ticker(self, ticker: str) -> None:
        self._require_ingestion().ai_candidate_deduplicator.release(ticker)

    async def warmup_selected_m1_history(
        self, tickers: tuple[str, ...] | list[str]
    ) -> None:
        await self._require_ingestion().m1_history_warmup.warmup_selected_m1_history(
            tickers
        )

    def schedule_m1_history_warmup(
        self, tickers: tuple[str, ...] | list[str]
    ) -> None:
        self._require_ingestion().m1_history_warmup.schedule(tickers)

    def drop_pending_bbo_candidate(
        self, ticker: str, expected: dict[str, Any] | None = None
    ) -> bool:
        return self._require_ingestion().pending_bbo_runtime.drop(ticker, expected)

    def cancel_pending_bbo_candidate(
        self, ticker: str, expected: dict[str, Any] | None = None
    ) -> bool:
        return self._require_ingestion().pending_bbo_runtime.cancel(ticker, expected)

    def expire_pending_bbo_candidates(
        self, now: float | None = None
    ) -> None:
        self._require_ingestion().pending_bbo_runtime.expire(now)

    def store_pending_bbo_candidate(
        self,
        ticker: str,
        payload: dict[str, Any],
        received_monotonic: object | None = None,
    ) -> None:
        self._require_ingestion().pending_bbo_runtime.store_candidate(
            ticker, payload, received_monotonic=received_monotonic
        )

    async def resume_pending_bbo_candidate(
        self, ticker: str, *, trigger: str = "ORDER_BOOK"
    ) -> None:
        await self._require_ingestion().pending_bbo_runtime.resume(
            ticker, trigger=trigger
        )

    async def process_market_event(self, event: Any) -> None:
        await self._require_ingestion().market_data_event_runtime.process_event(event)

    def create(self) -> "MarketApplicationRuntime":
        """Construct the ingestion, worker and background-service graph once."""
        self.ingestion_components = create_market_ingestion_components(
            self, MarketIngestionRuntimeFactory
        )
        self.market_worker_factory = create_market_worker_factory(
            self, MarketWorkerFactory
        )
        self.market_background_services = create_market_background_services(
            self, MarketBackgroundServicesRuntime
        )
        return self
