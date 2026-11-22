"""Composition root for the complete market runtime graph.

This module wires ingestion, candidate admission, MarketWorker processing and
background Finam services. Trading policy stays in the existing strategy,
execution, risk and service runtimes.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import Mapping
from typing import Any

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
        self._require_ingestion().pending_bbo_candidate_store.store_candidate(
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
        dependencies = self.dependencies
        callbacks = self.callbacks
        self.ingestion_components = MarketIngestionRuntimeFactory(
            dependencies={
                "broker": dependencies["broker"],
                "screener": dependencies["screener"],
                "execution_engine": dependencies["execution_engine"],
                "db_manager": dependencies["db_manager"],
                "anomaly_queue": dependencies["anomaly_queue"],
                "queue_item_factory": dependencies["queue_item_factory"],
                "shadow_tracker": dependencies["shadow_tracker"],
                "logger": dependencies["logger"],
                "terminal_status": dependencies["terminal_status"],
                "recorder_provider": dependencies["recorder_provider"],
                "session_generation_provider": dependencies["session_generation_provider"],
                "monotonic": dependencies.get("monotonic", time.monotonic),
                "sleep": dependencies.get("sleep", asyncio.sleep),
                "environment": dependencies["environment"],
            },
            callbacks={
                "trade_flow_increment": callbacks["trade_flow_increment"],
                "guard_reason_bucket": callbacks["guard_reason_bucket"],
                "record_position_order_book_event": callbacks["record_position_order_book_event"],
                "refresh_bbo_metrics": callbacks["refresh_bbo_metrics"],
                "persist_shadow_outcomes": callbacks["persist_shadow_outcomes"],
                "resume_pending_bbo_candidate": self.resume_pending_bbo_candidate,
                "infer_candidate_direction": callbacks["infer_candidate_direction"],
                "paper_mode_provider": callbacks["paper_mode_provider"],
                "validate_entry_evidence": callbacks["validate_entry_evidence"],
                "validate_atr_readiness": callbacks["validate_atr_readiness"],
                "queue_flow_conflict_reason": self.paper_queue_flow_conflict_reason,
                "reserve_ai_ticker": self.reserve_ai_ticker,
                "release_ai_ticker": self.release_ai_ticker,
                "mark_shadow_llm_not_called": callbacks["mark_shadow_llm_not_called"],
                "log_shadow_candidate": callbacks["log_shadow_candidate"],
                "trade_flow_guard_block": callbacks["trade_flow_guard_block"],
                "log_prefilter_blocked": callbacks["log_prefilter_blocked"],
                "update_numpy_impulse_status": callbacks["update_numpy_impulse_status"],
                "store_pending_bbo_candidate": self.store_pending_bbo_candidate,
                "build_candidate_payload": callbacks["build_candidate_payload"],
                "classify_direction": callbacks["classify_direction"],
                "schedule_m1_history_warmup": self.schedule_m1_history_warmup,
            },
        ).create()

        candidate_ttl_sec = self.ingestion_components.ai_candidate_ttl_sec
        execution_runtime_dependencies = {
            "execution_engine": dependencies["execution_engine"],
            "risk_manager": dependencies["risk_manager"],
            "session_lock": dependencies["session_lock"],
            "session_generation_provider": dependencies["session_generation_provider"],
            "candidate_is_expired": callbacks["candidate_is_expired"],
            "candidate_ttl_sec": candidate_ttl_sec,
            "trade_flow_increment": callbacks["trade_flow_increment"],
            "logger": dependencies["logger"],
            "shadow_tracker": dependencies["shadow_tracker"],
            "normalise_execution_status": callbacks["normalise_execution_status"],
            "nonnegative_int": callbacks["nonnegative_int"],
            "normalize_execution_fill": callbacks["normalize_execution_fill"],
            "lot_sizes": dependencies["finam_trade_lot_sizes"],
            "publish_execution_outcome": callbacks["publish_execution_outcome"],
            "verdict_cache": dependencies["verdict_cache"],
            "get_open_positions": dependencies["get_open_positions"],
            "refresh_balance_snapshot": dependencies["refresh_balance_snapshot"],
            "manager": dependencies["manager"],
        }
        processing_runtime_dependencies = {
            "anomaly_queue": dependencies["anomaly_queue"],
            "execution_engine": dependencies["execution_engine"],
            "risk_manager": dependencies["risk_manager"],
            "session_lock": dependencies["session_lock"],
            "session_generation_provider": dependencies["session_generation_provider"],
            "candidate_is_expired": callbacks["candidate_is_expired"],
            "candidate_ttl_sec": candidate_ttl_sec,
            "trade_flow_increment": callbacks["trade_flow_increment"],
            "logger": dependencies["logger"],
            "settings": dependencies["settings"],
            "validate_daily_loss_limit": callbacks["validate_daily_loss_limit"],
            "parse_finite_float": callbacks["parse_finite_float"],
            "background_operations": dependencies["background_operations"],
            "log_trading_event": callbacks["log_trading_event"],
            "attach_llm_rule_context": callbacks["attach_llm_rule_context"],
            "build_llm_feature_snapshot": callbacks["build_llm_feature_snapshot"],
            "build_llm_error_response": callbacks["build_llm_error_response"],
            "process_llm_response": callbacks["process_llm_response"],
            "validate_post_llm_entry": callbacks["validate_post_llm_entry"],
            "screener": dependencies["screener"],
            "queue_flow_conflict_reason": self.paper_queue_flow_conflict_reason,
            "prepare_entry_execution_plan": callbacks["prepare_entry_execution_plan"],
            "lot_sizes": dependencies["finam_trade_lot_sizes"],
            "trade_flow_guard_block": callbacks["trade_flow_guard_block"],
            "finite_float": callbacks["finite_float"],
            "nonnegative_int": callbacks["nonnegative_int"],
            "build_default_signal_reason": callbacks["build_default_signal_reason"],
            "build_verdict_explanation": callbacks["build_verdict_explanation"],
            "classify_local_evidence_diagnostic": callbacks["classify_local_evidence_diagnostic"],
            "build_pre_llm_risk_block_verdict": callbacks["build_pre_llm_risk_block_verdict"],
            "build_llm_verdict_record": callbacks["build_llm_verdict_record"],
            "link_shadow_candidate_verdict": callbacks["link_shadow_candidate_verdict"],
            "shadow_tracker": dependencies["shadow_tracker"],
            "enrich_ai_analysis_with_verdict": callbacks["enrich_ai_analysis_with_verdict"],
            "verdict_cache": dependencies["verdict_cache"],
            "max_cache_size": dependencies["max_cache_size"],
            "manager": dependencies["manager"],
            "balance_snapshot": dependencies["balance_snapshot"],
            "publish_market_verdict": callbacks["publish_market_verdict"],
            "release_ai_ticker": self.release_ai_ticker,
            "paper_session_generation_lock": dependencies["session_lock"],
        }
        self.market_worker_factory = MarketWorkerFactory(
            client_id=dependencies["client_id"],
            execution_runtime_dependencies=execution_runtime_dependencies,
            processing_runtime_dependencies=processing_runtime_dependencies,
        )
        self.market_background_services = MarketBackgroundServicesRuntime(
            broker=dependencies["broker"],
            execution_engine=dependencies["execution_engine"],
            logger=dependencies["logger"],
            tickers=dependencies["tickers"],
            finam_trade_lot_sizes=dependencies["finam_trade_lot_sizes"],
            screener=dependencies["screener"],
            process_market_event=self.process_market_event,
            db_manager=dependencies["db_manager"],
            manager=dependencies["manager"],
            get_dashboard_open_positions=dependencies["get_open_positions"],
            refresh_dashboard_balance_snapshot=dependencies[
                "refresh_dashboard_balance_snapshot"
            ],
        )
        return self
