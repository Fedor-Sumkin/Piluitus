"""Composition boundary for the market-ingestion and candidate-admission graph.

This module owns runtime assembly and shared ingestion state only. Market filters,
gate ordering, TTL values, and trading decisions remain in their policy runtimes.
"""
from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass
from typing import Any, Mapping

from runtime.market_data_event_runtime import MarketDataEventRuntime
from runtime.market_event_router import (
    MarketAuxiliaryEventRouter,
    PaperBBOReplayAdapter,
)
from strategy.candidate_dedup import CandidateDeduplicator
from strategy.market_candidate_pipeline import MarketCandidatePipeline
from strategy.market_data_runtime import (
    M1HistoryWarmup,
    PendingBBOCandidateRuntime,
    PendingBBOCandidateStore,
)


@dataclass(frozen=True)
class MarketIngestionComponents:
    """Shared state and collaborating runtimes for market-event ingestion."""

    ai_candidate_deduplicator: Any
    ai_candidate_ttl_sec: float
    pending_bbo_candidates: dict[str, dict[str, Any]]
    pending_bbo_lock: Any
    m1_history_warmup: Any
    pending_bbo_candidate_store: Any
    pending_bbo_runtime: Any
    market_auxiliary_event_router: Any
    market_candidate_pipeline: Any
    market_data_event_runtime: Any


class MarketIngestionRuntimeFactory:
    """Build the M1, deferred-BBO, routing, and candidate-admission graph."""

    def __init__(
        self,
        *,
        dependencies: Mapping[str, Any],
        callbacks: Mapping[str, Any],
    ) -> None:
        self.dependencies = dict(dependencies)
        self.callbacks = dict(callbacks)

    def create(self) -> MarketIngestionComponents:
        dependencies = self.dependencies
        callbacks = self.callbacks
        environment = dependencies.get("environment", {})

        try:
            candidate_cooldown_sec = max(
                0.5,
                float(environment.get("PILUITUS_AI_CANDIDATE_COOLDOWN_SEC", "8.0")),
            )
        except (TypeError, ValueError, OverflowError):
            candidate_cooldown_sec = 8.0

        try:
            candidate_ttl_sec = max(
                1.0,
                float(environment.get("PILUITUS_AI_CANDIDATE_TTL_SEC", "5.0")),
            )
        except (TypeError, ValueError, OverflowError):
            candidate_ttl_sec = 5.0

        try:
            pending_bbo_ttl_sec = min(
                5.0,
                max(0.25, float(environment.get("PILUITUS_PENDING_BBO_TTL_SEC", "1.5"))),
            )
        except (TypeError, ValueError):
            pending_bbo_ttl_sec = 1.5

        monotonic = dependencies.get("monotonic", time.monotonic)
        sleep = dependencies.get("sleep", asyncio.sleep)
        screener = dependencies["screener"]
        execution_engine = dependencies["execution_engine"]
        logger = dependencies["logger"]

        ai_candidate_deduplicator = CandidateDeduplicator(candidate_cooldown_sec)
        m1_history_warmup = M1HistoryWarmup(
            dependencies["broker"],
            screener,
            logger,
            retry_sec=30.0,
            monotonic=monotonic,
            sleep=sleep,
        )
        pending_bbo_candidates: dict[str, dict[str, Any]] = {}
        pending_bbo_lock = threading.Lock()
        pending_bbo_candidate_store = PendingBBOCandidateStore(
            pending_bbo_ttl_sec,
            callbacks["trade_flow_increment"],
            logger,
            candidates=pending_bbo_candidates,
            lock=pending_bbo_lock,
            monotonic=monotonic,
        )
        pending_bbo_runtime = PendingBBOCandidateRuntime(
            store=pending_bbo_candidate_store,
            screener=screener,
            execution_engine=execution_engine,
            anomaly_queue=dependencies["anomaly_queue"],
            queue_item_factory=dependencies["queue_item_factory"],
            refresh_bbo_metrics=callbacks["refresh_bbo_metrics"],
            infer_candidate_direction=callbacks["infer_candidate_direction"],
            paper_mode_provider=callbacks["paper_mode_provider"],
            validate_entry_evidence=callbacks["validate_entry_evidence"],
            validate_atr_readiness=callbacks["validate_atr_readiness"],
            queue_flow_conflict_reason=callbacks["queue_flow_conflict_reason"],
            reserve_ai_ticker=callbacks["reserve_ai_ticker"],
            release_ai_ticker=callbacks["release_ai_ticker"],
            mark_shadow_llm_not_called=callbacks["mark_shadow_llm_not_called"],
            log_shadow_candidate=callbacks["log_shadow_candidate"],
            trade_flow_increment=callbacks["trade_flow_increment"],
            guard_reason_bucket=callbacks["guard_reason_bucket"],
            trade_flow_guard_block=callbacks["trade_flow_guard_block"],
            log_prefilter_blocked=callbacks["log_prefilter_blocked"],
            update_numpy_impulse_status=callbacks["update_numpy_impulse_status"],
            session_generation_provider=dependencies["session_generation_provider"],
            logger=logger,
            monotonic=monotonic,
        )
        bbo_replay = PaperBBOReplayAdapter(
            recorder_provider=dependencies["recorder_provider"],
            screener=screener,
            monotonic=monotonic,
        )
        market_auxiliary_event_router = MarketAuxiliaryEventRouter(
            screener=screener,
            execution_engine=execution_engine,
            db_manager=dependencies["db_manager"],
            logger=logger,
            trade_flow_increment=callbacks["trade_flow_increment"],
            record_position_order_book_event=callbacks["record_position_order_book_event"],
            bbo_replay=bbo_replay,
            refresh_bbo_metrics=callbacks["refresh_bbo_metrics"],
            shadow_tracker=dependencies["shadow_tracker"],
            persist_shadow_outcomes=callbacks["persist_shadow_outcomes"],
            resume_pending_bbo_candidate=callbacks["resume_pending_bbo_candidate"],
            monotonic=monotonic,
        )
        market_candidate_pipeline = MarketCandidatePipeline(
            execution_engine=execution_engine,
            anomaly_queue=dependencies["anomaly_queue"],
            queue_item_factory=dependencies["queue_item_factory"],
            trade_flow_increment=callbacks["trade_flow_increment"],
            infer_direction=callbacks["infer_candidate_direction"],
            classify_direction=callbacks["classify_direction"],
            log_shadow_candidate=callbacks["log_shadow_candidate"],
            queue_flow_conflict_reason=callbacks["queue_flow_conflict_reason"],
            validate_entry_evidence=callbacks["validate_entry_evidence"],
            paper_mode_provider=callbacks["paper_mode_provider"],
            guard_reason_bucket=callbacks["guard_reason_bucket"],
            store_pending_bbo_candidate=callbacks["store_pending_bbo_candidate"],
            mark_shadow_llm_not_called=callbacks["mark_shadow_llm_not_called"],
            trade_flow_guard_block=callbacks["trade_flow_guard_block"],
            log_prefilter_blocked=callbacks["log_prefilter_blocked"],
            validate_atr_readiness=callbacks["validate_atr_readiness"],
            reserve_ai_ticker=callbacks["reserve_ai_ticker"],
            build_candidate_payload=callbacks["build_candidate_payload"],
            release_ai_ticker=callbacks["release_ai_ticker"],
            update_numpy_impulse_status=callbacks["update_numpy_impulse_status"],
            logger=logger,
            monotonic=monotonic,
        )
        market_data_event_runtime = MarketDataEventRuntime(
            execution_engine=execution_engine,
            db_manager=dependencies["db_manager"],
            screener=screener,
            auxiliary_event_router=market_auxiliary_event_router,
            candidate_pipeline=market_candidate_pipeline,
            pending_bbo_runtime=pending_bbo_runtime,
            shadow_tracker=dependencies["shadow_tracker"],
            persist_shadow_outcomes=callbacks["persist_shadow_outcomes"],
            schedule_m1_history_warmup=callbacks["schedule_m1_history_warmup"],
            trade_flow_increment=callbacks["trade_flow_increment"],
            terminal_status=dependencies["terminal_status"],
            logger=logger,
            session_generation_provider=dependencies["session_generation_provider"],
            monotonic=monotonic,
        )
        return MarketIngestionComponents(
            ai_candidate_deduplicator=ai_candidate_deduplicator,
            ai_candidate_ttl_sec=candidate_ttl_sec,
            pending_bbo_candidates=pending_bbo_candidates,
            pending_bbo_lock=pending_bbo_lock,
            m1_history_warmup=m1_history_warmup,
            pending_bbo_candidate_store=pending_bbo_candidate_store,
            pending_bbo_runtime=pending_bbo_runtime,
            market_auxiliary_event_router=market_auxiliary_event_router,
            market_candidate_pipeline=market_candidate_pipeline,
            market_data_event_runtime=market_data_event_runtime,
        )
