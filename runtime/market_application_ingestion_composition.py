"""Build ingestion services and candidate-admission callbacks for MarketApplicationRuntime."""
from __future__ import annotations

import asyncio
import time
from typing import Any


def create_market_ingestion_components(runtime: Any, factory_cls: Any) -> Any:
    """Create the market ingestion graph without moving factory patch points."""
    dependencies = runtime.dependencies
    callbacks = runtime.callbacks
    return factory_cls(
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
            "resume_pending_bbo_candidate": runtime.resume_pending_bbo_candidate,
            "infer_candidate_direction": callbacks["infer_candidate_direction"],
            "paper_mode_provider": callbacks["paper_mode_provider"],
            "validate_pre_llm_entry_evidence": callbacks["validate_pre_llm_entry_evidence"],
            "validate_atr_readiness": callbacks["validate_atr_readiness"],
            "queue_flow_conflict_reason": runtime.paper_queue_flow_conflict_reason,
            "reserve_ai_ticker": runtime.reserve_ai_ticker,
            "release_ai_ticker": runtime.release_ai_ticker,
            "mark_shadow_llm_not_called": callbacks["mark_shadow_llm_not_called"],
            "log_shadow_candidate": callbacks["log_shadow_candidate"],
            "trade_flow_guard_block": callbacks["trade_flow_guard_block"],
            "log_prefilter_blocked": callbacks["log_prefilter_blocked"],
            "update_numpy_impulse_status": callbacks["update_numpy_impulse_status"],
            "store_pending_bbo_candidate": runtime.store_pending_bbo_candidate,
            "build_candidate_payload": callbacks["build_candidate_payload"],
            "classify_direction": callbacks["classify_direction"],
            "schedule_m1_history_warmup": runtime.schedule_m1_history_warmup,
        },
    ).create()
