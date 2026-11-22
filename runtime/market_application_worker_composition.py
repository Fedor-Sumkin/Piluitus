"""Compose the execution and queued-LLM worker dependency graphs."""
from __future__ import annotations

from typing import Any


def create_market_worker_factory(runtime: Any, factory_cls: Any) -> Any:
    """Build worker dependencies after ingestion has supplied the candidate TTL."""
    dependencies = runtime.dependencies
    callbacks = runtime.callbacks
    candidate_ttl_sec = runtime.ingestion_components.ai_candidate_ttl_sec
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
        "queue_flow_conflict_reason": runtime.paper_queue_flow_conflict_reason,
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
        "release_ai_ticker": runtime.release_ai_ticker,
        "paper_session_generation_lock": dependencies["session_lock"],
    }
    runtime.market_worker_factory = factory_cls(
        client_id=dependencies["client_id"],
        execution_runtime_dependencies=execution_runtime_dependencies,
        processing_runtime_dependencies=processing_runtime_dependencies,
    )
    return runtime.market_worker_factory
