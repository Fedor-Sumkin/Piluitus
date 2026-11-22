"""Compatibility exports for legacy main-module names.

The application bootstrap owns construction. This module only projects the
already-created runtime graph into stable names expected by existing operator
tooling and integrations.
"""
from __future__ import annotations

from typing import Any


def build_application_compatibility_exports(bootstrap: Any) -> dict[str, Any]:
    """Expose historical main-module names without placing composition in main."""
    if not bootstrap._created:
        raise RuntimeError("ApplicationRuntimeBootstrap.create() must run first.")
    core, shared = bootstrap.application_core_runtime, bootstrap.application_shared_state_runtime
    paper = bootstrap.paper_session_composition_runtime
    market, root = bootstrap.market_runtime_composition, bootstrap.application_composition_root_runtime
    dashboard = core.dashboard_application_runtime
    observability, market_app = market.market_observability_runtime, market.market_application_runtime
    ingestion, background = market_app.ingestion_components, market_app.market_background_services
    composition = root.application_composition_runtime
    paper_ops = composition.paper_operations_runtime
    return {
        "application_runtime_bootstrap": self,
        "application_core_runtime": core,
        "db_manager": core.db_manager, "ai_engine": core.ai_engine,
        "broker": core.broker, "screener": core.screener,
        "execution_engine": core.execution_engine, "risk_manager": core.risk_manager,
        "dashboard_application_runtime": dashboard,
        "dashboard_runtime": dashboard.dashboard_runtime,
        "_dashboard_balance_snapshot": bootstrap.dashboard_balance_snapshot,
        "_current_paper_open_pnl": dashboard.current_paper_open_pnl,
        "_current_paper_assets_cost": dashboard.current_paper_assets_cost,
        "application_shared_state_runtime": shared,
        "anomaly_queue": shared.anomaly_queue,
        "priority_queue_item_factory": shared.priority_queue_item_factory,
        "_paper_session_lock": shared.paper_session_lock,
        "_paper_session_generation": shared.paper_session_generation,
        "bg_market_task": shared.bg_market_task,
        "scalping_shadow_tracker": shared.shadow_tracker,
        "paper_bbo_replay_recorder": shared.bbo_replay_recorder,
        "paper_shadow_labels": shared.paper_shadow_labels,
        "_background_operations": shared.background_operations,
        "manager": shared.connection_manager,
        "last_ai_verdicts_cache": bootstrap.verdict_cache,
        "MAX_CACHE_SIZE": bootstrap.max_cache_size,
        "_set_paper_bbo_replay_recorder": bootstrap.set_bbo_replay_recorder,
        "_set_market_task": bootstrap.set_market_task,
        "_set_verdict_cache": bootstrap.set_verdict_cache,
        "_advance_paper_session_generation": bootstrap.advance_paper_session_generation,
        "paper_session_composition_runtime": paper,
        "shadow_outcome_persistence": paper.shadow_outcome_persistence,
        "paper_session_runtime": paper.paper_session_runtime,
        "_persist_shadow_outcomes": paper.persist_shadow_outcomes,
        "_restore_shadow_outcomes": paper.restore_shadow_outcomes,
        "_rotate_paper_bbo_replay_session": paper.rotate_bbo_replay_session,
        "_restore_paper_performance": paper.restore_paper_performance,
        "_latest_screener_prices": dashboard.latest_screener_prices,
        "_get_dashboard_open_positions": dashboard.get_dashboard_open_positions,
        "_refresh_dashboard_balance_snapshot": dashboard.refresh_balance_snapshot,
        "market_runtime_composition": market,
        "market_observability_runtime": observability,
        "market_application_runtime": market_app,
        "_parse_finite_float": market.parse_finite_float,
        "_trade_flow_counters": observability.trade_flow_counters,
        "position_order_book_diagnostics": observability.position_order_book_diagnostics,
        "shadow_scorecard_logger": observability.shadow_scorecard_logger,
        "trade_flow_report_logger": observability.trade_flow_report_logger,
        "strategy_shadow_diagnostics": observability.strategy_shadow_diagnostics,
        "prefilter_block_logger": observability.prefilter_block_logger,
        "_record_position_order_book_event": observability.record_position_order_book_event,
        "_log_open_position_bbo_diagnostics": observability.log_open_position_bbo_diagnostics,
        "_trade_flow_inc": observability.increment_trade_flow,
        "_trade_flow_maybe_log": observability.maybe_log_trade_flow,
        "_trade_flow_guard_block": observability.guard_block,
        "_mark_shadow_llm_not_called": observability.mark_shadow_llm_not_called,
        "_log_scalping_shadow": observability.log_shadow_candidate,
        "_log_prefilter_blocked": observability.log_prefilter_blocked,
        "market_ingestion_components": ingestion,
        "market_worker_factory": market_app.market_worker_factory,
        "market_background_services": background,
        "_ai_candidate_deduplicator": ingestion.ai_candidate_deduplicator,
        "_AI_CANDIDATE_TTL_SEC": ingestion.ai_candidate_ttl_sec,
        "_pending_bbo_candidates": ingestion.pending_bbo_candidates,
        "_pending_bbo_lock": ingestion.pending_bbo_lock,
        "m1_history_warmup": ingestion.m1_history_warmup,
        "pending_bbo_candidate_store": ingestion.pending_bbo_candidate_store,
        "pending_bbo_runtime": ingestion.pending_bbo_runtime,
        "market_auxiliary_event_router": ingestion.market_auxiliary_event_router,
        "market_candidate_pipeline": ingestion.market_candidate_pipeline,
        "market_data_event_runtime": ingestion.market_data_event_runtime,
        "process_market_event": market_app.process_market_event,
        "_paper_queue_flow_conflict_reason": market_app.paper_queue_flow_conflict_reason,
        "_reserve_ai_ticker": market_app.reserve_ai_ticker,
        "_release_ai_ticker": market_app.release_ai_ticker,
        "_warmup_selected_m1_history": market_app.warmup_selected_m1_history,
        "_schedule_m1_history_warmup": market_app.schedule_m1_history_warmup,
        "_drop_pending_bbo_candidate": market_app.drop_pending_bbo_candidate,
        "_cancel_pending_bbo_candidate": market_app.cancel_pending_bbo_candidate,
        "_expire_pending_bbo_candidates": market_app.expire_pending_bbo_candidates,
        "_store_pending_bbo_candidate": market_app.store_pending_bbo_candidate,
        "_resume_pending_bbo_candidate": market_app.resume_pending_bbo_candidate,
        "background_broker_init": background.background_broker_init,
        "_start_news_updater": background._start_news_updater,
        "_finam_metadata_refresh_loop": background._finam_metadata_refresh_loop,
        "_start_tables_sync_loop": background._start_tables_sync_loop,
        "application_composition_root_runtime": root,
        "application_composition_runtime": composition,
        "application_lifecycle_runtime": composition.application_lifecycle_runtime,
        "lifespan": composition.lifespan, "app": composition.app,
        "paper_operations_runtime": paper_ops,
        "paper_api_helpers": paper_ops.paper_api_helpers,
        "paper_research_runtime": paper_ops.paper_research_runtime,
        "paper_session_maintenance_runtime": paper_ops.paper_session_maintenance_runtime,
        "_require_paper_mode": paper_ops.require_paper_mode,
        "_paper_trade_rows": paper_ops.paper_trade_rows,
        "_broadcast_paper_tables": paper_ops.broadcast_paper_tables,
        "_broadcast_paper_session_reset": paper_ops.broadcast_paper_session_reset,
        "_reset_balance_guard_signature": dashboard.reset_balance_guard_signature,
    }
