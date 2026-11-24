"""Behavioral contracts for the market runtime composition boundary."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from runtime import market_runtime_composition_runtime as module


class _Observability:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.calls = []

    def increment_trade_flow(self, *args, **kwargs):
        self.calls.append(("increment", args, kwargs))

    def record_position_order_book_event(self, *args, **kwargs):
        self.calls.append(("record", args, kwargs))

    def mark_shadow_llm_not_called(self, *args, **kwargs):
        self.calls.append(("mark", args, kwargs))

    def log_shadow_candidate(self, *args, **kwargs):
        self.calls.append(("shadow", args, kwargs))

    def guard_block(self, *args, **kwargs):
        self.calls.append(("guard", args, kwargs))

    def log_prefilter_blocked(self, *args, **kwargs):
        self.calls.append(("prefilter", args, kwargs))


class _MarketApplication:
    def __init__(self, *, dependencies, callbacks):
        self.dependencies = dependencies
        self.callbacks = callbacks
        self.ingestion_components = None

    def create(self):
        self.ingestion_components = SimpleNamespace(
            pending_bbo_candidate_store=SimpleNamespace(count=lambda: 17)
        )
        return self


class _PaperSessionComposition:
    async def persist_shadow_outcomes(self, labels, *, source):
        return None


def _runtime(monkeypatch):
    monkeypatch.setattr(module, "MarketObservabilityRuntime", _Observability)
    monkeypatch.setattr(module, "MarketApplicationRuntime", _MarketApplication)
    engine = SimpleNamespace(paper_mode=True)
    dependencies = {
        key: object()
        for key in (
            "broker", "screener", "execution_engine", "db_manager",
            "anomaly_queue", "queue_item_factory", "shadow_tracker", "logger",
            "terminal_status", "recorder_provider", "session_generation_provider",
            "environment", "client_id", "ai_engine", "risk_manager",
            "session_lock", "settings", "background_operations", "manager",
            "get_open_positions", "refresh_balance_snapshot",
            "refresh_dashboard_balance_snapshot", "verdict_cache",
            "max_cache_size", "balance_snapshot", "tickers",
            "finam_trade_lot_sizes",
        )
    }
    dependencies["execution_engine"] = engine
    paper_session = _PaperSessionComposition()
    runtime = module.MarketRuntimeCompositionRuntime(
        dependencies=dependencies,
        paper_session_composition_runtime=paper_session,
        monotonic=lambda: 123.0,
    )
    return runtime, dependencies, paper_session


def test_market_composition_builds_observability_before_application_runtime(monkeypatch):
    runtime, dependencies, paper_session = _runtime(monkeypatch)

    result = runtime.create()

    assert result is runtime
    assert isinstance(runtime.market_observability_runtime, _Observability)
    assert isinstance(runtime.market_application_runtime, _MarketApplication)
    assert runtime.market_application_runtime.dependencies == dependencies
    assert runtime.market_observability_runtime.kwargs["logger"] is dependencies["logger"]
    assert runtime.market_observability_runtime.kwargs["monotonic"]() == 123.0
    assert runtime.market_application_runtime.callbacks[
        "persist_shadow_outcomes"
    ] == paper_session.persist_shadow_outcomes


def test_market_composition_preserves_callback_bindings_and_paper_mode_provider(monkeypatch):
    runtime, dependencies, _ = _runtime(monkeypatch)
    runtime.create()
    callbacks = runtime.market_application_runtime.callbacks
    observability = runtime.market_observability_runtime

    assert callbacks["trade_flow_increment"] == observability.increment_trade_flow
    assert callbacks["record_position_order_book_event"] == observability.record_position_order_book_event
    assert callbacks["mark_shadow_llm_not_called"] == observability.mark_shadow_llm_not_called
    assert callbacks["log_shadow_candidate"] == observability.log_shadow_candidate
    assert callbacks["trade_flow_guard_block"] == observability.guard_block
    assert callbacks["log_prefilter_blocked"] == observability.log_prefilter_blocked
    assert callbacks["paper_mode_provider"]() is True

    dependencies["execution_engine"].paper_mode = False
    assert callbacks["paper_mode_provider"]() is False
    assert callbacks["parse_finite_float"] is module.parse_finite_float


def test_market_composition_pending_bbo_counter_is_safe_before_and_after_ingestion(monkeypatch):
    runtime, *_ = _runtime(monkeypatch)

    assert runtime._pending_bbo_count() == 0
    runtime.create()
    assert runtime._pending_bbo_count() == 17



def test_market_composition_registers_the_complete_strategy_and_execution_callback_catalog(monkeypatch):
    runtime, _, paper_session = _runtime(monkeypatch)
    runtime.create()
    callbacks = runtime.market_application_runtime.callbacks
    observability = runtime.market_observability_runtime

    expected = {
        "guard_reason_bucket": module.guard_reason_bucket,
        "refresh_bbo_metrics": module.refresh_candidate_bbo_metrics,
        "persist_shadow_outcomes": paper_session.persist_shadow_outcomes,
        "infer_candidate_direction": module.infer_candidate_direction,
        "validate_pre_llm_entry_evidence": module.validate_pre_llm_entry_evidence,
        "validate_atr_readiness": module.validate_atr_readiness,
        "update_numpy_impulse_status": module.update_numpy_impulse_status,
        "build_candidate_payload": module.build_market_candidate_payload,
        "classify_direction": module.classify_direction_evidence,
        "candidate_is_expired": module.candidate_is_expired,
        "normalise_execution_status": module.normalise_execution_status,
        "nonnegative_int": module.nonnegative_int,
        "normalize_execution_fill": module.normalize_execution_fill,
        "publish_execution_outcome": module.publish_execution_outcome,
        "validate_daily_loss_limit": module.validate_daily_loss_limit,
        "parse_finite_float": module.parse_finite_float,
        "log_trading_event": module.log_trading_event_to_file,
        "attach_llm_rule_context": module.attach_llm_rule_context,
        "build_llm_feature_snapshot": module.build_llm_feature_snapshot,
        "build_llm_error_response": module.build_llm_error_response,
        "process_llm_response": module.process_llm_response,
        "validate_post_llm_entry": module.validate_post_llm_entry,
        "prepare_entry_execution_plan": module.prepare_entry_execution_plan,
        "finite_float": module.finite_float,
        "build_default_signal_reason": module.build_default_signal_reason,
        "build_verdict_explanation": module.build_verdict_explanation,
        "classify_local_evidence_diagnostic": module.classify_local_evidence_diagnostic,
        "build_pre_llm_risk_block_verdict": module.build_pre_llm_risk_block_verdict,
        "build_llm_verdict_record": module.build_llm_verdict_record,
        "link_shadow_candidate_verdict": module.link_shadow_candidate_verdict,
        "enrich_ai_analysis_with_verdict": module.enrich_ai_analysis_with_verdict,
        "publish_market_verdict": module.publish_market_verdict,
    }
    assert set(callbacks) == {
        "trade_flow_increment", "guard_reason_bucket",
        "record_position_order_book_event", "refresh_bbo_metrics",
        "persist_shadow_outcomes", "infer_candidate_direction",
        "paper_mode_provider", "validate_pre_llm_entry_evidence",
        "validate_atr_readiness", "mark_shadow_llm_not_called",
        "log_shadow_candidate", "trade_flow_guard_block",
        "log_prefilter_blocked", "update_numpy_impulse_status",
        "build_candidate_payload", "classify_direction",
        "candidate_is_expired", "normalise_execution_status",
        "nonnegative_int", "normalize_execution_fill",
        "publish_execution_outcome", "validate_daily_loss_limit",
        "parse_finite_float", "log_trading_event",
        "attach_llm_rule_context", "build_llm_feature_snapshot",
        "build_llm_error_response", "process_llm_response",
        "validate_post_llm_entry", "prepare_entry_execution_plan",
        "finite_float", "build_default_signal_reason",
        "build_verdict_explanation", "classify_local_evidence_diagnostic",
        "build_pre_llm_risk_block_verdict", "build_llm_verdict_record",
        "link_shadow_candidate_verdict", "enrich_ai_analysis_with_verdict",
        "publish_market_verdict",
    }
    for name, callback in expected.items():
        assert callbacks[name] == callback
    assert callbacks["trade_flow_increment"] == observability.increment_trade_flow
    assert callbacks["record_position_order_book_event"] == observability.record_position_order_book_event
    assert callbacks["mark_shadow_llm_not_called"] == observability.mark_shadow_llm_not_called
    assert callbacks["log_shadow_candidate"] == observability.log_shadow_candidate
    assert callbacks["trade_flow_guard_block"] == observability.guard_block
    assert callbacks["log_prefilter_blocked"] == observability.log_prefilter_blocked

def test_market_composition_rejects_duplicate_creation(monkeypatch):
    runtime, *_ = _runtime(monkeypatch)
    runtime.create()

    with pytest.raises(RuntimeError, match="only run once"):
        runtime.create()
