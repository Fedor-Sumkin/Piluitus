from __future__ import annotations

import asyncio

import runtime.market_ingestion_runtime as ingestion_module
from runtime.market_ingestion_runtime import MarketIngestionRuntimeFactory


class Captured:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


def make_inputs(environment=None):
    dependency_names = (
        "broker", "screener", "execution_engine", "db_manager",
        "anomaly_queue", "queue_item_factory", "shadow_tracker",
        "logger", "terminal_status", "recorder_provider",
        "session_generation_provider", "monotonic", "sleep",
    )
    dependencies = {name: object() for name in dependency_names}
    dependencies.update(
        environment=environment or {},
        monotonic=lambda: 10.0,
        sleep=asyncio.sleep,
        recorder_provider=lambda: object(),
        session_generation_provider=lambda: 3,
    )
    callback_names = (
        "trade_flow_increment", "guard_reason_bucket",
        "record_position_order_book_event", "refresh_bbo_metrics",
        "persist_shadow_outcomes", "resume_pending_bbo_candidate",
        "infer_candidate_direction", "paper_mode_provider",
        "validate_pre_llm_entry_evidence", "validate_atr_readiness",
        "queue_flow_conflict_reason", "reserve_ai_ticker",
        "release_ai_ticker", "mark_shadow_llm_not_called",
        "log_shadow_candidate", "trade_flow_guard_block",
        "log_prefilter_blocked", "update_numpy_impulse_status",
        "store_pending_bbo_candidate", "build_candidate_payload",
        "classify_direction", "schedule_m1_history_warmup",
    )
    callbacks = {name: (lambda *args, **kwargs: None) for name in callback_names}
    return dependencies, callbacks


def patch_components(monkeypatch):
    captured = {}
    names = (
        "CandidateDeduplicator", "M1HistoryWarmup",
        "PendingBBOCandidateStore", "PendingBBOCandidateRuntime",
        "PaperBBOReplayAdapter", "MarketAuxiliaryEventRouter",
        "MarketCandidatePipeline", "MarketDataEventRuntime",
    )
    for name in names:
        def factory(name):
            class Fake(Captured):
                def __init__(self, *args, **kwargs):
                    super().__init__(*args, **kwargs)
                    captured[name] = self
            return Fake
        monkeypatch.setattr(ingestion_module, name, factory(name))
    return captured


def test_factory_builds_one_shared_bbo_and_market_event_graph(monkeypatch):
    captured = patch_components(monkeypatch)
    dependencies, callbacks = make_inputs()
    components = MarketIngestionRuntimeFactory(
        dependencies=dependencies, callbacks=callbacks
    ).create()

    assert components.ai_candidate_ttl_sec == 5.0
    assert captured["CandidateDeduplicator"].args == (8.0,)
    assert captured["M1HistoryWarmup"].kwargs["retry_sec"] == 30.0
    assert captured["M1HistoryWarmup"].kwargs["monotonic"] is dependencies["monotonic"]

    store = captured["PendingBBOCandidateStore"]
    assert store.args[0] == 1.5
    assert store.kwargs["candidates"] is components.pending_bbo_candidates
    assert store.kwargs["lock"] is components.pending_bbo_lock

    pending = captured["PendingBBOCandidateRuntime"]
    assert pending.kwargs["store"] is components.pending_bbo_candidate_store
    assert pending.kwargs["session_generation_provider"] is dependencies["session_generation_provider"]

    replay = captured["PaperBBOReplayAdapter"]
    router = captured["MarketAuxiliaryEventRouter"]
    assert router.kwargs["bbo_replay"] is replay
    assert router.kwargs["resume_pending_bbo_candidate"] is callbacks["resume_pending_bbo_candidate"]

    candidate = captured["MarketCandidatePipeline"]
    assert candidate.kwargs["store_pending_bbo_candidate"] is callbacks["store_pending_bbo_candidate"]
    event_runtime = captured["MarketDataEventRuntime"]
    assert event_runtime.kwargs["pending_bbo_runtime"] is components.pending_bbo_runtime
    assert event_runtime.kwargs["candidate_pipeline"] is components.market_candidate_pipeline
    assert event_runtime.kwargs["auxiliary_event_router"] is components.market_auxiliary_event_router
    assert event_runtime.kwargs["schedule_m1_history_warmup"] is callbacks["schedule_m1_history_warmup"]


def test_factory_preserves_candidate_and_pending_bbo_ttl_bounds(monkeypatch):
    captured = patch_components(monkeypatch)
    dependencies, callbacks = make_inputs({
        "PILUITUS_AI_CANDIDATE_COOLDOWN_SEC": "0.1",
        "PILUITUS_AI_CANDIDATE_TTL_SEC": "0.2",
        "PILUITUS_PENDING_BBO_TTL_SEC": "9.0",
    })
    components = MarketIngestionRuntimeFactory(
        dependencies=dependencies, callbacks=callbacks
    ).create()

    assert captured["CandidateDeduplicator"].args == (0.5,)
    assert components.ai_candidate_ttl_sec == 1.0
    assert captured["PendingBBOCandidateStore"].args[0] == 5.0


def test_factory_uses_original_fallbacks_for_invalid_ttl_settings(monkeypatch):
    captured = patch_components(monkeypatch)
    dependencies, callbacks = make_inputs({
        "PILUITUS_AI_CANDIDATE_COOLDOWN_SEC": "invalid",
        "PILUITUS_AI_CANDIDATE_TTL_SEC": "invalid",
        "PILUITUS_PENDING_BBO_TTL_SEC": "invalid",
    })
    components = MarketIngestionRuntimeFactory(
        dependencies=dependencies, callbacks=callbacks
    ).create()

    assert captured["CandidateDeduplicator"].args == (8.0,)
    assert components.ai_candidate_ttl_sec == 5.0
    assert captured["PendingBBOCandidateStore"].args[0] == 1.5
