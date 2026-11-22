"""Behavioral tests for the full application runtime bootstrap."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from runtime import application_runtime_bootstrap as module
from runtime.application_compatibility_exports import build_application_compatibility_exports


class _Core:
    def __init__(self, events, **kwargs):
        self.events, self.kwargs = events, kwargs

    def create(self):
        self.events.append("core")
        self.db_manager, self.ai_engine, self.broker = object(), object(), object()
        self.execution_engine, self.risk_manager = object(), object()
        self.dashboard_application_runtime = SimpleNamespace(
            dashboard_runtime=object(),
            current_paper_open_pnl=lambda: 1.0,
            current_paper_assets_cost=lambda: 2.0,
            latest_screener_prices=lambda: {},
            get_dashboard_open_positions=lambda: [],
            refresh_balance_snapshot=lambda: None,
            reset_balance_guard_signature=lambda: None,
        )
        self.screener = SimpleNamespace(
            set_universe=lambda tickers: self.events.append(("universe", tuple(tickers)))
        )
        self.kwargs["set_balance_snapshot"]({"equity": self.kwargs["deposit"]})
        return self


class _Shared:
    def __init__(self, events, **kwargs):
        self.events, self.kwargs = events, kwargs

    def create(self):
        self.events.append("shared")
        self.anomaly_queue, self.priority_queue_item_factory = object(), object()
        self.paper_session_lock, self.paper_session_generation = object(), 0
        self.bg_market_task = None
        self.shadow_tracker = object()
        self.bbo_replay_recorder = SimpleNamespace(session_id="replay-1")
        self.paper_shadow_labels = []
        self.background_operations, self.connection_manager = object(), object()
        return self

    def set_bbo_replay_recorder(self, recorder): self.bbo_replay_recorder = recorder
    def set_market_task(self, task): self.bg_market_task = task
    def advance_paper_session_generation(self):
        self.paper_session_generation += 1
        return self.paper_session_generation


class _Paper:
    def __init__(self, events, **kwargs): self.events, self.kwargs = events, kwargs
    def create(self):
        self.events.append("paper")
        self.shadow_outcome_persistence, self.paper_session_runtime = object(), object()
        self.persist_shadow_outcomes = lambda labels, *, source: None
        self.restore_shadow_outcomes = lambda: None
        self.rotate_bbo_replay_session = lambda: None
        self.restore_paper_performance = lambda: None
        return self


class _Market:
    def __init__(self, events, **kwargs):
        self.events, self.dependencies = events, kwargs["dependencies"]

    def create(self):
        self.events.append("market")
        self.market_observability_runtime = SimpleNamespace(**{
            key: object() for key in (
                "trade_flow_counters", "position_order_book_diagnostics",
                "shadow_scorecard_logger", "trade_flow_report_logger",
                "strategy_shadow_diagnostics", "prefilter_block_logger",
            )
        })
        for name in (
            "record_position_order_book_event", "log_open_position_bbo_diagnostics",
            "increment_trade_flow", "maybe_log_trade_flow", "guard_block",
            "mark_shadow_llm_not_called", "log_shadow_candidate", "log_prefilter_blocked",
        ):
            setattr(self.market_observability_runtime, name, lambda *a, **k: None)
        ingestion = SimpleNamespace(**{
            key: object() for key in (
                "ai_candidate_deduplicator", "pending_bbo_candidates", "pending_bbo_lock",
                "m1_history_warmup", "pending_bbo_candidate_store", "pending_bbo_runtime",
                "market_auxiliary_event_router", "market_candidate_pipeline",
                "market_data_event_runtime",
            )
        })
        ingestion.ai_candidate_ttl_sec = 1.5
        background = SimpleNamespace(**{
            key: (lambda: None) for key in (
                "background_broker_init", "_start_news_updater",
                "_finam_metadata_refresh_loop", "_start_tables_sync_loop",
            )
        })
        app = SimpleNamespace(
            ingestion_components=ingestion, market_worker_factory=object(),
            market_background_services=background, process_market_event=lambda e: None,
        )
        for name in (
            "paper_queue_flow_conflict_reason", "reserve_ai_ticker", "release_ai_ticker",
            "warmup_selected_m1_history", "schedule_m1_history_warmup",
            "drop_pending_bbo_candidate", "cancel_pending_bbo_candidate",
            "expire_pending_bbo_candidates", "store_pending_bbo_candidate",
            "resume_pending_bbo_candidate",
        ):
            setattr(app, name, lambda *a, **k: None)
        self.market_application_runtime = app
        self.parse_finite_float = float
        return self


class _Root:
    def __init__(self, events, **kwargs): self.events, self.kwargs = events, kwargs
    def create(self):
        self.events.append("root")
        paper = SimpleNamespace(
            paper_api_helpers=object(), paper_research_runtime=object(),
            paper_session_maintenance_runtime=object(),
            require_paper_mode=lambda: None, paper_trade_rows=lambda s: [],
            broadcast_paper_tables=lambda: None, broadcast_paper_session_reset=lambda p: None,
        )
        self.application_composition_runtime = SimpleNamespace(
            application_lifecycle_runtime=object(), lifespan=object(),
            app=object(), paper_operations_runtime=paper,
        )
        return self


def _build(monkeypatch):
    events, snapshots, bbo, caches, tasks, generations = [], [], [], [], [], []
    monkeypatch.setattr(module, "ApplicationCoreRuntime", lambda **kw: _Core(events, **kw))
    monkeypatch.setattr(module, "ApplicationSharedStateRuntime", lambda **kw: _Shared(events, **kw))
    monkeypatch.setattr(module, "PaperSessionCompositionRuntime", lambda **kw: _Paper(events, **kw))
    monkeypatch.setattr(module, "MarketRuntimeCompositionRuntime", lambda **kw: _Market(events, **kw))
    monkeypatch.setattr(module, "ApplicationCompositionRootRuntime", lambda **kw: _Root(events, **kw))
    cache = []
    runtime = module.ApplicationRuntimeBootstrap(
        settings=object(), finam_token="token", client_id="PILUITUS_PAPER",
        deposit=400_000, logger=object(), terminal_status=object(),
        tickers=("SBER", "GAZP"), environment={"MODE": "PAPER"},
        verdict_cache=cache, monotonic=lambda: 42.0,
        set_balance_snapshot=snapshots.append, set_bbo_replay_recorder=bbo.append,
        set_verdict_cache=caches.append, set_market_task=tasks.append,
        set_session_generation=generations.append,
    )
    return runtime, events, snapshots, bbo, caches, tasks, generations


def test_bootstrap_preserves_creation_order_and_shared_dependency_identity(monkeypatch):
    runtime, events, snapshots, *_ = _build(monkeypatch)
    assert runtime.create() is runtime
    assert events == ["core", "shared", "paper", ("universe", ("SBER", "GAZP")), "market", "root"]
    assert snapshots == [{"equity": 400_000}]
    deps = runtime.market_runtime_composition.dependencies
    assert deps["broker"] is runtime.application_core_runtime.broker
    assert deps["anomaly_queue"] is runtime.application_shared_state_runtime.anomaly_queue
    assert deps["shadow_tracker"] is runtime.application_shared_state_runtime.shadow_tracker
    assert deps["background_operations"] is runtime.application_shared_state_runtime.background_operations
    assert deps["session_generation_provider"]() == 0
    assert deps["recorder_provider"]() is runtime.application_shared_state_runtime.bbo_replay_recorder
    assert deps["balance_snapshot"] == {"equity": 400_000}
    assert deps["tickers"] is runtime.tickers
    assert deps["finam_trade_lot_sizes"] is runtime.finam_trade_lot_sizes
    assert runtime.application_core_runtime.kwargs["lot_sizes"] is runtime.lot_sizes
    root_kwargs = runtime.application_composition_root_runtime.kwargs
    assert root_kwargs["application_core_runtime"] is runtime.application_core_runtime
    assert root_kwargs["market_runtime_composition"] is runtime.market_runtime_composition


def test_bootstrap_callbacks_keep_shared_and_external_references_synchronized(monkeypatch):
    runtime, _, _, bbo, caches, tasks, generations = _build(monkeypatch)
    runtime.create()
    shared = runtime.application_shared_state_runtime
    recorder, task, cache = SimpleNamespace(session_id="replay-2"), object(), [{"ticker": "SBER"}]
    runtime.set_bbo_replay_recorder(recorder)
    runtime.set_market_task(task)
    runtime.set_verdict_cache(cache)
    assert shared.bbo_replay_recorder is recorder and bbo == [recorder]
    assert shared.bg_market_task is task and tasks == [task]
    assert runtime.verdict_cache is cache and caches == [cache]
    assert runtime.advance_paper_session_generation() == 1
    assert shared.paper_session_generation == 1 and generations == [1]


def test_bootstrap_exports_compatibility_names_and_rejects_duplicate_create(monkeypatch):
    runtime, *_ = _build(monkeypatch)
    with pytest.raises(RuntimeError, match="must run first"):
        build_application_compatibility_exports(runtime)
    runtime.create()
    exports = build_application_compatibility_exports(runtime)
    for name in (
        "application_core_runtime", "db_manager", "application_shared_state_runtime",
        "anomaly_queue", "paper_session_composition_runtime", "market_runtime_composition",
        "market_application_runtime", "application_composition_runtime", "app",
        "paper_operations_runtime", "paper_research_runtime",
    ):
        assert name in exports
    with pytest.raises(RuntimeError, match="only run once"):
        runtime.create()
