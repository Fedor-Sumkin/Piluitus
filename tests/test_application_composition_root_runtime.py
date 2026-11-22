"""Behavioral contracts for high-level application dependency composition."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from runtime import application_composition_root_runtime as module


class _FakeApplicationComposition:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def create(self):
        return self


def _services():
    db_manager = object()
    ai_engine = object()
    broker = object()
    screener = object()
    execution_engine = SimpleNamespace(
        quarantine_unreconciled_paper_positions=lambda db: ("quarantined", db)
    )
    risk_manager = object()
    dashboard = SimpleNamespace(
        dashboard_runtime=object(),
        get_dashboard_open_positions=lambda: [],
        refresh_balance_snapshot=lambda: None,
        reset_balance_guard_signature=lambda: None,
    )
    core = SimpleNamespace(
        db_manager=db_manager,
        ai_engine=ai_engine,
        broker=broker,
        screener=screener,
        execution_engine=execution_engine,
        risk_manager=risk_manager,
        dashboard_application_runtime=dashboard,
    )

    queue = object()
    queue_factory = object()
    session_lock = object()
    shadow_tracker = object()
    recorder = SimpleNamespace(session_id="first")
    labels = []
    background_operations = object()
    manager = object()
    shared = SimpleNamespace(
        anomaly_queue=queue,
        priority_queue_item_factory=queue_factory,
        paper_session_lock=session_lock,
        shadow_tracker=shadow_tracker,
        bbo_replay_recorder=recorder,
        paper_shadow_labels=labels,
        background_operations=background_operations,
        connection_manager=manager,
        paper_session_generation=0,
    )

    shadow_persist = lambda labels, *, source: None
    shadow_restore = lambda: None
    performance_restore = lambda: None
    rotate_recorder = lambda: None
    paper_session = SimpleNamespace(
        persist_shadow_outcomes=shadow_persist,
        restore_shadow_outcomes=shadow_restore,
        restore_paper_performance=performance_restore,
        rotate_bbo_replay_session=rotate_recorder,
    )

    deduplicator = object()
    pending = object()
    pending_lock = object()
    worker_factory = SimpleNamespace(create=lambda *args: ("worker", args))
    ingestion = SimpleNamespace(
        ai_candidate_deduplicator=deduplicator,
        pending_bbo_candidates=pending,
        pending_bbo_lock=pending_lock,
    )
    background_services = SimpleNamespace(
        _finam_metadata_refresh_loop=lambda: None,
        _start_news_updater=lambda: None,
        _start_tables_sync_loop=lambda: None,
        background_broker_init=lambda: None,
    )
    market_app = SimpleNamespace(
        ingestion_components=ingestion,
        market_worker_factory=worker_factory,
        market_background_services=background_services,
        process_market_event=lambda event: None,
    )
    trade_flow_counters = object()
    position_diagnostics = object()
    observability = SimpleNamespace(
        trade_flow_counters=trade_flow_counters,
        position_order_book_diagnostics=position_diagnostics,
        maybe_log_trade_flow=lambda **kwargs: None,
    )
    market = SimpleNamespace(
        market_application_runtime=market_app,
        market_observability_runtime=observability,
        parse_finite_float=lambda value: float(value),
    )
    settings = SimpleNamespace(DAILY_LOSS_LIMIT_PCT=0.03)
    logger = object()
    cache = []
    verdict_setter = lambda values: None
    market_task_setter = lambda task: None
    generation_advance = lambda: None

    kwargs = dict(
        application_core_runtime=core,
        application_shared_state_runtime=shared,
        paper_session_composition_runtime=paper_session,
        market_runtime_composition=market,
        settings=settings,
        deposit=400_000.0,
        logger=logger,
        verdict_cache=cache,
        max_cache_size=100,
        set_verdict_cache=verdict_setter,
        set_market_task=market_task_setter,
        advance_session_generation=generation_advance,
    )
    expected = SimpleNamespace(
        core=core,
        shared=shared,
        paper_session=paper_session,
        market=market,
        db_manager=db_manager,
        ai_engine=ai_engine,
        broker=broker,
        screener=screener,
        execution_engine=execution_engine,
        risk_manager=risk_manager,
        dashboard=dashboard,
        queue=queue,
        queue_factory=queue_factory,
        session_lock=session_lock,
        shadow_tracker=shadow_tracker,
        labels=labels,
        background_operations=background_operations,
        manager=manager,
        deduplicator=deduplicator,
        pending=pending,
        pending_lock=pending_lock,
        trade_flow_counters=trade_flow_counters,
        position_diagnostics=position_diagnostics,
        cache=cache,
        verdict_setter=verdict_setter,
        market_task_setter=market_task_setter,
        generation_advance=generation_advance,
    )
    return kwargs, expected


def test_root_composition_preserves_dependency_identity_and_callback_wiring(monkeypatch):
    kwargs, expected = _services()
    monkeypatch.setattr(
        module, "ApplicationCompositionRuntime", _FakeApplicationComposition
    )

    runtime = module.ApplicationCompositionRootRuntime(**kwargs).create()
    composed = runtime.application_composition_runtime.kwargs

    lifecycle = composed["lifecycle_dependencies"]
    dashboard = composed["dashboard_route_dependencies"]
    paper = composed["paper_operations_dependencies"]
    callbacks = composed["paper_operations_callbacks"]

    assert lifecycle["db_manager"] is expected.db_manager
    assert lifecycle["execution_engine"] is expected.execution_engine
    assert lifecycle["paper_shadow_labels"] is expected.labels
    assert lifecycle["background_operations"] is expected.background_operations
    assert lifecycle["set_verdict_cache"] is expected.verdict_setter
    assert lifecycle["set_market_task"] is expected.market_task_setter
    assert lifecycle["max_cache_size"] == 100
    assert lifecycle["paper_bbo_replay_recorder_provider"]() is expected.shared.bbo_replay_recorder
    assert lifecycle["worker_factory"]() == (
        "worker", (expected.db_manager, expected.ai_engine, expected.broker)
    )
    assert lifecycle["quarantine_unreconciled_paper_positions"]() == (
        "quarantined", expected.db_manager
    )

    assert dashboard["anomaly_queue"] is expected.queue
    assert dashboard["priority_queue_item_factory"] is expected.queue_factory
    assert dashboard["manager"] is expected.manager
    assert dashboard["last_ai_verdicts_cache"] is expected.cache
    assert dashboard["default_daily_loss_limit"] == 0.03

    assert paper["db_manager"] is expected.db_manager
    assert paper["execution_engine"] is expected.execution_engine
    assert paper["risk_manager"] is expected.risk_manager
    assert paper["screener"] is expected.screener
    assert paper["shadow_tracker"] is expected.shadow_tracker
    assert paper["paper_shadow_labels"] is expected.labels
    assert paper["paper_session_lock"] is expected.session_lock
    assert paper["connection_manager"] is expected.manager
    assert paper["anomaly_queue"] is expected.queue
    assert paper["ai_candidate_deduplicator"] is expected.deduplicator
    assert paper["pending_bbo_candidates"] is expected.pending
    assert paper["pending_bbo_lock"] is expected.pending_lock
    assert paper["trade_flow_counters"] is expected.trade_flow_counters
    assert paper["position_order_book_diagnostics"] is expected.position_diagnostics
    assert paper["last_ai_verdicts_cache"] is expected.cache
    assert paper["deposit"] == 400_000.0

    assert callbacks["advance_session_generation"] is expected.generation_advance
    assert callbacks["rotate_bbo_replay_session"] is expected.paper_session.rotate_bbo_replay_session
    assert callbacks["reset_balance_guard_signature"] is expected.dashboard.reset_balance_guard_signature
    assert callbacks["get_bbo_replay_session_id"]() == "first"
    expected.shared.bbo_replay_recorder = SimpleNamespace(session_id="rotated")
    assert callbacks["get_bbo_replay_session_id"]() == "rotated"


def test_root_composition_rejects_duplicate_creation(monkeypatch):
    kwargs, _ = _services()
    monkeypatch.setattr(
        module, "ApplicationCompositionRuntime", _FakeApplicationComposition
    )

    runtime = module.ApplicationCompositionRootRuntime(**kwargs).create()
    with pytest.raises(RuntimeError, match="only run once"):
        runtime.create()


def test_root_composition_uses_expected_application_metadata(monkeypatch):
    kwargs, _ = _services()
    monkeypatch.setattr(
        module, "ApplicationCompositionRuntime", _FakeApplicationComposition
    )

    runtime = module.ApplicationCompositionRootRuntime(**kwargs).create()
    composed = runtime.application_composition_runtime.kwargs
    assert composed["app_title"] == "PILUITUS v35.2 — AI Trading Core"
    assert composed["static_dir"].name == "static"
    assert composed["dashboard_route_dependencies"]["template_dir"].name == "templates"
