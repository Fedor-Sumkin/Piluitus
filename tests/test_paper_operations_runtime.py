"""Contracts for PAPER operator/research API composition."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

from runtime import paper_operations_runtime as module


class _App:
    def __init__(self):
        self.routers = []

    def include_router(self, router):
        self.routers.append(router)


class _Manager:
    def __init__(self):
        self.messages = []

    async def broadcast(self, message):
        self.messages.append(message)


class _PaperAPIHelpers:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.calls = []

    def require_paper_mode(self):
        self.calls.append("require")

    def paper_trade_rows(self, snapshot):
        self.calls.append(("rows", snapshot))
        return [{"id": "paper-1"}]

    async def broadcast_paper_tables(self):
        self.calls.append("broadcast")


class _Research:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


class _Maintenance:
    def __init__(self, **kwargs):
        self.kwargs = kwargs


def _inputs():
    dependencies = {
        key: object()
        for key in (
            "db_manager", "is_paper_trade", "execution_engine", "risk_manager",
            "screener", "paper_performance_tracker", "shadow_tracker",
            "paper_shadow_labels", "paper_session_lock", "connection_manager",
            "get_open_positions", "refresh_balance", "anomaly_queue",
            "ai_candidate_deduplicator", "last_ai_verdicts_cache",
            "trade_flow_counters", "pending_bbo_candidates", "pending_bbo_lock",
            "position_order_book_diagnostics", "bbo_replay_storage_key",
            "bbo_replay_storage_limit", "paper_closed_trades_storage_limit",
            "shadow_outcomes_storage_limit", "lot_sizes", "parse_finite_float",
            "replay_exit_batch", "replay_exit_scenarios",
            "build_shadow_reconciliation", "build_actual_vs_replay_reconciliation",
            "build_shadow_cost_sensitivity", "build_shadow_walk_forward_validation",
            "deposit", "logger", "monotonic",
        )
    }
    manager = _Manager()
    dependencies["connection_manager"] = manager
    dependencies["monotonic"] = lambda: 10.0
    callbacks = {
        "advance_session_generation": lambda: None,
        "reset_balance_guard_signature": lambda: None,
        "rotate_bbo_replay_session": lambda: None,
        "get_bbo_replay_session_id": lambda: "session-1",
    }
    return dependencies, callbacks, manager


def test_paper_operations_runtime_composes_helpers_research_and_maintenance(monkeypatch):
    monkeypatch.setattr(module, "PaperAPIHelpers", _PaperAPIHelpers)
    monkeypatch.setattr(module, "PaperResearchRuntime", _Research)
    monkeypatch.setattr(module, "PaperSessionMaintenanceRuntime", _Maintenance)
    monkeypatch.setattr(module, "create_paper_research_router", lambda runtime: ("research", runtime))
    monkeypatch.setattr(module, "create_paper_session_router", lambda runtime: ("maintenance", runtime))

    dependencies, callbacks, _ = _inputs()
    app = _App()
    runtime = module.PaperOperationsRuntime(
        dependencies=dependencies, callbacks=callbacks
    ).create(app)

    assert isinstance(runtime.paper_api_helpers, _PaperAPIHelpers)
    assert isinstance(runtime.paper_research_runtime, _Research)
    assert isinstance(runtime.paper_session_maintenance_runtime, _Maintenance)
    assert [router[0] for router in app.routers] == ["research", "maintenance"]
    assert runtime.paper_research_runtime.kwargs["broadcast_paper_tables"] == runtime.broadcast_paper_tables
    assert runtime.paper_session_maintenance_runtime.kwargs["require_paper_mode"] == runtime.require_paper_mode
    assert runtime.paper_session_maintenance_runtime.kwargs["broadcast_paper_tables"] == runtime.broadcast_paper_tables


def test_paper_operations_runtime_preserves_helper_and_reset_broadcast_contract(monkeypatch):
    monkeypatch.setattr(module, "PaperAPIHelpers", _PaperAPIHelpers)
    monkeypatch.setattr(module, "PaperResearchRuntime", _Research)
    monkeypatch.setattr(module, "PaperSessionMaintenanceRuntime", _Maintenance)
    monkeypatch.setattr(module, "create_paper_research_router", lambda runtime: runtime)
    monkeypatch.setattr(module, "create_paper_session_router", lambda runtime: runtime)

    dependencies, callbacks, manager = _inputs()
    runtime = module.PaperOperationsRuntime(
        dependencies=dependencies, callbacks=callbacks
    ).create(_App())

    runtime.require_paper_mode()
    assert runtime.paper_trade_rows({}) == [{"id": "paper-1"}]
    asyncio.run(runtime.broadcast_paper_tables())
    asyncio.run(runtime.broadcast_paper_session_reset({"type": "PAPER_SESSION_RESET"}))

    assert runtime.paper_api_helpers.calls == [
        "require", ("rows", {}), "broadcast"
    ]
    assert manager.messages == ['{"type": "PAPER_SESSION_RESET"}']
