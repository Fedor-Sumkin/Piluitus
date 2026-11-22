"""Behavioral contracts for dashboard application composition."""
from __future__ import annotations

import asyncio

import pytest

from runtime import dashboard_application_runtime as module


class _DashboardRuntime:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.calls = []
        self.snapshot_result = {"equity": 101_250.0, "margin_used": 5_000.0}

    def current_paper_open_pnl(self):
        self.calls.append(("open_pnl",))
        return 125.0

    def current_paper_assets_cost(self):
        self.calls.append(("assets_cost",))
        return 5_000.0

    def latest_screener_prices(self):
        self.calls.append(("prices",))
        return {"SBER": 250.0}

    async def get_dashboard_open_positions(self):
        self.calls.append(("positions",))
        return [{"ticker": "SBER", "status": "OPEN"}]

    async def refresh_balance_snapshot(self, positions, *, risk_manager, initial_deposit):
        self.calls.append(("refresh", positions, risk_manager, initial_deposit))
        return dict(self.snapshot_result)

    def reset_balance_guard_signature(self):
        self.calls.append(("reset_guard",))


class _App:
    def __init__(self):
        self.routers = []

    def include_router(self, router):
        self.routers.append(router)


def _runtime(monkeypatch):
    monkeypatch.setattr(module, "DashboardRuntime", _DashboardRuntime)
    execution_engine = object()
    db_manager = object()
    screener = object()
    lot_sizes = {"SBER": 1}
    logger = object()
    risk_manager = object()
    published_snapshots = []
    runtime = module.DashboardApplicationRuntime(
        execution_engine=execution_engine,
        db_manager=db_manager,
        screener=screener,
        lot_sizes=lot_sizes,
        logger=logger,
        risk_manager=risk_manager,
        initial_deposit=400_000,
        set_balance_snapshot=published_snapshots.append,
    )
    return runtime, {
        "execution_engine": execution_engine,
        "db_manager": db_manager,
        "screener": screener,
        "lot_sizes": lot_sizes,
        "logger": logger,
        "risk_manager": risk_manager,
        "published_snapshots": published_snapshots,
    }


def test_dashboard_application_composes_portfolio_runtime_and_initial_snapshot(monkeypatch):
    runtime, deps = _runtime(monkeypatch)

    assert runtime.create() is runtime
    assert isinstance(runtime.dashboard_runtime, _DashboardRuntime)
    assert runtime.dashboard_runtime.kwargs == {
        "execution_engine": deps["execution_engine"],
        "db_manager": deps["db_manager"],
        "screener": deps["screener"],
        "lot_sizes": deps["lot_sizes"],
        "logger": deps["logger"],
    }
    assert runtime.balance_snapshot == {
        "equity": 400_000.0,
        "initial_deposit": 400_000.0,
        "free_margin": 400_000.0,
        "margin_used": 0.0,
        "exposure_cap": 60_000.0,
        "exposure_cap_pct": 0.15,
        "single_position_cap": 20_000.0,
        "unreconciled_positions_count": 0,
        "pnl": 0.0,
        "realized_pnl": 0.0,
        "open_pnl": 0.0,
        "active_positions_count": 0,
    }
    assert deps["published_snapshots"] == [runtime.balance_snapshot]


def test_dashboard_application_preserves_data_provider_contracts_and_snapshot_updates(monkeypatch):
    runtime, deps = _runtime(monkeypatch)
    runtime.create()
    service = runtime.dashboard_runtime

    assert runtime.current_paper_open_pnl() == 125.0
    assert runtime.current_paper_assets_cost() == 5_000.0
    assert runtime.latest_screener_prices() == {"SBER": 250.0}
    positions = asyncio.run(runtime.get_dashboard_open_positions())
    assert positions == [{"ticker": "SBER", "status": "OPEN"}]

    refreshed = asyncio.run(runtime.refresh_balance_snapshot(positions))
    assert refreshed == {"equity": 101_250.0, "margin_used": 5_000.0}
    assert runtime.balance_snapshot == refreshed
    assert deps["published_snapshots"][-1] == refreshed
    assert service.calls[-1] == ("refresh", positions, deps["risk_manager"], 400_000.0)

    runtime.reset_balance_guard_signature()
    assert service.calls[-1] == ("reset_guard",)


def test_dashboard_application_registers_router_with_all_existing_dependencies(monkeypatch):
    runtime, deps = _runtime(monkeypatch)
    runtime.create()
    router = object()
    captured = {}

    def make_router(**kwargs):
        captured.update(kwargs)
        return router

    monkeypatch.setattr(module, "create_dashboard_router", make_router)
    app = _App()
    inputs = {
        "anomaly_queue": object(),
        "priority_queue_item_factory": object(),
        "validate_daily_loss_limit": lambda value: float(value),
        "default_daily_loss_limit": 0.10,
        "template_dir": object(),
        "manager": object(),
        "last_ai_verdicts_cache": [{"verdict_id": "v1"}],
        "process_market_event": lambda event: None,
    }

    result = runtime.register_routes(app, **inputs)

    assert result is runtime
    assert app.routers == [router]
    assert runtime.dashboard_router is router
    assert captured["execution_engine"] is deps["execution_engine"]
    assert captured["db_manager"] is deps["db_manager"]
    assert captured["logger"] is deps["logger"]
    assert captured["get_dashboard_open_positions"] == runtime.get_dashboard_open_positions
    assert captured["refresh_dashboard_balance_snapshot"] == runtime.refresh_balance_snapshot
    assert captured["last_ai_verdicts_cache"] is inputs["last_ai_verdicts_cache"]
    assert captured["process_market_event"] is inputs["process_market_event"]


def test_dashboard_application_rejects_use_before_create(monkeypatch):
    runtime, _ = _runtime(monkeypatch)
    with pytest.raises(RuntimeError, match="create"):
        runtime.current_paper_open_pnl()
