"""Behavioral contracts for core service composition and capital safety."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from runtime import application_core_runtime as module


class _Logger:
    def __init__(self):
        self.messages = []

    def info(self, *args):
        self.messages.append(args)


class _Engine:
    def __init__(self, *, finam_broker_client, screener, initial_capital, max_open_stop_risk_pct):
        self.finam_broker_client = finam_broker_client
        self.screener = screener
        self.initial_capital = initial_capital
        self.max_open_stop_risk_pct = max_open_stop_risk_pct
        self.paper_mode = True

    def get_open_position_tickers(self):
        return ["SBER"]


class _Dashboard:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.dashboard_runtime = object()
        self.balance_snapshot = {"equity": kwargs["initial_deposit"]}
        self.current_paper_open_pnl = lambda: 120.0
        self.current_paper_assets_cost = lambda: 18_000.0

    def create(self):
        self.kwargs["set_balance_snapshot"](self.balance_snapshot)
        return self


class _RiskManager:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.initial_deposit = kwargs["initial_deposit"]
        self.open_pnl_provider = None
        self.open_assets_cost_provider = None


class _Broker:
    def __init__(self, *, token, is_demo):
        self.token = token
        self.is_demo = is_demo
        self.open_positions_provider = None


class _Screener:
    def __init__(self, *, window_size):
        self.window_size = window_size


def _settings(**overrides):
    values = {
        "FINAM_DEMO": True,
        "MAX_OPEN_STOP_RISK_PCT": 0.025,
        "MAX_RISK_PCT": 0.01,
        "DAILY_LOSS_LIMIT_PCT": 0.03,
        "DRAWDOWN_WARNING_PCT": 0.08,
        "MAX_DRAWDOWN_PCT": 0.15,
        "LOSS_STREAK_3_PAUSE_SEC": 60,
        "LOSS_STREAK_5_PAUSE_SEC": 300,
        "LOSS_STREAK_HARD_STOP": 8,
        "TRADE_DEPOSIT_SOURCE": "TRADE_DEPOSIT",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _runtime(monkeypatch, *, paper_mode=True, capital_delta=0.0):
    monkeypatch.setattr(module, "DBManager", lambda: object())
    monkeypatch.setattr(module, "AIEngine", lambda: object())
    monkeypatch.setattr(module, "FinamBrokerClient", _Broker)
    monkeypatch.setattr(module, "HFTMemoryScreener", _Screener)

    class _ConfiguredEngine(_Engine):
        def __init__(self, **kwargs):
            super().__init__(**kwargs)
            self.initial_capital += capital_delta
            self.paper_mode = paper_mode

    monkeypatch.setattr(module, "BaseExecutionEngine", _ConfiguredEngine)
    monkeypatch.setattr(module, "RiskManager", _RiskManager)
    monkeypatch.setattr(module, "DashboardApplicationRuntime", _Dashboard)

    snapshots = []
    logger = _Logger()
    runtime = module.ApplicationCoreRuntime(
        settings=_settings(),
        finam_token="token-123",
        deposit=400_000.0,
        lot_sizes={"SBER": 1},
        logger=logger,
        set_balance_snapshot=snapshots.append,
    )
    return runtime, snapshots, logger


def test_core_composition_preserves_service_dependencies_and_capital_synchronization(monkeypatch):
    runtime, snapshots, logger = _runtime(monkeypatch)

    assert runtime.create() is runtime
    assert runtime.db_manager is not None
    assert runtime.ai_engine is not None
    assert runtime.broker.token == "token-123"
    assert runtime.broker.is_demo is True
    assert runtime.screener.window_size == 30
    assert runtime.execution_engine.finam_broker_client is runtime.broker
    assert runtime.execution_engine.screener is runtime.screener
    assert runtime.execution_engine.max_open_stop_risk_pct == 0.025
    assert runtime.execution_engine.initial_capital == 400_000.0
    assert runtime.risk_manager.initial_deposit == 400_000.0
    assert runtime.broker.open_positions_provider() == ["SBER"]
    assert runtime.dashboard_application_runtime.kwargs["execution_engine"] is runtime.execution_engine
    assert runtime.dashboard_application_runtime.kwargs["risk_manager"] is runtime.risk_manager
    assert runtime.dashboard_application_runtime.kwargs["initial_deposit"] == 400_000.0
    assert snapshots == [{"equity": 400_000.0}]
    assert len(logger.messages) == 1
    assert logger.messages[0][0].startswith("💰 [CAPITAL SYNC]")


def test_core_composition_binds_paper_pnl_and_cost_providers_to_dashboard(monkeypatch):
    runtime, _, _ = _runtime(monkeypatch, paper_mode=True)
    runtime.create()

    assert runtime.risk_manager.open_pnl_provider is runtime.dashboard_application_runtime.current_paper_open_pnl
    assert runtime.risk_manager.open_assets_cost_provider is runtime.dashboard_application_runtime.current_paper_assets_cost
    assert runtime.risk_manager.open_pnl_provider() == 120.0
    assert runtime.risk_manager.open_assets_cost_provider() == 18_000.0


def test_core_composition_does_not_override_pnl_providers_outside_paper_mode(monkeypatch):
    runtime, _, _ = _runtime(monkeypatch, paper_mode=False)
    runtime.create()

    assert runtime.risk_manager.open_pnl_provider is None
    assert runtime.risk_manager.open_assets_cost_provider is None


def test_core_composition_rejects_mismatched_initial_capital_before_dashboard_creation(monkeypatch):
    runtime, snapshots, logger = _runtime(monkeypatch, capital_delta=1.0)

    with pytest.raises(RuntimeError, match="Несогласованный стартовый капитал"):
        runtime.create()

    assert runtime.dashboard_application_runtime is None
    assert snapshots == []
    assert logger.messages == []


def test_core_composition_rejects_duplicate_creation(monkeypatch):
    runtime, _, _ = _runtime(monkeypatch)
    runtime.create()

    with pytest.raises(RuntimeError, match="only run once"):
        runtime.create()
