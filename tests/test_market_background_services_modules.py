"""Contracts for the split market background-service mixins."""
from __future__ import annotations

from runtime.market_background_dashboard_runtime import MarketBackgroundDashboardMixin
from runtime.market_background_finam_runtime import MarketBackgroundFinamMixin
from ai.news_parser import MarketNewsParser
from runtime.market_background_services import MarketBackgroundServicesRuntime


def test_background_services_facade_keeps_constructor_and_method_contracts() -> None:
    values = {
        "broker": object(),
        "execution_engine": object(),
        "logger": object(),
        "tickers": ("SBER", "GAZP"),
        "finam_trade_lot_sizes": {"SBER": 10},
        "screener": object(),
        "process_market_event": lambda event: None,
        "db_manager": object(),
        "manager": object(),
        "get_dashboard_open_positions": lambda: None,
        "refresh_dashboard_balance_snapshot": lambda positions: None,
    }
    runtime = MarketBackgroundServicesRuntime(**values)

    assert isinstance(runtime, MarketBackgroundFinamMixin)
    assert isinstance(runtime, MarketBackgroundDashboardMixin)
    for name, value in values.items():
        assert getattr(runtime, name) is value
    assert runtime.market_news_parser is MarketNewsParser
