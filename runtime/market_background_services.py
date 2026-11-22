"""Compatibility facade for the market background service mixins."""
from __future__ import annotations

from typing import Any, Awaitable, Callable

from ai.news_parser import MarketNewsParser
from risk.risk_manager import RiskManager
from runtime.market_background_dashboard_runtime import MarketBackgroundDashboardMixin
from runtime.market_background_finam_runtime import MarketBackgroundFinamMixin


class MarketBackgroundServicesRuntime(
    MarketBackgroundFinamMixin,
    MarketBackgroundDashboardMixin,
):
    """Expose the stable background-service object while separating task domains."""

    def __init__(
        self,
        *,
        broker: Any,
        execution_engine: Any,
        logger: Any,
        tickers: list[str] | tuple[str, ...],
        finam_trade_lot_sizes: dict[str, int],
        screener: Any,
        process_market_event: Callable[[Any], Awaitable[None]],
        db_manager: Any,
        manager: Any,
        get_dashboard_open_positions: Callable[[], Awaitable[list[dict]]],
        refresh_dashboard_balance_snapshot: Callable[[list[dict] | None], Awaitable[dict]],
    ) -> None:
        self.broker = broker
        self.execution_engine = execution_engine
        self.logger = logger
        self.tickers = tickers
        self.finam_trade_lot_sizes = finam_trade_lot_sizes
        self.screener = screener
        self.process_market_event = process_market_event
        self.db_manager = db_manager
        self.manager = manager
        self.get_dashboard_open_positions = get_dashboard_open_positions
        self.refresh_dashboard_balance_snapshot = refresh_dashboard_balance_snapshot
        self.market_news_parser = MarketNewsParser
