"""Construct long-running market background services."""
from __future__ import annotations

from typing import Any


def create_market_background_services(runtime: Any, services_cls: Any) -> Any:
    """Bind the broker bridge, news cycle and dashboard synchronization."""
    dependencies = runtime.dependencies
        runtime.market_background_services = services_cls(
            broker=dependencies["broker"],
            execution_engine=dependencies["execution_engine"],
            logger=dependencies["logger"],
            tickers=dependencies["tickers"],
            finam_trade_lot_sizes=dependencies["finam_trade_lot_sizes"],
            screener=dependencies["screener"],
            process_market_event=runtime.process_market_event,
            db_manager=dependencies["db_manager"],
            manager=dependencies["manager"],
            get_dashboard_open_positions=dependencies["get_open_positions"],
            refresh_dashboard_balance_snapshot=dependencies[
                "refresh_dashboard_balance_snapshot"
            ],
        )
    return runtime.market_background_services
