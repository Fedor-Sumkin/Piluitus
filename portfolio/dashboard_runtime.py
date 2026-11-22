"""Low-frequency dashboard data access and PAPER account providers."""
from __future__ import annotations

from typing import Any

from portfolio.dashboard_accounting import build_dashboard_balance_snapshot
from portfolio.paper_accounting import calculate_open_pnl
from portfolio.portfolio_metrics import calculate_assets_used


class DashboardRuntime:
    """Read dashboard state without mutating trading or risk state."""

    def __init__(
        self,
        execution_engine: Any,
        db_manager: Any,
        screener: Any,
        lot_sizes: dict[str, int],
        logger: Any,
    ) -> None:
        self.execution_engine = execution_engine
        self.db_manager = db_manager
        self.screener = screener
        self.lot_sizes = lot_sizes
        self.logger = logger

    def current_paper_open_pnl(self) -> float:
        return calculate_open_pnl(
            getattr(self.execution_engine, "paper_positions", {}).values(),
            paper_mode=getattr(self.execution_engine, "paper_mode", True),
        )

    def current_paper_assets_cost(self) -> float:
        if not getattr(self.execution_engine, "paper_mode", True):
            return 0.0
        positions = list(
            getattr(self.execution_engine, "paper_positions", {}).values()
        )
        unresolved = getattr(
            self.execution_engine, "_unreconciled_paper_positions", {}
        )
        if isinstance(unresolved, dict):
            positions.extend(unresolved.values())
        return calculate_assets_used(positions, lot_sizes=self.lot_sizes)

    def latest_screener_prices(self) -> dict[str, float]:
        """Return the latest positive finite prices from the local screener."""
        prices: dict[str, float] = {}
        for ticker, data in getattr(self.screener, "history", {}).items():
            try:
                count = int(data.get("count", 0))
                values = data.get("prices")
                if count > 0 and values is not None:
                    price = float(values[min(count, len(values)) - 1])
                    if price > 0 and price == price and price != float("inf"):
                        prices[str(ticker).strip().upper().split("@")[0]] = price
            except (TypeError, ValueError, IndexError, AttributeError):
                continue
        return prices

    async def get_dashboard_open_positions(self) -> list[dict]:
        """Read open positions from the authoritative source for the current mode."""
        if getattr(self.execution_engine, "paper_mode", True):
            raw = getattr(
                self.execution_engine,
                "paper_positions",
                getattr(self.execution_engine, "_paper_positions", {}),
            )
            values = list(raw.values()) if isinstance(raw, dict) else []
        else:
            # LIVE accepted orders are persisted in the trade journal. This is a
            # low-frequency dashboard read, not part of the market tick path.
            try:
                active = await self.db_manager.get_active_positions()
                values = list(active.values()) if isinstance(active, dict) else []
            except Exception as exc:
                self.logger.debug(
                    "Не удалось получить LIVE-позиции для расчёта капитала: %s", exc
                )
                values = []
        return [
            dict(item)
            for item in values
            if isinstance(item, dict)
            and str(item.get("status", "OPEN")).upper() == "OPEN"
        ]
