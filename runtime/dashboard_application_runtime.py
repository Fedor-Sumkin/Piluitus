"""Composition root for dashboard data providers and HTTP/WebSocket routes."""
from __future__ import annotations

from typing import Any, Callable

from interface.dashboard_routes import create_dashboard_router
from monitoring.trading_status import build_trading_status_snapshot
from portfolio.dashboard_runtime import DashboardRuntime


class DashboardApplicationRuntime:
    """Compose dashboard accounting providers with the dashboard route group."""

    def __init__(
        self,
        *,
        execution_engine: Any,
        db_manager: Any,
        screener: Any,
        lot_sizes: dict[str, int],
        logger: Any,
        risk_manager: Any,
        initial_deposit: float,
        set_balance_snapshot: Callable[[dict[str, Any]], None] | None = None,
    ) -> None:
        self.execution_engine = execution_engine
        self.db_manager = db_manager
        self.screener = screener
        self.lot_sizes = lot_sizes
        self.logger = logger
        self.risk_manager = risk_manager
        self.initial_deposit = float(initial_deposit)
        self.set_balance_snapshot = set_balance_snapshot
        self.dashboard_runtime: Any | None = None
        self.balance_snapshot: dict[str, Any] | None = None
        self.dashboard_router: Any | None = None

    def create(self) -> "DashboardApplicationRuntime":
        """Construct the portfolio dashboard service and initial UI snapshot."""
        self.dashboard_runtime = DashboardRuntime(
            execution_engine=self.execution_engine,
            db_manager=self.db_manager,
            screener=self.screener,
            lot_sizes=self.lot_sizes,
            logger=self.logger,
        )
        deposit = round(self.initial_deposit, 2)
        self.balance_snapshot = {
            "equity": deposit,
            "initial_deposit": deposit,
            "free_margin": deposit,
            "margin_used": 0.0,
            "exposure_cap": round(self.initial_deposit * 0.15, 2),
            "exposure_cap_pct": 0.15,
            "single_position_cap": round(self.initial_deposit * 0.05, 2),
            "unreconciled_positions_count": 0,
            "pnl": 0.0,
            "realized_pnl": 0.0,
            "open_pnl": 0.0,
            "active_positions_count": 0,
        }
        self._publish_balance_snapshot()
        return self

    def _require_created(self) -> None:
        if self.dashboard_runtime is None or self.balance_snapshot is None:
            raise RuntimeError("DashboardApplicationRuntime.create() must run first.")

    def _publish_balance_snapshot(self) -> None:
        if self.set_balance_snapshot is not None and self.balance_snapshot is not None:
            self.set_balance_snapshot(self.balance_snapshot)

    def current_paper_open_pnl(self) -> float:
        self._require_created()
        return self.dashboard_runtime.current_paper_open_pnl()

    def current_paper_assets_cost(self) -> float:
        self._require_created()
        return self.dashboard_runtime.current_paper_assets_cost()

    def latest_screener_prices(self) -> dict[str, float]:
        self._require_created()
        return self.dashboard_runtime.latest_screener_prices()

    async def get_dashboard_open_positions(self) -> list[dict]:
        self._require_created()
        return await self.dashboard_runtime.get_dashboard_open_positions()

    async def refresh_balance_snapshot(
        self, positions: list[dict] | None = None
    ) -> dict[str, Any]:
        self._require_created()
        self.balance_snapshot = await self.dashboard_runtime.refresh_balance_snapshot(
            positions,
            risk_manager=self.risk_manager,
            initial_deposit=self.initial_deposit,
        )
        self._publish_balance_snapshot()
        return dict(self.balance_snapshot)

    def get_trading_status(self) -> dict[str, Any]:
        """Return the latest read-only risk/market status for the dashboard."""
        self._require_created()
        return build_trading_status_snapshot(
            risk_manager=self.risk_manager,
            execution_engine=self.execution_engine,
        )

    def reset_balance_guard_signature(self) -> None:
        self._require_created()
        self.dashboard_runtime.reset_balance_guard_signature()

    def register_routes(
        self,
        app: Any,
        *,
        anomaly_queue: Any,
        priority_queue_item_factory: Any,
        validate_daily_loss_limit: Callable[[Any], float],
        default_daily_loss_limit: float,
        template_dir: Any,
        manager: Any,
        last_ai_verdicts_cache: list[dict[str, Any]],
        process_market_event: Callable[[Any], Any],
    ) -> "DashboardApplicationRuntime":
        """Build the dashboard router and mount it on the FastAPI application."""
        self._require_created()
        router = create_dashboard_router(
            anomaly_queue=anomaly_queue,
            priority_queue_item_factory=priority_queue_item_factory,
            validate_daily_loss_limit=validate_daily_loss_limit,
            default_daily_loss_limit=default_daily_loss_limit,
            template_dir=template_dir,
            manager=manager,
            logger=self.logger,
            get_dashboard_open_positions=self.get_dashboard_open_positions,
            refresh_dashboard_balance_snapshot=self.refresh_balance_snapshot,
            db_manager=self.db_manager,
            execution_engine=self.execution_engine,
            last_ai_verdicts_cache=last_ai_verdicts_cache,
            process_market_event=process_market_event,
            get_trading_status=self.get_trading_status,
        )
        app.include_router(router)
        self.dashboard_router = router
        return self
