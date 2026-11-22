"""Composition root for PILUITUS core services and dashboard state.

The runtime preserves the application's original construction order and capital
consistency guard. It wires domain services but does not change their policies.
"""
from __future__ import annotations

import math
from typing import Any, Callable

from ai.ai_engine import AIEngine
from exchange.finam_client import FinamBrokerClient
from execution.execution_layer import BaseExecutionEngine
from risk.risk_manager import RiskManager
from runtime.dashboard_application_runtime import DashboardApplicationRuntime
from storage.db_manager import DBManager
from strategy.screener_funnel import HFTMemoryScreener


class ApplicationCoreRuntime:
    """Construct shared core services, validate capital and initialize Dashboard."""

    def __init__(
        self,
        *,
        settings: Any,
        finam_token: str,
        deposit: float,
        lot_sizes: dict[str, Any],
        logger: Any,
        set_balance_snapshot: Callable[[dict[str, Any]], None],
    ) -> None:
        self.settings = settings
        self.finam_token = str(finam_token or "")
        self.deposit = float(deposit)
        self.lot_sizes = lot_sizes
        self.logger = logger
        self.set_balance_snapshot = set_balance_snapshot

        self.db_manager: Any | None = None
        self.ai_engine: Any | None = None
        self.broker: Any | None = None
        self.screener: Any | None = None
        self.execution_engine: Any | None = None
        self.risk_manager: Any | None = None
        self.dashboard_application_runtime: Any | None = None
        self._created = False

    def create(self) -> "ApplicationCoreRuntime":
        """Build core services once, preserving dependency and callback order."""
        if self._created:
            raise RuntimeError("ApplicationCoreRuntime.create() may only run once.")

        settings = self.settings
        self.db_manager = DBManager()
        self.ai_engine = AIEngine()
        self.broker = FinamBrokerClient(
            token=self.finam_token,
            is_demo=settings.FINAM_DEMO,
        )
        self.screener = HFTMemoryScreener(window_size=30)
        self.execution_engine = BaseExecutionEngine(
            finam_broker_client=self.broker,
            screener=self.screener,
            initial_capital=self.deposit,
            max_open_stop_risk_pct=settings.MAX_OPEN_STOP_RISK_PCT,
        )

        # Keep ORDER_BOOK subscriptions for every active PAPER position even
        # when its ticker rotates out of the screener's Top-N list.
        self.broker.open_positions_provider = (
            self.execution_engine.get_open_position_tickers
        )
        self.risk_manager = RiskManager(
            max_risk_per_trade=settings.MAX_RISK_PCT,
            daily_loss_limit=settings.DAILY_LOSS_LIMIT_PCT,
            initial_deposit=self.deposit,
            drawdown_warning_pct=settings.DRAWDOWN_WARNING_PCT,
            max_drawdown_pct=settings.MAX_DRAWDOWN_PCT,
            loss_streak_3_pause_sec=settings.LOSS_STREAK_3_PAUSE_SEC,
            loss_streak_5_pause_sec=settings.LOSS_STREAK_5_PAUSE_SEC,
            loss_streak_hard_stop=settings.LOSS_STREAK_HARD_STOP,
        )

        if not math.isclose(
            float(self.execution_engine.initial_capital),
            float(self.risk_manager.initial_deposit),
            rel_tol=0.0,
            abs_tol=0.01,
        ):
            raise RuntimeError(
                "Несогласованный стартовый капитал: ExecutionLayer и RiskManager "
                "должны использовать единый Settings.TRADE_DEPOSIT."
            )
        self.logger.info(
            "💰 [CAPITAL SYNC] source=%s; TRADE_DEPOSIT=%.2f ₽; "
            "ExecutionLayer=%.2f ₽; RiskManager=%.2f ₽.",
            settings.TRADE_DEPOSIT_SOURCE,
            self.deposit,
            self.execution_engine.initial_capital,
            self.risk_manager.initial_deposit,
        )

        self.dashboard_application_runtime = DashboardApplicationRuntime(
            execution_engine=self.execution_engine,
            db_manager=self.db_manager,
            screener=self.screener,
            lot_sizes=self.lot_sizes,
            logger=self.logger,
            risk_manager=self.risk_manager,
            initial_deposit=self.deposit,
            set_balance_snapshot=self.set_balance_snapshot,
        ).create()

        # In PAPER, in-memory positions are the source of floating PnL and
        # acquisition cost. Avoid counting OPEN journal rows from Redis twice.
        if self.execution_engine.paper_mode:
            self.risk_manager.open_pnl_provider = (
                self.dashboard_application_runtime.current_paper_open_pnl
            )
            self.risk_manager.open_assets_cost_provider = (
                self.dashboard_application_runtime.current_paper_assets_cost
            )

        self._created = True
        return self
