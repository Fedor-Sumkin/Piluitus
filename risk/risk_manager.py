"""Stable RiskManager facade composed from focused risk-policy mixins."""
from __future__ import annotations

from risk.risk_shared import *  # noqa: F401,F403
from risk.market_simulator import RealMarketSimulator
from risk.risk_lot_metadata import RiskLotMetadataMixin
from risk.risk_capital_sync import RiskCapitalSyncMixin
from risk.risk_trading_limits import RiskTradingLimitsMixin
from risk.risk_lot_sizing import RiskLotSizingMixin


class RiskManager(
    RiskLotMetadataMixin,
    RiskCapitalSyncMixin,
    RiskTradingLimitsMixin,
    RiskLotSizingMixin,
):
    def __init__(
        self,
        max_risk_per_trade=0.005,
        daily_loss_limit=0.015,
        initial_deposit=100000.0,
        drawdown_warning_pct=0.03,
        max_drawdown_pct=0.05,
        loss_streak_3_pause_sec=600,
        loss_streak_5_pause_sec=1800,
        loss_streak_hard_stop=5,
    ):
        self.initial_deposit = initial_deposit
        self.current_equity = initial_deposit
        self.free_margin = initial_deposit
        self.total_session_pnl = 0.0
        self.realized_pnl = 0.0
        self.open_pnl = 0.0
        self.high_water_mark = max(0.0, float(initial_deposit))
        self.current_drawdown_pct = 0.0
        self.max_risk_per_trade = max_risk_per_trade
        self.daily_loss_limit = daily_loss_limit
        self.drawdown_warning_pct = max(0.0, min(float(drawdown_warning_pct), 0.50))
        self.max_drawdown_pct = max(
            self.drawdown_warning_pct, min(float(max_drawdown_pct), 0.50)
        )
        self.loss_streak_3_pause_sec = max(0, int(loss_streak_3_pause_sec))
        self.loss_streak_5_pause_sec = max(
            self.loss_streak_3_pause_sec, int(loss_streak_5_pause_sec)
        )
        self.loss_streak_hard_stop = max(3, int(loss_streak_hard_stop))
        self.block_trading = False
        self._drawdown_warning_active = False
        self._loss_streak_count = 0
        self._loss_pause_signature: str | None = None
        self._last_loss_streak_diagnostic_signature: str | None = None
        self._last_risk_snapshot_monotonic = 0.0
        self._cached_trading_allowed: bool | None = None
        self._balance_initialized = False
        self._blocked_date: date | None = None
        self.open_pnl_provider: Callable[[], float] | None = None
        self.open_assets_cost_provider: Callable[[], float] | None = None
