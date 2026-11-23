"""Risk-based, Finam-lot-aware position sizing mixin."""
from __future__ import annotations

from risk.risk_shared import *  # noqa: F401,F403


class RiskLotSizingMixin:
    def calculate_lot_size(self, ticker: str, current_price: float, atr_stop: float) -> int:
        """Return affordable lots under the risk cap; return 0 if even one lot exceeds it."""
        if current_price <= 0 or atr_stop <= 0:
            return 0
        risk_per_share_rub = abs(current_price - atr_stop)
        if risk_per_share_rub <= 0:
            return 0
        allowed_risk_money = max(0.0, self.initial_deposit * self.max_risk_per_trade)
        normalized_ticker = str(ticker or "").strip().upper().split("@", 1)[0]
        raw_lot_size = FINAM_TRADE_LOT_SIZES.get(normalized_ticker)
        try:
            lot_size = int(raw_lot_size)
        except (TypeError, ValueError, OverflowError):
            risk_logger = logging.getLogger("Piluitus.RiskManager")
            risk_logger.error(
                "Размер позиции заблокирован: %s lot_size для %s не подтверждён; "
                "статический LOT_SIZES не используется для нового ордера.",
                "биржевой" if PAPER_TRADING else "Finam account trade",
                normalized_ticker,
            )
            return 0
        if lot_size <= 0:
            return 0
        risk_per_lot_rub = risk_per_share_rub * lot_size
        notional_per_lot = float(current_price) * lot_size
        if risk_per_lot_rub <= 0 or notional_per_lot <= 0 or allowed_risk_money < risk_per_lot_rub:
            return 0
        lots_by_risk = int(allowed_risk_money // risk_per_lot_rub)
        # Pre-limit is intentionally capped at 3% per instrument. ExecutionLayer
        # applies the final aggregate / single-position guard atomically.
        try:
            max_single_pct = float(os.getenv("PILUITUS_MAX_SINGLE_POSITION_PCT", "0.03"))
        except (TypeError, ValueError, OverflowError):
            max_single_pct = 0.03
        if not math.isfinite(max_single_pct) or max_single_pct <= 0:
            max_single_pct = 0.03
        max_single_pct = min(max_single_pct, 0.03)
        lots_by_notional = int((self.initial_deposit * max_single_pct) // notional_per_lot)
        return max(0, min(lots_by_risk, lots_by_notional))

if __name__ == "__main__":
    emulator = RealMarketSimulator(uri="ws://127.0.0.1:8080/ws/frontend", batch_size=10)
    try:
        asyncio.run(emulator.generate_hft_stream())
    except KeyboardInterrupt:
        logger.info("🛑 Симулятор торгов успешно остановлен.")
