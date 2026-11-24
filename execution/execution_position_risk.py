"""Position inventory, exposure budgets, stop risk and order sizing mixin."""
from __future__ import annotations

from execution.execution_shared import *  # noqa: F401,F403
from decimal import Decimal, ROUND_CEILING, ROUND_FLOOR


class ExecutionPositionRiskMixin:
    def get_open_position_tickers(self) -> set[str]:
        """Return normalized symbols whose active positions require continuous BBO.

        This intentionally reports only positions currently managed in memory.
        Quarantined legacy PAPER positions are not resumed or monitored automatically.
        """
        positions = self.paper_positions
        tickers: set[str] = set()
        for key, position in positions.items():
            if not isinstance(position, dict):
                continue
            if str(position.get("status", "OPEN")).upper() not in {"OPEN", "ACTIVE"}:
                continue
            raw_ticker = position.get("ticker") or position.get("symbol") or key
            ticker = str(raw_ticker).strip().upper().split("@")[0]
            if ticker:
                tickers.add(ticker)
        return tickers

    @staticmethod
    def _bounded_pct(value: object, default: float, maximum: float = 1.0) -> float:
        pct = _finite_or_default(value, math.nan)
        if not math.isfinite(pct) or pct <= 0.0:
            return min(default, max(0.0, maximum))
        return min(pct, max(0.0, maximum))

    def _latest_price(
        self, ticker: str, trade: Mapping[str, object] | None = None
    ) -> float:
        """Use latest screener quote first, then the position's current/entry price."""
        ticker = str(ticker or "").strip().upper().split("@")[0]
        if self.screener is not None:
            data_obj: object = self.screener.history.get(ticker, {})
            data = _as_string_mapping(data_obj)
            if data is not None:
                count = int(_finite_or_default(data.get("count", 0), 0.0))
                prices_obj = data.get("prices")
                if isinstance(prices_obj, (list, tuple, np.ndarray)) and count > 0 and len(prices_obj) > 0:
                    index = min(count, len(prices_obj)) - 1
                    price = _finite_or_default(prices_obj[index], 0.0)
                    if price > 0.0:
                        return price

        trade_map = _as_string_mapping(trade) if trade is not None else None
        if trade_map is not None:
            for key in ("current_price", "entry_price", "price", "average_price"):
                price = _finite_or_default(trade_map.get(key, 0.0), 0.0)
                if price > 0.0:
                    return price
        return 0.0

    @staticmethod
    def _share_quantity(ticker: str, trade: Mapping[str, object]) -> int:
        """Position volume is stored in shares; fallback converts lots to shares."""
        volume = int(_finite_or_default(trade.get("volume", 0), 0.0))
        if volume > 0:
            return volume
        lots = int(_finite_or_default(trade.get("lots", 0), 0.0))
        lot_size = int(
            _finite_or_default(
                trade.get("lot_size", LOT_SIZES.get(ticker, 1)),
                float(LOT_SIZES.get(ticker, 1) or 1),
            )
        )
        return max(0, lots * max(1, lot_size))

    def _gross_exposure(self, positions: object) -> float:
        """Calculate acquisition-cost exposure; malformed OPEN rows fail closed."""
        if isinstance(positions, Mapping):
            iterable: Iterable[object] = positions.values()
        elif isinstance(positions, (list, tuple)):
            iterable = positions
        else:
            return math.inf

        total = 0.0
        for trade_obj in iterable:
            trade = _as_string_mapping(trade_obj)
            if trade is None or str(trade.get("status", "OPEN")).upper() != "OPEN":
                continue

            ticker = str(trade.get("ticker", "")).strip().upper().split("@")[0]
            if not ticker:
                logger.error(
                    "[PORTFOLIO GUARD] У открытой позиции отсутствует тикер; новые входы заблокированы."
                )
                return math.inf

            qty = self._share_quantity(ticker, trade)
            entry_price = 0.0
            for key in ("entry_price", "purchase_price", "average_entry_price", "average_price"):
                candidate = _finite_or_default(trade.get(key, 0.0), 0.0)
                if candidate > 0.0:
                    entry_price = candidate
                    break

            if qty > 0 and entry_price > 0.0:
                total += abs(qty * entry_price)
                continue

            saved_notional = _finite_or_default(trade.get("notional_entry_rub", 0.0), 0.0)
            if saved_notional > 0.0:
                total += saved_notional
                continue

            logger.critical(
                "[PORTFOLIO GUARD] Не удалось определить цену покупки/объём открытой позиции %s; "
                "новые входы заблокированы.",
                ticker,
            )
            return math.inf

        return round(total, 2)

    def _stop_risk_with_costs(
        self,
        entry_price: float,
        stop_price: float,
        quantity: int,
        side: str,
        *,
        entry_commission_rub: float | None = None,
    ) -> float:
        """Estimate loss at the stop including configured PAPER exit costs.

        The entry price is already the simulated fill price. Additional slippage
        is therefore applied only to the stop exit; commissions cover both sides.
        LIVE sizing keeps the existing gross stop-distance model because broker
        fees are not inferred from the PAPER tariff settings.
        """
        entry = _finite_or_default(entry_price, 0.0)
        stop = _finite_or_default(stop_price, 0.0)
        qty = max(0, int(quantity))
        direction = str(side or "").strip().upper()
        if entry <= 0.0 or stop <= 0.0 or qty <= 0 or direction not in {"BUY", "SELL"}:
            return math.inf

        exit_price = stop
        fee_pct = 0.0
        if self.paper_mode:
            slippage_bps = min(100.0, max(0.0, _finite_or_default(
                getattr(self, "paper_slippage_bps", 0.0), 0.0
            )))
            slippage_rate = slippage_bps / 10_000.0
            exit_price = stop * (1.0 - slippage_rate if direction == "BUY" else 1.0 + slippage_rate)
            fee_pct = min(1.0, max(0.0, _finite_or_default(
                getattr(self, "paper_commission_pct", 0.0), 0.0
            )))

        gross_loss = (
            (entry - exit_price) * qty
            if direction == "BUY"
            else (exit_price - entry) * qty
        )
        if not math.isfinite(gross_loss) or gross_loss < 0.0:
            return math.inf
        if not self.paper_mode:
            return gross_loss

        if entry_commission_rub is None:
            entry_fee = entry * qty * fee_pct / 100.0
        else:
            entry_fee = max(0.0, _finite_or_default(entry_commission_rub, 0.0))
        exit_fee = exit_price * qty * fee_pct / 100.0
        total = gross_loss + entry_fee + exit_fee
        return total if math.isfinite(total) else math.inf

    def _open_stop_risk(self, positions: object) -> float:
        """Sum downside from entry to stop for every OPEN position; fail closed on unknown data."""
        if isinstance(positions, Mapping):
            iterable: Iterable[object] = positions.values()
        elif isinstance(positions, (list, tuple)):
            iterable = positions
        else:
            return math.inf

        total = 0.0
        for trade_obj in iterable:
            trade = _as_string_mapping(trade_obj)
            if trade is None:
                return math.inf
            if str(trade.get("status", "OPEN")).upper() != "OPEN":
                continue
            ticker = str(trade.get("ticker", "")).strip().upper().split("@")[0]
            side = str(trade.get("side", "")).strip().upper()
            qty = self._share_quantity(ticker, trade)
            entry = _finite_or_default(trade.get("entry_price"), 0.0)
            stop = _finite_or_default(trade.get("stop_loss"), 0.0)
            if not ticker or side not in {"BUY", "SELL"} or qty <= 0 or entry <= 0 or stop <= 0:
                logger.error(
                    "[MONEY MANAGEMENT] Не удалось определить риск открытой позиции %s; "
                    "новые входы заблокированы.", ticker or "<unknown>"
                )
                return math.inf
            distance = entry - stop if side == "BUY" else stop - entry
            if not math.isfinite(distance) or distance <= 0:
                logger.error(
                    "[MONEY MANAGEMENT] Некорректное направление SL у %s (%s, entry=%.6f, SL=%.6f); "
                    "новые входы заблокированы.", ticker, side, entry, stop
                )
                return math.inf
            entry_fee_raw = trade.get("entry_commission_rub")
            entry_fee = (
                _finite_or_default(entry_fee_raw, 0.0)
                if entry_fee_raw is not None else None
            )
            position_risk = self._stop_risk_with_costs(
                entry, stop, qty, side, entry_commission_rub=entry_fee
            )
            if not math.isfinite(position_risk):
                logger.error(
                    "[MONEY MANAGEMENT] Не удалось оценить риск позиции %s с учётом издержек; "
                    "новые входы заблокированы.", ticker
                )
                return math.inf
            total += position_risk
        return round(total, 2)

    @staticmethod
    def _price_tick_rule(ticker: str, price: float) -> tuple[float, float]:
        """Return the applicable tick and grid origin for a fixed or tiered price rule."""
        normalized = str(ticker or "").strip().upper().split("@", 1)[0]
        metadata = ASSET_METADATA.get(normalized, {})
        if not isinstance(metadata, Mapping):
            return 0.0, 0.0
        # A tiered Finam price_step rule is more specific than a fallback
        # fixed min_step and must take precedence when both are supplied.
        rule = metadata.get("price_step")
        if isinstance(rule, Mapping):
            threshold = _finite_or_default(rule.get("threshold_price"), 0.0)
            below_step = _finite_or_default(rule.get("step"), 0.0)
            above_step = _finite_or_default(rule.get("step_from_threshold"), 0.0)
            if threshold > 0.0 and below_step > 0.0 and above_step > 0.0:
                if price < threshold:
                    return below_step, 0.0
                return above_step, threshold
        fixed_tick = _finite_or_default(metadata.get("price_tick"), 0.0)
        if math.isfinite(fixed_tick) and fixed_tick > 0.0:
            return fixed_tick, 0.0
        return 0.0, 0.0

    @classmethod
    def _round_price_to_valid_tick(cls, ticker: str, price: float, rounding) -> float:
        """Round directionally to a valid tick, rechecking when rounding crosses a band."""
        candidate = _finite_or_default(price, 0.0)
        if candidate <= 0.0:
            return 0.0
        metadata = ASSET_METADATA.get(str(ticker or "").strip().upper().split("@", 1)[0], {})
        decimals = int(_finite_or_default(metadata.get("decimals"), 8)) if isinstance(metadata, Mapping) else 8
        for _ in range(4):
            tick, origin = cls._price_tick_rule(ticker, candidate)
            if not math.isfinite(tick) or tick <= 0.0:
                return 0.0
            try:
                tick_decimal = Decimal(str(tick))
                origin_decimal = Decimal(str(origin))
                price_decimal = Decimal(str(candidate))
                units = ((price_decimal - origin_decimal) / tick_decimal).to_integral_value(
                    rounding=rounding
                )
                rounded_value = float(origin_decimal + units * tick_decimal)
            except (ArithmeticError, TypeError, ValueError, OverflowError):
                return 0.0
            next_tick, next_origin = cls._price_tick_rule(ticker, rounded_value)
            if (
                math.isclose(next_tick, tick, rel_tol=1e-12, abs_tol=1e-12)
                and math.isclose(next_origin, origin, rel_tol=1e-12, abs_tol=1e-12)
            ):
                return round(rounded_value, max(0, min(10, decimals)))
            candidate = rounded_value
        return 0.0

    def calculate_atr_stop_with_diagnostics(
        self,
        ticker: str,
        current_price: float,
        side: str,
        *,
        atr_value: float | None = None,
        atr_periods: int | None = None,
        bid: float = 0.0,
        ask: float = 0.0,
    ) -> tuple[float, float, str]:
        """Build SL/TP from measured M1 true-range ATR, spread and a safety floor.

        ATR is read from completed one-minute OHLC bars (not a short tick-price
        window). We require a small warm-up sample, use up to ATR(14), preserve the
        configured minimum noise buffer, and cap the maximum stop width.
        """
        side = str(side).upper()
        try:
            price = float(current_price)
        except (TypeError, ValueError):
            return 0.0, 0.0, "INVALID_ENTRY_PRICE"
        if not math.isfinite(price) or price <= 0.0 or side not in {"BUY", "SELL"}:
            return 0.0, 0.0, "INVALID_ENTRY_PRICE"

        period = 14
        try:
            min_periods = max(2, int(os.getenv("PILUITUS_ATR_MIN_PERIODS", "5")))
        except (TypeError, ValueError):
            min_periods = 5
        min_periods = min(min_periods, period)

        atr = 0.0
        periods = 0
        try:
            if atr_value is not None:
                atr = float(atr_value)
                periods = int(atr_periods or 0)
            elif self.screener is not None:
                snapshot = self.screener.minute_bar_patterns.get_atr_snapshot(
                    ticker, period=period
                )
                atr = _finite_or_default(snapshot.get("atr", 0.0), 0.0)
                periods = int(_finite_or_default(snapshot.get("periods", 0.0), 0.0))
        except (TypeError, ValueError, AttributeError, OverflowError):
            atr, periods = 0.0, 0

        if not math.isfinite(atr) or atr <= 0.0 or periods < min_periods:
            return 0.0, 0.0, "ATR_HISTORY_UNAVAILABLE"

        try:
            atr_multiplier = float(os.getenv("ATR_MULTIPLIER", "1.5"))
            if not math.isfinite(atr_multiplier) or atr_multiplier <= 0.0:
                atr_multiplier = 1.5
        except (TypeError, ValueError):
            atr_multiplier = 1.5
        atr_multiplier = min(max(atr_multiplier, 0.5), 5.0)

        try:
            min_stop_pct = float(os.getenv("PILUITUS_MIN_STOP_PCT", "0.0025"))
            if not math.isfinite(min_stop_pct) or min_stop_pct <= 0.0:
                min_stop_pct = 0.0025
        except (TypeError, ValueError):
            min_stop_pct = 0.0025
        min_stop_pct = min(min_stop_pct, 0.015)

        spread_distance = 0.0
        try:
            bid_value = float(bid)
            ask_value = float(ask)
            if (
                math.isfinite(bid_value) and math.isfinite(ask_value)
                and bid_value > 0.0 and ask_value >= bid_value
            ):
                spread_distance = (ask_value - bid_value) * 2.0
        except (TypeError, ValueError):
            spread_distance = 0.0

        minimum_distance = price * min_stop_pct
        maximum_distance = price * 0.015
        stop_distance = min(
            max(atr * atr_multiplier, spread_distance, minimum_distance),
            maximum_distance,
        )
        try:
            reward_r = float(os.getenv("PILUITUS_TAKE_PROFIT_R_MULTIPLE", "2.0"))
            if not math.isfinite(reward_r) or reward_r <= 0.0:
                reward_r = 2.0
        except (TypeError, ValueError):
            reward_r = 2.0
        reward_r = min(max(reward_r, 1.0), 5.0)
        target_distance = stop_distance * reward_r

        if side == "BUY":
            sl = price - stop_distance
            tp = price + target_distance
        else:
            sl = price + stop_distance
            tp = price - target_distance

        # Use Finam's confirmed fixed or price-tiered tick rule. If metadata is
        # unavailable, do not fabricate a price that may be rejected.
        sl_tick, _ = self._price_tick_rule(ticker, sl)
        tp_tick, _ = self._price_tick_rule(ticker, tp)
        if (
            not math.isfinite(sl_tick) or sl_tick <= 0.0
            or not math.isfinite(tp_tick) or tp_tick <= 0.0
        ):
            return 0.0, 0.0, "PRICE_TICK_METADATA_UNAVAILABLE"
        sl_rounding = ROUND_FLOOR if side == "BUY" else ROUND_CEILING
        tp_rounding = ROUND_CEILING if side == "BUY" else ROUND_FLOOR
        rounded_sl = ExecutionPositionRiskMixin._round_price_to_valid_tick(ticker, sl, sl_rounding)
        rounded_tp = ExecutionPositionRiskMixin._round_price_to_valid_tick(ticker, tp, tp_rounding)
        if rounded_sl <= 0.0 or rounded_tp <= 0.0:
            return 0.0, 0.0, "PRICE_TICK_ROUNDING_FAILED"
        if side == "BUY" and not (rounded_sl < price < rounded_tp):
            return 0.0, 0.0, "INVALID_STOP_TARGET_GEOMETRY"
        if side == "SELL" and not (rounded_tp < price < rounded_sl):
            return 0.0, 0.0, "INVALID_STOP_TARGET_GEOMETRY"
        return rounded_sl, rounded_tp, "OK"

    def calculate_atr_stop(
        self,
        ticker: str,
        current_price: float,
        side: str,
        *,
        atr_value: float | None = None,
        atr_periods: int | None = None,
        bid: float = 0.0,
        ask: float = 0.0,
    ) -> tuple[float, float]:
        """Backward-compatible SL/TP interface for existing callers."""
        stop_loss, take_profit, _ = ExecutionPositionRiskMixin.calculate_atr_stop_with_diagnostics(
            self,
            ticker,
            current_price,
            side,
            atr_value=atr_value,
            atr_periods=atr_periods,
            bid=bid,
            ask=ask,
        )
        return stop_loss, take_profit
