"""Position inventory, exposure budgets, stop risk and order sizing mixin."""
from __future__ import annotations

from execution.execution_shared import *  # noqa: F401,F403


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

    async def _reserve_order_size(
        self, ticker: str, requested_lots: int, price: float, db: DBManager, *,
        side: str, stop_loss: float
    ) -> tuple[int, str]:
        """Atomically reserve notional exposure and aggregate downside-to-stop budget."""
        async with self._portfolio_lock:
            if self._maintenance_active:
                return 0, self._maintenance_reason or "PAPER-торговля приостановлена для обслуживания сессии."
            now = time.monotonic()
            cooldown_key = (str(ticker).strip().upper().split("@")[0], str(side).strip().upper())
            capital_cooldowns = getattr(self, "_capital_reject_cooldowns", None)
            if not isinstance(capital_cooldowns, dict):
                capital_cooldowns = {}
                self._capital_reject_cooldowns = capital_cooldowns
            cooldown_until = self._ticker_cooldowns.get(cooldown_key, 0.0)
            if cooldown_until > now:
                remaining = cooldown_until - now
                return 0, f"STOP_LOSS cooldown для {cooldown_key[1]} ещё {remaining:.0f} сек."
            self._ticker_cooldowns.pop(cooldown_key, None)

            if self.paper_mode:
                capital_retry_until = capital_cooldowns.get(cooldown_key, 0.0)
                if capital_retry_until > now:
                    return 0, (
                        f"PAPER capital retry cooldown для {cooldown_key[1]} ещё "
                        f"{capital_retry_until - now:.0f} сек.; повторная проверка будет позже."
                    )
                capital_cooldowns.pop(cooldown_key, None)

            if ticker in self._opening_tickers:
                return 0, "По тикеру уже создаётся позиция."

            if self.paper_mode:
                if self._unreconciled_paper_positions:
                    return 0, (
                        f"Есть {len(self._unreconciled_paper_positions)} старых PAPER-позиций в STALE_REVIEW; "
                        "сначала сверьте/очистите их журнал. Новые входы заблокированы."
                    )
                positions = self.paper_positions
            else:
                try:
                    positions = await db.get_active_positions()
                except Exception as exc:
                    logger.error("Не удалось проверить существующую экспозицию для LIVE-ордера %s: %s", ticker, exc)
                    return 0, "Не удалось проверить текущую экспозицию; ордер заблокирован."

            active_values = list(positions.values()) if isinstance(positions, dict) else []
            for item in active_values:
                if not isinstance(item, dict) or str(item.get("status", "OPEN")).upper() != "OPEN":
                    continue
                existing_ticker = str(item.get("ticker", "")).strip().upper().split("@")[0]
                if existing_ticker == ticker:
                    return 0, "По тикеру уже есть открытая позиция."

            open_notional = self._gross_exposure(positions)
            if not math.isfinite(open_notional):
                return 0, "Не удалось достоверно определить себестоимость всех открытых позиций; новый ордер заблокирован."
            open_stop_risk = self._open_stop_risk(positions)
            if not math.isfinite(open_stop_risk):
                return 0, "Не удалось достоверно определить суммарный риск открытых стопов; новый ордер заблокирован."

            normalized_ticker = str(ticker or "").strip().upper().split("@", 1)[0]
            raw_lot_size = FINAM_TRADE_LOT_SIZES.get(normalized_ticker)
            try:
                lot_size = int(raw_lot_size)
            except (TypeError, ValueError, OverflowError):
                lot_size = 0
            if lot_size <= 0:
                return 0, (
                    f"{'биржевой lot_size' if self.paper_mode else 'Finam account trade_lot_size'} для {normalized_ticker} не подтверждён; "
                    "резервирование нового ордера заблокировано."
                )
            notional_per_lot = price * lot_size
            if notional_per_lot <= 0 or not math.isfinite(notional_per_lot):
                return 0, "Некорректная стоимость лота."
            stop_distance = price - stop_loss if side == "BUY" else stop_loss - price
            if not math.isfinite(stop_distance) or stop_distance <= 0:
                return 0, "Некорректный стоп-уровень: риск нового ордера не рассчитан."
            risk_per_lot = self._stop_risk_with_costs(
                price, stop_loss, lot_size, side
            )
            if not math.isfinite(risk_per_lot) or risk_per_lot <= 0.0:
                return 0, "Не удалось достоверно рассчитать риск лота с учётом издержек."

            already_reserved = sum(self._reserved_notional.values())
            already_reserved_risk = sum(self._reserved_stop_risk.values())
            gross_cap = self.initial_capital * self.max_gross_exposure_pct
            single_cap = self.initial_capital * self.max_single_position_pct
            stop_risk_cap = self.initial_capital * self.max_open_stop_risk_pct
            headroom = max(0.0, min(gross_cap - open_notional - already_reserved, single_cap))
            risk_headroom = max(0.0, stop_risk_cap - open_stop_risk - already_reserved_risk)
            affordable_lots = int(math.floor((headroom + 1e-9) / notional_per_lot))
            risk_lots = int(math.floor((risk_headroom + 1e-9) / risk_per_lot))
            approved_lots = min(int(requested_lots), affordable_lots, risk_lots)
            if approved_lots <= 0:
                if self.paper_mode:
                    try:
                        retry_cooldown_sec = float(
                            os.getenv("PILUITUS_PAPER_CAPITAL_REJECT_COOLDOWN_SEC", "10")
                        )
                    except (TypeError, ValueError, OverflowError):
                        retry_cooldown_sec = 10.0
                    if not math.isfinite(retry_cooldown_sec):
                        retry_cooldown_sec = 10.0
                    retry_cooldown_sec = min(120.0, max(0.0, retry_cooldown_sec))
                    if retry_cooldown_sec > 0.0:
                        capital_cooldowns[cooldown_key] = now + retry_cooldown_sec
                return 0, (
                    f"Лимиты портфеля: капитал доступен {headroom:.2f} ₽ "
                    f"(экспозиция {open_notional:.2f}/{gross_cap:.2f} ₽), "
                    f"риск до стопов доступен {risk_headroom:.2f} ₽ "
                    f"(открытый риск {open_stop_risk:.2f}/{stop_risk_cap:.2f} ₽), "
                    f"лот {notional_per_lot:.2f} ₽, риск лота {risk_per_lot:.2f} ₽."
                )

            notional_reserved = approved_lots * notional_per_lot
            risk_reserved = approved_lots * risk_per_lot
            self._opening_tickers.add(ticker)
            self._reserved_notional[ticker] = notional_reserved
            self._reserved_stop_risk[ticker] = risk_reserved
            return approved_lots, (
                f"notional={notional_reserved:.2f} ₽; gross_before={open_notional:.2f} ₽; "
                f"gross_cap={gross_cap:.2f} ₽; single_cap={single_cap:.2f} ₽; "
                f"open_stop_risk={open_stop_risk:.2f}/{stop_risk_cap:.2f} ₽; "
                f"new_stop_risk={risk_reserved:.2f} ₽"
            )

    # ------------------------------------------------------------------
    # 2️⃣  ATR‑стоп (Синхронизирован с динамическим кэшем LOT_SIZES)
    # ------------------------------------------------------------------

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
        """Build SL/TP from measured M1 true-range ATR, spread and a safety floor.

        ATR is read from completed one-minute OHLC bars (not a short tick-price
        window). We require a small warm-up sample, use up to ATR(14), preserve the
        configured minimum noise buffer, and cap the maximum stop width.
        """
        side = str(side).upper()
        try:
            price = float(current_price)
        except (TypeError, ValueError):
            return 0.0, 0.0
        if not math.isfinite(price) or price <= 0.0 or side not in {"BUY", "SELL"}:
            return 0.0, 0.0

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
            return 0.0, 0.0

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

        # Round protective levels to the actual Finam price tick. Directional
        # rounding never moves a stop closer to the entry price.
        metadata = ASSET_METADATA.get(str(ticker or "").strip().upper(), {})
        tick = _finite_or_default(metadata.get("price_tick"), 0.0) if isinstance(metadata, Mapping) else 0.0
        decimals = int(_finite_or_default(metadata.get("decimals"), 4)) if isinstance(metadata, Mapping) else 4
        if tick > 0.0 and math.isfinite(tick):
            if side == "BUY":
                sl = math.floor((sl / tick) + 1e-10) * tick
                tp = math.ceil((tp / tick) - 1e-10) * tick
            else:
                sl = math.ceil((sl / tick) - 1e-10) * tick
                tp = math.floor((tp / tick) + 1e-10) * tick
            return round(sl, max(0, min(8, decimals))), round(tp, max(0, min(8, decimals)))
        return round(sl, 4), round(tp, 4)
