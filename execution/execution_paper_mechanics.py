"""PAPER PnL, slippage, stop cooldown and depth-fill mechanics mixin."""
from __future__ import annotations

from execution.execution_shared import *  # noqa: F401,F403


class ExecutionPaperMechanicsMixin:
    @staticmethod
    def _lot_multiplier_for(ticker: str) -> int:
        """Return the confirmed account-specific share multiplier; 0 means unavailable."""
        symbol = str(ticker).strip().upper().split("@")[0]
        try:
            return max(int(FINAM_TRADE_LOT_SIZES.get(symbol, 0) or 0), 0)
        except (TypeError, ValueError, OverflowError):
            return 0

    @staticmethod
    def _calculate_paper_pnl(
        entry_price: float, exit_price: float, share_qty: int, side: str,
        commission_pct_per_side: float, entry_commission: float = 0.0,
    ) -> tuple[float, float, float, float]:
        """Return gross PnL, exit fee, total fees and net PnL after both sides."""
        values = (entry_price, exit_price, commission_pct_per_side, entry_commission)
        if side not in {"BUY", "SELL"} or share_qty <= 0:
            raise ValueError("Для PAPER PnL нужны BUY/SELL и положительное количество акций.")
        if not all(math.isfinite(float(value)) for value in values):
            raise ValueError("Параметры PAPER PnL должны быть конечными числами.")
        if entry_price <= 0.0 or exit_price <= 0.0 or commission_pct_per_side < 0.0 or entry_commission < 0.0:
            raise ValueError("Цена должна быть положительной, комиссия — неотрицательной.")
        sign = 1 if side == "BUY" else -1
        gross_pnl = (exit_price - entry_price) * share_qty * sign
        exit_commission = exit_price * share_qty * commission_pct_per_side / 100.0
        total_commission = entry_commission + exit_commission
        return gross_pnl, exit_commission, total_commission, gross_pnl - total_commission

    def _apply_paper_slippage(self, price: float, transaction_side: str) -> float:
        """Apply optional adverse slippage after selecting the executable BBO side."""
        if not self.paper_mode or self.paper_slippage_bps <= 0.0:
            return price
        factor = self.paper_slippage_bps / 10_000.0
        adjusted = price * (1.0 + factor if transaction_side == "BUY" else 1.0 - factor)
        return adjusted if math.isfinite(adjusted) and adjusted > 0.0 else price

    @staticmethod
    def _calculate_shadow_be_raw_trigger_price(
        entry_price: float,
        side: str,
        share_qty: int,
        entry_commission: float,
        commission_pct_per_side: float,
        slippage_bps: float,
    ) -> float:
        """Raw executable BBO threshold whose simulated fill is net break-even."""
        if entry_price <= 0.0 or share_qty <= 0 or side not in {"BUY", "SELL"}:
            return 0.0
        fee_rate = max(0.0, commission_pct_per_side) / 100.0
        slip_rate = max(0.0, slippage_bps) / 10_000.0
        entry_fee_per_share = max(0.0, entry_commission) / share_qty
        if side == "BUY":
            if fee_rate >= 1.0 or slip_rate >= 1.0:
                return 0.0
            required_fill = (entry_price + entry_fee_per_share) / (1.0 - fee_rate)
            raw_bbo = required_fill / (1.0 - slip_rate)
        else:
            required_fill = (entry_price - entry_fee_per_share) / (1.0 + fee_rate)
            raw_bbo = required_fill / (1.0 + slip_rate)
        return raw_bbo if math.isfinite(raw_bbo) and raw_bbo > 0.0 else 0.0

    async def _hydrate_stop_loss_cooldown(
        self,
        db: DBManager,
        ticker: str,
        side: str,
        *,
        now_monotonic: float | None = None,
        now_wall: datetime | None = None,
    ) -> tuple[int, float]:
        """Restore same-session stop history from the persisted closed-trade journal.

        In-memory cooldowns alone disappear on process restart. Hydration is
        fail-closed: if the journal cannot be read, the caller must not open a
        PAPER position while the recent-stop state is unknown.
        """
        symbol = str(ticker or "").strip().upper().split("@")[0]
        direction = str(side or "").strip().upper()
        if not symbol or direction not in {"BUY", "SELL"}:
            raise ValueError("Некорректный тикер/направление для восстановления STOP_LOSS cooldown.")
        key = (symbol, direction)
        if key in self._hydrated_stop_loss_keys:
            remaining = max(0.0, self._ticker_cooldowns.get(key, 0.0) - (
                time.monotonic() if now_monotonic is None else float(now_monotonic)
            ))
            return len(self._recent_stop_losses.get(key, ())), remaining

        getter = getattr(db, "get_all_daily_trades", None)
        if not callable(getter):
            raise RuntimeError("DBManager не предоставляет полный журнал для восстановления STOP_LOSS cooldown.")
        rows = await getter()
        if not isinstance(rows, Mapping):
            raise RuntimeError("Полный журнал сделок имеет некорректный формат; STOP_LOSS cooldown не восстановлен.")

        moscow_tz = timezone(timedelta(hours=3), name="MSK")
        session_now = (now_wall or datetime.now(moscow_tz)).astimezone(moscow_tz).replace(tzinfo=None)
        now_mono = time.monotonic() if now_monotonic is None else float(now_monotonic)
        recent_ages: list[float] = []
        expected_mode = "PAPER" if self.paper_mode else "LIVE"
        for order_id, trade in rows.items():
            if not isinstance(trade, Mapping):
                continue
            if str(trade.get("status", "")).strip().upper() != "CLOSED":
                continue
            mode = str(trade.get("mode", "")).strip().upper()
            if mode and mode != expected_mode:
                continue
            trade_ticker = str(trade.get("ticker") or "").strip().upper().split("@")[0]
            trade_side = str(trade.get("side") or "").strip().upper()
            reason = str(trade.get("exit_reason") or trade.get("close_reason") or "").strip().upper()
            if trade_ticker != symbol or trade_side != direction or reason != "STOP_LOSS":
                continue

            raw_timestamp = (
                trade.get("exit_timestamp") or trade.get("closed_at")
                or trade.get("timestamp") or trade.get("exit_time")
            )
            if not raw_timestamp:
                raise RuntimeError(
                    f"STOP_LOSS {order_id} по {symbol}/{direction} не содержит времени закрытия; "
                    "повторный вход заблокирован до проверки журнала."
                )
            try:
                parsed = datetime.fromisoformat(str(raw_timestamp).replace("Z", "+00:00"))
                if parsed.tzinfo is not None:
                    parsed = parsed.astimezone(moscow_tz).replace(tzinfo=None)
                # Match RiskManager: a stop from the previous Moscow trading
                # date does not carry its cooldown into the next session.
                if parsed.date() != session_now.date():
                    continue
                age = (session_now - parsed).total_seconds()
            except (TypeError, ValueError, OverflowError) as exc:
                raise RuntimeError(
                    f"Некорректное время STOP_LOSS {order_id} по {symbol}/{direction}; "
                    "повторный вход заблокирован до проверки журнала."
                ) from exc
            if age < -2.0:
                raise RuntimeError(
                    f"Время STOP_LOSS {order_id} по {symbol}/{direction} находится в будущем; "
                    "повторный вход заблокирован."
                )
            if 0.0 <= age <= self.repeat_stop_window_sec:
                recent_ages.append(max(0.0, age))

        # Merge in-memory stop events with the journal. A close can register its
        # cooldown just before its Redis write completes; replacing local history
        # with an immediately-read journal snapshot would otherwise erase it.
        history_times: list[float] = []
        for raw_monotonic in self._recent_stop_losses.get(key, ()):
            try:
                stamp = float(raw_monotonic)
            except (TypeError, ValueError, OverflowError):
                continue
            if math.isfinite(stamp) and now_mono - self.repeat_stop_window_sec <= stamp <= now_mono + 0.5:
                history_times.append(stamp)
        for age in recent_ages:
            stamp = now_mono - age
            if not any(abs(stamp - existing) <= 0.25 for existing in history_times):
                history_times.append(stamp)
        history_times.sort()
        history = deque(history_times)
        self._recent_stop_losses[key] = history
        cooldown = 0.0
        remaining = 0.0
        if history:
            recent_count = len(history)
            cooldown = (
                self.repeat_stop_cooldown_sec
                if recent_count >= self.repeat_stop_limit
                else self.stop_loss_cooldown_sec
            )
            latest_stop_monotonic = history[-1]
            until = max(
                latest_stop_monotonic + cooldown,
                float(self._ticker_cooldowns.get(key, 0.0) or 0.0),
            )
            if until > now_mono:
                self._ticker_cooldowns[key] = until
                remaining = until - now_mono
            else:
                self._ticker_cooldowns.pop(key, None)
        else:
            recent_count = 0
            self._ticker_cooldowns.pop(key, None)
        self._hydrated_stop_loss_keys.add(key)
        logger.info(
            "[STOP COOLDOWN HYDRATION] ticker=%s side=%s mode=%s recent_stops=%d window=%.0fs "
            "cooldown=%.0fs remaining=%.1fs source=closed_trade_journal+memory",
            symbol, direction, expected_mode, recent_count,
            self.repeat_stop_window_sec, cooldown, remaining,
        )
        return recent_count, remaining

    def _register_stop_loss_cooldown(
        self, ticker: str, side: str, *, now: float | None = None
    ) -> tuple[float, int]:
        """Apply a short first-stop pause and a longer pause after repeated same-side stops."""
        current = time.monotonic() if now is None else float(now)
        key = (str(ticker).strip().upper().split("@")[0], str(side).strip().upper())
        history = self._recent_stop_losses.setdefault(key, deque())
        cutoff = current - self.repeat_stop_window_sec
        while history and history[0] < cutoff:
            history.popleft()
        history.append(current)
        repeated = len(history) >= self.repeat_stop_limit
        cooldown = self.repeat_stop_cooldown_sec if repeated else self.stop_loss_cooldown_sec
        if cooldown > 0.0:
            self._ticker_cooldowns[key] = current + cooldown
        return cooldown, len(history)

    @staticmethod
    def _simulate_paper_depth_fill(
        levels: object,
        requested_lots: int,
        lot_size: int,
        size_unit: str,
        max_participation_pct: float,
    ) -> dict[str, Any] | None:
        """Simulate a conservative top-to-deeper-book fill when size units are known.

        Finam's published OrderBook schema does not document whether row sizes are
        shares or lots. Callers must therefore opt in with an explicit unit.
        """
        unit = str(size_unit or "").strip().lower()
        if unit not in {"shares", "lots"}:
            return None
        requested = max(0, int(requested_lots))
        lot = max(1, int(lot_size))
        participation = min(100.0, max(1.0, _finite_or_default(max_participation_pct, 100.0)))
        if requested <= 0 or not isinstance(levels, (list, tuple)):
            return {"filled_lots": 0, "filled_shares": 0, "requested_shares": requested * lot,
                    "average_price": 0.0, "levels_used": 0, "fill_ratio": 0.0}

        remaining_shares = requested * lot
        filled_shares = 0
        filled_notional = 0.0
        levels_used = 0
        for level in levels:
            if isinstance(level, Mapping):
                raw_price = level.get("price")
                raw_size = level.get("size", level.get("quantity"))
            elif isinstance(level, (list, tuple)) and len(level) >= 2:
                raw_price, raw_size = level[0], level[1]
            else:
                continue
            level_price = _finite_or_default(raw_price, 0.0)
            level_size = _finite_or_default(raw_size, 0.0)
            if level_price <= 0.0 or level_size <= 0.0:
                continue
            available_shares = level_size * lot if unit == "lots" else level_size
            participation_shares = int(math.floor(available_shares * participation / 100.0))
            fillable_lots = participation_shares // lot
            level_fill_shares = min(remaining_shares, fillable_lots * lot)
            if level_fill_shares <= 0:
                continue
            filled_shares += level_fill_shares
            filled_notional += level_fill_shares * level_price
            remaining_shares -= level_fill_shares
            levels_used += 1
            if remaining_shares <= 0:
                break

        average_price = filled_notional / filled_shares if filled_shares > 0 else 0.0
        return {
            "filled_lots": filled_shares // lot,
            "filled_shares": filled_shares,
            "requested_shares": requested * lot,
            "average_price": average_price,
            "levels_used": levels_used,
            "fill_ratio": filled_shares / (requested * lot) if requested > 0 else 0.0,
        }
