"""PAPER partial/full close execution and accounting mixin."""
from __future__ import annotations

from execution.paper_exit_shared import (
    asyncio, logging, math, os, time, Mapping, datetime, timedelta, timezone, Any, cast,
    LOT_SIZES, DBManager, PAPER_CLOSED_TRADES_STORAGE_KEY, PAPER_CLOSED_TRADES_STORAGE_LIMIT,
    paper_performance_tracker, build_exit_execution_diagnostics,
    BBOExitQuote, TradeRecord, _as_string_mapping, _finite_or_default, logger,
)

class PaperExitFinalizationMixin:
    def _paper_exit_depth_fill(
        self,
        ticker: str,
        trade: TradeRecord,
        bbo: BBOExitQuote,
    ) -> dict[str, Any] | None:
        """Return a depth fill plan only when the configured Finam size unit is explicit."""
        if self.paper_book_size_unit not in {"shares", "lots"}:
            return None
        book_age = _finite_or_default(bbo.get("order_book_age_sec"), math.inf)
        book_max_age = _finite_or_default(bbo.get("max_age_sec"), 2.0)
        if (
            not bool(bbo.get("order_book_valid", False))
            or not math.isfinite(book_age) or book_age < 0.0 or book_age > book_max_age
        ):
            return None
        side = str(trade.get("side", "BUY")).upper()
        if side not in {"BUY", "SELL"}:
            return None
        lot_size = max(1, int(_finite_or_default(
            trade.get("lot_size", LOT_SIZES.get(ticker, 1)), 1.0
        )))
        share_qty = self._share_quantity(ticker, trade)
        if share_qty <= 0 or share_qty % lot_size:
            return None
        requested_lots = share_qty // lot_size
        # A long is liquidated against bids; a short is covered against asks.
        levels_key = "bid_levels" if side == "BUY" else "ask_levels"
        return self._simulate_paper_depth_fill(
            bbo.get(levels_key, []),
            requested_lots,
            lot_size,
            self.paper_book_size_unit,
            self.paper_max_book_participation_pct,
        )

    async def _finalize_paper_partial_close(
        self,
        ticker: str,
        trade: TradeRecord,
        db: DBManager,
        bbo: BBOExitQuote,
        depth_fill: Mapping[str, Any],
        *,
        exit_reason: str,
        trigger_price: float,
        observed_last_price: float,
    ) -> TradeRecord:
        """Persist a confirmed partial exit while keeping the residual position OPEN."""
        order_id = str(trade.get("id", ""))
        side = str(trade.get("side", "BUY")).upper()
        entry_price = _finite_or_default(trade.get("entry_price"), 0.0)
        current_shares = self._share_quantity(ticker, trade)
        filled_shares = int(_finite_or_default(depth_fill.get("filled_shares"), 0.0))
        lot_size = max(1, int(_finite_or_default(
            trade.get("lot_size", LOT_SIZES.get(ticker, 1)), 1.0
        )))
        if (
            not order_id or side not in {"BUY", "SELL"} or entry_price <= 0.0
            or filled_shares <= 0 or filled_shares >= current_shares
            or filled_shares % lot_size
        ):
            raise ValueError("Некорректные параметры частичного PAPER-закрытия.")

        raw_exit_price = _finite_or_default(depth_fill.get("average_price"), 0.0)
        if raw_exit_price <= 0.0:
            raise ValueError("PAPER depth fill returned an invalid partial exit price.")
        exit_transaction_side = "SELL" if side == "BUY" else "BUY"
        exit_price = self._apply_paper_slippage(raw_exit_price, exit_transaction_side)
        commission_pct = _finite_or_default(
            trade.get("commission_pct_per_side"), self.paper_commission_pct
        )
        entry_fee_remaining = _finite_or_default(trade.get("entry_commission_rub"), 0.0)
        entry_fee_allocated = entry_fee_remaining * filled_shares / current_shares
        gross_pnl, exit_fee, _partial_total_fee, net_pnl = self._calculate_paper_pnl(
            entry_price, exit_price, filled_shares, side, commission_pct, entry_fee_allocated
        )
        exit_diagnostics = build_exit_execution_diagnostics(
            position_side=side,
            quantity=filled_shares,
            fill_price=exit_price,
            executable_bbo_price=bbo.get("executable_price"),
            bid=bbo.get("bid"),
            ask=bbo.get("ask"),
            bid_age_sec=bbo.get("bid_age_sec"),
            ask_age_sec=bbo.get("ask_age_sec"),
            order_book_age_sec=bbo.get("order_book_age_sec"),
            depth_vwap=raw_exit_price,
            depth_fill_ratio=depth_fill.get("fill_ratio"),
            depth_levels_used=depth_fill.get("levels_used"),
        )
        sign = 1 if side == "BUY" else -1
        now = datetime.now()
        fill_record = {
            "timestamp": now.isoformat(timespec="milliseconds"),
            "quantity_shares": filled_shares,
            "quantity_lots": filled_shares // lot_size,
            "raw_average_price": raw_exit_price,
            "fill_price_after_slippage": exit_price,
            "gross_pnl_rub": round(gross_pnl, 4),
            "allocated_entry_commission_rub": round(entry_fee_allocated, 4),
            "exit_commission_rub": round(exit_fee, 4),
            "net_pnl_rub": round(net_pnl, 4),
            "exit_reason": exit_reason,
            "trigger_price": trigger_price if trigger_price > 0.0 else None,
            "depth_levels_used": int(depth_fill.get("levels_used", 0)),
            "depth_fill_ratio": round(_finite_or_default(depth_fill.get("fill_ratio"), 0.0), 6),
            "depth_unit": self.paper_book_size_unit,
            "execution_diagnostics": exit_diagnostics,
        }
        updated = dict(trade)
        previous_fills = updated.get("exit_partial_fills", [])
        fills = list(previous_fills) if isinstance(previous_fills, list) else []
        fills.append(fill_record)
        updated["original_volume"] = int(_finite_or_default(
            updated.get("original_volume"), current_shares
        ))
        updated["original_lots"] = int(_finite_or_default(
            updated.get("original_lots"), updated.get("lots", current_shares // lot_size)
        ))
        updated["volume"] = current_shares - filled_shares
        updated["lots"] = updated["volume"] // lot_size
        updated["entry_commission_rub"] = max(0.0, entry_fee_remaining - entry_fee_allocated)
        updated["realized_gross_pnl_rub"] = (
            _finite_or_default(updated.get("realized_gross_pnl_rub"), 0.0) + gross_pnl
        )
        updated["realized_entry_commission_rub"] = (
            _finite_or_default(updated.get("realized_entry_commission_rub"), 0.0) + entry_fee_allocated
        )
        updated["realized_exit_commission_rub"] = (
            _finite_or_default(updated.get("realized_exit_commission_rub"), 0.0) + exit_fee
        )
        updated["realized_net_pnl_rub"] = (
            _finite_or_default(updated.get("realized_net_pnl_rub"), 0.0) + net_pnl
        )
        updated["realized_exit_slippage_rub"] = (
            _finite_or_default(updated.get("realized_exit_slippage_rub"), 0.0)
            + (exit_price - raw_exit_price) * filled_shares * sign
        )
        updated["realized_exit_execution_shortfall_rub"] = (
            _finite_or_default(updated.get("realized_exit_execution_shortfall_rub"), 0.0)
            + _finite_or_default(exit_diagnostics.get("exit_execution_shortfall_rub"), 0.0)
        )
        updated["realized_exit_reference_notional_rub"] = (
            _finite_or_default(updated.get("realized_exit_reference_notional_rub"), 0.0)
            + _finite_or_default(bbo.get("executable_price"), 0.0) * filled_shares
        )
        if exit_diagnostics.get("exit_depth_impact_rub") is not None:
            updated["realized_exit_depth_impact_rub"] = (
                _finite_or_default(updated.get("realized_exit_depth_impact_rub"), 0.0)
                + _finite_or_default(exit_diagnostics.get("exit_depth_impact_rub"), 0.0)
            )
            updated["realized_exit_depth_observation_count"] = (
                int(_finite_or_default(updated.get("realized_exit_depth_observation_count"), 0.0)) + 1
            )
        updated["realized_exit_notional_rub"] = (
            _finite_or_default(updated.get("realized_exit_notional_rub"), 0.0)
            + exit_price * filled_shares
        )
        updated["realized_exit_quantity_shares"] = (
            int(_finite_or_default(updated.get("realized_exit_quantity_shares"), 0.0))
            + filled_shares
        )
        updated["realized_market_move_after_trigger_rub"] = (
            _finite_or_default(updated.get("realized_market_move_after_trigger_rub"), 0.0)
            + ((raw_exit_price - trigger_price) * filled_shares * sign if trigger_price > 0.0 else 0.0)
        )
        updated["exit_partial_fills"] = fills[-100:]
        updated["exit_partial_fill_count"] = (
            int(_finite_or_default(updated.get("exit_partial_fill_count"), 0.0)) + 1
        )
        updated["exit_partial_fill_enabled"] = True
        updated["exit_fill_model"] = "DEPTH_PARTIAL_FILL"
        updated["exit_depth_unit"] = self.paper_book_size_unit
        updated["exit_depth_levels_used_last"] = int(depth_fill.get("levels_used", 0))
        updated["exit_depth_fill_ratio_last"] = round(
            _finite_or_default(depth_fill.get("fill_ratio"), 0.0), 6
        )
        updated["last_partial_exit_at"] = fill_record["timestamp"]
        updated["last_partial_exit_reason"] = exit_reason
        remaining_shares = int(updated["volume"])
        remaining_entry_fee = _finite_or_default(updated.get("entry_commission_rub"), 0.0)
        mark_raw_price = _finite_or_default(bbo.get("executable_price"), raw_exit_price)
        mark_exit_price = self._apply_paper_slippage(mark_raw_price, exit_transaction_side)
        mark_gross, mark_exit_fee, _mark_total_fee, mark_net = self._calculate_paper_pnl(
            entry_price, mark_exit_price, remaining_shares, side,
            commission_pct, remaining_entry_fee,
        )
        realized_net = _finite_or_default(updated.get("realized_net_pnl_rub"), 0.0)
        realized_gross = _finite_or_default(updated.get("realized_gross_pnl_rub"), 0.0)
        realized_exit_fee = _finite_or_default(updated.get("realized_exit_commission_rub"), 0.0)
        entry_notional = _finite_or_default(
            updated.get("notional_entry_rub"), entry_price * current_shares
        )
        updated["current_price"] = mark_raw_price
        updated["estimated_exit_price"] = mark_exit_price
        updated["pnl"] = round(realized_net + mark_net, 2)
        updated["gross_pnl"] = round(realized_gross + mark_gross, 2)
        updated["pnl_percent"] = (
            round(updated["pnl"] / entry_notional * 100.0, 4)
            if entry_notional > 0.0 else 0.0
        )
        updated["current_pnl_source"] = "PARTIAL_EXIT_PLUS_OPEN_BBO_ESTIMATE"
        updated["estimated_exit_commission_rub"] = round(realized_exit_fee + mark_exit_fee, 4)

        # Persist the reduced OPEN quantity before mutating the in-memory position.
        # If storage fails, the original quantity remains authoritative and no fill is booked.
        await db.set_json(f"piluitus:trade:{order_id}", updated)
        trade.clear()
        trade.update(updated)
        logger.warning(
            "[PAPER PARTIAL EXIT] %s %s reason=%s filled=%d shares (%d lots), remaining=%d shares; "
            "gross=%+.2f₽ net=%+.2f₽ avg=%.6f unit=%s",
            ticker, side, exit_reason, filled_shares, filled_shares // lot_size,
            updated["volume"], gross_pnl, net_pnl, exit_price, self.paper_book_size_unit,
        )
        return trade

    async def _finalize_paper_close(
        self,
        ticker: str,
        trade: TradeRecord,
        db: DBManager,
        bbo: BBOExitQuote,
        *,
        exit_reason: str,
        trigger_price: float = 0.0,
        observed_last_price: float = 0.0,
    ) -> TradeRecord:
        """Persist one PAPER close using the executable BBO and configured costs."""
        order_id = str(trade.get("id", ""))
        side = str(trade.get("side", "BUY")).upper()
        if not order_id or side not in {"BUY", "SELL"}:
            raise ValueError("Некорректный идентификатор или направление PAPER-позиции.")
        entry_price = _finite_or_default(trade.get("entry_price"), 0.0)
        share_qty = self._share_quantity(ticker, trade)
        if entry_price <= 0.0 or share_qty <= 0:
            raise ValueError("Некорректная цена входа или количество акций PAPER-позиции.")

        raw_exit_price = _finite_or_default(bbo.get("executable_price"), 0.0)
        raw_bbo_exit_price = raw_exit_price
        if raw_exit_price <= 0.0:
            raise ValueError("В BBO отсутствует положительная исполнимая цена выхода.")
        depth_fill = self._paper_exit_depth_fill(ticker, trade, bbo)
        depth_confirmed = self.paper_book_size_unit in {"shares", "lots"}
        if depth_confirmed:
            lot_size = max(1, int(_finite_or_default(
                trade.get("lot_size", LOT_SIZES.get(ticker, 1)), 1.0
            )))
            requested_lots = share_qty // lot_size
            filled_lots = int(depth_fill.get("filled_lots", 0)) if isinstance(depth_fill, Mapping) else 0
            if depth_fill is None or filled_lots <= 0:
                logger.warning(
                    "[PAPER EXIT DEPTH WAIT] %s %s reason=DEPTH_UNAVAILABLE requested=%d lots unit=%s; "
                    "позиция остаётся OPEN, SL/TP trigger сохранён.",
                    ticker, side, requested_lots, self.paper_book_size_unit,
                )
                trade["exit_depth_last_attempt_at"] = datetime.now().astimezone().isoformat(timespec="milliseconds")
                trade["exit_depth_last_fill_ratio"] = 0.0
                trade["exit_depth_last_levels_used"] = 0
                return trade
            if filled_lots < requested_lots:
                return await self._finalize_paper_partial_close(
                    ticker, trade, db, bbo, depth_fill,
                    exit_reason=exit_reason,
                    trigger_price=trigger_price,
                    observed_last_price=observed_last_price,
                )
            raw_exit_price = _finite_or_default(depth_fill.get("average_price"), raw_exit_price)
            if raw_exit_price <= 0.0:
                raise ValueError("PAPER depth fill returned an invalid average exit price.")
        exit_transaction_side = "SELL" if side == "BUY" else "BUY"
        exit_price = self._apply_paper_slippage(raw_exit_price, exit_transaction_side)
        entry_commission = _finite_or_default(trade.get("entry_commission_rub"), 0.0)
        commission_pct = _finite_or_default(
            trade.get("commission_pct_per_side"), self.paper_commission_pct
        )
        gross_pnl, exit_commission, total_commission, net_pnl = self._calculate_paper_pnl(
            entry_price, exit_price, share_qty, side, commission_pct, entry_commission
        )
        prior_realized_gross = _finite_or_default(trade.get("realized_gross_pnl_rub"), 0.0)
        prior_realized_entry_fee = _finite_or_default(trade.get("realized_entry_commission_rub"), 0.0)
        prior_realized_exit_fee = _finite_or_default(trade.get("realized_exit_commission_rub"), 0.0)
        prior_realized_net = _finite_or_default(trade.get("realized_net_pnl_rub"), 0.0)
        gross_pnl += prior_realized_gross
        exit_commission += prior_realized_exit_fee
        total_commission += prior_realized_entry_fee + prior_realized_exit_fee
        net_pnl += prior_realized_net
        entry_commission += prior_realized_entry_fee
        entry_notional = _finite_or_default(
            trade.get("notional_entry_rub"), entry_price * share_qty
        )
        net_pct = (net_pnl / entry_notional * 100.0) if entry_notional > 0.0 else 0.0

        held_seconds: float | None = None
        try:
            entry_dt = datetime.fromisoformat(str(trade.get("entry_timestamp") or ""))
            now_dt = datetime.now(entry_dt.tzinfo) if entry_dt.tzinfo else datetime.now()
            held_seconds = max(0.0, (now_dt - entry_dt).total_seconds())
        except (TypeError, ValueError, OverflowError):
            held_seconds = None

        exit_size = _finite_or_default(
            bbo.get("bid_size" if side == "BUY" else "ask_size"), 0.0
        )
        now = datetime.now()
        trigger_monotonic = _finite_or_default(trade.get("exit_trigger_monotonic"), 0.0)
        trigger_to_bbo_delay = (
            max(0.0, time.monotonic() - trigger_monotonic)
            if trigger_monotonic > 0.0 else None
        )
        trigger_source = str(trade.get("exit_trigger_source") or "EXECUTABLE_BBO").upper()
        market_move_after_trigger_rub = (
            (raw_exit_price - trigger_price) * share_qty * (1 if side == "BUY" else -1)
            if trigger_price > 0.0 else None
        )
        configured_slippage_rub = (
            (exit_price - raw_exit_price) * share_qty * (1 if side == "BUY" else -1)
        )
        exit_diagnostics = build_exit_execution_diagnostics(
            position_side=side,
            quantity=share_qty,
            fill_price=exit_price,
            executable_bbo_price=bbo.get("executable_price"),
            bid=bbo.get("bid"),
            ask=bbo.get("ask"),
            bid_age_sec=bbo.get("bid_age_sec"),
            ask_age_sec=bbo.get("ask_age_sec"),
            order_book_age_sec=bbo.get("order_book_age_sec"),
            depth_vwap=raw_exit_price if depth_confirmed else None,
            depth_fill_ratio=depth_fill.get("fill_ratio") if isinstance(depth_fill, Mapping) else None,
            depth_levels_used=depth_fill.get("levels_used") if isinstance(depth_fill, Mapping) else None,
        )
        prior_shortfall = _finite_or_default(
            trade.get("realized_exit_execution_shortfall_rub"), 0.0
        )
        current_shortfall = _finite_or_default(
            exit_diagnostics.get("exit_execution_shortfall_rub"), 0.0
        )
        prior_reference_notional = _finite_or_default(
            trade.get("realized_exit_reference_notional_rub"), 0.0
        )
        current_reference_notional = (
            _finite_or_default(bbo.get("executable_price"), 0.0) * share_qty
        )
        total_shortfall = prior_shortfall + current_shortfall
        total_reference_notional = prior_reference_notional + current_reference_notional
        prior_depth_impact = _finite_or_default(trade.get("realized_exit_depth_impact_rub"), 0.0)
        current_depth_impact = _finite_or_default(exit_diagnostics.get("exit_depth_impact_rub"), 0.0)
        closed_trade = dict(trade)
        prior_exit_quantity = int(_finite_or_default(
            trade.get("realized_exit_quantity_shares"), 0.0
        ))
        prior_exit_notional = _finite_or_default(trade.get("realized_exit_notional_rub"), 0.0)
        total_exit_quantity = prior_exit_quantity + share_qty
        weighted_exit_price = (
            (prior_exit_notional + exit_price * share_qty) / total_exit_quantity
            if total_exit_quantity > 0 else exit_price
        )
        closed_trade.update({
            "status": "CLOSED",
            "volume": int(_finite_or_default(trade.get("original_volume"), total_exit_quantity)),
            "lots": int(_finite_or_default(trade.get("original_lots"), trade.get("lots", 0))),
            "exit_price": round(weighted_exit_price, 6),
            "exit_time": now.strftime("%H:%M:%S"),
            "exit_timestamp": now.isoformat(timespec="milliseconds"),
            "exit_last_fill_price": exit_price,
            "current_price": round(weighted_exit_price, 6),
            "exit_raw_bbo_price": raw_bbo_exit_price,
            "exit_raw_fill_average_price": raw_exit_price,
            "trigger_price": trigger_price if trigger_price > 0.0 else None,
            "exit_trigger_source": trigger_source,
            "exit_trigger_timestamp": trade.get("exit_trigger_timestamp"),
            "exit_trigger_market_timestamp": trade.get("exit_trigger_market_timestamp"),
            "exit_trigger_observed_price": _finite_or_default(
                trade.get("exit_trigger_observed_price"), observed_last_price
            ),
            "trigger_to_executable_bbo_delay_sec": (
                round(trigger_to_bbo_delay, 3) if trigger_to_bbo_delay is not None else None
            ),
            "executable_bbo_confirmed_at": now.astimezone().isoformat(timespec="milliseconds"),
            "market_move_after_trigger_rub": (
                round(
                    _finite_or_default(trade.get("realized_market_move_after_trigger_rub"), 0.0)
                    + market_move_after_trigger_rub, 4
                )
                if market_move_after_trigger_rub is not None else None
            ),
            "configured_exit_slippage_rub": round(
                _finite_or_default(trade.get("realized_exit_slippage_rub"), 0.0)
                + configured_slippage_rub, 4
            ),
            "exit_execution_diagnostics": exit_diagnostics,
            "exit_execution_shortfall_rub": round(total_shortfall, 6),
            "exit_execution_shortfall_bps": (
                round(total_shortfall / total_reference_notional * 10_000.0, 6)
                if total_reference_notional > 0.0 else None
            ),
            "exit_depth_impact_rub": (
                round(prior_depth_impact + current_depth_impact, 6)
                if (
                    int(_finite_or_default(trade.get("realized_exit_depth_observation_count"), 0.0))
                    + int(exit_diagnostics.get("exit_depth_impact_rub") is not None)
                ) > 0 else None
            ),
            "exit_depth_observation_count": (
                int(_finite_or_default(trade.get("realized_exit_depth_observation_count"), 0.0))
                + int(exit_diagnostics.get("exit_depth_impact_rub") is not None)
            ),
            "fill_vs_trigger_rub": (
                round(
                    _finite_or_default(trade.get("realized_market_move_after_trigger_rub"), 0.0)
                    + market_move_after_trigger_rub + configured_slippage_rub, 4
                )
                if market_move_after_trigger_rub is not None else None
            ),
            "observed_exit_price": observed_last_price,
            "exit_reason": exit_reason,
            "exit_price_source": (
                ("ORDER_BOOK_BID_DEPTH" if side == "BUY" else "ORDER_BOOK_ASK_DEPTH")
                if depth_confirmed else ("BBO_BID" if side == "BUY" else "BBO_ASK")
            ),
            "exit_fill_model": (
                "DEPTH_PARTIALS_PLUS_FINAL_DEPTH_FILL"
                if bool(trade.get("exit_partial_fills")) and depth_confirmed
                else "FULL_FILL_AT_CONFIRMED_DEPTH" if depth_confirmed
                else "FULL_FILL_AT_EXECUTABLE_BBO_DEPTH_UNVERIFIED"
            ),
            "exit_partial_fill_enabled": depth_confirmed,
            "exit_partial_fill_count": int(_finite_or_default(
                trade.get("exit_partial_fill_count"), 0.0
            )),
            "exit_depth_unit": self.paper_book_size_unit,
            "exit_depth_levels_used": (
                int(depth_fill.get("levels_used", 0)) if isinstance(depth_fill, Mapping) else None
            ),
            "exit_depth_fill_ratio": (
                round(_finite_or_default(depth_fill.get("fill_ratio"), 0.0), 6)
                if isinstance(depth_fill, Mapping) else None
            ),
            "exit_bbo_gap_count": int(trade.get("exit_bbo_gap_count", 0) or 0),
            "last_exit_bbo_gap_sec": _finite_or_default(trade.get("last_exit_bbo_gap_sec"), 0.0),
            "max_exit_bbo_gap_sec": _finite_or_default(trade.get("max_exit_bbo_gap_sec"), 0.0),
            "total_exit_bbo_gap_sec": _finite_or_default(trade.get("total_exit_bbo_gap_sec"), 0.0),
            "current_exit_bbo_gap_sec": _finite_or_default(trade.get("current_exit_bbo_gap_sec"), 0.0),
            "exit_bid": _finite_or_default(bbo.get("bid"), 0.0),
            "exit_ask": _finite_or_default(bbo.get("ask"), 0.0),
            "exit_bid_size": _finite_or_default(bbo.get("bid_size"), 0.0),
            "exit_ask_size": _finite_or_default(bbo.get("ask_size"), 0.0),
            "exit_top_size_raw": exit_size,
            "exit_top_size_unit": self.paper_book_size_unit if depth_confirmed else "FINAM_UNCONFIRMED",
            "exit_bbo_age_sec": round(_finite_or_default(bbo.get("age_sec"), 0.0), 3),
            "exit_spread_pct": round(_finite_or_default(bbo.get("spread_pct"), 0.0), 6),
            "exit_slippage_bps": self.paper_slippage_bps,
            "entry_commission_rub": round(entry_commission, 4),
            "exit_commission_rub": round(exit_commission, 4),
            "total_commission_rub": round(total_commission, 4),
            "gross_pnl": round(gross_pnl, 2),
            "pnl": round(net_pnl, 2),
            "pnl_percent": round(net_pct, 4),
            "fill_vs_trigger_pct": (
                round((weighted_exit_price - trigger_price) / trigger_price * 100.0 * (1 if side == "BUY" else -1), 6)
                if trigger_price > 0.0 else None
            ),
            "time_in_trade_sec": round(held_seconds, 3) if held_seconds is not None else None,
            "final_outcome": "SUCCESSFUL_EXPERIENCE" if exit_reason == "TAKE_PROFIT" else (
                "MANUAL_CLOSE" if exit_reason == "MANUAL_CLOSE" else "FAILED_MISTAKE"
            ),
        })

        if exit_reason == "STOP_LOSS":
            cooldown, recent_count = self._register_stop_loss_cooldown(ticker, side)
            closed_trade["same_side_stop_losses_in_window"] = recent_count
            closed_trade["same_side_stop_window_sec"] = self.repeat_stop_window_sec
            closed_trade["reentry_cooldown_sec"] = cooldown
            logger.warning(
                "🧊 [STOP COOLDOWN] %s %s: стопов=%d за %.0f сек.; повторный вход этой стороной "
                "заблокирован на %.0f сек.",
                ticker, side, recent_count, self.repeat_stop_window_sec, cooldown,
            )
        else:
            closed_trade["same_side_stop_losses_in_window"] = None
            closed_trade["reentry_cooldown_sec"] = 0.0
        closed_trade["shadow_be_outcome"] = (
            "TRIGGERED" if bool(trade.get("shadow_be_triggered"))
            else "ARMED_NOT_TRIGGERED" if bool(trade.get("shadow_be_armed"))
            else "NOT_ARMED"
        )

        await db.set_json(f"piluitus:trade:{order_id}", closed_trade)
        trade.update(closed_trade)

        # Keep a dedicated bounded realized-PAPER journal; the dashboard's
        # 100-row trade_history is intentionally not the analytics source.
        try:
            await db.push_to_list(
                PAPER_CLOSED_TRADES_STORAGE_KEY,
                closed_trade,
                max_len=PAPER_CLOSED_TRADES_STORAGE_LIMIT,
            )
        except Exception as journal_exc:
            logger.critical(
                "PAPER trade %s закрыта, но durable performance journal не записан: %s",
                order_id, journal_exc, exc_info=True,
            )
        paper_performance_tracker.record(closed_trade)

        try:
            await db.push_to_list("piluitus:trade_history", closed_trade, max_len=100)
        except Exception as history_exc:
            logger.error(
                "Позиция %s закрыта в основном журнале, но запись истории не удалась: %s",
                order_id, history_exc,
            )

        if self.paper_positions.get(ticker) is trade:
            self.paper_positions.pop(ticker, None)
        self.closed_trades.append(dict(closed_trade))
        self._last_bbo_exit_warning_at.pop(ticker, None)
        last_exit_logs = getattr(self, "_last_exit_status_log_at", None)
        if isinstance(last_exit_logs, dict):
            order_prefix = f"{order_id}:"
            for log_key in list(last_exit_logs):
                if str(log_key).startswith(order_prefix):
                    last_exit_logs.pop(log_key, None)
        logger.warning(
            "💾 [PAPER JOURNAL] Сделка %s (%s) закрыта. Gross=%+.2f₽, commission=%.2f₽, Net=%+.2f₽; "
            "qty=%d; entry=%.4f; exit=%.4f; source=%s; reason=%s; top_size_raw=%.2f (unit unconfirmed); "
            "fill_model=%s depth_unit=%s; BBO_gap=%d episodes total=%.1fs max=%.1fs; "
            "trigger_to_fresh_BBO=%.3fs; MFE=%.4f%%; MAE=%.4f%%; held=%.1fs",
            order_id, ticker, gross_pnl, total_commission, net_pnl, share_qty,
            entry_price, exit_price, closed_trade["exit_price_source"], exit_reason,
            exit_size, closed_trade.get("exit_fill_model", "UNKNOWN"),
            closed_trade.get("exit_top_size_unit", "UNKNOWN"),
            int(closed_trade.get("exit_bbo_gap_count", 0) or 0),
            _finite_or_default(closed_trade.get("total_exit_bbo_gap_sec"), 0.0),
            _finite_or_default(closed_trade.get("max_exit_bbo_gap_sec"), 0.0),
            _finite_or_default(closed_trade.get("trigger_to_executable_bbo_delay_sec"), -1.0),
            _finite_or_default(trade.get("max_favorable_excursion_pct"), 0.0),
            _finite_or_default(trade.get("max_adverse_excursion_pct"), 0.0),
            held_seconds if held_seconds is not None else -1.0,
        )

        try:
            from interface import tg_bot
            current_bot = getattr(tg_bot, "bot", None)
            chat_id = os.getenv("TELEGRAM_USER_ID")
            if current_bot and chat_id:
                message = (
                    f"🎯 **PAPER-ПОЗИЦИЯ ЗАКРЫТА:** {ticker} ({side})\n"
                    f"💰 Gross: `{gross_pnl:+.2f} ₽`\n"
                    f"Комиссия: `{total_commission:.2f} ₽`\n"
                    f"Чистый PnL: `{net_pnl:+.2f} ₽`\n"
                    f"📝 Причина: `{exit_reason}`\n"
                    f"📍 Исполнение: `{exit_price:.4f} ₽` ({closed_trade['exit_price_source']})"
                )
                async def _safe_send(bot, target, txt):
                    try:
                        async with asyncio.timeout(2.0):
                            await bot.send_message(chat_id=target, text=txt, parse_mode="Markdown")
                    except Exception:
                        pass
                asyncio.create_task(_safe_send(current_bot, chat_id, message))
        except Exception:
            pass
        return closed_trade
