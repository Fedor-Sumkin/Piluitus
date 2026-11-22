"""PAPER position exit monitoring, depth execution and close lifecycle."""
from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from config import LOT_SIZES
from storage.db_manager import DBManager
from strategy.paper_performance import (
    PAPER_CLOSED_TRADES_STORAGE_KEY,
    PAPER_CLOSED_TRADES_STORAGE_LIMIT,
    paper_performance_tracker,
)
from strategy.paper_exit_diagnostics import build_exit_execution_diagnostics
from execution.execution_primitives import (
    BBOExitQuote,
    TradeRecord,
    _as_string_mapping,
    _finite_or_default,
)


logger = logging.getLogger("Piluitus.ExecutionLayer")


class PaperPositionExitRuntimeMixin:
    """Own PAPER exit triggers, monitoring queues and durable close execution.

    Methods intentionally operate on the owning execution engine through its
    existing state and helper contracts. The mixin does not create or copy
    position state.
    """

    def _capture_paper_exit_trigger(
        self,
        ticker: str,
        trade: TradeRecord,
        observed_price: float,
        *,
        source: str,
        market_timestamp: object = None,
    ) -> str:
        """Latch the first SL/TP trigger independently from executable BBO availability."""
        existing = str(trade.get("pending_exit_reason") or "").upper()
        if existing in {"STOP_LOSS", "TAKE_PROFIT"}:
            return existing
        side = str(trade.get("side", "BUY")).upper()
        entry = _finite_or_default(trade.get("entry_price"), 0.0)
        if side not in {"BUY", "SELL"} or entry <= 0.0:
            return ""
        stop = _finite_or_default(trade.get("stop_loss"), entry * (0.995 if side == "BUY" else 1.005))
        target = _finite_or_default(trade.get("take_profit"), entry * (1.015 if side == "BUY" else 0.985))
        price = _finite_or_default(observed_price, 0.0)
        if price <= 0.0:
            return ""
        if side == "BUY":
            reason = "STOP_LOSS" if price <= stop else "TAKE_PROFIT" if price >= target else ""
            trigger_price = stop if reason == "STOP_LOSS" else target
        else:
            reason = "STOP_LOSS" if price >= stop else "TAKE_PROFIT" if price <= target else ""
            trigger_price = stop if reason == "STOP_LOSS" else target
        if not reason:
            return ""
        now = datetime.now().astimezone()
        trade.update({
            "pending_exit_reason": reason,
            "pending_exit_trigger_price": trigger_price,
            "exit_trigger_source": str(source or "UNKNOWN").upper(),
            "exit_trigger_observed_price": price,
            "exit_trigger_timestamp": now.isoformat(timespec="milliseconds"),
            "exit_trigger_monotonic": time.monotonic(),
            "exit_trigger_market_timestamp": str(market_timestamp) if market_timestamp not in (None, "") else None,
            "exit_trigger_wait_logged": False,
        })
        logger.warning(
            "🚨 [PAPER EXIT TRIGGER] %s %s: %s; source=%s observed=%.6f trigger=%.6f; "
            "закрытие ожидает свежую исполнимую сторону BBO.",
            ticker, side, reason, str(source or "UNKNOWN").upper(), price, trigger_price,
        )
        recorder = getattr(self, "paper_exit_event_recorder", None)
        if callable(recorder):
            try:
                executable_quote = self._latest_fresh_exit_quote(ticker, side)
                recorder(ticker, trade, executable_quote)
            except Exception as recorder_exc:
                logger.warning(
                    "[PAPER BBO REPLAY] failed to record exit trigger for %s: %s",
                    ticker, recorder_exc,
                )
        return reason

    # ------------------------------------------------------------------
    # 3️⃣  EXECUTE_ORDER (Единая точка входа исполнения)
    # ------------------------------------------------------------------

    def schedule_paper_position_monitor(
        self,
        ticker: str,
        current_price: float | None,
        db: DBManager,
        *,
        event_source: str = "LAST_TRADE",
        event_timestamp: object = None,
    ) -> None:
        """Coalesce market events while latching last-trade SL/TP triggers immediately."""
        if not self.paper_mode or self._maintenance_active or ticker not in self.paper_positions:
            return
        try:
            price = float(current_price) if current_price is not None else 0.0
        except (TypeError, ValueError, OverflowError):
            price = 0.0
        if not math.isfinite(price) or price <= 0.0:
            # ORDER_BOOK may recover a fresh executable BBO without a new last trade.
            # Use the position's persisted mark/entry only for monitor bookkeeping;
            # SL/TP decisions still require _latest_fresh_exit_quote().
            price = self._latest_price(ticker, self.paper_positions.get(ticker))
        if not math.isfinite(price) or price <= 0.0:
            return

        source = str(event_source or "LAST_TRADE").strip().upper()
        if source not in {"LAST_TRADE", "ORDER_BOOK"}:
            source = "LAST_TRADE"
        trade = self.paper_positions.get(ticker)
        if source == "LAST_TRADE" and isinstance(trade, dict):
            self._capture_paper_exit_trigger(
                ticker, trade, price, source=source, market_timestamp=event_timestamp
            )
        self._pending_monitor_prices[ticker] = (price, source, event_timestamp)
        if self._monitor_worker_task is None or self._monitor_worker_task.done():
            self._monitor_worker_task = asyncio.create_task(self._drain_monitor_prices(db))

    async def _drain_monitor_prices(self, db: DBManager) -> None:
        """Обрабатывает только последние цены; не создаёт отдельную task на каждый тик."""
        try:
            while self._pending_monitor_prices:
                pending = self._pending_monitor_prices
                self._pending_monitor_prices = {}
                for ticker, pending_value in pending.items():
                    try:
                        if isinstance(pending_value, tuple) and len(pending_value) == 3:
                            price, source, event_timestamp = pending_value
                        elif isinstance(pending_value, tuple) and len(pending_value) == 2:
                            price, source = pending_value
                            event_timestamp = None
                        else:
                            price, source, event_timestamp = pending_value, "LAST_TRADE", None
                        await self.monitor_paper_positions(
                            ticker,
                            float(price),
                            db,
                            event_source=str(source),
                            event_timestamp=event_timestamp,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception as ticker_exc:
                        # A malformed event for one instrument must not discard
                        # the rest of the coalesced batch.
                        logger.error(
                            "[PAPER MONITOR ISOLATED FAILURE] ticker=%s: %s",
                            ticker, ticker_exc, exc_info=True,
                        )
                        continue
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Ошибка фонового PAPER-монитора: %s", exc, exc_info=True)
        finally:
            self._monitor_worker_task = None
            # Если цена попала в очередь в момент завершения worker, поднимаем новый worker.
            if self._pending_monitor_prices:
                self._monitor_worker_task = asyncio.create_task(self._drain_monitor_prices(db))

    async def quarantine_unreconciled_paper_positions(self, db: DBManager) -> int:
        """На старте помечает старые PAPER OPEN как требующие проверки, не рисуя фиктивное закрытие."""
        if not self.paper_mode:
            return 0

        # A complete journal is mandatory: get_active_positions() omits
        # STALE_REVIEW records and therefore cannot prove that a prior quarantine
        # has been resolved. Any read/shape failure must retain the entry lock.
        def _lock_unavailable_journal(reason: str) -> None:
            self._unreconciled_paper_positions = {
                "__PAPER_JOURNAL_UNAVAILABLE__": {
                    "status": "STALE_REVIEW",
                    "reconciliation_status": "JOURNAL_UNAVAILABLE",
                    "stale_reason": reason,
                }
            }

        getter = getattr(db, "get_all_daily_trades", None)
        if not callable(getter):
            reason = "DBManager не предоставляет полный журнал PAPER-сделок."
            _lock_unavailable_journal(reason)
            raise RuntimeError(reason)
        try:
            all_positions = await getter()
        except Exception as exc:
            reason = f"Не удалось прочитать полный журнал PAPER-сделок: {exc}"
            _lock_unavailable_journal(reason)
            logger.critical("[PAPER RECONCILIATION LOCK] %s; новые входы заблокированы.", reason)
            raise RuntimeError(reason) from exc
        if not isinstance(all_positions, Mapping):
            reason = "Полный журнал PAPER-сделок вернул некорректный формат."
            _lock_unavailable_journal(reason)
            logger.critical("[PAPER RECONCILIATION LOCK] %s; новые входы заблокированы.", reason)
            raise RuntimeError(reason)

        quarantined = 0
        unresolved: dict[str, TradeRecord] = {}
        self._unreconciled_paper_positions = unresolved
        now = datetime.now().isoformat(timespec="seconds")
        for order_id, raw_trade in all_positions.items():
            if not isinstance(raw_trade, dict):
                # The key is part of the trade journal, but its mode/status can
                # no longer be established. Do not assume it was a harmless
                # non-PAPER record and release the PAPER entry gate.
                reason = f"Запись сделки {order_id} имеет некорректный формат; режим/статус неизвестен."
                unresolved[str(order_id)] = {
                    "status": "STALE_REVIEW",
                    "reconciliation_status": "JOURNAL_RECORD_INVALID",
                    "stale_reason": reason,
                }
                logger.critical("[PAPER RECONCILIATION LOCK] %s; новые входы заблокированы.", reason)
                continue
            trade = dict(raw_trade)
            # TRADE_ — legacy ID used by old PAPER builds. In PAPER mode these
            # records are simulated trades, not a broker's inventory.
            paper_id = str(order_id).startswith(("PAPER_", "TRADE_"))
            explicitly_paper = str(trade.get("mode", "")).upper() == "PAPER"
            if not (paper_id or explicitly_paper):
                continue

            status = str(trade.get("status", "")).upper()
            if status == "OPEN":
                trade["status"] = "STALE_REVIEW"
                trade["reconciliation_status"] = "REVIEW_REQUIRED"
                trade["stale_detected_at"] = now
                trade["stale_reason"] = (
                    "PAPER-позиция осталась в постоянном хранилище после перезапуска. "
                    "Симулятор не восстановил её в ОЗУ; цена выхода и PnL не определялись. "
                    "До ручной сверки новые сделки заблокированы."
                )
                # Publish the in-memory lock before persistence. If the write
                # fails, startup may continue for diagnostics, but new entries
                # must remain blocked in this process.
                unresolved[str(order_id)] = trade
                self._unreconciled_paper_positions = unresolved
                await db.set_json(f"piluitus:trade:{order_id}", trade)
                quarantined += 1
            elif status == "STALE_REVIEW" and str(trade.get("reconciliation_status", "")).upper() == "REVIEW_REQUIRED":
                unresolved[str(order_id)] = trade

        self._unreconciled_paper_positions = unresolved
        if unresolved:
            for stale_id, stale_trade in sorted(unresolved.items()):
                side = str(stale_trade.get("side") or "").upper()
                entry = _finite_or_default(stale_trade.get("entry_price"), 0.0)
                stop = _finite_or_default(stale_trade.get("stop_loss"), 0.0)
                target = _finite_or_default(stale_trade.get("take_profit"), 0.0)
                levels_valid = (
                    entry > 0.0 and stop > 0.0 and target > 0.0
                    and (
                        (side == "BUY" and stop < entry < target)
                        or (side == "SELL" and target < entry < stop)
                    )
                )
                logger.warning(
                    "🔎 [PAPER STALE POSITION DETAIL] id=%s ticker=%s side=%s status=%s "
                    "shares=%s lots=%s entry=%.6f SL=%.6f TP=%.6f SLTP=%s "
                    "pending_exit=%s entry_timestamp=%s; позиция сохранена, "
                    "автоматическое закрытие/удаление не выполнялось.",
                    stale_id,
                    str(stale_trade.get("ticker") or stale_id).upper(),
                    side or "UNKNOWN",
                    str(stale_trade.get("status") or "UNKNOWN"),
                    stale_trade.get("volume", "N/A"),
                    stale_trade.get("lots", "N/A"),
                    entry, stop, target,
                    "VALID" if levels_valid else "INVALID_OR_MISSING",
                    stale_trade.get("pending_exit_reason") or "нет",
                    stale_trade.get("entry_timestamp") or "нет",
                )
            from portfolio.portfolio_metrics import calculate_assets_used
            unresolved_cost = calculate_assets_used(unresolved.values(), lot_sizes=LOT_SIZES)
            logger.critical(
                "🛑 [PAPER RECONCILIATION GUARD] Найдено %d не сверенных PAPER-позиций (себестоимость %.2f ₽). "
                "Новые входы заблокированы, пока позиции не будут проверены оператором.",
                len(unresolved), unresolved_cost,
            )
        if quarantined:
            logger.warning(
                "⚠️ Переведено в STALE_REVIEW старых PAPER-позиций: %d. "
                "Они не закрыты автоматически и не добавлены в realized PnL.",
                quarantined,
            )
        return quarantined

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

    @staticmethod
    def _track_exit_bbo_gap(trade: TradeRecord, now_monotonic: float, *, fresh: bool) -> float | None:
        """Track stale executable-BBO episodes without ever using stale prices."""
        started = _finite_or_default(trade.get("exit_bbo_gap_started_monotonic"), 0.0)
        if not fresh:
            if started <= 0.0:
                started = now_monotonic
                trade["exit_bbo_gap_started_monotonic"] = started
                trade["exit_bbo_gap_count"] = int(trade.get("exit_bbo_gap_count", 0) or 0) + 1
            duration = max(0.0, now_monotonic - started)
            trade["current_exit_bbo_gap_sec"] = round(duration, 3)
            trade["max_exit_bbo_gap_sec"] = round(max(_finite_or_default(trade.get("max_exit_bbo_gap_sec"), 0.0), duration), 3)
            return None
        if started > 0.0:
            duration = max(0.0, now_monotonic - started)
            trade["last_exit_bbo_gap_sec"] = round(duration, 3)
            trade["max_exit_bbo_gap_sec"] = round(max(_finite_or_default(trade.get("max_exit_bbo_gap_sec"), 0.0), duration), 3)
            trade["total_exit_bbo_gap_sec"] = round(_finite_or_default(trade.get("total_exit_bbo_gap_sec"), 0.0) + duration, 3)
            trade.pop("exit_bbo_gap_started_monotonic", None)
            trade["current_exit_bbo_gap_sec"] = 0.0
            return duration
        trade["current_exit_bbo_gap_sec"] = 0.0
        return None

    def _should_log_paper_exit_status(
        self,
        order_id: str,
        event_type: str,
        now_monotonic: float | None = None,
    ) -> bool:
        """Throttle repetitive BBO wait/recovery messages per order and event type."""
        order_key = str(order_id or "").strip()
        event_key = str(event_type or "status").strip().lower()[:24] or "status"
        if not order_key:
            return True
        last_logs = getattr(self, "_last_exit_status_log_at", None)
        if not isinstance(last_logs, dict):
            last_logs = {}
            self._last_exit_status_log_at = last_logs
        interval = _finite_or_default(
            getattr(self, "paper_exit_status_log_interval_sec", 300.0), 300.0
        )
        interval = min(900.0, max(60.0, interval if math.isfinite(interval) else 300.0))
        now = time.monotonic() if now_monotonic is None else _finite_or_default(now_monotonic, math.nan)
        if not math.isfinite(now) or now < 0.0:
            return False
        key = f"{order_key}:{event_key}"
        if key in last_logs:
            previous = _finite_or_default(last_logs.get(key), now)
            if now - previous < interval:
                return False
        last_logs[key] = now
        return True

    async def monitor_paper_positions(
        self,
        ticker: str,
        current_price: float,
        db: DBManager,
        *,
        event_source: str = "LAST_TRADE",
        event_timestamp: object = None,
    ):
        """Latch SL/TP on market events; execute only against a fresh executable BBO."""
        if not self.paper_mode:
            # This monitor never places a LIVE exit order.
            return
        if self._maintenance_active:
            return

        trade_obj: object = self.paper_positions.get(ticker)
        trade_map = _as_string_mapping(trade_obj)
        if trade_map is None or trade_map.get("status") != "OPEN":
            return
        trade = cast(TradeRecord, trade_map)

        order_id = str(trade.get("id", ""))
        if not order_id or order_id in self._lock_closing:
            return

        try:
            observed_last_price = float(current_price)
            entry_price = float(trade.get("entry_price", 0.0))
            if (
                not math.isfinite(observed_last_price) or observed_last_price <= 0.0
                or not math.isfinite(entry_price) or entry_price <= 0.0
            ):
                return

            side = str(trade.get("side", "BUY")).upper()
            if side not in {"BUY", "SELL"}:
                logger.error("Некорректная сторона PAPER-позиции %s: %s", order_id, side)
                return

            sign = 1 if side == "BUY" else -1
            saved_lot_size = int(_finite_or_default(
                trade.get("lot_size", LOT_SIZES.get(ticker, 1)), 1.0
            ))
            share_qty = int(trade.get("volume") or (
                int(trade.get("lots", 1)) * max(saved_lot_size, 1)
            ))
            if share_qty <= 0:
                logger.error("Некорректный объём позиции %s: %s", order_id, share_qty)
                return

            default_sl = entry_price * (0.995 if side == "BUY" else 1.005)
            default_tp = entry_price * (1.015 if side == "BUY" else 0.985)
            stop_loss = float(trade.get("stop_loss") or default_sl)
            take_profit = float(trade.get("take_profit") or default_tp)
            if side == "BUY" and not (stop_loss < entry_price < take_profit):
                logger.error("Некорректные SL/TP в PAPER-позиции %s; монитор закрытия остановлен.", order_id)
                return
            if side == "SELL" and not (take_profit < entry_price < stop_loss):
                logger.error("Некорректные SL/TP в PAPER-позиции %s; монитор закрытия остановлен.", order_id)
                return

            if str(event_source or "LAST_TRADE").upper() == "LAST_TRADE":
                self._capture_paper_exit_trigger(
                    ticker, trade, observed_last_price, source="LAST_TRADE"
                )

            # Excursion stats use every observed last trade; exit decisions below
            # use the executable side of a fresh two-sided BBO.
            observed_signed_pct = ((observed_last_price - entry_price) / entry_price) * 100.0 * sign
            observed_signed_rub = (observed_last_price - entry_price) * share_qty * sign
            max_fav_pct = max(
                float(trade.get("max_favorable_excursion_pct", 0.0) or 0.0),
                observed_signed_pct,
            )
            max_adv_pct = min(
                float(trade.get("max_adverse_excursion_pct", 0.0) or 0.0),
                observed_signed_pct,
            )
            max_fav_rub = max(
                float(trade.get("max_favorable_excursion_rub", 0.0) or 0.0),
                observed_signed_rub,
            )
            max_adv_rub = min(
                float(trade.get("max_adverse_excursion_rub", 0.0) or 0.0),
                observed_signed_rub,
            )
            stop_distance = abs(entry_price - stop_loss)
            stop_distance_pct = (stop_distance / entry_price * 100.0) if entry_price > 0 else 0.0
            trade.update({
                "max_favorable_excursion_pct": round(max_fav_pct, 6),
                "max_adverse_excursion_pct": round(max_adv_pct, 6),
                "max_favorable_excursion_rub": round(max_fav_rub, 4),
                "max_adverse_excursion_rub": round(max_adv_rub, 4),
                "max_favorable_excursion_r": round(max_fav_pct / stop_distance_pct, 4) if stop_distance_pct > 0 else 0.0,
                "max_adverse_excursion_r": round(max_adv_pct / stop_distance_pct, 4) if stop_distance_pct > 0 else 0.0,
                "observed_last_price": observed_last_price,
            })

            bbo = self._latest_fresh_exit_quote(ticker, side)
            if bbo is None:
                self._track_exit_bbo_gap(trade, time.monotonic(), fresh=False)
                pending_exit = str(trade.get("pending_exit_reason") or "").upper()
                # Once SL/TP has fired, do not let a calendar REST lookup delay
                # the trigger audit or persistence; only a fresh executable BBO
                # can release the pending close.
                market_state = (
                    {}
                    if pending_exit in {"STOP_LOSS", "TAKE_PROFIT"}
                    else await self._market_session_status(ticker)
                )
                if pending_exit in {"STOP_LOSS", "TAKE_PROFIT"}:
                    wait_state = "EXIT_TRIGGERED_WAITING_FOR_BBO"
                    wait_reason = f"{pending_exit}: триггер зафиксирован; ожидается свежая исполнимая сторона BBO"
                elif market_state.get("known") and not market_state.get("is_open"):
                    wait_state = "MARKET_CLOSED"
                    wait_reason = "биржевая сессия закрыта; позиция сохранена до следующего доступного BBO"
                elif not market_state.get("known"):
                    wait_state = "WAITING_FOR_BBO"
                    wait_reason = "календарь недоступен или исполнимый BBO ещё не получен"
                else:
                    wait_state = "WAITING_FOR_BBO"
                    wait_reason = "сессия открыта, но свежая исполнимая сторона BBO отсутствует"
                previous_wait_state = str(trade.get("exit_monitor_state") or "")
                if previous_wait_state != wait_state:
                    trade["exit_monitor_state"] = wait_state
                    trade["exit_monitor_wait_reason"] = wait_reason
                    if self._should_log_paper_exit_status(
                        order_id, "wait", time.monotonic()
                    ):
                        logger.warning(
                            "⏸️ [PAPER POSITION WAIT] %s %s: %s; позиция и SL/TP сохранены, "
                            "закрытие будет проверено после появления свежего исполнимого BBO.",
                            ticker, side, wait_reason,
                        )
                exit_transaction_side = "SELL" if side == "BUY" else "BUY"
                estimated_exit_price = self._apply_paper_slippage(observed_last_price, exit_transaction_side)
                estimated_gross_pnl = (estimated_exit_price - entry_price) * share_qty * sign
                estimated_exit_commission = (
                    estimated_exit_price * share_qty
                    * _finite_or_default(trade.get("commission_pct_per_side"), self.paper_commission_pct) / 100.0
                )
                estimated_net_pnl = estimated_gross_pnl - _finite_or_default(
                    trade.get("entry_commission_rub"), 0.0
                ) - estimated_exit_commission
                entry_notional = _finite_or_default(trade.get("notional_entry_rub"), entry_price * share_qty)
                trade["current_price"] = observed_last_price
                trade["pnl"] = round(
                    _finite_or_default(trade.get("realized_net_pnl_rub"), 0.0)
                    + estimated_net_pnl, 2
                )
                trade["gross_pnl"] = round(
                    _finite_or_default(trade.get("realized_gross_pnl_rub"), 0.0)
                    + estimated_gross_pnl, 2
                )
                trade["estimated_exit_price"] = estimated_exit_price
                trade["estimated_exit_commission_rub"] = round(
                    _finite_or_default(trade.get("realized_exit_commission_rub"), 0.0)
                    + estimated_exit_commission, 4
                )
                trade["pnl_percent"] = round(trade["pnl"] / entry_notional * 100.0, 4) if entry_notional > 0 else 0.0
                trade["current_pnl_source"] = "LAST_TRADE_ESTIMATE"
                if pending_exit in {"STOP_LOSS", "TAKE_PROFIT"}:
                    if not bool(trade.get("exit_trigger_wait_logged")):
                        trigger_at = _finite_or_default(trade.get("exit_trigger_monotonic"), 0.0)
                        delay = max(0.0, time.monotonic() - trigger_at) if trigger_at > 0.0 else 0.0
                        logger.warning(
                            "⏳ [PAPER EXIT TRIGGER WAIT] %s %s: %s; wait=%.3fs; "
                            "last_trade=%.6f; execution is deferred until fresh BBO.",
                            ticker, side, pending_exit, delay, observed_last_price,
                        )
                        trade["exit_trigger_wait_logged"] = True
                    await db.set_json(f"piluitus:trade:{order_id}", trade)
                now_mono = time.monotonic()
                previous_gap_count = int(_finite_or_default(trade.get("exit_bbo_gap_count"), 0.0))
                self._track_exit_bbo_gap(trade, now_mono, fresh=False)
                if int(_finite_or_default(trade.get("exit_bbo_gap_count"), 0.0)) > previous_gap_count:
                    # Persist the start/count of a new outage episode immediately;
                    # the completed duration is persisted when fresh BBO returns.
                    try:
                        await db.set_json(f"piluitus:trade:{order_id}", trade)
                    except Exception as persist_exc:
                        logger.warning(
                            "[PAPER BBO GAP PERSIST] %s: не удалось сохранить начало эпизода: %s",
                            ticker, persist_exc,
                        )
                last_warn = self._last_bbo_exit_warning_at.get(ticker, 0.0)
                if now_mono - last_warn >= self.paper_exit_status_log_interval_sec:
                    quote_obj: object = (
                        self.screener.quotes.get(ticker, {})
                        if self.screener is not None else {}
                    )
                    quote = _as_string_mapping(quote_obj) or {}
                    required_side = "Bid" if side == "BUY" else "Ask"
                    seen_key = "last_bid_confirmed_seen" if side == "BUY" else "last_ask_confirmed_seen"
                    fallback_seen_key = "last_bid_seen" if side == "BUY" else "last_ask_seen"
                    bid_value = _finite_or_default(quote.get("bid", 0.0), 0.0)
                    ask_value = _finite_or_default(quote.get("ask", 0.0), 0.0)
                    try:
                        seen_at = _finite_or_default(
                            quote.get(seen_key, quote.get(fallback_seen_key, 0.0)), 0.0
                        )
                        side_age = time.monotonic() - seen_at if seen_at > 0.0 else math.inf
                    except (TypeError, ValueError, AttributeError):
                        side_age = math.inf
                    # Report the exact same effective TTL used by
                    # _latest_fresh_exit_quote(); a separate EXIT_MAX_AGE value
                    # previously made healthy (<2s) quotes appear inconsistent.
                    configured_warning_age = (
                        getattr(self.screener, "bbo_max_age_sec", None)
                        if self.screener is not None else None
                    )
                    if configured_warning_age is None:
                        configured_warning_age = os.getenv(
                            "PILUITUS_BBO_MAX_AGE_SEC",
                            os.getenv("PILUITUS_BBO_EXIT_MAX_AGE_SEC", "2.0"),
                        )
                    warning_max_age = _finite_or_default(configured_warning_age, 2.0)
                    if not math.isfinite(warning_max_age) or warning_max_age <= 0.0:
                        warning_max_age = 2.0
                    warning_max_age = min(5.0, max(0.25, warning_max_age))

                    def _quote_age(field: str) -> float:
                        stamp = _finite_or_default(quote.get(field, 0.0), 0.0)
                        age = time.monotonic() - stamp if stamp > 0.0 else math.inf
                        return age if math.isfinite(age) and age >= 0.0 else math.inf

                    book_event_age = _quote_age("last_order_book_event_seen")
                    book_receive_age = _quote_age("last_order_book_received")
                    bid_confirm_age = _quote_age("last_bid_confirmed_seen")
                    ask_confirm_age = _quote_age("last_ask_confirmed_seen")
                    if bid_value > 0.0 and ask_value > 0.0 and ask_value < bid_value:
                        guard_reason = "пересекающийся BBO (Ask ниже Bid)"
                    elif (bid_value if side == "BUY" else ask_value) <= 0.0:
                        guard_reason = f"отсутствует исполнимая сторона {required_side}"
                    elif not math.isfinite(side_age) or side_age > warning_max_age:
                        guard_reason = f"сторона {required_side} устарела"
                    elif side_age < 0.0:
                        guard_reason = f"временная метка стороны {required_side} находится в будущем"
                    else:
                        guard_reason = "BBO не прошёл проверку исполнимости"
                    age_text = f"{side_age:.2f}s" if math.isfinite(side_age) else "нет метки"
                    logger.warning(
                        "⏸️ [PAPER EXIT GUARD] %s: %s; executable_age=%s, лимит=%.2fs, "
                        "Bid=%.4f Ask=%.4f; book_valid=%s event_age=%s receive_age=%s "
                        "bid_confirm_age=%s ask_confirm_age=%s; SL/TP отложены, last=%.4f.",
                        ticker, guard_reason, age_text, warning_max_age,
                        bid_value, ask_value, bool(quote.get("order_book_valid", False)),
                        f"{book_event_age:.2f}s" if math.isfinite(book_event_age) else "нет",
                        f"{book_receive_age:.2f}s" if math.isfinite(book_receive_age) else "нет",
                        f"{bid_confirm_age:.2f}s" if math.isfinite(bid_confirm_age) else "нет",
                        f"{ask_confirm_age:.2f}s" if math.isfinite(ask_confirm_age) else "нет",
                        observed_last_price,
                    )
                    self._last_bbo_exit_warning_at[ticker] = now_mono
                return

            gap_duration = self._track_exit_bbo_gap(trade, time.monotonic(), fresh=True)
            if gap_duration is not None:
                try:
                    await db.set_json(f"piluitus:trade:{order_id}", trade)
                except Exception as persist_exc:
                    logger.warning(
                        "[PAPER BBO GAP PERSIST] %s: не удалось сохранить восстановленный BBO: %s",
                        ticker, persist_exc,
                    )
                if self._should_log_paper_exit_status(
                    order_id, "recovery", time.monotonic()
                ):
                    logger.warning(
                        "[PAPER BBO GAP RECOVERED] %s %s: gap=%.3fs max_gap=%.3fs total_gap=%.3fs; "
                        "SL/TP monitor resumed only on fresh executable BBO.",
                        ticker, side, gap_duration,
                        _finite_or_default(trade.get("max_exit_bbo_gap_sec"), 0.0),
                        _finite_or_default(trade.get("total_exit_bbo_gap_sec"), 0.0),
                    )

            # A coherent executable BBO has returned; resume the monitor without
            # manufacturing a fill or changing the saved SL/TP levels.
            if str(trade.get("exit_monitor_state") or "") != "ACTIVE":
                previous_reason = str(trade.get("exit_monitor_wait_reason") or "ожидание BBO")
                trade["exit_monitor_state"] = "ACTIVE"
                trade.pop("exit_monitor_wait_reason", None)
                if gap_duration is None and self._should_log_paper_exit_status(
                    order_id, "resume", time.monotonic()
                ):
                    logger.info(
                        "▶️ [PAPER POSITION RESUMED] %s %s: получен свежий исполнимый BBO; "
                        "мониторинг SL/TP продолжен (предыдущее состояние: %s).",
                        ticker, side, previous_reason,
                    )

            # Closing a long means selling at Bid; closing a short means buying at Ask.
            executable_exit_price = float(bbo["executable_price"])
            if not math.isfinite(executable_exit_price) or executable_exit_price <= 0.0:
                return

            # BBO-based excursions are separate from legacy last-trade MFE/MAE.
            bbo_signed_pct = ((executable_exit_price - entry_price) / entry_price) * 100.0 * sign
            bbo_signed_rub = (executable_exit_price - entry_price) * share_qty * sign
            trade["max_favorable_bbo_excursion_pct"] = round(max(
                _finite_or_default(trade.get("max_favorable_bbo_excursion_pct"), 0.0),
                bbo_signed_pct,
            ), 6)
            trade["max_adverse_bbo_excursion_pct"] = round(min(
                _finite_or_default(trade.get("max_adverse_bbo_excursion_pct"), 0.0),
                bbo_signed_pct,
            ), 6)
            trade["max_favorable_bbo_excursion_rub"] = round(max(
                _finite_or_default(trade.get("max_favorable_bbo_excursion_rub"), 0.0),
                bbo_signed_rub,
            ), 4)
            trade["max_adverse_bbo_excursion_rub"] = round(min(
                _finite_or_default(trade.get("max_adverse_bbo_excursion_rub"), 0.0),
                bbo_signed_rub,
            ), 4)

            # Shadow-only break-even: track whether a stop moved to net BE after
            # the configured R threshold would have triggered. Fixed SL/TP is untouched.
            if self.shadow_be_enabled:
                stop_distance = abs(entry_price - stop_loss)
                favorable_r = bbo_signed_rub / (share_qty * stop_distance) if stop_distance > 0.0 else 0.0
                if not bool(trade.get("shadow_be_armed")) and favorable_r >= self.shadow_be_arm_r:
                    shadow_trigger = self._calculate_shadow_be_raw_trigger_price(
                        entry_price,
                        side,
                        share_qty,
                        _finite_or_default(trade.get("entry_commission_rub"), 0.0),
                        _finite_or_default(trade.get("commission_pct_per_side"), self.paper_commission_pct),
                        self.paper_slippage_bps,
                    )
                    if shadow_trigger > 0.0:
                        trade["shadow_be_armed"] = True
                        trade["shadow_be_trigger_price"] = round(shadow_trigger, 6)
                        trade["shadow_be_favorable_r_at_arm"] = round(favorable_r, 4)
                        trade["shadow_be_armed_timestamp"] = datetime.now().astimezone().isoformat(timespec="milliseconds")
                        logger.info(
                            "🧪 [SHADOW BE ARMED] %s %s: BBO достиг %.2fR; гипотетический net-BE stop=%.6f; "
                            "фактические SL/TP не изменены.",
                            ticker, side, favorable_r, shadow_trigger,
                        )
                if bool(trade.get("shadow_be_armed")) and not bool(trade.get("shadow_be_triggered")):
                    shadow_trigger = _finite_or_default(trade.get("shadow_be_trigger_price"), 0.0)
                    shadow_hit = (
                        shadow_trigger > 0.0 and (
                            (side == "BUY" and executable_exit_price <= shadow_trigger)
                            or (side == "SELL" and executable_exit_price >= shadow_trigger)
                        )
                    )
                    if shadow_hit:
                        exit_transaction_side = "SELL" if side == "BUY" else "BUY"
                        shadow_fill = self._apply_paper_slippage(executable_exit_price, exit_transaction_side)
                        shadow_gross = (shadow_fill - entry_price) * share_qty * sign
                        shadow_fee = shadow_fill * share_qty * (
                            _finite_or_default(trade.get("commission_pct_per_side"), self.paper_commission_pct) / 100.0
                        )
                        shadow_net = shadow_gross - _finite_or_default(
                            trade.get("entry_commission_rub"), 0.0
                        ) - shadow_fee
                        trade["shadow_be_triggered"] = True
                        trade["shadow_be_trigger_timestamp"] = datetime.now().astimezone().isoformat(timespec="milliseconds")
                        trade["shadow_be_trigger_bbo_price"] = round(executable_exit_price, 6)
                        trade["shadow_be_estimated_net_pnl_rub"] = round(shadow_net, 4)
                        logger.info(
                            "🧪 [SHADOW BE TRIGGERED] %s %s: BBO=%.6f, гипотетический net PnL=%+.4f ₽; "
                            "позиция продолжает жить по исходным SL/TP.",
                            ticker, side, executable_exit_price, shadow_net,
                        )

            if not str(trade.get("pending_exit_reason") or "").upper() in {"STOP_LOSS", "TAKE_PROFIT"}:
                self._capture_paper_exit_trigger(
                    ticker, trade, executable_exit_price,
                    source="EXECUTABLE_BBO", market_timestamp=event_timestamp,
                )
            exit_reason = str(trade.get("pending_exit_reason") or "").upper()
            if exit_reason not in {"STOP_LOSS", "TAKE_PROFIT"}:
                exit_reason = ""

            exit_transaction_side = "SELL" if side == "BUY" else "BUY"
            estimated_exit_price = self._apply_paper_slippage(executable_exit_price, exit_transaction_side)
            mark_pct = ((estimated_exit_price - entry_price) / entry_price) * 100.0 * sign
            entry_fee = _finite_or_default(trade.get("entry_commission_rub"), 0.0)
            fee_pct = _finite_or_default(trade.get("commission_pct_per_side"), self.paper_commission_pct)
            mark_rub, estimated_exit_fee, _mark_total_fees, mark_net = self._calculate_paper_pnl(
                entry_price, estimated_exit_price, share_qty, side, fee_pct, entry_fee
            )
            entry_notional = _finite_or_default(trade.get("notional_entry_rub"), entry_price * share_qty)
            trade["current_price"] = executable_exit_price
            trade["estimated_exit_price"] = estimated_exit_price
            trade["pnl"] = round(
                _finite_or_default(trade.get("realized_net_pnl_rub"), 0.0) + mark_net, 2
            )
            trade["gross_pnl"] = round(
                _finite_or_default(trade.get("realized_gross_pnl_rub"), 0.0) + mark_rub, 2
            )
            trade["estimated_exit_commission_rub"] = round(
                _finite_or_default(trade.get("realized_exit_commission_rub"), 0.0)
                + estimated_exit_fee, 4
            )
            trade["pnl_percent"] = round(trade["pnl"] / entry_notional * 100.0, 4) if entry_notional > 0 else 0.0
            trade["current_pnl_source"] = "EXECUTABLE_BBO_ESTIMATE"
            trade["current_bbo_age_sec"] = round(float(bbo["age_sec"]), 3)
            trade["current_spread_pct"] = round(float(bbo["spread_pct"]), 6)
            if not exit_reason:
                return

            trigger_price = _finite_or_default(
                trade.get("pending_exit_trigger_price"),
                stop_loss if exit_reason == "STOP_LOSS" else take_profit,
            )
            async with self._portfolio_lock:
                if self._maintenance_active or order_id in self._lock_closing:
                    return
                self._lock_closing.add(order_id)
            try:
                await self._finalize_paper_close(
                    ticker, trade, db, bbo,
                    exit_reason=exit_reason,
                    trigger_price=trigger_price,
                    observed_last_price=observed_last_price,
                )
            finally:
                self._lock_closing.discard(order_id)

        except Exception as exc:
            logger.error("❗ Ошибка мониторинга PAPER-позиции по %s: %s", ticker, exc, exc_info=True)
        finally:
            if order_id:
                self._lock_closing.discard(order_id)

    async def close_all_positions(self, db: DBManager) -> dict[str, Any]:
        """Close active PAPER positions while atomically blocking concurrent entries.

        The maintenance latch and PAPER entry commit use the same portfolio lock:
        an entry either becomes fully journaled before this snapshot, or is rejected
        before it can become an untracked open position.
        """
        if not self.paper_mode:
            logger.error("CMD_CLOSE_ALL в этом менеджере поддерживает только активные PAPER-позиции.")
            return {"closed": 0, "partial": 0, "blocked": 0, "message": "PAPER-позиции не активны."}

        emergency_reason = "Экстренное закрытие PAPER-позиций."
        async with self._portfolio_lock:
            if self._maintenance_active:
                existing_count = (
                    len(self.paper_positions)
                    if isinstance(self.paper_positions, dict) else 0
                )
                reason = self._maintenance_reason or "Другое обслуживание уже выполняется."
                return {
                    "closed": 0,
                    "partial": 0,
                    "blocked": existing_count,
                    "message": f"Экстренное закрытие не запущено: {reason}",
                }

            raw_positions_obj: object = self.paper_positions
            if not isinstance(raw_positions_obj, dict) or not raw_positions_obj:
                logger.info("📭 Нет активных позиций для экстренного закрытия.")
                return {
                    "closed": 0, "partial": 0, "blocked": 0,
                    "message": "Активных PAPER-позиций нет.",
                }

            raw_positions = dict(cast(dict[object, object], raw_positions_obj))
            self._maintenance_active = True
            self._maintenance_reason = emergency_reason

        logger.warning(
            "🚨 Запущено экстренное PAPER-закрытие %s позиций; новые входы заблокированы.",
            len(raw_positions),
        )
        closed_count = 0
        blocked_count = 0
        partial_count = 0
        try:
            for raw_ticker, pos_obj in raw_positions.items():
                ticker = str(raw_ticker).strip().upper().split("@")[0]
                if not ticker or not isinstance(pos_obj, dict):
                    blocked_count += 1
                    logger.error("Пропуск некорректной позиции %s при CMD_CLOSE_ALL.", raw_ticker)
                    continue

                pos = cast(TradeRecord, pos_obj)
                if str(pos.get("status", "OPEN")).strip().upper() != "OPEN":
                    continue
                side = str(pos.get("side", "BUY")).strip().upper()
                order_id = str(pos.get("id", "")).strip()
                if side not in {"BUY", "SELL"} or not order_id:
                    blocked_count += 1
                    logger.error("Пропуск позиции %s: некорректная сторона или ID.", ticker)
                    continue
                if order_id in self._lock_closing:
                    blocked_count += 1
                    logger.warning(
                        "CMD_CLOSE_ALL: позиция %s уже находится в процессе закрытия.", ticker
                    )
                    continue

                try:
                    bbo = self._latest_fresh_exit_quote(ticker, side)
                except Exception as exc:
                    bbo = None
                    logger.error(
                        "CMD_CLOSE_ALL: ошибка получения исполнимого BBO для %s: %s",
                        ticker, exc, exc_info=True,
                    )
                if bbo is None:
                    blocked_count += 1
                    logger.error(
                        "CMD_CLOSE_ALL: %s не закрыта — нет свежего исполнимого %s (порог %.2fs).",
                        ticker, "Bid" if side == "BUY" else "Ask",
                        _finite_or_default(os.getenv("PILUITUS_BBO_EXIT_MAX_AGE_SEC", "2.0"), 2.0),
                    )
                    continue

                partial_count_before = int(
                    _finite_or_default(pos.get("exit_partial_fill_count"), 0.0)
                )
                self._lock_closing.add(order_id)
                try:
                    last_price = self._latest_price(ticker, pos)
                    close_result = await self._finalize_paper_close(
                        ticker, pos, db, bbo,
                        exit_reason="MANUAL_CLOSE",
                        observed_last_price=last_price,
                    )
                    if str(close_result.get("status", "")).strip().upper() == "CLOSED":
                        closed_count += 1
                    elif int(
                        _finite_or_default(close_result.get("exit_partial_fill_count"), 0.0)
                    ) > partial_count_before:
                        partial_count += 1
                    else:
                        blocked_count += 1
                except Exception as exc:
                    blocked_count += 1
                    logger.error(
                        "CMD_CLOSE_ALL: не удалось закрыть %s: %s", ticker, exc, exc_info=True
                    )
                finally:
                    self._lock_closing.discard(order_id)

            result = {
                "closed": closed_count,
                "partial": partial_count,
                "blocked": blocked_count,
                "message": (
                    f"Экстренное PAPER-закрытие: полностью закрыто {closed_count}, "
                    f"частично закрыто {partial_count}, ожидает исполнения/заблокировано {blocked_count}."
                ),
            }
            logger.warning("🚨 [PAPER CLOSE ALL] %s", result["message"])
            return result
        finally:
            async with self._portfolio_lock:
                if self._maintenance_reason == emergency_reason:
                    self._maintenance_active = False
                    self._maintenance_reason = ""
                    logger.info(
                        "[PAPER MAINTENANCE] Экстренное закрытие завершено; "
                        "новые входы снова разрешены при выполнении всех риск-условий."
                    )

