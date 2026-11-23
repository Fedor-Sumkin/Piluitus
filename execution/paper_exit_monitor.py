"""PAPER exit trigger scheduling and BBO-driven monitoring mixin."""
from __future__ import annotations

from execution.paper_exit_shared import (
    asyncio, logging, math, os, time, Mapping, datetime, timedelta, timezone, Any, cast,
    LOT_SIZES, DBManager, PAPER_CLOSED_TRADES_STORAGE_KEY, PAPER_CLOSED_TRADES_STORAGE_LIMIT,
    paper_performance_tracker, build_exit_execution_diagnostics,
    BBOExitQuote, TradeRecord, _as_string_mapping, _finite_or_default, logger,
)

class PaperExitMonitorMixin:
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
