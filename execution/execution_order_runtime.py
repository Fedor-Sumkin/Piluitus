"""Order lifecycle orchestration for the public execution engine facade."""
from __future__ import annotations

from execution.execution_shared import *  # noqa: F401,F403


class ExecutionOrderRuntimeMixin:
    async def execute_order(
        self,
        ticker: str,
        lots: int,
        side: str,
        price: float,
        client_id: str,
        db: DBManager,
        ai_analysis: TradeRecord,
    ) -> TradeRecord:
        """Единая точка входа; PAPER-позиция создаётся только после успешной записи в журнал."""
        ticker = str(ticker).strip().upper().split("@")[0]
        side = str(side).strip().upper()
        lot_multiplier = self._lot_multiplier_for(ticker)
        reserved_ticker = False

        try:
            if self._maintenance_active:
                return {
                    "status": "SKIPPED",
                    "message": self._maintenance_reason or "PAPER-торговля приостановлена для обслуживания сессии.",
                }
            if lot_multiplier <= 0:
                return {
                    "status": "SKIPPED",
                    "message": (
                        f"биржевой lot_size для {ticker} не подтверждён; "
                        "новая PAPER-позиция не создаётся."
                    ),
                }
            if side not in {"BUY", "SELL"}:
                return {"status": "FAILED", "message": f"Недопустимая сторона сделки: {side}"}
            if int(lots) <= 0 or float(price) <= 0:
                return {"status": "FAILED", "message": "Количество лотов и цена должны быть положительными."}
            lots = int(lots)
            requested_lots = lots
            price = float(price)
            signal_reference_price = price

            if ai_analysis.get("final_decision", "HOLD").upper() == "HOLD":
                return {"status": "SKIPPED", "message": "AI Decision is HOLD"}

            session_allowed, session_message, _session_state = await self._check_entry_session(ticker)
            if not session_allowed:
                logger.warning(
                    "⛔ [ENTRY SESSION GUARD] %s %s: вход запрещён — %s.",
                    side, ticker, session_message,
                )
                return {"status": "SKIPPED", "message": f"ENTRY_SESSION_BLOCK: {session_message}"}
            logger.info(
                "🕒 [ENTRY SESSION GUARD] %s: вход разрешён; %s; cutoff=%.1f мин.",
                ticker, session_message, self._entry_cutoff_minutes(),
            )

            entry_bbo: BBOExitQuote | None = None
            paper_depth_fill: dict[str, Any] | None = None
            entry_bbo_wait_attempts = 0
            if self.paper_mode:
                # LLM inference can consume the quote TTL. Wait briefly for the
                # next coherent ORDER_BOOK confirmation; never relax freshness.
                wait_sec = _finite_or_default(
                    os.getenv("PILUITUS_PAPER_ENTRY_BBO_WAIT_SEC", "1.0"), 1.0
                )
                wait_sec = min(2.0, max(0.0, wait_sec))
                deadline = time.monotonic() + wait_sec
                while True:
                    entry_bbo = self._latest_fresh_entry_quote(ticker, side)
                    if entry_bbo is not None:
                        break
                    remaining = deadline - time.monotonic()
                    if remaining <= 0.0:
                        break
                    entry_bbo_wait_attempts += 1
                    await asyncio.sleep(min(0.05, remaining))

                if entry_bbo is None:
                    diagnostic = self._paper_entry_bbo_diagnostic(ticker)
                    message = (
                        "BBO_ENTRY_BLOCK: нет пригодного двустороннего BBO (свежесть/спред) для PAPER-входа; "
                        f"ожидание={wait_sec:.2f}s; {diagnostic}. Позиция не открыта."
                    )
                    logger.warning("🧱 [PAPER BBO ENTRY BLOCK] %s %s: %s", side, ticker, message)
                    return {"status": "SKIPPED", "message": message}
                if entry_bbo_wait_attempts:
                    logger.info(
                        "🔄 [PAPER BBO ENTRY RETRY] %s %s: свежий двусторонний BBO получен "
                        "после %d проверок; bid_age=%.3fs ask_age=%.3fs limit=%.3fs.",
                        side, ticker, entry_bbo_wait_attempts,
                        float(entry_bbo["bid_age_sec"]), float(entry_bbo["ask_age_sec"]),
                        float(entry_bbo["max_age_sec"]),
                    )
                entry_mid = (
                    _finite_or_default(entry_bbo.get("bid"), 0.0)
                    + _finite_or_default(entry_bbo.get("ask"), 0.0)
                ) / 2.0
                book_age = _finite_or_default(entry_bbo.get("order_book_age_sec"), math.inf)
                book_max_age = min(5.0, max(0.25, _finite_or_default(entry_bbo.get("max_age_sec"), 2.0)))
                if not bool(entry_bbo.get("order_book_valid")) or not math.isfinite(book_age) or book_age < 0.0 or book_age > book_max_age:
                    message = (
                        "PAPER_ORDER_BOOK_ENTRY_BLOCK: подтверждённый свежий стакан отсутствует "
                        f"(valid={bool(entry_bbo.get('order_book_valid'))}, age={book_age:.2f}s, "
                        f"limit={book_max_age:.2f}s)."
                    )
                    logger.warning("🧱 [PAPER ORDER_BOOK ENTRY BLOCK] %s %s: %s", side, ticker, message)
                    return {"status": "BLOCKED", "message": message}

                spread_atr_ok, spread_atr_reason = validate_spread_to_atr(
                    {
                        "bar_atr_periods": ai_analysis.get("atr_periods_at_entry", 0),
                        "bar_atr_age_sec": ai_analysis.get("atr_age_sec_at_entry", -1.0),
                    },
                    spread_pct=_finite_or_default(entry_bbo.get("spread_pct"), math.inf),
                    atr_value=_finite_or_default(ai_analysis.get("atr_at_entry"), 0.0),
                    reference_price=entry_mid,
                    require_atr=True,
                )
                if not spread_atr_ok:
                    message = f"SPREAD_ATR_ENTRY_BLOCK: {spread_atr_reason}"
                    logger.warning("🧱 [PAPER SPREAD/ATR BLOCK] %s %s: %s", side, ticker, message)
                    return {"status": "BLOCKED", "message": message}

                atr_for_costs = _finite_or_default(ai_analysis.get("atr_at_entry"), 0.0)
                atr_pct_for_costs = (
                    atr_for_costs / entry_mid * 100.0
                    if entry_mid > 0.0 and atr_for_costs > 0.0 else 0.0
                )
                try:
                    max_cost_to_atr = float(os.getenv("PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO", "0.50"))
                except (TypeError, ValueError, OverflowError):
                    max_cost_to_atr = 0.50
                entry_economics = validate_entry_economics(
                    side=side,
                    bid=_finite_or_default(entry_bbo.get("bid"), 0.0),
                    ask=_finite_or_default(entry_bbo.get("ask"), 0.0),
                    commission_pct_per_side=self.paper_commission_pct,
                    slippage_bps_per_side=self.paper_slippage_bps,
                    atr_pct=atr_pct_for_costs,
                    max_cost_to_atr_ratio=max_cost_to_atr,
                )
                if not entry_economics.get("available") or not entry_economics.get("eligible"):
                    message = (
                        "PAPER_ENTRY_COST_BLOCK: round-trip издержки слишком велики или не измерены "
                        f"(cost/ATR={entry_economics.get('cost_to_atr_ratio')}, "
                        f"limit={entry_economics.get('max_cost_to_atr_ratio', max_cost_to_atr):.2f}, "
                        f"reason={entry_economics.get('reason', 'UNKNOWN')})."
                    )
                    logger.warning("🧱 [PAPER ENTRY ECONOMICS BLOCK] %s %s: %s", side, ticker, message)
                    return {"status": "BLOCKED", "message": message}
                logger.info(
                    "[PAPER ENTRY ECONOMICS] %s %s cost/ATR=%.3f limit=%.2f "
                    "breakeven_move=%.4f%% cost_model=%s.",
                    side, ticker,
                    float(entry_economics["cost_to_atr_ratio"]),
                    float(entry_economics["max_cost_to_atr_ratio"]),
                    float(entry_economics["minimum_favorable_move_pct"]),
                    entry_economics.get("cost_model_status", "UNKNOWN"),
                )

                executable_price = _finite_or_default(entry_bbo.get("executable_price"), 0.0)
                if executable_price <= 0.0:
                    return {"status": "SKIPPED", "message": "Некорректная исполнимая цена BBO для PAPER-входа."}
                if self.paper_book_size_unit in {"shares", "lots"}:
                    depth_key = "ask_levels" if side == "BUY" else "bid_levels"
                    paper_depth_fill = self._simulate_paper_depth_fill(
                        entry_bbo.get(depth_key, []),
                        lots,
                        lot_multiplier,
                        self.paper_book_size_unit,
                        self.paper_max_book_participation_pct,
                    )
                    if not paper_depth_fill or int(paper_depth_fill.get("filled_lots", 0)) <= 0:
                        return {
                            "status": "SKIPPED",
                            "message": (
                                "PAPER_DEPTH_FILL_BLOCK: подтверждённой глубины недостаточно "
                                "для минимального лота при заданной доле участия."
                            ),
                        }
                    filled_lots = int(paper_depth_fill["filled_lots"])
                    if filled_lots < lots:
                        logger.warning(
                            "🧩 [PAPER PARTIAL ENTRY] %s %s: requested=%d lots, filled=%d lots "
                            "(%.1f%%); depth_unit=%s participation=%.1f%% levels=%d.",
                            ticker, side, lots, filled_lots,
                            float(paper_depth_fill["fill_ratio"]) * 100.0,
                            self.paper_book_size_unit, self.paper_max_book_participation_pct,
                            int(paper_depth_fill["levels_used"]),
                        )
                    lots = filled_lots
                    executable_price = _finite_or_default(paper_depth_fill.get("average_price"), executable_price)
                price = self._apply_paper_slippage(executable_price, side)
                entry_bid_value = _finite_or_default(entry_bbo.get("bid"), 0.0)
                entry_ask_value = _finite_or_default(entry_bbo.get("ask"), 0.0)
                atr_at_entry_value = _finite_or_default(ai_analysis.get("atr_at_entry"), 0.0)
                atr_periods_value = int(_finite_or_default(ai_analysis.get("atr_periods_at_entry"), 0.0))
                if atr_at_entry_value > 0.0 and atr_periods_value > 0:
                    refreshed_sl, refreshed_tp = self.calculate_atr_stop(
                        ticker, price, side,
                        atr_value=atr_at_entry_value,
                        atr_periods=atr_periods_value,
                        bid=entry_bid_value,
                        ask=entry_ask_value,
                    )
                    if refreshed_sl <= 0.0 or refreshed_tp <= 0.0:
                        return {
                            "status": "SKIPPED",
                            "message": "Не удалось пересчитать ATR-уровни от фактической цены PAPER-входа.",
                        }
                    ai_analysis["stop_loss"] = refreshed_sl
                    ai_analysis["take_profit"] = refreshed_tp
                ai_analysis["entry_bid"] = entry_bid_value
                ai_analysis["entry_ask"] = entry_ask_value
                ai_analysis["entry_spread_pct"] = _finite_or_default(entry_bbo.get("spread_pct"), 0.0)
                ai_analysis["entry_bbo_age_sec"] = max(
                    _finite_or_default(entry_bbo.get("bid_age_sec"), math.inf),
                    _finite_or_default(entry_bbo.get("ask_age_sec"), math.inf),
                )

            # Validate the final SL/TP from the executable entry before reserving portfolio risk.
            stop_loss = _finite_or_default(ai_analysis.get("stop_loss"), 0.0)
            take_profit = _finite_or_default(ai_analysis.get("take_profit"), 0.0)
            valid_levels = (
                stop_loss > 0 and take_profit > 0
                and ((side == "BUY" and stop_loss < price < take_profit)
                     or (side == "SELL" and take_profit < price < stop_loss))
            )
            if not valid_levels:
                stop_loss, take_profit = self.calculate_atr_stop(ticker, price, side)
            valid_levels = (
                stop_loss > 0 and take_profit > 0
                and ((side == "BUY" and stop_loss < price < take_profit)
                     or (side == "SELL" and take_profit < price < stop_loss))
            )
            if not valid_levels:
                return {
                    "status": "SKIPPED",
                    "message": "Недостаточно подтверждённой M1-истории для расчёта ATR; позиция не открыта.",
                }
            ai_analysis["stop_loss"] = stop_loss
            ai_analysis["take_profit"] = take_profit

            # Restore same-side stop pauses after a process restart. Never bypass
            # the guard if the persisted close journal cannot be read reliably.
            if self.paper_mode:
                try:
                    await self._hydrate_stop_loss_cooldown(db, ticker, side)
                except Exception as cooldown_exc:
                    logger.error(
                        "[STOP COOLDOWN HYDRATION FAILED] %s %s: %s; PAPER-вход заблокирован.",
                        ticker, side, cooldown_exc,
                    )
                    return {
                        "status": "SKIPPED",
                        "message": f"STOP_COOLDOWN_STATE_UNKNOWN: {cooldown_exc}",
                    }

            # Atomically reserve both committed capital and downside to the final SL.
            pre_risk_lots = lots
            approved_lots, reservation_info = await self._reserve_order_size(
                ticker, lots, price, db, side=side, stop_loss=stop_loss
            )
            if approved_lots <= 0:
                # Keep a single-line console readable if many candidates hit the
                # same capital guard during a fast market.
                now_mono = time.monotonic()
                if now_mono - self._last_portfolio_guard_log_at >= 15.0:
                    logger.warning("🛑 [PORTFOLIO GUARD] Ордер %s %s заблокирован: %s", side, ticker, reservation_info)
                    self._last_portfolio_guard_log_at = now_mono
                return {"status": "SKIPPED", "message": reservation_info}
            if approved_lots != lots:
                logger.warning(
                    "🛡️ [PORTFOLIO GUARD] Размер %s %s сокращён: requested=%d lots, approved=%d lots; %s",
                    side, ticker, lots, approved_lots, reservation_info,
                )
            lots = approved_lots
            reserved_ticker = True

            # The validated SL/TP has already been included in the portfolio stop-risk reservation.
            logger.info("📊 [ATR ENFORCER] Уровни: SL=%s₽ TP=%s₽; %s", stop_loss, take_profit, reservation_info)
            if approved_lots != pre_risk_lots:
                logger.warning(
                    "📉 [POSITION SIZE FINAL] %s %s: before_risk=%d lots/%d shares; approved=%d lots/%d shares; notional=%.2f ₽; reduction_reason=PORTFOLIO_CAP; %s",
                    side, ticker, pre_risk_lots, pre_risk_lots * lot_multiplier,
                    approved_lots, approved_lots * lot_multiplier,
                    approved_lots * lot_multiplier * price, reservation_info,
                )
            else:
                logger.info(
                    "📐 [POSITION SIZE FINAL] %s %s: requested=approved=%d lots/%d shares; notional=%.2f ₽; %s",
                    side, ticker, approved_lots, approved_lots * lot_multiplier,
                    approved_lots * lot_multiplier * price, reservation_info,
                )

            # Recheck immediately before creating a PAPER position or sending a LIVE order.
            # This catches a session boundary crossed during inference/BBO/size preparation.
            session_allowed, session_message, _session_state = await self._check_entry_session(ticker)
            if not session_allowed:
                logger.warning(
                    "⛔ [ENTRY SESSION GUARD] %s %s: повторная проверка запретила вход — %s.",
                    side, ticker, session_message,
                )
                return {"status": "SKIPPED", "message": f"ENTRY_SESSION_BLOCK_RECHECK: {session_message}"}

            trade_resp: object | None = None
            if not self.paper_mode:
                if self.broker is None:
                    logger.error("🛑 Нативный брокер Finam не подключён.")
                    return {"status": "FAILED", "message": "Broker client is None"}
                trade_resp = await self.broker.place_order(ticker, lots, side, 0.0, client_id)
                if not trade_resp:
                    return {
                        "status": "FAILED",
                        "message": "Finam не подтвердил принятие заявки; позиция не зарегистрирована.",
                    }

            if self.paper_mode:
                order_id = f"PAPER_{int(datetime.now().timestamp() * 1000)}_{uuid4().hex[:8]}"
            else:
                # Broker SDK typing is intentionally treated as object: only a Mapping
                # response is accepted, and its ID is extracted by a guarded helper.
                if not isinstance(trade_resp, Mapping):
                    return {
                        "status": "FAILED",
                        "message": "Finam вернул пустой или некорректный ответ на заявку.",
                    }
                order_id = _extract_order_id(trade_resp)
            if not order_id:
                return {"status": "FAILED", "message": "В ответе Finam отсутствует ID заявки."}

            # Store an offset-aware entry instant so time-of-day cohorts and
            # chronological diagnostics never have to guess the machine timezone.
            now = datetime.now().astimezone()
            atr_at_entry = _finite_or_default(ai_analysis.get("atr_at_entry"), 0.0)
            try:
                atr_periods_at_entry = max(0, int(ai_analysis.get("atr_periods_at_entry", 0) or 0))
            except (TypeError, ValueError, OverflowError):
                atr_periods_at_entry = 0
            atr_multiplier_at_entry = _finite_or_default(
                ai_analysis.get("atr_multiplier", os.getenv("ATR_MULTIPLIER", "1.5")), 1.5
            )
            entry_bid = _finite_or_default(ai_analysis.get("entry_bid"), 0.0)
            entry_ask = _finite_or_default(ai_analysis.get("entry_ask"), 0.0)
            entry_spread_pct = _finite_or_default(ai_analysis.get("entry_spread_pct"), 0.0)
            entry_bbo_age_sec = _finite_or_default(ai_analysis.get("entry_bbo_age_sec"), 0.0)
            entry_notional = lots * lot_multiplier * price
            entry_commission = (entry_notional * self.paper_commission_pct / 100.0) if self.paper_mode else 0.0
            if entry_bbo is not None:
                entry_quote_size = _finite_or_default(
                    entry_bbo.get("ask_size") if side == "BUY" else entry_bbo.get("bid_size"), 0.0
                )
            else:
                entry_quote_size = 0.0
            entry_diagnostics = build_entry_execution_diagnostics(
                side=side,
                quantity=lots * lot_multiplier,
                fill_price=price,
                executable_bbo_price=(
                    entry_bbo.get("executable_price") if entry_bbo is not None else None
                ),
                bid=entry_bid,
                ask=entry_ask,
                bid_age_sec=(
                    entry_bbo.get("bid_age_sec") if entry_bbo is not None else None
                ),
                ask_age_sec=(
                    entry_bbo.get("ask_age_sec") if entry_bbo is not None else None
                ),
                atr=atr_at_entry,
                signal_reference_price=signal_reference_price,
                depth_vwap=(
                    paper_depth_fill.get("average_price")
                    if isinstance(paper_depth_fill, Mapping) else None
                ),
            )
            # Finam's protobuf exposes buy_size/sell_size as decimals, but the
            # documented schema does not specify whether these quantities are
            # shares or lots. Keep the raw value for diagnostics; do not report a
            # fabricated percentage coverage or use it as a fill constraint.
            stop_distance_abs = abs(price - stop_loss)
            trade_log = {
                "id": order_id,
                "status": "OPEN",
                "mode": "PAPER" if self.paper_mode else "LIVE",
                "paper_execution_model": (
                    "BBO_DEPTH_PARTIAL_ENTRY_V1"
                    if self.paper_mode and self.paper_book_size_unit in {"shares", "lots"}
                    else "BBO_V8" if self.paper_mode else None
                ),
                "paper_fill_assumption": (
                    "DEPTH_LIMITED_PARTIAL_ENTRY_FULL_BBO_EXIT"
                    if self.paper_mode and self.paper_book_size_unit in {"shares", "lots"}
                    else "FULL_FILL_AT_TOP_OF_BOOK_DEPTH_UNIT_UNCONFIRMED" if self.paper_mode else None
                ),
                "entry_requested_lots": requested_lots if self.paper_mode else None,
                "entry_filled_lots": lots if self.paper_mode else None,
                "entry_fill_ratio": round(lots / requested_lots, 4) if self.paper_mode and requested_lots > 0 else None,
                "entry_depth_fill_vwap": (
                    round(float(paper_depth_fill["average_price"]), 6)
                    if self.paper_mode and paper_depth_fill is not None else None
                ),
                "entry_depth_levels_used": (
                    int(paper_depth_fill["levels_used"])
                    if self.paper_mode and paper_depth_fill is not None else None
                ),
                "session_id": self.session_id,
                "bbo_replay_session_id": str(
                    getattr(self, "bbo_replay_session_id", "") or ""
                ) or None,
                **_decision_attribution_fields(ai_analysis),
                "ticker": ticker,
                "entry_time": now.strftime("%H:%M:%S"),
                "entry_timestamp": now.isoformat(timespec="milliseconds"),
                "entry_time_epoch": now.timestamp(),
                "entry_price": price,
                "current_price": price,
                "side": side,
                "lots": lots,
                "volume": lots * lot_multiplier,  # число акций, не число лотов
                "lot_size": lot_multiplier,
                "notional_entry_rub": round(entry_notional, 2),
                "planned_risk_rub": round(
                    self._stop_risk_with_costs(price, stop_loss, lots * lot_multiplier, side), 2
                ),
                "entry_commission_rub": round(entry_commission, 4),
                # Preserve the initial position size for later partial-exit
                # accounting. At entry, the accepted lots are the original size.
                "original_volume": int(lots * lot_multiplier),
                "original_lots": int(lots),
                "commission_pct_per_side": self.paper_commission_pct if self.paper_mode else 0.0,
                "paper_slippage_bps": self.paper_slippage_bps if self.paper_mode else 0.0,
                "entry_executable_side": entry_bbo.get("executable_side") if entry_bbo is not None else None,
                "entry_top_size_raw": entry_quote_size,
                "entry_top_size_unit": (
                    self.paper_book_size_unit
                    if self.paper_mode and self.paper_book_size_unit in {"shares", "lots"}
                    else "FINAM_UNCONFIRMED"
                ),
                "entry_depth_units_note": (
                    "Configured by operator; depth participation is capped and applied to entry fills."
                    if self.paper_mode and self.paper_book_size_unit in {"shares", "lots"}
                    else "Finam ORDER_BOOK size unit not confirmed; raw quantity is informational only."
                ),
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "atr_at_entry": atr_at_entry,
                "atr_periods_at_entry": atr_periods_at_entry,
                "atr_multiplier": atr_multiplier_at_entry,
                "stop_distance_rub": round(stop_distance_abs, 6),
                "stop_distance_pct": round(stop_distance_abs / price * 100.0, 6),
                "stop_distance_atr": round(stop_distance_abs / atr_at_entry, 4) if atr_at_entry > 0.0 else None,
                "entry_bid": entry_bid,
                "entry_ask": entry_ask,
                "entry_spread_pct": entry_spread_pct,
                "entry_bbo_age_sec": entry_bbo_age_sec,
                **entry_diagnostics,
                "max_favorable_excursion_pct": 0.0,
                "max_adverse_excursion_pct": 0.0,
                "max_favorable_excursion_r": 0.0,
                "max_adverse_excursion_r": 0.0,
                "max_favorable_excursion_rub": 0.0,
                "max_adverse_excursion_rub": 0.0,
                "max_favorable_bbo_excursion_pct": 0.0,
                "max_adverse_bbo_excursion_pct": 0.0,
                "max_favorable_bbo_excursion_rub": 0.0,
                "max_adverse_bbo_excursion_rub": 0.0,
                "shadow_be_enabled": self.shadow_be_enabled,
                "shadow_be_arm_r": self.shadow_be_arm_r,
                "shadow_be_armed": False,
                "shadow_be_triggered": False,
                "shadow_be_trigger_price": None,
                "shadow_be_trigger_timestamp": None,
                "shadow_be_trigger_bbo_price": None,
                "shadow_be_estimated_net_pnl_rub": None,
                "shadow_be_favorable_r_at_arm": None,
                "pnl": 0.0,
                "pnl_percent": 0.0,
                "final_outcome": "PENDING",
                "tactic_opinion": ai_analysis.get("tactic_opinion", ""),
                "critic_opinion": ai_analysis.get("critic_opinion", ""),
                "confidence_score": ai_analysis.get("confidence_score", 100),
            }

            # PAPER entry commit and emergency-close latch share the portfolio
            # lock. If a panic close starts while an entry is being prepared, the
            # uncommitted entry is rejected; if persistence has begun, the close
            # waits until the position is visible in paper_positions.
            if self.paper_mode:
                async with self._portfolio_lock:
                    if self._maintenance_active:
                        return {
                            "status": "SKIPPED",
                            "message": (
                                self._maintenance_reason
                                or "PAPER-вход отменён: запущено экстренное закрытие."
                            ),
                        }
                    await db.log_trade(order_id, trade_log)
                    self.paper_positions[ticker] = trade_log
            else:
                # A LIVE broker fill must always be journaled, even if a PAPER-only
                # maintenance flag changes while the broker response is in flight.
                await db.log_trade(order_id, trade_log)
            if self.paper_mode and entry_bbo is not None:
                logger.info(
                    "🧾 [PAPER FILL] %s %s: requested=%d lots, filled=%d lots; last_reference=%.4f, "
                    "executable_%s=%.4f, fill=%.4f, top_size_raw=%.2f unit=%s fill_model=%s, "
                    "commission=%.4f ₽, slippage=%.2f bps.",
                    ticker, side, requested_lots, lots, signal_reference_price,
                    str(entry_bbo.get("executable_side", "BBO")),
                    _finite_or_default(entry_bbo.get("executable_price"), 0.0), price,
                    entry_quote_size,
                    self.paper_book_size_unit if self.paper_book_size_unit in {"shares", "lots"} else "unconfirmed",
                    "CONFIRMED_DEPTH" if self.paper_book_size_unit in {"shares", "lots"} else "FULL_TOP_OF_BOOK_UNVERIFIED",
                    entry_commission, self.paper_slippage_bps,
                )
            logger.info("✅ [EXECUTION] Позиция %s по %s записана; mode=%s.",
                        order_id, ticker, trade_log["mode"])
            return {
                "status": "SUCCESS",
                "orderId": order_id,
                "requested_lots": requested_lots,
                "lots": lots,
                "shares": lots * lot_multiplier,
                "lot_size": lot_multiplier,
                "notional_entry_rub": trade_log["notional_entry_rub"],
                "entry_price": price,
                "entry_commission_rub": round(entry_commission, 4),
                "side": side,
                "ticker": ticker,
                "mode": trade_log["mode"],
            }

        except Exception as e:
            logger.error("❗ Критический сбой исполнения ордера по %s: %s", ticker, e, exc_info=True)
            return {"status": "FAILED", "message": str(e)}
        finally:
            if reserved_ticker:
                async with self._portfolio_lock:
                    self._opening_tickers.discard(ticker)
                    self._reserved_notional.pop(ticker, None)
                    self._reserved_stop_risk.pop(ticker, None)
