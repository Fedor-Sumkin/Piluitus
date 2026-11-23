"""Stable public execution engine facade composed from focused runtime mixins."""
from __future__ import annotations

from execution.execution_shared import *  # noqa: F401,F403
from execution.execution_position_risk import ExecutionPositionRiskMixin
from execution.execution_market_session import ExecutionMarketSessionMixin
from execution.execution_paper_mechanics import ExecutionPaperMechanicsMixin
from execution.execution_order_runtime import ExecutionOrderRuntimeMixin
from execution.paper_position_exit_runtime import PaperPositionExitRuntimeMixin


class BaseExecutionEngine(
    ExecutionPositionRiskMixin,
    ExecutionMarketSessionMixin,
    ExecutionPaperMechanicsMixin,
    ExecutionOrderRuntimeMixin,
    PaperPositionExitRuntimeMixin,
):
    def __init__(
        self,
        finam_broker_client: BrokerClientProtocol | None = None,
        screener: ScreenerProtocol | None = None,
        initial_capital: float | None = None,
        max_gross_exposure_pct: float | None = None,
        max_single_position_pct: float | None = None,
        stop_loss_cooldown_sec: float | None = None,
        max_open_stop_risk_pct: float | None = None,
    ):
        self.broker = finam_broker_client
        self.screener = screener
        self.paper_mode = bool(getattr(Settings, "PAPER_TRADING", PAPER_TRADING))

        try:
            self.initial_capital = max(0.0, float(
                initial_capital if initial_capital is not None else Settings.TRADE_DEPOSIT
            ))
        except (TypeError, ValueError):
            self.initial_capital = 100000.0
        # Default committed capital is 10%; hard ceiling remains 15% of deposit.
        # Clamp explicitly configured legacy values (1.0 / 0.20) to ensure
        # stale .env settings cannot silently restore aggressive exposure.
        requested_gross_pct = (
            max_gross_exposure_pct
            if max_gross_exposure_pct is not None
            else os.getenv("PILUITUS_MAX_GROSS_EXPOSURE_PCT", "0.10")
        )
        self.max_gross_exposure_pct = self._bounded_pct(
            requested_gross_pct,
            default=0.10,
            maximum=MAX_ALLOWED_GROSS_EXPOSURE_PCT,
        )
        requested_single_pct = (
            max_single_position_pct
            if max_single_position_pct is not None
            else os.getenv("PILUITUS_MAX_SINGLE_POSITION_PCT", "0.03")
        )
        self.max_single_position_pct = self._bounded_pct(
            requested_single_pct,
            default=MAX_ALLOWED_SINGLE_POSITION_PCT,
            maximum=min(self.max_gross_exposure_pct, MAX_ALLOWED_SINGLE_POSITION_PCT),
        )
        requested_open_stop_risk_pct = (
            max_open_stop_risk_pct
            if max_open_stop_risk_pct is not None
            else os.getenv("PILUITUS_MAX_OPEN_STOP_RISK_PCT", "0.01")
        )
        self.max_open_stop_risk_pct = self._bounded_pct(
            requested_open_stop_risk_pct, default=0.01, maximum=0.05
        )
        try:
            cooldown = float(
                stop_loss_cooldown_sec if stop_loss_cooldown_sec is not None
                else os.getenv("PILUITUS_STOP_LOSS_COOLDOWN_SEC", "180")
            )
            self.stop_loss_cooldown_sec = max(0.0, cooldown) if math.isfinite(cooldown) else 180.0
        except (TypeError, ValueError):
            self.stop_loss_cooldown_sec = 180.0
        try:
            self.repeat_stop_window_sec = max(
                60.0, _finite_or_default(os.getenv("PILUITUS_REPEAT_SL_WINDOW_SEC", "1800"), 1800.0)
            )
            self.repeat_stop_limit = max(2, int(os.getenv("PILUITUS_REPEAT_SL_LIMIT", "2")))
            self.repeat_stop_cooldown_sec = max(
                60.0, _finite_or_default(os.getenv("PILUITUS_REPEAT_SL_COOLDOWN_SEC", "900"), 900.0)
            )
        except (TypeError, ValueError):
            self.repeat_stop_window_sec = 1800.0
            self.repeat_stop_limit = 2
            self.repeat_stop_cooldown_sec = 900.0
        try:
            self.shadow_be_enabled = os.getenv("PILUITUS_SHADOW_BE_ENABLED", "true").strip().lower() in {"1", "true", "yes", "on"}
            self.shadow_be_arm_r = min(
                3.0, max(0.25, _finite_or_default(os.getenv("PILUITUS_SHADOW_BE_ARM_R", "1.0"), 1.0))
            )
        except (TypeError, ValueError):
            self.shadow_be_enabled = True
            self.shadow_be_arm_r = 1.0

        # Fees are explicit configuration: never guess the user's Finam tariff.
        # Percentage is charged on each side (entry and exit). Slippage is an
        # additional adverse adjustment beyond the executable Bid/Ask price.
        self.paper_commission_pct = min(
            1.0, max(0.0, _finite_or_default(
                os.getenv("PILUITUS_PAPER_COMMISSION_PCT", str(getattr(Settings, "PAPER_COMMISSION_PCT", 0.0354))),
                0.0354,
            ))
        )
        self.paper_slippage_bps = min(
            100.0, max(0.0, _finite_or_default(os.getenv("PILUITUS_PAPER_SLIPPAGE_BPS", "0"), 0.0))
        )
        # Depth-based partial fills stay disabled until the Finam ORDER_BOOK size
        # unit is explicitly confirmed. The official schema exposes Decimal sizes
        # but does not define shares-vs-lots semantics.
        configured_book_unit = str(getattr(Settings, "PAPER_BOOK_SIZE_UNIT", "unknown")).strip().lower()
        self.paper_book_size_unit = configured_book_unit if configured_book_unit in {"shares", "lots"} else "unknown"
        self.paper_max_book_participation_pct = min(
            100.0,
            max(1.0, _finite_or_default(
                getattr(Settings, "PAPER_MAX_BOOK_PARTICIPATION_PCT", 100.0), 10.0
            )),
        )

        logger.info(
            "[PAPER DEPTH MODEL] book_size_unit=%s max_participation=%.1f%% "
            "partial_exit=%s; unit=unknown keeps the explicitly unverified BBO fallback",
            self.paper_book_size_unit,
            self.paper_max_book_participation_pct,
            self.paper_book_size_unit in {"shares", "lots"},
        )

        # ОЗУ-структуры PAPER-позиций.
        self.paper_positions: dict[str, TradeRecord] = {}
        self.closed_trades: list[TradeRecord] = []
        self._lock_closing: set[str] = set()
        self._opening_tickers: set[str] = set()
        self._reserved_notional: dict[str, float] = {}
        self._reserved_stop_risk: dict[str, float] = {}
        self._portfolio_lock = asyncio.Lock()
        # Cooldowns are side-specific: a short stop does not suppress a valid long setup.
        self._ticker_cooldowns: dict[tuple[str, str], float] = {}
        # Repeated capital/risk-limit rejections are briefly suppressed per ticker/side.
        self._capital_reject_cooldowns: dict[tuple[str, str], float] = {}
        self._recent_stop_losses: dict[tuple[str, str], deque[float]] = {}
        self._hydrated_stop_loss_keys: set[tuple[str, str]] = set()
        self._pending_monitor_prices: dict[str, tuple[float, str, object]] = {}
        self._monitor_worker_task: asyncio.Task | None = None
        self._last_bbo_exit_warning_at: dict[str, float] = {}
        try:
            self.paper_exit_status_log_interval_sec = min(
                900.0,
                max(
                    60.0,
                    _finite_or_default(
                        os.getenv("PILUITUS_PAPER_EXIT_STATUS_LOG_INTERVAL_SEC", "300"),
                        300.0,
                    ),
                ),
            )
        except (TypeError, ValueError, OverflowError):
            self.paper_exit_status_log_interval_sec = 300.0
        # Per-order/event throttling keeps repeated BBO gaps visible in metrics
        # without flooding the console with every stale/fresh transition.
        self._last_exit_status_log_at: dict[str, float] = {}
        # Old PAPER positions cannot be resumed safely after restart because the
        # simulator does not restore their monitor state. Keep them visible as
        # unresolved committed capital and fail closed until they are reviewed.
        self._unreconciled_paper_positions: dict[str, TradeRecord] = {}
        # Explicit service lock used during a PAPER-session maintenance/reset.
        # It is fail-closed and released only by the dedicated resume endpoint.
        self._maintenance_active = False
        self._maintenance_reason = ""
        self._last_portfolio_guard_log_at = 0.0
        self.session_id = datetime.now().strftime("%Y%m%d-%H%M%S-%f")

        logger.info(
            "🛡️ [PORTFOLIO GUARD] capital=%.2f ₽ gross_cap=%.0f%% single_position_cap=%.0f%% "
            "open_stop_risk_cap=%.0f%% stop_cooldown=%.0fs repeat_SL=%d/%.0fs->%.0fs shadow_BE=%s@%.2fR",
            self.initial_capital,
            self.max_gross_exposure_pct * 100,
            self.max_single_position_pct * 100,
            self.max_open_stop_risk_pct * 100,
            self.stop_loss_cooldown_sec,
            self.repeat_stop_limit, self.repeat_stop_window_sec,
            self.repeat_stop_cooldown_sec, self.shadow_be_enabled, self.shadow_be_arm_r,
        )
        if self.paper_mode:
            logger.info("🛡️ РЕЖИМ АГЕНТА: [PAPER TRADING] — Симуляция ордеров через Redis ОЗУ.")
            logger.info(
                "🧪 [PAPER EXECUTION MODEL] Вход BUY=Ask/SELL=Bid, выход long=Bid/short=Ask; "
                "спред учитывается через стороны BBO; depth_unit=%s, participation=%.1f%%.",
                self.paper_book_size_unit, self.paper_max_book_participation_pct,
            )
            if self.paper_book_size_unit in {"shares", "lots"}:
                logger.warning(
                    "🧩 [PAPER DEPTH MODEL] Частичное исполнение включено: unit=%s, "
                    "лимит участия в каждом уровне=%.1f%%; depth-fill применяется и на выходе.",
                    self.paper_book_size_unit, self.paper_max_book_participation_pct,
                )
            else:
                logger.warning(
                    "⚠️ [PAPER DEPTH MODEL DISABLED] Единицы buy_size/sell_size Finam не подтверждены; "
                    "для входов и выходов сохраняется допущение полного fill по top-of-book. "
                    "Не устанавливайте unit=shares/lots без проверки данных."
                )
            if self.paper_commission_pct <= 0.0 or self.paper_slippage_bps <= 0.0:
                logger.warning(
                    "⚠️ [PAPER COST MODEL INCOMPLETE] комиссия=%.4f%% за сторону, доп.проскальзывание=%.2f bps. "
                    "При нулевом параметре PnL не является полностью net-of-costs; укажите подтверждённый тариф Finam "
                    "и обоснованную дополнительную модель проскальзывания в .env. Спред уже включён в Ask/Bid.",
                    self.paper_commission_pct, self.paper_slippage_bps,
                )
            else:
                logger.info(
                    "🧾 [PAPER COST MODEL] комиссия=%.4f%% за сторону, дополнительное проскальзывание=%.2f bps; "
                    "проверьте соответствие действующему тарифу Finam.",
                    self.paper_commission_pct, self.paper_slippage_bps,
                )
        else:
            logger.warning("🔥 РЕЖИМ АГЕНТА: [LIVE TRADING] — Работа на реальном счёте Finam!")

    # These policy boundary methods intentionally remain on this facade.
    # Existing integrations patch execution.execution_layer dependencies
    # (datetime, time, FINAM_TRADE_LOT_SIZES) in tests and operational harnesses.

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

    @staticmethod
    def _entry_cutoff_minutes() -> float:
        value = _finite_or_default(os.getenv("PILUITUS_ENTRY_CUTOFF_MINUTES", "5"), 5.0)
        return min(30.0, max(0.0, value))

    @staticmethod
    def _entry_hard_cutoff_msk(now: datetime) -> datetime | None:
        """Return the configured hard cutoff for new entries on this Moscow date."""
        raw = str(os.getenv("PILUITUS_ENTRY_HARD_CUTOFF_MSK", getattr(
            Settings, "ENTRY_HARD_CUTOFF_MSK", "22:00"
        )) or "22:00").strip()
        try:
            clock = datetime_time.fromisoformat(raw)
        except (TypeError, ValueError):
            logger.error(
                "[ENTRY HARD CUTOFF CONFIG] invalid PILUITUS_ENTRY_HARD_CUTOFF_MSK=%r; "
                "using safe default 22:00 MSK", raw
            )
            clock = datetime_time(22, 0)
        return datetime.combine(now.date(), clock, tzinfo=_MOSCOW_TZ)

    async def _market_session_status(self, ticker: str) -> dict[str, Any]:
        """Read the current instrument-specific Finam schedule in Moscow time."""
        getter = getattr(self.broker, "get_asset_schedule", None)
        if not callable(getter):
            return {"known": False, "is_open": False, "entry_allowed": False, "reason": "schedule_api_unavailable"}
        try:
            result = getter(ticker)
            payload = await result if inspect.isawaitable(result) else result
        except Exception as exc:
            return {
                "known": False, "is_open": False, "entry_allowed": False,
                "reason": f"schedule_request_failed:{type(exc).__name__}",
            }
        if not isinstance(payload, Mapping) or not isinstance(payload.get("sessions"), list):
            return {"known": False, "is_open": False, "entry_allowed": False, "reason": "schedule_missing_or_invalid"}

        now = datetime.now(_MOSCOW_TZ)
        parsed_windows: list[tuple[datetime, datetime, str]] = []
        for raw_session in payload.get("sessions", []):
            if not isinstance(raw_session, Mapping):
                continue
            interval_obj = raw_session.get("interval")
            if not isinstance(interval_obj, Mapping):
                continue
            start = _parse_schedule_datetime(interval_obj.get("start_time"), now.date())
            end = _parse_schedule_datetime(interval_obj.get("end_time"), now.date())
            if start is None or end is None or end <= start:
                continue
            if start.date() <= now.date() <= end.date():
                parsed_windows.append((start, end, str(raw_session.get("type") or "unknown")))

        if payload.get("sessions") and not parsed_windows:
            return {
                "known": False, "is_open": False, "entry_allowed": False,
                "reason": "schedule_intervals_unparseable_or_not_for_today",
            }
        if not parsed_windows:
            return {
                "known": True, "is_open": False, "entry_allowed": False,
                "reason": "no_sessions_today", "session_type": "",
            }

        active = [(start, end, kind) for start, end, kind in parsed_windows if start <= now < end]
        if active:
            start, provider_end, kind = min(active, key=lambda item: item[1])
            hard_cutoff = self._entry_hard_cutoff_msk(now)
            effective_end = min(provider_end, hard_cutoff) if hard_cutoff is not None else provider_end
            seconds_to_close = max(0.0, (effective_end - now).total_seconds())
            cutoff_minutes = self._entry_cutoff_minutes()
            allowed = seconds_to_close > cutoff_minutes * 60.0
            hard_cutoff_reached = hard_cutoff is not None and now >= hard_cutoff
            reason = (
                "hard_entry_cutoff_reached" if hard_cutoff_reached
                else "active_session" if allowed
                else "entry_cutoff_reached"
            )
            return {
                "known": True, "is_open": True, "entry_allowed": allowed,
                "reason": reason, "session_type": kind,
                "session_start": start.isoformat(),
                "session_end": provider_end.isoformat(),
                "entry_hard_cutoff": hard_cutoff.isoformat() if hard_cutoff else None,
                "seconds_to_close": seconds_to_close,
                "cutoff_minutes": cutoff_minutes,
            }

        future = [(start, end, kind) for start, end, kind in parsed_windows if start > now]
        next_start = min(future, key=lambda item: item[0])[0] if future else None
        return {
            "known": True, "is_open": False, "entry_allowed": False,
            "reason": "between_or_outside_sessions",
            "next_session_start": next_start.isoformat() if next_start else None,
            "session_type": "",
        }
