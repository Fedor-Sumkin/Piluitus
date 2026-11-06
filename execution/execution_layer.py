from __future__ import annotations

import logging
import os
import inspect
from collections import deque
from collections.abc import Iterable, Mapping
from typing import Any, Protocol, TypedDict, cast
import math
import time
import numpy as np
from datetime import datetime, timezone, timedelta, time as datetime_time
from db_manager import DBManager
import asyncio
from uuid import uuid4

# Единый справочник лотности хранится в config.py.
from config import LOT_SIZES

logger = logging.getLogger("Piluitus.ExecutionLayer")


class BrokerClientProtocol(Protocol):
    async def place_order(
        self, ticker: str, lots: int, side: str, price: float, client_id: str
    ) -> object: ...

    async def get_asset_schedule(self, ticker: str) -> dict[str, Any] | None: ...


class MinuteBarPatternProtocol(Protocol):
    def get_atr_snapshot(self, ticker: str, period: int = 14) -> dict[str, float]: ...


class ScreenerProtocol(Protocol):
    history: dict[str, dict[str, Any]]
    quotes: dict[str, dict[str, Any]]

    @property
    def minute_bar_patterns(self) -> MinuteBarPatternProtocol: ...


class BBOExitQuote(TypedDict):
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    bid_levels: list[list[float]]
    ask_levels: list[list[float]]
    bid_age_sec: float
    ask_age_sec: float
    age_sec: float
    executable_price: float
    executable_side: str
    spread_pct: float
    max_age_sec: float


TradeRecord = dict[str, Any]
_MOSCOW_TZ = timezone(timedelta(hours=3), name="MSK")


def _parse_schedule_datetime(value: object, session_date) -> datetime | None:
    """Parse Finam REST Timestamp, ISO datetime or local HH:MM[:SS] safely."""
    parsed: datetime | None = None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, Mapping):
        seconds_raw = value.get("seconds")
        nanos_raw = value.get("nanos", 0)
        if seconds_raw is not None:
            try:
                seconds = float(seconds_raw) + float(nanos_raw or 0.0) / 1_000_000_000.0
                if math.isfinite(seconds):
                    parsed = datetime.fromtimestamp(seconds, tz=timezone.utc)
            except (TypeError, ValueError, OverflowError, OSError):
                parsed = None
        if parsed is None and value.get("value") is not None:
            return _parse_schedule_datetime(value.get("value"), session_date)
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            seconds = float(value)
            if seconds >= 1e18:
                seconds /= 1_000_000_000.0
            elif seconds >= 1e15:
                seconds /= 1_000_000.0
            elif seconds >= 1e12:
                seconds /= 1_000.0
            if math.isfinite(seconds):
                parsed = datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (TypeError, ValueError, OverflowError, OSError):
            parsed = None
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            numeric = float(raw)
        except ValueError:
            numeric = math.nan
        if math.isfinite(numeric):
            return _parse_schedule_datetime(numeric, session_date)
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            try:
                clock = datetime_time.fromisoformat(raw)
                parsed = datetime.combine(session_date, clock)
            except ValueError:
                return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_MOSCOW_TZ)
    try:
        return parsed.astimezone(_MOSCOW_TZ)
    except (ValueError, OverflowError):
        return None


def _finite_or_default(value: object, default: float = 0.0) -> float:
    """Convert only known scalar input types; reject arbitrary objects safely."""
    if not isinstance(value, (int, float, str)):
        return default
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError, OverflowError):
        return default


def _as_string_mapping(value: object) -> Mapping[str, object] | None:
    """Narrow untrusted dynamic data before any mapping operation."""
    if not isinstance(value, Mapping):
        return None
    return cast(Mapping[str, object], value)


def _extract_order_id(response: object) -> str:
    """Safely read an order identifier from an untyped broker response."""
    response_map = _as_string_mapping(response)
    if response_map is None:
        return ""
    raw_order_id = response_map.get("orderId") or response_map.get("order_id") or ""
    return str(raw_order_id).strip()


# Hard safety ceilings: legacy .env values such as 1.0/0.20 are clamped to these.
MAX_ALLOWED_GROSS_EXPOSURE_PCT = 0.15
MAX_ALLOWED_SINGLE_POSITION_PCT = 0.05


class BaseExecutionEngine:
    """
    Основной исполнительный движок проекта PILUITUS V3.0.
    Управляет логикой открытия и ведения позиций в LIVE и PAPER режимах.
    """
    def __init__(
        self,
        finam_broker_client: BrokerClientProtocol | None = None,
        screener: ScreenerProtocol | None = None,
        initial_capital: float | None = None,
        max_gross_exposure_pct: float | None = None,
        max_single_position_pct: float | None = None,
        stop_loss_cooldown_sec: float | None = None,
    ):
        self.broker = finam_broker_client
        self.screener = screener
        self.paper_mode = os.getenv("PAPER_TRADING", "True").strip().lower() in ("true", "1", "yes")

        try:
            self.initial_capital = max(0.0, float(
                initial_capital if initial_capital is not None else os.getenv("TRADING_DEPOSIT", "100000")
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
            else os.getenv("PILUITUS_MAX_SINGLE_POSITION_PCT", "0.05")
        )
        self.max_single_position_pct = self._bounded_pct(
            requested_single_pct,
            default=MAX_ALLOWED_SINGLE_POSITION_PCT,
            maximum=min(self.max_gross_exposure_pct, MAX_ALLOWED_SINGLE_POSITION_PCT),
        )
        try:
            cooldown = float(
                stop_loss_cooldown_sec if stop_loss_cooldown_sec is not None
                else os.getenv("PILUITUS_STOP_LOSS_COOLDOWN_SEC", "60")
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
            1.0, max(0.0, _finite_or_default(os.getenv("PILUITUS_PAPER_COMMISSION_PCT", "0"), 0.0))
        )
        self.paper_slippage_bps = min(
            100.0, max(0.0, _finite_or_default(os.getenv("PILUITUS_PAPER_SLIPPAGE_BPS", "0"), 0.0))
        )

        # ОЗУ-структуры PAPER-позиций.
        self.paper_positions: dict[str, TradeRecord] = {}
        self.closed_trades: list[TradeRecord] = []
        self._lock_closing: set[str] = set()
        self._opening_tickers: set[str] = set()
        self._reserved_notional: dict[str, float] = {}
        self._portfolio_lock = asyncio.Lock()
        # Cooldowns are side-specific: a short stop does not suppress a valid long setup.
        self._ticker_cooldowns: dict[tuple[str, str], float] = {}
        self._recent_stop_losses: dict[tuple[str, str], deque[float]] = {}
        self._pending_monitor_prices: dict[str, float] = {}
        self._monitor_worker_task: asyncio.Task | None = None
        self._last_bbo_exit_warning_at: dict[str, float] = {}
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
            "stop_cooldown=%.0fs repeat_SL=%d/%0.fs->%.0fs shadow_BE=%s@%.2fR",
            self.initial_capital,
            self.max_gross_exposure_pct * 100,
            self.max_single_position_pct * 100,
            self.stop_loss_cooldown_sec,
            self.repeat_stop_limit, self.repeat_stop_window_sec,
            self.repeat_stop_cooldown_sec, self.shadow_be_enabled, self.shadow_be_arm_r,
        )
        if self.paper_mode:
            logger.info("🛡️ РЕЖИМ АГЕНТА: [PAPER TRADING] — Симуляция ордеров через Redis ОЗУ.")
            logger.info(
                "🧪 [PAPER EXECUTION MODEL] Вход BUY=Ask/SELL=Bid, выход long=Bid/short=Ask; "
                "спред учитывается через стороны BBO; полное исполнение на top-of-book предполагается, "
                "а объём стакана не ограничивает fill, т.к. единицы Finam ещё не подтверждены."
            )
            if self.paper_commission_pct <= 0.0 or self.paper_slippage_bps <= 0.0:
                logger.warning(
                    "⚠️ [PAPER COST SETTINGS] комиссия=%.4f%% за сторону, доп.проскальзывание=%.2f bps. "
                    "Нулевые параметры не учитываются в PnL; укажите собственный тариф Finam и модель "
                    "дополнительного проскальзывания в .env. Спред уже включён в Ask/Bid.",
                    self.paper_commission_pct, self.paper_slippage_bps,
                )
        else:
            logger.warning("🔥 РЕЖИМ АГЕНТА: [LIVE TRADING] — Работа на реальном счёте Finam!")

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

    async def _reserve_order_size(
        self, ticker: str, requested_lots: int, price: float, db: DBManager, *, side: str
    ) -> tuple[int, str]:
        """Atomically cap order by risk-independent notional budget and reserve it against races."""
        async with self._portfolio_lock:
            if self._maintenance_active:
                return 0, self._maintenance_reason or "PAPER-торговля приостановлена для обслуживания сессии."
            now = time.monotonic()
            cooldown_key = (str(ticker).strip().upper().split("@")[0], str(side).strip().upper())
            cooldown_until = self._ticker_cooldowns.get(cooldown_key, 0.0)
            if cooldown_until > now:
                remaining = cooldown_until - now
                return 0, f"STOP_LOSS cooldown для {cooldown_key[1]} ещё {remaining:.0f} сек."
            self._ticker_cooldowns.pop(cooldown_key, None)

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
            already_reserved = sum(self._reserved_notional.values())
            gross_cap = self.initial_capital * self.max_gross_exposure_pct
            single_cap = self.initial_capital * self.max_single_position_pct
            headroom = max(0.0, min(gross_cap - open_notional - already_reserved, single_cap))
            lot_size = max(int(LOT_SIZES.get(ticker, 1) or 1), 1)
            notional_per_lot = price * lot_size
            if notional_per_lot <= 0 or not math.isfinite(notional_per_lot):
                return 0, "Некорректная стоимость лота."
            affordable_lots = int(math.floor((headroom + 1e-9) / notional_per_lot))
            approved_lots = min(int(requested_lots), affordable_lots)
            if approved_lots <= 0:
                return 0, (
                    f"Лимит экспозиции исчерпан/недостаточен: доступно {headroom:.2f} ₽, "
                    f"минимальный лот стоит {notional_per_lot:.2f} ₽; текущая экспозиция {open_notional:.2f} ₽ "
                    f"из лимита {gross_cap:.2f} ₽."
                )

            notional_reserved = approved_lots * notional_per_lot
            self._opening_tickers.add(ticker)
            self._reserved_notional[ticker] = notional_reserved
            return approved_lots, (
                f"notional={notional_reserved:.2f} ₽; gross_before={open_notional:.2f} ₽; "
                f"gross_cap={gross_cap:.2f} ₽; single_cap={single_cap:.2f} ₽"
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

        return round(sl, 4), round(tp, 4)

    def _latest_fresh_exit_quote(self, ticker: str, position_side: str) -> BBOExitQuote | None:
        """Return the fresh executable side needed to close this position.

        A long position exits by selling at Bid; a short position exits by buying
        at Ask. Requiring the *opposite* side to be fresh can unnecessarily defer
        a valid stop/target check, so freshness is evaluated independently for
        the executable side only.
        """
        if self.screener is None:
            return None
        try:
            quote = self.screener.quotes.get(ticker)
            if not isinstance(quote, Mapping):
                return None
            side = str(position_side or "").strip().upper()
            if side not in {"BUY", "SELL"}:
                return None
            bid = _finite_or_default(quote.get("bid", 0.0), 0.0)
            ask = _finite_or_default(quote.get("ask", 0.0), 0.0)
            bid_seen = _finite_or_default(
                quote.get("last_bid_confirmed_seen", quote.get("last_bid_seen", 0.0)), 0.0
            )
            ask_seen = _finite_or_default(
                quote.get("last_ask_confirmed_seen", quote.get("last_ask_seen", 0.0)), 0.0
            )
            now = time.monotonic()
            bid_age = now - bid_seen if bid_seen > 0.0 else math.inf
            ask_age = now - ask_seen if ask_seen > 0.0 else math.inf
            # A crossed book is not a valid executable market. Do not open/close
            # a simulated position against an internally inconsistent BBO.
            if bid > 0.0 and ask > 0.0 and ask < bid:
                return None
            # The screener TTL is the single source of truth for candidate
            # validation and PAPER execution; do not let two env settings drift.
            configured_age = getattr(self.screener, "bbo_max_age_sec", None)
            if configured_age is None:
                configured_age = os.getenv(
                    "PILUITUS_BBO_MAX_AGE_SEC",
                    os.getenv("PILUITUS_BBO_EXIT_MAX_AGE_SEC", "2.0"),
                )
            max_age = _finite_or_default(configured_age, 2.0)
            if not math.isfinite(max_age) or max_age <= 0.0:
                max_age = 2.0
            max_age = min(5.0, max(0.25, max_age))

            executable_price = bid if side == "BUY" else ask
            executable_age = bid_age if side == "BUY" else ask_age
            if (
                not math.isfinite(executable_price) or executable_price <= 0.0
                or executable_age < 0.0 or executable_age > max_age
            ):
                return None

            two_sided_valid = (
                math.isfinite(bid) and math.isfinite(ask)
                and bid > 0.0 and ask >= bid
                and bid_age >= 0.0 and ask_age >= 0.0
                and bid_age <= max_age and ask_age <= max_age
            )
            mid = (bid + ask) / 2.0 if two_sided_valid else 0.0
            spread_pct = ((ask - bid) / mid * 100.0) if mid > 0.0 else 0.0
            bid_levels_obj = quote.get("bid_levels", [])
            ask_levels_obj = quote.get("ask_levels", [])
            bid_levels = bid_levels_obj if isinstance(bid_levels_obj, list) else []
            ask_levels = ask_levels_obj if isinstance(ask_levels_obj, list) else []
            return {
                "bid": bid,
                "ask": ask,
                "bid_size": _finite_or_default(quote.get("bid_size", 0.0), 0.0),
                "ask_size": _finite_or_default(quote.get("ask_size", 0.0), 0.0),
                "bid_levels": cast(list[list[float]], bid_levels),
                "ask_levels": cast(list[list[float]], ask_levels),
                "bid_age_sec": bid_age,
                "ask_age_sec": ask_age,
                "age_sec": executable_age,
                "executable_price": executable_price,
                "executable_side": "BID" if side == "BUY" else "ASK",
                "spread_pct": spread_pct,
                "max_age_sec": max_age,
            }
        except (TypeError, ValueError, AttributeError, OverflowError):
            return None

    def _latest_fresh_entry_quote(self, ticker: str, entry_side: str) -> BBOExitQuote | None:
        """Require a fresh two-sided BBO and select Ask for BUY / Bid for SELL."""
        side = str(entry_side or "").strip().upper()
        if side not in {"BUY", "SELL"}:
            return None
        # Existing exit helper already applies the same per-side max-age policy.
        bid_quote = self._latest_fresh_exit_quote(ticker, "BUY")
        ask_quote = self._latest_fresh_exit_quote(ticker, "SELL")
        if bid_quote is None or ask_quote is None:
            return None
        if (
            float(bid_quote["bid_age_sec"]) > float(bid_quote["max_age_sec"])
            or float(ask_quote["ask_age_sec"]) > float(ask_quote["max_age_sec"])
        ):
            return None
        chosen = ask_quote if side == "BUY" else bid_quote
        return {
            **chosen,
            "executable_price": float(chosen["ask"] if side == "BUY" else chosen["bid"]),
            "executable_side": "ASK" if side == "BUY" else "BID",
            "age_sec": float(chosen["ask_age_sec"] if side == "BUY" else chosen["bid_age_sec"]),
        }

    @staticmethod
    def _entry_cutoff_minutes() -> float:
        value = _finite_or_default(os.getenv("PILUITUS_ENTRY_CUTOFF_MINUTES", "5"), 5.0)
        return min(30.0, max(0.0, value))

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
            start, end, kind = min(active, key=lambda item: item[1])
            seconds_to_close = max(0.0, (end - now).total_seconds())
            cutoff_minutes = self._entry_cutoff_minutes()
            allowed = seconds_to_close > cutoff_minutes * 60.0
            return {
                "known": True, "is_open": True, "entry_allowed": allowed,
                "reason": "active_session" if allowed else "entry_cutoff_reached",
                "session_type": kind, "session_start": start.isoformat(),
                "session_end": end.isoformat(), "seconds_to_close": seconds_to_close,
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

    async def _check_entry_session(self, ticker: str) -> tuple[bool, str, dict[str, Any]]:
        """Fail closed unless Finam confirms an active instrument session away from close."""
        state = await self._market_session_status(ticker)
        if not state.get("known"):
            return False, f"расписание Finam недоступно/не распознано ({state.get('reason', 'unknown')})", state
        if not state.get("is_open"):
            next_start = state.get("next_session_start")
            suffix = f"; следующая сессия={next_start}" if next_start else ""
            return False, f"торги по инструменту сейчас закрыты ({state.get('reason')}){suffix}", state
        if not state.get("entry_allowed"):
            seconds = _finite_or_default(state.get("seconds_to_close"), 0.0)
            cutoff = self._entry_cutoff_minutes()
            return False, f"до закрытия сессии {seconds / 60.0:.2f} мин; защитный интервал={cutoff:.1f} мин", state
        seconds = _finite_or_default(state.get("seconds_to_close"), 0.0)
        return True, f"сессия={state.get('session_type')}; до закрытия {seconds / 60.0:.1f} мин", state

    def _paper_entry_bbo_diagnostic(self, ticker: str) -> str:
        """Describe why the current two-sided BBO is not executable."""
        if self.screener is None:
            return "screener=отсутствует"
        try:
            quote = self.screener.quotes.get(ticker)
            if not isinstance(quote, Mapping):
                return "quote=отсутствует"
            now = time.monotonic()
            bid = _finite_or_default(quote.get("bid"), 0.0)
            ask = _finite_or_default(quote.get("ask"), 0.0)
            bid_seen = _finite_or_default(
                quote.get("last_bid_confirmed_seen", quote.get("last_bid_seen", 0.0)), 0.0
            )
            ask_seen = _finite_or_default(
                quote.get("last_ask_confirmed_seen", quote.get("last_ask_seen", 0.0)), 0.0
            )
            bid_age = now - bid_seen if bid_seen > 0.0 else math.inf
            ask_age = now - ask_seen if ask_seen > 0.0 else math.inf
            max_age = _finite_or_default(getattr(self.screener, "bbo_max_age_sec", 2.0), 2.0)
            if not math.isfinite(max_age) or max_age <= 0.0:
                max_age = 2.0
            max_age = min(5.0, max(0.25, max_age))
            valid = bool(quote.get("order_book_valid", False))
            reason = str(quote.get("order_book_last_reason", "UNKNOWN") or "UNKNOWN")
            bid_label = f"{bid_age:.3f}s" if math.isfinite(bid_age) else "нет"
            ask_label = f"{ask_age:.3f}s" if math.isfinite(ask_age) else "нет"
            return (
                f"bid={bid:.6f} age={bid_label}; ask={ask:.6f} age={ask_label}; "
                f"limit={max_age:.3f}s; book_valid={valid}; book_reason={reason}"
            )
        except Exception as exc:
            return f"diagnostic_error={type(exc).__name__}: {exc}"

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

    # ------------------------------------------------------------------
    # 3️⃣  EXECUTE_ORDER (Единая точка входа исполнения)
    # ------------------------------------------------------------------
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
        reserved_ticker = False

        try:
            if self._maintenance_active:
                return {
                    "status": "SKIPPED",
                    "message": self._maintenance_reason or "PAPER-торговля приостановлена для обслуживания сессии.",
                }
            if side not in {"BUY", "SELL"}:
                return {"status": "FAILED", "message": f"Недопустимая сторона сделки: {side}"}
            if int(lots) <= 0 or float(price) <= 0:
                return {"status": "FAILED", "message": "Количество лотов и цена должны быть положительными."}
            lots = int(lots)
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
                        "BBO_ENTRY_BLOCK: нет свежего двустороннего BBO для PAPER-входа; "
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
                executable_price = _finite_or_default(entry_bbo.get("executable_price"), 0.0)
                if executable_price <= 0.0:
                    return {"status": "SKIPPED", "message": "Некорректная исполнимая цена BBO для PAPER-входа."}
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

            # Учитываем уже открытые и параллельно резервируемые позиции.
            approved_lots, reservation_info = await self._reserve_order_size(ticker, lots, price, db, side=side)
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

            # Используем тот же стоп, по которому RiskManager рассчитывал объём.
            # Это исключает расхождение между sizing и фактическим стопом из-за нового тика.
            stop_loss = float(ai_analysis.get("stop_loss") or 0.0)
            take_profit = float(ai_analysis.get("take_profit") or 0.0)
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
            logger.info("📊 [ATR ENFORCER] Уровни: SL=%s₽ TP=%s₽; %s", stop_loss, take_profit, reservation_info)

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

            lot_multiplier = max(int(LOT_SIZES.get(ticker, 1)), 1)
            now = datetime.now()
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
            # Finam's protobuf exposes buy_size/sell_size as decimals, but the
            # documented schema does not specify whether these quantities are
            # shares or lots. Keep the raw value for diagnostics; do not report a
            # fabricated percentage coverage or use it as a fill constraint.
            stop_distance_abs = abs(price - stop_loss)
            trade_log = {
                "id": order_id,
                "status": "OPEN",
                "mode": "PAPER" if self.paper_mode else "LIVE",
                "paper_execution_model": "BBO_V8" if self.paper_mode else None,
                "paper_fill_assumption": "FULL_FILL_AT_TOP_OF_BOOK_NO_DEPTH_CHECK" if self.paper_mode else None,
                "session_id": self.session_id,
                "ticker": ticker,
                "entry_time": now.strftime("%H:%M:%S"),
                "entry_timestamp": now.isoformat(timespec="milliseconds"),
                "entry_price": price,
                "current_price": price,
                "side": side,
                "lots": lots,
                "volume": lots * lot_multiplier,  # число акций, не число лотов
                "lot_size": lot_multiplier,
                "notional_entry_rub": round(entry_notional, 2),
                "planned_risk_rub": round(abs(price - stop_loss) * lots * lot_multiplier, 2),
                "entry_commission_rub": round(entry_commission, 4),
                "commission_pct_per_side": self.paper_commission_pct if self.paper_mode else 0.0,
                "paper_slippage_bps": self.paper_slippage_bps if self.paper_mode else 0.0,
                "entry_executable_side": entry_bbo.get("executable_side") if entry_bbo is not None else None,
                "entry_top_size_raw": entry_quote_size,
                "entry_top_size_unit": "FINAM_UNCONFIRMED",
                "entry_depth_units_note": "Finam ORDER_BOOK size unit not confirmed; raw quantity is informational only",
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

            # Сначала сохраняем в Redis/Memurai. Не показываем позицию активной,
            # если сохранение не удалось.
            await db.log_trade(order_id, trade_log)
            if self.paper_mode:
                self.paper_positions[ticker] = trade_log
            if self.paper_mode and entry_bbo is not None:
                logger.info(
                    "🧾 [PAPER FILL] %s %s: last_reference=%.4f, executable_%s=%.4f, fill=%.4f, "
                    "top_size_raw=%.2f (unit unconfirmed), commission=%.4f ₽, slippage=%.2f bps.",
                    ticker, side, signal_reference_price,
                    str(entry_bbo.get("executable_side", "BBO")),
                    _finite_or_default(entry_bbo.get("executable_price"), 0.0), price,
                    entry_quote_size, entry_commission, self.paper_slippage_bps,
                )
            logger.info("✅ [EXECUTION] Позиция %s по %s записана; mode=%s.",
                        order_id, ticker, trade_log["mode"])
            return {
                "status": "SUCCESS",
                "orderId": order_id,
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

    def schedule_paper_position_monitor(
        self, ticker: str, current_price: float | None, db: DBManager
    ) -> None:
        """Coalesce ticks and valid BBO refreshes into one PAPER exit-monitor worker."""
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

        self._pending_monitor_prices[ticker] = price
        if self._monitor_worker_task is None or self._monitor_worker_task.done():
            self._monitor_worker_task = asyncio.create_task(self._drain_monitor_prices(db))

    async def _drain_monitor_prices(self, db: DBManager) -> None:
        """Обрабатывает только последние цены; не создаёт отдельную task на каждый тик."""
        try:
            while self._pending_monitor_prices:
                pending = self._pending_monitor_prices
                self._pending_monitor_prices = {}
                for ticker, price in pending.items():
                    await self.monitor_paper_positions(ticker, price, db)
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

        # Inspect the whole persisted journal, not just OPEN rows, so previously
        # quarantined positions remain a safety lock across repeated restarts.
        # DBManager exposes this as an async method. Use the declared method
        # directly so static type checkers keep its Awaitable return type;
        # hasattr preserves compatibility with lightweight test doubles.
        try:
            if hasattr(db, "get_all_daily_trades"):
                all_positions = await db.get_all_daily_trades()
            else:
                all_positions = await db.get_active_positions()
        except Exception:
            all_positions = await db.get_active_positions()

        quarantined = 0
        unresolved: dict[str, TradeRecord] = {}
        now = datetime.now().isoformat(timespec="seconds")
        for order_id, raw_trade in all_positions.items():
            if not isinstance(raw_trade, dict):
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
                await db.set_json(f"piluitus:trade:{order_id}", trade)
                quarantined += 1
                unresolved[str(order_id)] = trade
            elif status == "STALE_REVIEW" and str(trade.get("reconciliation_status", "")).upper() == "REVIEW_REQUIRED":
                unresolved[str(order_id)] = trade

        self._unreconciled_paper_positions = unresolved
        if unresolved:
            from portfolio_metrics import calculate_assets_used
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
        if raw_exit_price <= 0.0:
            raise ValueError("В BBO отсутствует положительная исполнимая цена выхода.")
        exit_transaction_side = "SELL" if side == "BUY" else "BUY"
        exit_price = self._apply_paper_slippage(raw_exit_price, exit_transaction_side)
        sign = 1 if side == "BUY" else -1
        gross_pnl = (exit_price - entry_price) * share_qty * sign
        entry_commission = _finite_or_default(trade.get("entry_commission_rub"), 0.0)
        commission_pct = _finite_or_default(
            trade.get("commission_pct_per_side"), self.paper_commission_pct
        )
        exit_notional = exit_price * share_qty
        exit_commission = exit_notional * commission_pct / 100.0
        total_commission = entry_commission + exit_commission
        net_pnl = gross_pnl - total_commission
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
        closed_trade = dict(trade)
        closed_trade.update({
            "status": "CLOSED",
            "exit_time": now.strftime("%H:%M:%S"),
            "exit_timestamp": now.isoformat(timespec="milliseconds"),
            "exit_price": exit_price,
            "current_price": exit_price,
            "exit_raw_bbo_price": raw_exit_price,
            "trigger_price": trigger_price if trigger_price > 0.0 else None,
            "observed_exit_price": observed_last_price,
            "exit_reason": exit_reason,
            "exit_price_source": "BBO_BID" if side == "BUY" else "BBO_ASK",
            "exit_bid": _finite_or_default(bbo.get("bid"), 0.0),
            "exit_ask": _finite_or_default(bbo.get("ask"), 0.0),
            "exit_bid_size": _finite_or_default(bbo.get("bid_size"), 0.0),
            "exit_ask_size": _finite_or_default(bbo.get("ask_size"), 0.0),
            "exit_top_size_raw": exit_size,
            "exit_top_size_unit": "FINAM_UNCONFIRMED",
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
                round((exit_price - trigger_price) / trigger_price * 100.0, 6)
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
        logger.warning(
            "💾 [PAPER JOURNAL] Сделка %s (%s) закрыта. Gross=%+.2f₽, commission=%.2f₽, Net=%+.2f₽; "
            "qty=%d; entry=%.4f; exit=%.4f; source=%s; reason=%s; top_size_raw=%.2f (unit unconfirmed); "
            "MFE=%.4f%%; MAE=%.4f%%; held=%.1fs",
            order_id, ticker, gross_pnl, total_commission, net_pnl, share_qty,
            entry_price, exit_price, closed_trade["exit_price_source"], exit_reason,
            exit_size,
            _finite_or_default(trade.get("max_favorable_excursion_pct"), 0.0),
            _finite_or_default(trade.get("max_adverse_excursion_pct"), 0.0),
            held_seconds if held_seconds is not None else -1.0,
        )

        try:
            import tg_bot
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

    async def monitor_paper_positions(self, ticker: str, current_price: float, db: DBManager):
        """Monitor PAPER risk using executable BBO; track excursions on every quote."""
        if not self.paper_mode:
            # This monitor never places a LIVE exit order.
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
            share_qty = int(trade.get("volume") or (
                int(trade.get("lots", 1)) * max(int(LOT_SIZES.get(ticker, 1)), 1)
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
                market_state = await self._market_session_status(ticker)
                if market_state.get("known") and not market_state.get("is_open"):
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
                trade["pnl"] = round(estimated_net_pnl, 2)
                trade["gross_pnl"] = round(estimated_gross_pnl, 2)
                trade["estimated_exit_price"] = estimated_exit_price
                trade["estimated_exit_commission_rub"] = round(estimated_exit_commission, 4)
                trade["pnl_percent"] = round(estimated_net_pnl / entry_notional * 100.0, 4) if entry_notional > 0 else 0.0
                trade["current_pnl_source"] = "LAST_TRADE_ESTIMATE"
                now_mono = time.monotonic()
                last_warn = self._last_bbo_exit_warning_at.get(ticker, 0.0)
                if now_mono - last_warn >= 60.0:
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
                    try:
                        warning_max_age = float(os.getenv("PILUITUS_BBO_EXIT_MAX_AGE_SEC", "2.0"))
                    except (TypeError, ValueError):
                        warning_max_age = 2.0
                    warning_max_age = min(5.0, max(0.25, warning_max_age))
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
                        "⏸️ [PAPER EXIT GUARD] %s: %s; age=%s, лимит=%.2fs, "
                        "Bid=%.4f Ask=%.4f; проверка SL/TP отложена, last=%.4f.",
                        ticker, guard_reason, age_text, warning_max_age,
                        bid_value, ask_value, observed_last_price,
                    )
                    self._last_bbo_exit_warning_at[ticker] = now_mono
                return

            # A coherent executable BBO has returned; resume the monitor without
            # manufacturing a fill or changing the saved SL/TP levels.
            if str(trade.get("exit_monitor_state") or "") != "ACTIVE":
                previous_reason = str(trade.get("exit_monitor_wait_reason") or "ожидание BBO")
                trade["exit_monitor_state"] = "ACTIVE"
                trade.pop("exit_monitor_wait_reason", None)
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

            if side == "BUY":
                exit_reason = "STOP_LOSS" if executable_exit_price <= stop_loss else (
                    "TAKE_PROFIT" if executable_exit_price >= take_profit else ""
                )
            else:
                exit_reason = "STOP_LOSS" if executable_exit_price >= stop_loss else (
                    "TAKE_PROFIT" if executable_exit_price <= take_profit else ""
                )

            exit_transaction_side = "SELL" if side == "BUY" else "BUY"
            estimated_exit_price = self._apply_paper_slippage(executable_exit_price, exit_transaction_side)
            mark_pct = ((estimated_exit_price - entry_price) / entry_price) * 100.0 * sign
            mark_rub = (estimated_exit_price - entry_price) * share_qty * sign
            entry_fee = _finite_or_default(trade.get("entry_commission_rub"), 0.0)
            fee_pct = _finite_or_default(trade.get("commission_pct_per_side"), self.paper_commission_pct)
            estimated_exit_fee = estimated_exit_price * share_qty * fee_pct / 100.0
            mark_net = mark_rub - entry_fee - estimated_exit_fee
            entry_notional = _finite_or_default(trade.get("notional_entry_rub"), entry_price * share_qty)
            trade["current_price"] = executable_exit_price
            trade["estimated_exit_price"] = estimated_exit_price
            trade["pnl"] = round(mark_net, 2)
            trade["gross_pnl"] = round(mark_rub, 2)
            trade["estimated_exit_commission_rub"] = round(estimated_exit_fee, 4)
            trade["pnl_percent"] = round(mark_net / entry_notional * 100.0, 4) if entry_notional > 0 else 0.0
            trade["current_pnl_source"] = "EXECUTABLE_BBO_ESTIMATE"
            trade["current_bbo_age_sec"] = round(float(bbo["age_sec"]), 3)
            trade["current_spread_pct"] = round(float(bbo["spread_pct"]), 6)
            if not exit_reason:
                return

            trigger_price = stop_loss if exit_reason == "STOP_LOSS" else take_profit
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
        """Force-close every active PAPER position at its fresh executable BBO side.

        This intentionally bypasses execute_order(): that method is an entry path
        and defaults HOLD when called without a new AI authorization.
        """
        if not self.paper_mode:
            logger.error("CMD_CLOSE_ALL в этом менеджере поддерживает только активные PAPER-позиции.")
            return {"closed": 0, "blocked": 0, "message": "PAPER-позиции не активны."}

        raw_positions_obj: object = self.paper_positions
        if not isinstance(raw_positions_obj, dict) or not raw_positions_obj:
            logger.info("📭 Нет активных позиций для экстренного закрытия.")
            return {"closed": 0, "blocked": 0, "message": "Активных PAPER-позиций нет."}

        raw_positions = cast(dict[object, object], raw_positions_obj)
        logger.warning("🚨 Запущено экстренное PAPER-закрытие %s позиций.", len(raw_positions))
        closed_count = 0
        blocked_count = 0
        for raw_ticker in list(raw_positions.keys()):
            ticker = str(raw_ticker).strip().upper().split("@")[0]
            pos_obj = raw_positions.get(raw_ticker)
            if not ticker or not isinstance(pos_obj, dict):
                blocked_count += 1
                logger.error("Пропуск некорректной позиции %s при CMD_CLOSE_ALL.", raw_ticker)
                continue
            pos = cast(TradeRecord, pos_obj)
            if str(pos.get("status", "OPEN")).upper() != "OPEN":
                continue
            side = str(pos.get("side", "BUY")).upper()
            order_id = str(pos.get("id", ""))
            if side not in {"BUY", "SELL"} or not order_id:
                blocked_count += 1
                logger.error("Пропуск позиции %s: некорректная сторона или ID.", ticker)
                continue
            if order_id in self._lock_closing:
                blocked_count += 1
                logger.warning("CMD_CLOSE_ALL: позиция %s уже находится в процессе закрытия.", ticker)
                continue

            bbo = self._latest_fresh_exit_quote(ticker, side)
            if bbo is None:
                blocked_count += 1
                logger.error(
                    "CMD_CLOSE_ALL: %s не закрыта — нет свежего исполнимого %s (порог %.2fs).",
                    ticker, "Bid" if side == "BUY" else "Ask",
                    _finite_or_default(os.getenv("PILUITUS_BBO_EXIT_MAX_AGE_SEC", "2.0"), 2.0),
                )
                continue

            self._lock_closing.add(order_id)
            try:
                last_price = self._latest_price(ticker, pos)
                await self._finalize_paper_close(
                    ticker, pos, db, bbo,
                    exit_reason="MANUAL_CLOSE",
                    observed_last_price=last_price,
                )
                closed_count += 1
            except Exception as exc:
                blocked_count += 1
                logger.error("CMD_CLOSE_ALL: не удалось закрыть %s: %s", ticker, exc, exc_info=True)
            finally:
                self._lock_closing.discard(order_id)

        result = {
            "closed": closed_count,
            "blocked": blocked_count,
            "message": f"Экстренное PAPER-закрытие завершено: закрыто {closed_count}, не закрыто {blocked_count}.",
        }
        logger.warning("🚨 [PAPER CLOSE ALL] %s", result["message"])
        return result

