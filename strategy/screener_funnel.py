"""Локальная численная воронка для отбора наиболее перспективных тикеров.

Все котировки продолжают поступать по полной вселенной инструментов. Рейтинг
считается локально; LLM получает только аномалии по тикерам из выбранного top-N.
Сохраняем совместимую структуру ``history`` для execution_layer/evolution_agent.
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import threading
import time
from datetime import datetime, timezone
from collections import deque
from typing import Any

import numpy as np
from minute_bar_patterns import MinuteBarPatternEngine

logger = logging.getLogger("Piluitus.Screener")


def _env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


def _env_float(name: str, default: float, minimum: float = 0.0) -> float:
    try:
        value = float(os.getenv(name, str(default)))
        return value if math.isfinite(value) and value >= minimum else default
    except (TypeError, ValueError):
        return default


def _event_timestamp_to_monotonic(value: object, receive_monotonic: float) -> float:
    """Map an exchange timestamp into the local monotonic clock domain.

    Missing/unparseable timestamps fall back to receive time. Old provider
    timestamps stay old (and therefore fail freshness checks); they are never
    promoted to fresh merely because the websocket delivered them now.
    """
    event_dt: datetime | None = None
    try:
        if isinstance(value, datetime):
            event_dt = value
        elif isinstance(value, dict):
            # Protobuf JSON Timestamp representation: {seconds, nanos}.
            seconds_raw = value.get("seconds")
            nanos_raw = value.get("nanos", 0)
            if seconds_raw is not None:
                seconds = float(seconds_raw)
                nanos = float(nanos_raw or 0.0)
                if math.isfinite(seconds) and math.isfinite(nanos):
                    event_dt = datetime.fromtimestamp(
                        seconds + nanos / 1_000_000_000.0, tz=timezone.utc
                    )
        elif isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(float(value)):
            epoch = float(value)
            if epoch >= 1e18:
                epoch /= 1_000_000_000.0
            elif epoch >= 1e15:
                epoch /= 1_000_000.0
            elif epoch >= 1e12:
                epoch /= 1_000.0
            event_dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        elif isinstance(value, str) and value.strip():
            text = value.strip()
            try:
                epoch = float(text)
            except ValueError:
                event_dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            else:
                if not math.isfinite(epoch):
                    return receive_monotonic
                if epoch >= 1e18:
                    epoch /= 1_000_000_000.0
                elif epoch >= 1e15:
                    epoch /= 1_000_000.0
                elif epoch >= 1e12:
                    epoch /= 1_000.0
                event_dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        if event_dt is None:
            return receive_monotonic
        if event_dt.tzinfo is None:
            event_dt = event_dt.replace(tzinfo=timezone.utc)
        age_seconds = (datetime.now(timezone.utc) - event_dt.astimezone(timezone.utc)).total_seconds()
        # Small clock skews are clamped to receive time. A timestamp more than
        # two seconds in the future is not trusted; the receiver clock is used.
        if age_seconds < -2.0:
            return receive_monotonic
        age_seconds = max(0.0, age_seconds)
        return receive_monotonic - age_seconds
    except (TypeError, ValueError, OverflowError, OSError):
        return receive_monotonic


class HFTMemoryScreener:
    """Короткая тик-история + локальный cross-sectional рейтинг top-N."""

    def __init__(
        self,
        window_size: int = 30,
        top_n: int | None = None,
        min_samples: int | None = None,
        selection_interval: float | None = None,
        warmup_timeout: float | None = None,
        minimum_coverage: float | None = None,
        stale_after: float | None = None,
        max_spread_pct: float | None = None,
        bbo_max_age_sec: float | None = None,
    ):
        self.window_size = max(8, int(window_size))
        self.top_n = max(1, int(top_n if top_n is not None else _env_int("PILUITUS_FUNNEL_TOP_N", 15)))
        # This calibration phase deliberately caps score-only churn at one
        # replacement per refresh, even if an older .env still says 3 or more.
        self.max_replacements_per_refresh = max(
            0, min(1, _env_int("PILUITUS_FUNNEL_MAX_REPLACEMENTS_PER_REFRESH", 1))
        )
        self.retention_score_gap = _env_float("PILUITUS_FUNNEL_RETENTION_SCORE_GAP", 5.0)
        self.min_selected_hold_sec = max(
            0.0, _env_float("PILUITUS_FUNNEL_MIN_HOLD_SEC", 60.0)
        )
        self.replacement_confirmations = max(
            1, _env_int("PILUITUS_FUNNEL_REPLACEMENT_CONFIRMATIONS", 2)
        )
        self.min_samples = max(
            3,
            int(min_samples if min_samples is not None else _env_int("PILUITUS_FUNNEL_MIN_SAMPLES", 8)),
        )
        self.selection_interval = max(
            15.0,
            float(selection_interval if selection_interval is not None else _env_float("PILUITUS_FUNNEL_REFRESH_SEC", 15.0)),
        )
        self.warmup_timeout = max(
            5.0,
            float(warmup_timeout if warmup_timeout is not None else _env_float("PILUITUS_FUNNEL_WARMUP_SEC", 90.0)),
        )
        self.minimum_coverage = min(
            1.0,
            max(0.1, float(minimum_coverage if minimum_coverage is not None else _env_float("PILUITUS_FUNNEL_COVERAGE", 0.60))),
        )
        self.stale_after = max(
            5.0,
            float(stale_after if stale_after is not None else _env_float("PILUITUS_FUNNEL_STALE_SEC", 60.0)),
        )
        self.max_spread_pct = max(
            0.01,
            float(max_spread_pct if max_spread_pct is not None else _env_float("PILUITUS_FUNNEL_MAX_SPREAD_PCT", 0.50)),
        )
        # A quote may omit unchanged fields. Retain the last explicitly observed
        # bid/ask only for a short TTL; never treat old prices as current forever.
        self.size_signal_ttl_sec = _env_float("PILUITUS_SIZE_SIGNAL_MAX_AGE_SEC", 2.0, minimum=0.25)
        configured_bbo_age = (
            float(bbo_max_age_sec) if bbo_max_age_sec is not None
            else _env_float("PILUITUS_BBO_MAX_AGE_SEC", 2.0, minimum=0.25)
        )
        self.bbo_max_age_sec = min(5.0, max(0.25, configured_bbo_age))

        # Keep the historical array layout compatible with ATR calculations.
        self.history: dict[str, dict[str, Any]] = {}
        self.quotes: dict[str, dict[str, Any]] = {}
        self.base_metrics: dict[str, dict[str, float]] = {}
        self.universe: list[str] = []
        self._universe_set: set[str] = set()
        self.selected_tickers: list[str] = []
        self.selection_ready = False
        self.eligible_count = 0
        self._ranking: dict[str, dict[str, float | int]] = {}
        self._started_at = time.monotonic()
        self._last_selection_update = 0.0
        self._selected_since: dict[str, float] = {}
        self._replacement_streak: dict[str, int] = {}
        self._lock = threading.RLock()
        self._last_selection_log_key: tuple[str, ...] | None = None
        # Closed 1-minute OHLC patterns are measured locally; no LLM is used to detect them.
        self.minute_bar_patterns = MinuteBarPatternEngine()
        self.trade_data_mode = False

    @staticmethod
    def normalize_ticker(ticker: str) -> str:
        return str(ticker or "").strip().upper().split("@")[0]

    def register_ticker(self, ticker: str) -> None:
        ticker = self.normalize_ticker(ticker)
        if not ticker or ticker in self.history:
            return
        self.history[ticker] = {
            "prices": np.zeros(self.window_size, dtype=np.float64),
            "volumes": np.zeros(self.window_size, dtype=np.float64),
            "count": 0,
            "trade_events": deque(maxlen=5000),
            "trade_seen_ids": deque(maxlen=10000),
            "trade_seen_set": set(),
        }
        self.quotes[ticker] = {
            "bid": 0.0,
            "ask": 0.0,
            "bid_size": 0.0,
            "ask_size": 0.0,
            "bid_levels": [],
            "ask_levels": [],
            "day_volume": 0.0,
            "last_seen": 0.0,
            "last_bid_seen": 0.0,
            "last_ask_seen": 0.0,
            # Confirmation clocks are distinct from level-change clocks. A valid
            # ORDER_BOOK delta confirms the maintained book even when the BBO
            # itself did not change; QUOTES confirms only explicitly present sides.
            "last_bid_confirmed_seen": 0.0,
            "last_ask_confirmed_seen": 0.0,
            "last_order_book_seen": 0.0,
            "last_order_book_event_seen": 0.0,
            # Event-time watermarks remain set after an explicit zero clears a
            # side, so an older packet cannot resurrect a withdrawn quote.
            "last_bid_event_seen": 0.0,
            "last_ask_event_seen": 0.0,
            "last_bid_quotes_event_seen": 0.0,
            "last_ask_quotes_event_seen": 0.0,
            "last_bid_book_event_seen": 0.0,
            "last_ask_book_event_seen": 0.0,
            "last_bid_source_received_seen": 0.0,
            "last_ask_source_received_seen": 0.0,
            "last_bid_size_event_seen": 0.0,
            "last_ask_size_event_seen": 0.0,
            "last_order_book_received": 0.0,
            "order_book_valid": False,
            "order_book_last_reason": "NO_ORDER_BOOK",
            # Confirmation clocks used by entry/exit guards. Event watermarks
            # above remain the last actual side-level update for ordering/audit.
            "last_bbo_seen": 0.0,
            "last_bid_size_seen": 0.0,
            "last_ask_size_seen": 0.0,
            "bid_size_drop_pct": 0.0,
            "ask_size_drop_pct": 0.0,
            "bid_size_baseline": 0.0,
            "ask_size_baseline": 0.0,
            "bid_size_current": 0.0,
            "ask_size_current": 0.0,
            "bid_size_drop_at": 0.0,
            "ask_size_drop_at": 0.0,
            "top_bid_history_samples": 0,
            "top_ask_history_samples": 0,
            "tick_times": deque(maxlen=self.window_size),
            "price_events": deque(maxlen=5000),
            # Наблюдения размера только лучшего уровня котировки (не полный стакан).
            # QUOTES и ORDER_BOOK могут пополнять одну короткую историю; размер
            # учитывается только при фактическом наличии соответствующего поля.
            "bid_size_history": deque(maxlen=self.window_size),
            "ask_size_history": deque(maxlen=self.window_size),
        }
        self.base_metrics[ticker] = {
            "volume_spike": 1.0,
            "vwap": 0.0,
            "std_dev": 0.0,
            "mean_price": 0.0,
        }

    def set_universe(self, tickers: list[str]) -> None:
        """Задать полный список котировок, из которого выбирается top-N."""
        normalized = list(dict.fromkeys(self.normalize_ticker(t) for t in tickers if self.normalize_ticker(t)))
        with self._lock:
            self.universe = normalized
            self._universe_set = set(normalized)
            for ticker in normalized:
                self.register_ticker(ticker)
            self.selected_tickers = []
            self.selection_ready = False
            self.eligible_count = 0
            self._ranking.clear()
            self._started_at = time.monotonic()
            self._last_selection_update = 0.0
            self._selected_since.clear()
            self._replacement_streak.clear()
            self._last_selection_log_key = None
        logger.info(
            "🎯 Локальная воронка инициализирована: universe=%d, top_n=%d, min_samples=%d, coverage=%.0f%%",
            len(normalized), min(self.top_n, len(normalized)), self.min_samples, self.minimum_coverage * 100,
        )

    @staticmethod
    def _percentile_ranks(values: dict[str, float]) -> dict[str, float]:
        """Percentile rank 0..1 с одинаковым средним рангом для одинаковых значений."""
        if not values:
            return {}
        ordered = sorted(values.items(), key=lambda item: item[1])
        if len(ordered) == 1:
            return {ordered[0][0]: 0.5}
        result: dict[str, float] = {}
        i = 0
        while i < len(ordered):
            j = i
            while j + 1 < len(ordered) and ordered[j + 1][1] == ordered[i][1]:
                j += 1
            avg_rank = (i + j) / 2.0
            rank = avg_rank / (len(ordered) - 1)
            for pos in range(i, j + 1):
                result[ordered[pos][0]] = rank
            i = j + 1
        return result

    def _features_for(self, ticker: str, now: float) -> dict[str, float] | None:
        data = self.history.get(ticker)
        quote = self.quotes.get(ticker)
        if not data or not quote:
            return None
        count = int(data["count"])
        if count < self.min_samples or now - float(quote["last_seen"]) > self.stale_after:
            return None

        prices = data["prices"][:count]
        volumes = data["volumes"][:count]
        if len(prices) < self.min_samples or not np.all(np.isfinite(prices)) or np.any(prices <= 0):
            return None

        returns_pct = np.diff(prices) / prices[:-1] * 100.0
        volatility_pct = float(np.std(returns_pct)) if len(returns_pct) else 0.0
        lookback = min(5, len(prices) - 1)
        base_price = float(prices[-1 - lookback])
        momentum_pct = abs(float(prices[-1]) - base_price) / base_price * 100.0 if base_price > 0 else 0.0

        if self.trade_data_mode:
            # Rank all 88 symbols on identical quote-derived fixed windows. The
            # trade stream is subscribed only for Top-15, so it must not bias ranking.
            momentum_pct = 0.0
            volatility_pct = 0.0
            price_events = quote.get("price_events", ())
            recent_price_events = [
                item for item in price_events
                if isinstance(item, (tuple, list)) and len(item) >= 2
                and 0.0 <= now - float(item[0]) <= 60.0
            ]
            if recent_price_events:
                latest_event = recent_price_events[-1]
                reference30 = next((
                    item for item in reversed(recent_price_events)
                    if float(item[0]) <= float(latest_event[0]) - 30.0
                ), None)
                if reference30 is not None and float(reference30[1]) > 0.0:
                    momentum_pct = abs(
                        (float(latest_event[1]) - float(reference30[1]))
                        / float(reference30[1]) * 100.0
                    )
                coverage = now - float(recent_price_events[0][0])
                if coverage >= 60.0:
                    time_returns = [
                        (float(recent_price_events[i][1]) - float(recent_price_events[i - 1][1]))
                        / float(recent_price_events[i - 1][1]) * 100.0
                        for i in range(1, len(recent_price_events))
                        if float(recent_price_events[i - 1][1]) > 0.0
                    ]
                    if len(time_returns) >= 2:
                        volatility_pct = float(np.std(time_returns))

        base = self.base_metrics.get(ticker, {})
        # The volume spike is an entry feature; it cannot rank only already-subscribed symbols.
        volume_spike = 0.0 if self.trade_data_mode else max(0.0, float(base.get("volume_spike", 1.0)))
        day_volume = max(0.0, float(quote.get("day_volume", 0.0)))
        mean_trade_volume = float(np.mean(volumes)) if len(volumes) else 0.0
        liquidity_measure = day_volume if day_volume > 0 else mean_trade_volume * len(volumes)

        bid = float(quote.get("bid", 0.0))
        ask = float(quote.get("ask", 0.0))
        bid_seen = float(quote.get("last_bid_confirmed_seen", quote.get("last_bid_seen", 0.0)) or 0.0)
        ask_seen = float(quote.get("last_ask_confirmed_seen", quote.get("last_ask_seen", 0.0)) or 0.0)
        bbo_fresh = (
            bid > 0 and ask > 0 and ask >= bid
            and bid_seen > 0 and ask_seen > 0
            and 0.0 <= now - bid_seen <= self.bbo_max_age_sec
            and 0.0 <= now - ask_seen <= self.bbo_max_age_sec
        )
        mid = (bid + ask) / 2.0 if bbo_fresh else 0.0
        spread_pct = (ask - bid) / mid * 100.0 if mid > 0 else -1.0
        tick_times = quote["tick_times"]
        recent_ticks = sum(1 for tick_time in tick_times if now - tick_time <= 30.0)

        order_book_seen = float(quote.get("last_order_book_seen", 0.0) or 0.0)
        order_book_received = float(quote.get("last_order_book_received", 0.0) or 0.0)
        return {
            "volatility_pct": max(0.0, volatility_pct),
            "momentum_pct": max(0.0, momentum_pct),
            "volume_spike": min(volume_spike, 1000.0),
            "liquidity_measure": max(0.0, liquidity_measure),
            "day_volume": day_volume,
            "order_book_event_age_sec": max(0.0, now - order_book_seen) if order_book_seen > 0.0 else -1.0,
            "order_book_receive_age_sec": max(0.0, now - order_book_received) if order_book_received > 0.0 else -1.0,
            "order_book_valid": float(bool(quote.get("order_book_valid", False))),
            "spread_pct": spread_pct,
            "tick_activity_30s": float(recent_ticks),
        }

    def _refresh_selection(self, now: float) -> None:
        candidates = {
            ticker: features
            for ticker in (self.universe or list(self.history.keys()))
            if (features := self._features_for(ticker, now)) is not None
        }
        self.eligible_count = len(candidates)
        universe_size = len(self.universe) if self.universe else len(self.history)
        minimum_for_selection = min(self.top_n, universe_size)
        # Если свежих кандидатов стало меньше top-N, не анализируем случайное
        # подмножество: снова ждём минимум полную пятнадцатку пригодных символов.
        if len(candidates) < minimum_for_selection:
            # A temporary stale quote must not flush the entire subscription set.
            # Retain only still-eligible previous members; no LLM candidate is
            # emitted until the full Top-N coverage is restored.
            self.selection_ready = False
            self._ranking = {}
            self.selected_tickers = [
                ticker for ticker in self.selected_tickers if ticker in candidates
            ]
            self._selected_since = {
                ticker: self._selected_since.get(ticker, now)
                for ticker in self.selected_tickers
            }
            self._replacement_streak.clear()
            self._last_selection_update = now
            return
        coverage_required = min(
            universe_size,
            max(min(self.top_n, universe_size), int(math.ceil(universe_size * self.minimum_coverage))),
        )
        coverage_ready = len(candidates) >= coverage_required
        timeout_ready = (now - self._started_at) >= self.warmup_timeout and len(candidates) >= min(self.top_n, universe_size)
        if coverage_ready or timeout_ready:
            self.selection_ready = True

        if not candidates:
            self._ranking = {}
            self.selected_tickers = []
            self._last_selection_update = now
            return

        # Нормируем признаки внутри наблюдаемого рынка: единицы измерения не влияют
        # на итоговый рейтинг. Главный вес — фактическая краткосрочная волатильность.
        volatility_ranks = self._percentile_ranks({t: f["volatility_pct"] for t, f in candidates.items()})
        momentum_ranks = self._percentile_ranks({t: f["momentum_pct"] for t, f in candidates.items()})
        volume_ranks = self._percentile_ranks({t: math.log1p(f["volume_spike"]) for t, f in candidates.items()})
        liquidity_ranks = self._percentile_ranks({t: math.log1p(f["liquidity_measure"]) for t, f in candidates.items()})
        activity_ranks = self._percentile_ranks({t: math.log1p(f["tick_activity_30s"]) for t, f in candidates.items()})

        ranked: dict[str, dict[str, float | int]] = {}
        for ticker, feature in candidates.items():
            spread_pct = feature["spread_pct"]
            # Нет bid/ask — нейтральная, а не максимальная оценка качества спреда.
            spread_quality = (
                max(0.0, 1.0 - spread_pct / self.max_spread_pct)
                if spread_pct >= 0.0 else 0.35
            )
            liquidity_score = 0.7 * liquidity_ranks[ticker] + 0.3 * activity_ranks[ticker]
            score = 100.0 * (
                0.40 * volatility_ranks[ticker]
                + 0.25 * momentum_ranks[ticker]
                + 0.15 * volume_ranks[ticker]
                + 0.10 * liquidity_score
                + 0.10 * spread_quality
            )
            ranked[ticker] = {
                **feature,
                "score": round(score, 3),
                "volatility_rank_pct": round(volatility_ranks[ticker] * 100.0, 1),
                "momentum_rank_pct": round(momentum_ranks[ticker] * 100.0, 1),
                "volume_rank_pct": round(volume_ranks[ticker] * 100.0, 1),
                "rank": 0,
            }

        ordered = sorted(ranked, key=lambda ticker: float(ranked[ticker]["score"]), reverse=True)
        for rank, ticker in enumerate(ordered, start=1):
            ranked[ticker]["rank"] = rank
        self._ranking = ranked

        if not self.selection_ready:
            # Preserve a previously active eligible cohort during recovery from
            # a temporary coverage dip. Initial warm-up still starts with no cohort.
            self.selected_tickers = [
                ticker for ticker in self.selected_tickers if ticker in ranked
            ]
            self._selected_since = {
                ticker: self._selected_since.get(ticker, now)
                for ticker in self.selected_tickers
            }
            self._replacement_streak.clear()
            self._last_selection_update = now
            return

        target_count = min(self.top_n, universe_size, len(ordered))
        if target_count <= 0:
            self.selected_tickers = []
            self._last_selection_update = now
            return

        # Keep the current cohort stable. Stale/ineligible members are removed
        # immediately, but score-only challengers must stay in Top-N for multiple
        # refreshes and the outgoing member must have passed its minimum hold time.
        cutoff = float(ranked[ordered[target_count - 1]]["score"])
        retention_floor = cutoff - self.retention_score_gap
        previous = list(self.selected_tickers)
        eligible_previous = [ticker for ticker in previous if ticker in ranked]
        invalid_previous = len(previous) - len(eligible_previous)
        target_set = set(ordered[:target_count])
        chosen = list(eligible_previous)
        replacements = 0

        # Initial fill and replacement of genuinely stale members are immediate:
        # these are coverage repairs, not score-driven churn.
        if len(chosen) < target_count:
            for ticker in ordered:
                if len(chosen) >= target_count:
                    break
                if ticker not in chosen:
                    chosen.append(ticker)
                    self._selected_since[ticker] = now
            self._replacement_streak.clear()
        else:
            current_challengers = target_set - set(chosen)
            self._replacement_streak = {
                ticker: streak
                for ticker, streak in self._replacement_streak.items()
                if ticker in current_challengers
            }
            for ticker in ordered[:target_count]:
                if ticker in chosen:
                    self._replacement_streak.pop(ticker, None)
                    continue
                self._replacement_streak[ticker] = self._replacement_streak.get(ticker, 0) + 1
                if self._replacement_streak[ticker] < self.replacement_confirmations:
                    continue
                replaceable = [
                    old for old in chosen
                    if old not in target_set
                    and float(ranked[old]["score"]) < retention_floor
                    and now - self._selected_since.get(old, now) >= self.min_selected_hold_sec
                ]
                if not replaceable or replacements >= self.max_replacements_per_refresh:
                    continue
                weakest = min(replaceable, key=lambda old: float(ranked[old]["score"]))
                chosen.remove(weakest)
                self._selected_since.pop(weakest, None)
                chosen.append(ticker)
                self._selected_since[ticker] = now
                self._replacement_streak.pop(ticker, None)
                replacements += 1

        self.selected_tickers = chosen
        self._selected_since = {
            ticker: self._selected_since.get(ticker, now)
            for ticker in self.selected_tickers
        }
        if set(previous) != set(chosen):
            logger.info(
                "[FUNNEL TURNOVER] previous=%d selected=%d retained=%d replaced=%d invalid_previous=%d",
                len(previous), len(chosen), len(set(previous) & set(chosen)),
                len(set(previous) - set(chosen)), invalid_previous,
            )
        self._last_selection_update = now

    @staticmethod
    def _top_size_drop_pct(history: deque, current_size: float, now: float) -> tuple[float, int]:
        """Снижение объёма лучшей котировки относительно медианы прошлых наблюдений.

        Это только best-bid/best-ask proxy, а не доказательство отмены лимитных
        ордеров или исчезновения плотности во всём стакане.
        """
        prior = [
            float(size) for ts, size in list(history)[-5:]
            if now - float(ts) <= 60.0 and float(size) > 0.0
        ]
        if current_size <= 0.0 or len(prior) < 3:
            return 0.0, len(prior)
        baseline = float(np.median(prior))
        if baseline <= 0.0:
            return 0.0, len(prior)
        drop_pct = max(0.0, (baseline - current_size) / baseline * 100.0)
        return min(100.0, drop_pct), len(prior)

    def _observe_top_size(self, quote: dict, side: str, size: float, now: float) -> None:
        """Record one real observation for the visible best Bid/Ask size."""
        history = quote[f"{side}_size_history"]
        drop_key = f"{side}_size_drop_pct"
        drop_at_key = f"{side}_size_drop_at"
        seen_key = f"last_{side}_size_seen"
        samples_key = "top_bid_history_samples" if side == "bid" else "top_ask_history_samples"
        size_key = f"{side}_size"
        size = max(0.0, float(size))
        old_drop = float(quote.get(drop_key, 0.0) or 0.0)
        old_drop_at = float(quote.get(drop_at_key, 0.0) or 0.0)
        fresh_old_drop = old_drop >= 50.0 and old_drop_at > 0.0 and now - old_drop_at <= self.size_signal_ttl_sec

        if size <= 0.0:
            prior_sizes = [
                float(value) for ts, value in history
                if now - float(ts) <= 60.0 and float(value) > 0.0
            ]
            quote[size_key] = 0.0
            quote[f"{side}_size_current"] = 0.0
            quote[f"{side}_size_baseline"] = float(np.median(prior_sizes)) if prior_sizes else 0.0
            quote[seen_key] = 0.0
            quote[drop_key] = 0.0
            quote[drop_at_key] = 0.0
            quote[samples_key] = len([1 for ts, value in history if now - float(ts) <= 60.0 and float(value) > 0.0])
            return

        drop, samples = self._top_size_drop_pct(history, size, now)
        prior_sizes = [
            float(value) for ts, value in list(history)[-5:]
            if now - float(ts) <= 60.0 and float(value) > 0.0
        ]
        quote[f"{side}_size_baseline"] = float(np.median(prior_sizes)) if prior_sizes else 0.0
        quote[f"{side}_size_current"] = size
        quote[size_key] = size
        quote[seen_key] = now
        quote[samples_key] = int(samples)
        if drop >= 50.0:
            quote[drop_key] = float(drop)
            quote[drop_at_key] = now
        elif fresh_old_drop:
            # Preserve a measured sudden drop for a short alignment window so the
            # next price/volume tick can validate the same event.
            quote[drop_key] = old_drop
            quote[drop_at_key] = old_drop_at
        else:
            quote[drop_key] = float(drop)
            quote[drop_at_key] = 0.0
        history.append((now, size))

    def _current_top_size(self, quote: dict, side: str, now: float) -> float:
        size = float(quote.get(f"{side}_size", 0.0) or 0.0)
        seen = float(quote.get(f"last_{side}_size_seen", 0.0) or 0.0)
        if size <= 0.0 or seen <= 0.0 or now - seen > self.bbo_max_age_sec:
            return 0.0
        return size

    def _current_size_drop(self, quote: dict, side: str, now: float) -> float:
        drop = float(quote.get(f"{side}_size_drop_pct", 0.0) or 0.0)
        observed_at = float(quote.get(f"{side}_size_drop_at", 0.0) or 0.0)
        if drop <= 0.0 or observed_at <= 0.0 or now - observed_at > self.size_signal_ttl_sec:
            return 0.0
        return drop

    def _sync_push_order_book(
        self,
        ticker: str,
        bid: float,
        ask: float,
        bid_size: float,
        ask_size: float,
        *,
        bid_present: bool = True,
        ask_present: bool = True,
        bid_size_present: bool = True,
        ask_size_present: bool = True,
        bid_timestamp: object | None = None,
        ask_timestamp: object | None = None,
        order_book_timestamp: object | None = None,
        received_monotonic: float | None = None,
        book_valid: bool = True,
        book_reason: str = "",
        bid_levels: list[list[float]] | None = None,
        ask_levels: list[list[float]] | None = None,
        bid_levels_present: bool = False,
        ask_levels_present: bool = False,
    ) -> dict[str, Any]:
        """Update BBO values and freshness independently for each observed side."""
        ticker = self.normalize_ticker(ticker)
        values = (bid, ask, bid_size, ask_size)
        if not ticker or not all(np.isfinite(float(value)) for value in values):
            return {"updated": False, "reason": "Некорректные значения ORDER_BOOK"}
        now = time.monotonic()
        received_at = now
        if received_monotonic is not None:
            try:
                supplied_received = float(received_monotonic)
                if math.isfinite(supplied_received) and 0.0 < supplied_received <= now + 0.05:
                    received_at = min(now, supplied_received)
            except (TypeError, ValueError, OverflowError):
                pass
        with self._lock:
            if self.universe and ticker not in self._universe_set:
                return {"updated": False, "reason": "Тикер вне вселенной"}
            self.register_ticker(ticker)
            quote = self.quotes[ticker]
            bid = float(bid)
            ask = float(ask)
            bid_seen_at = _event_timestamp_to_monotonic(bid_timestamp, received_at) if bid_present else 0.0
            ask_seen_at = _event_timestamp_to_monotonic(ask_timestamp, received_at) if ask_present else 0.0
            # ORDER_BOOK envelope time is the ordering/freshness clock for the
            # complete reconstructed state. Level timestamps describe when an
            # individual price level changed and must not age the whole book.
            book_seen_at = _event_timestamp_to_monotonic(order_book_timestamp, received_at)
            confirmation_seen_at = min(received_at, book_seen_at)
            prior_book_event = float(quote.get("last_order_book_event_seen", 0.0) or 0.0)
            accept_book_event = prior_book_event <= 0.0 or book_seen_at + 0.001 >= prior_book_event
            if not accept_book_event:
                return {
                    "updated": False,
                    "reason": "Устаревшее событие ORDER_BOOK",
                    "out_of_order": True,
                    "bbo_side_updated": False,
                }

            quote["last_order_book_event_seen"] = max(prior_book_event, book_seen_at)
            quote["last_order_book_seen"] = book_seen_at
            quote["last_order_book_received"] = received_at
            old_bid = float(quote.get("bid", 0.0) or 0.0)
            old_ask = float(quote.get("ask", 0.0) or 0.0)

            prior_bid_event = float(quote.get("last_bid_book_event_seen", 0.0) or 0.0)
            prior_ask_event = float(quote.get("last_ask_book_event_seen", 0.0) or 0.0)
            # Every emitted parser event contains a complete reconstructed book.
            # Its envelope clock orders this stream against earlier ORDER_BOOK
            # events only. Cross-stream arbitration uses socket-ingress time;
            # each stream retains its own event-time watermark.
            bid_state_present = bool(bid_present or bid_levels_present or order_book_timestamp is not None)
            ask_state_present = bool(ask_present or ask_levels_present or order_book_timestamp is not None)
            # The envelope timestamp orders the complete ORDER_BOOK message.
            # Without it, fall back to the captured socket-ingress monotonic time.
            # Per-level timestamps are diagnostic timestamps of the last change
            # at that price; they must never be used as the message ordering clock.
            bid_ordering_time = book_seen_at
            ask_ordering_time = book_seen_at
            accept_bid_event = bool(
                bid_state_present and (prior_bid_event <= 0.0 or bid_ordering_time + 0.001 >= prior_bid_event)
            )
            accept_ask_event = bool(
                ask_state_present and (prior_ask_event <= 0.0 or ask_ordering_time + 0.001 >= prior_ask_event)
            )
            if accept_bid_event:
                quote["last_bid_book_event_seen"] = max(prior_bid_event, bid_ordering_time)
            if accept_ask_event:
                quote["last_ask_book_event_seen"] = max(prior_ask_event, ask_ordering_time)
            accept_bid_update = bool(
                accept_bid_event
                and received_at >= float(quote.get("last_bid_source_received_seen", 0.0) or 0.0)
            )
            accept_ask_update = bool(
                accept_ask_event
                and received_at >= float(quote.get("last_ask_source_received_seen", 0.0) or 0.0)
            )

            old_bid = float(quote.get("bid", 0.0) or 0.0)
            old_ask = float(quote.get("ask", 0.0) or 0.0)
            candidate_bid = (bid if bid > 0.0 else 0.0) if accept_bid_update else old_bid
            candidate_ask = (ask if ask > 0.0 else 0.0) if accept_ask_update else old_ask

            # Crossed or incomplete snapshots are explicit invalidations. They
            # revoke executable confirmations immediately; prices are retained
            # only when the message authoritatively supplied that side.
            if not book_valid:
                if bid_state_present and accept_bid_update:
                    quote["bid"] = candidate_bid
                    quote["last_bid_source_received_seen"] = received_at
                    quote["last_bid_event_seen"] = bid_ordering_time
                    if bid_present:
                        quote["last_bid_seen"] = max(
                            float(quote.get("last_bid_seen", 0.0) or 0.0),
                            bid_seen_at if bid_seen_at > 0.0 else book_seen_at,
                        )
                if ask_state_present and accept_ask_update:
                    quote["ask"] = candidate_ask
                    quote["last_ask_source_received_seen"] = received_at
                    quote["last_ask_event_seen"] = ask_ordering_time
                    if ask_present:
                        quote["last_ask_seen"] = max(
                            float(quote.get("last_ask_seen", 0.0) or 0.0),
                            ask_seen_at if ask_seen_at > 0.0 else book_seen_at,
                        )
                quote["last_bid_confirmed_seen"] = 0.0
                quote["last_ask_confirmed_seen"] = 0.0
                quote["last_bbo_seen"] = 0.0
                quote["order_book_valid"] = False
                quote["order_book_last_reason"] = str(
                    book_reason or "INVALID_ORDER_BOOK"
                )
                return {
                    "updated": True,
                    "valid": False,
                    "reason": quote["order_book_last_reason"],
                    "ticker": ticker,
                    "bid": quote["bid"],
                    "ask": quote["ask"],
                    "bbo_side_updated": bool(bid_present or ask_present),
                    "bbo_side_touch_count": int(bool(bid_present)) + int(bool(ask_present)),
                    "confirmed_sides": 0,
                    "bbo_price_changed": bool(old_bid != quote["bid"] or old_ask != quote["ask"]),
                }

            current_bid = candidate_bid
            current_ask = candidate_ask
            crossed = current_bid > 0.0 and current_ask > 0.0 and current_ask < current_bid
            two_sided = current_bid > 0.0 and current_ask > 0.0 and current_ask >= current_bid
            if not two_sided or crossed:
                quote["bid"] = current_bid
                quote["ask"] = current_ask
                if accept_bid_update:
                    quote["last_bid_source_received_seen"] = received_at
                    quote["last_bid_event_seen"] = bid_ordering_time
                if accept_ask_update:
                    quote["last_ask_source_received_seen"] = received_at
                    quote["last_ask_event_seen"] = ask_ordering_time
                quote["last_bid_confirmed_seen"] = 0.0
                quote["last_ask_confirmed_seen"] = 0.0
                quote["last_bbo_seen"] = 0.0
                quote["order_book_valid"] = False
                quote["order_book_last_reason"] = "CROSSED_BBO" if crossed else "MISSING_BBO_SIDE"
                return {
                    "updated": True,
                    "valid": False,
                    "reason": quote["order_book_last_reason"],
                    "ticker": ticker,
                    "bid": current_bid,
                    "ask": current_ask,
                    "bbo_side_updated": bool(bid_present or ask_present),
                    "bbo_side_touch_count": int(bool(bid_present)) + int(bool(ask_present)),
                    "confirmed_sides": 0,
                    "bbo_price_changed": bool(old_bid != current_bid or old_ask != current_ask),
                }

            quote["order_book_valid"] = True
            quote["order_book_last_reason"] = "OK"
            quote["bid"] = current_bid
            quote["ask"] = current_ask
            if accept_bid_update:
                # Advance the side's event watermark even for a heartbeat. This
                # prevents a delayed QUOTES packet from rolling the maintained
                # BBO backwards. `last_bid_seen` remains the last actual best-
                # level update, separate from the book-confirmation clock.
                quote["last_bid_source_received_seen"] = received_at
                quote["last_bid_event_seen"] = bid_ordering_time
                if bid_present:
                    quote["last_bid_seen"] = max(
                        float(quote.get("last_bid_seen", 0.0) or 0.0),
                        bid_seen_at if bid_seen_at > 0.0 else book_seen_at,
                    )
            if accept_ask_update:
                quote["last_ask_source_received_seen"] = received_at
                quote["last_ask_event_seen"] = ask_ordering_time
                if ask_present:
                    quote["last_ask_seen"] = max(
                        float(quote.get("last_ask_seen", 0.0) or 0.0),
                        ask_seen_at if ask_seen_at > 0.0 else book_seen_at,
                    )
            # A coherent, in-order delta confirms both sides of the maintained
            # book even if neither best level changed. Preserve a newer QUOTES
            # confirmation rather than moving its clock backwards.
            if current_bid > 0.0 and accept_bid_update:
                # When the envelope clock is absent, `book_seen_at` is the
                # captured socket receive time. A level timestamp may be old
                # simply because that resting price has not changed recently.
                bid_confirmation_at = book_seen_at
                quote["last_bid_confirmed_seen"] = max(
                    float(quote.get("last_bid_confirmed_seen", 0.0) or 0.0), bid_confirmation_at
                )
            elif current_bid <= 0.0:
                quote["last_bid_confirmed_seen"] = 0.0
            if current_ask > 0.0 and accept_ask_update:
                ask_confirmation_at = book_seen_at
                quote["last_ask_confirmed_seen"] = max(
                    float(quote.get("last_ask_confirmed_seen", 0.0) or 0.0), ask_confirmation_at
                )
            elif current_ask <= 0.0:
                quote["last_ask_confirmed_seen"] = 0.0
            bid_confirmed = float(quote.get("last_bid_confirmed_seen", 0.0) or 0.0)
            ask_confirmed = float(quote.get("last_ask_confirmed_seen", 0.0) or 0.0)
            quote["last_bbo_seen"] = (
                min(bid_confirmed, ask_confirmed)
                if bid_confirmed > 0.0 and ask_confirmed > 0.0 else 0.0
            )
            if bid_size_present and accept_bid_update:
                quote["last_bid_size_event_seen"] = max(
                    float(quote.get("last_bid_size_event_seen", 0.0) or 0.0),
                    bid_seen_at if bid_seen_at > 0.0 else book_seen_at,
                )
                self._observe_top_size(quote, "bid", float(bid_size), confirmation_seen_at)
            if ask_size_present and accept_ask_update:
                quote["last_ask_size_event_seen"] = max(
                    float(quote.get("last_ask_size_event_seen", 0.0) or 0.0),
                    ask_seen_at if ask_seen_at > 0.0 else book_seen_at,
                )
                self._observe_top_size(quote, "ask", float(ask_size), confirmation_seen_at)
            # The parser emits the complete depth on each heartbeat. Refresh the
            # size timestamp only for sides whose source event is not older than
            # the current side watermark; never roll size/depth backwards from a
            # delayed ORDER_BOOK frame. Heartbeats refresh availability without
            # manufacturing a new size-history sample.
            if accept_bid_update:
                quote["bid_size"] = max(0.0, float(bid_size))
                quote["last_bid_size_seen"] = confirmation_seen_at if bid_size > 0.0 else 0.0
                if bid_levels_present and bid_levels is not None:
                    quote["bid_levels"] = bid_levels
            if accept_ask_update:
                quote["ask_size"] = max(0.0, float(ask_size))
                quote["last_ask_size_seen"] = confirmation_seen_at if ask_size > 0.0 else 0.0
                if ask_levels_present and ask_levels is not None:
                    quote["ask_levels"] = ask_levels
            return {
                "updated": True,
                "valid": True,
                "book_event_accepted": True,
                "bid_side_updated": bool(accept_bid_update),
                "ask_side_updated": bool(accept_ask_update),
                "bbo_side_updated": bool(bid_present or ask_present),
                "bbo_side_touch_count": int(bool(bid_present)) + int(bool(ask_present)),
                "confirmed_sides": int(bool(accept_bid_update)) + int(bool(accept_ask_update)),
                "bbo_price_changed": bool(old_bid != current_bid or old_ask != current_ask),
                "heartbeat_only": not bool(bid_present or ask_present),
                "ticker": ticker,
                "bid": quote["bid"],
                "ask": quote["ask"],
                "bid_size": self._current_top_size(quote, "bid", now),
                "ask_size": self._current_top_size(quote, "ask", now),
                "bid_size_drop_pct": self._current_size_drop(quote, "bid", now),
                "ask_size_drop_pct": self._current_size_drop(quote, "ask", now),
            }

    async def push_order_book(
        self,
        ticker: str,
        bid: float,
        ask: float,
        bid_size: float,
        ask_size: float,
        *,
        bid_present: bool = True,
        ask_present: bool = True,
        bid_size_present: bool = True,
        ask_size_present: bool = True,
        bid_timestamp: object | None = None,
        ask_timestamp: object | None = None,
        order_book_timestamp: object | None = None,
        received_monotonic: float | None = None,
        book_valid: bool = True,
        book_reason: str = "",
        bid_levels: list[list[float]] | None = None,
        ask_levels: list[list[float]] | None = None,
        bid_levels_present: bool = False,
        ask_levels_present: bool = False,
    ) -> dict[str, Any]:
        """Accept a normalized best-level state from the Finam ORDER_BOOK stream."""
        return self._sync_push_order_book(
            ticker, float(bid), float(ask), float(bid_size), float(ask_size),
            bid_present=bid_present, ask_present=ask_present,
            bid_size_present=bid_size_present, ask_size_present=ask_size_present,
            bid_timestamp=bid_timestamp, ask_timestamp=ask_timestamp,
            order_book_timestamp=order_book_timestamp,
            received_monotonic=received_monotonic,
            book_valid=book_valid,
            book_reason=book_reason,
            bid_levels=bid_levels, ask_levels=ask_levels,
            bid_levels_present=bid_levels_present, ask_levels_present=ask_levels_present,
        )

    def _sync_push_tick(
        self,
        ticker: str,
        price: float,
        volume: float,
        bid: float = 0.0,
        ask: float = 0.0,
        bid_size: float = 0.0,
        ask_size: float = 0.0,
        day_volume: float = 0.0,
        bid_present: bool | None = None,
        ask_present: bool | None = None,
        bid_size_present: bool | None = None,
        ask_size_present: bool | None = None,
        event_timestamp: object | None = None,
        received_monotonic: float | None = None,
    ) -> dict:
        ticker = self.normalize_ticker(ticker)
        numeric_values = (price, volume, bid, ask, bid_size, ask_size, day_volume)
        if not ticker or not all(np.isfinite(value) for value in numeric_values) or price <= 0 or volume < 0:
            safe_price = float(price) if np.isfinite(price) and price > 0 else 0.0
            return {
                "is_anomaly": False,
                "selected": False,
                "selection_ready": self.selection_ready,
                "metrics": {"current_price": safe_price, "vwap": 0.0, "std_dev": 0.0, "volume_spike": 0.0, "mean_price": 0.0},
            }

        now = time.monotonic()
        with self._lock:
            if self.universe and ticker not in self._universe_set:
                # Не позволяем неожиданному/тестовому тикеру повлиять на ранжирование 88 инструментов.
                return {
                    "is_anomaly": False,
                    "selected": False,
                    "selection_ready": self.selection_ready,
                    "metrics": {"current_price": price, "vwap": price, "std_dev": 0.0, "volume_spike": 1.0, "mean_price": price},
                }

            self.register_ticker(ticker)
            # M1 OHLC uses observed last-price updates from QUOTES across the full
            # 88-symbol universe. QUOTES.lastSize is never used for volume/VWAP;
            # selected INSTRUMENT_TRADES may enrich the same price-only candle.
            bar_pattern_metrics = self.minute_bar_patterns.update(
                ticker, price, now=now, event_timestamp=event_timestamp
            )
            data = self.history[ticker]
            prices = data["prices"]
            volumes = data["volumes"]
            count = int(data["count"])

            if count < self.window_size:
                history_prices = prices[:count].copy()
                history_volumes = volumes[:count].copy()
                prices[count] = price
                volumes[count] = 0.0 if self.trade_data_mode else volume
                data["count"] = count + 1
                current_window_prices = prices[:count + 1]
                current_window_volumes = volumes[:count + 1]
            else:
                history_prices = prices.copy()
                history_volumes = volumes.copy()
                prices[:-1] = prices[1:]
                volumes[:-1] = volumes[1:]
                prices[-1] = price
                volumes[-1] = 0.0 if self.trade_data_mode else volume
                current_window_prices = prices
                current_window_volumes = volumes

            mean_price = float(np.mean(current_window_prices))
            total_volume = float(np.sum(current_window_volumes))
            vwap = float(np.sum(current_window_prices * current_window_volumes) / total_volume) if total_volume > 0 else price
            if history_prices.size >= 2:
                std_dev = float(np.std(history_prices))
                mean_volume = float(np.mean(history_volumes))
                volume_spike = volume / mean_volume if mean_volume > 0 else 1.0
            else:
                std_dev = price * 0.001
                volume_spike = 1.0
            if std_dev <= 0:
                std_dev = price * 0.0005

            trade_metrics = self._trade_metrics_locked(data, now) if self.trade_data_mode else {}
            if self.trade_data_mode:
                self.quotes[ticker]["trade_metrics"] = trade_metrics
                trade_ready = bool(trade_metrics.get("trade_data_ready", 0.0))
                vwap = float(trade_metrics.get("trade_vwap_60s", 0.0) or 0.0) if trade_ready else price
                volume_spike = float(trade_metrics.get("volume_spike", 0.0) or 0.0) if trade_ready else 0.0
                trade_volatility = float(trade_metrics.get("trade_realized_volatility_60s_pct", 0.0) or 0.0)
                if trade_ready and trade_volatility > 0.0:
                    std_dev = max(price * trade_volatility / 100.0, price * 0.0001)

            # Порог согласован с активным правилом RAG (>=1.5x). Это только
            # поиск кандидата; окончательный guard по стакану/спреду/риску остаётся.
            base_anomaly = (volume_spike >= 1.5) and (abs(price - vwap) > (1.5 * std_dev))
            # A completed, measured 1m breakout/pullback is also a candidate event.
            # It still must pass BBO, spread, size/risk guards before execution.
            is_anomaly = bool(
                (not self.trade_data_mode or bool(trade_metrics.get("trade_data_ready", 0.0)))
                and (base_anomaly or float(bar_pattern_metrics.get("bar_pattern_confirmed", 0.0)) >= 0.5)
            )
            base = {"vwap": vwap, "std_dev": std_dev, "volume_spike": float(volume_spike), "mean_price": mean_price}
            self.base_metrics[ticker] = base

            quote = self.quotes[ticker]
            # Default flags preserve compatibility with tests and internal producers
            # that provide numeric values without explicit field-presence metadata.
            bid_present = (bid > 0) if bid_present is None else bool(bid_present)
            ask_present = (ask > 0) if ask_present is None else bool(ask_present)
            bid_size_present = (bid_size > 0) if bid_size_present is None else bool(bid_size_present)
            ask_size_present = (ask_size > 0) if ask_size_present is None else bool(ask_size_present)

            # QUOTES and ORDER_BOOK have independent event-time watermarks.
            # Their envelope timestamps can differ slightly even when the later
            # received packet is the newest observation. Keep ordering within
            # each source, then use captured ingress time to arbitrate sources.
            received_at = now
            if received_monotonic is not None:
                try:
                    supplied_received = float(received_monotonic)
                    if math.isfinite(supplied_received) and 0.0 < supplied_received <= now + 0.05:
                        received_at = min(now, supplied_received)
                except (TypeError, ValueError, OverflowError):
                    pass
            event_seen_at = _event_timestamp_to_monotonic(event_timestamp, received_at)
            prior_bid_event = float(quote.get("last_bid_quotes_event_seen", 0.0) or 0.0)
            prior_ask_event = float(quote.get("last_ask_quotes_event_seen", 0.0) or 0.0)
            accept_bid_event = bool(
                bid_present and (prior_bid_event <= 0.0 or event_seen_at >= prior_bid_event)
            )
            accept_ask_event = bool(
                ask_present and (prior_ask_event <= 0.0 or event_seen_at >= prior_ask_event)
            )
            accept_bid_update = bool(
                accept_bid_event
                and received_at >= float(quote.get("last_bid_source_received_seen", 0.0) or 0.0)
            )
            accept_ask_update = bool(
                accept_ask_event
                and received_at >= float(quote.get("last_ask_source_received_seen", 0.0) or 0.0)
            )
            if accept_bid_event:
                quote["last_bid_quotes_event_seen"] = event_seen_at
            if accept_ask_event:
                quote["last_ask_quotes_event_seen"] = event_seen_at
            if accept_bid_update:
                quote["last_bid_source_received_seen"] = received_at
                quote["last_bid_event_seen"] = event_seen_at
                quote["bid"] = float(bid) if bid > 0.0 else 0.0
                quote["last_bid_seen"] = event_seen_at if bid > 0.0 else 0.0
                quote["last_bid_confirmed_seen"] = event_seen_at if bid > 0.0 else 0.0
            if accept_ask_update:
                quote["last_ask_source_received_seen"] = received_at
                quote["last_ask_event_seen"] = event_seen_at
                quote["ask"] = float(ask) if ask > 0.0 else 0.0
                quote["last_ask_seen"] = event_seen_at if ask > 0.0 else 0.0
                quote["last_ask_confirmed_seen"] = event_seen_at if ask > 0.0 else 0.0

            quote_bid = float(quote.get("bid", 0.0) or 0.0)
            quote_ask = float(quote.get("ask", 0.0) or 0.0)
            if quote_bid > 0.0 and quote_ask > 0.0 and quote_ask < quote_bid:
                quote["last_bid_confirmed_seen"] = 0.0
                quote["last_ask_confirmed_seen"] = 0.0
                quote["last_bbo_seen"] = 0.0
                quote["order_book_valid"] = False
                quote["order_book_last_reason"] = "CROSSED_QUOTES_BBO"

            # A size is a separate observed field and may arrive with a partial
            # quote. Keep its own event-time watermark so delayed packets cannot
            # roll the depth sample backwards or revive a cleared size.
            prior_bid_size_event = float(
                quote.get("last_bid_size_event_seen", quote.get("last_bid_size_seen", 0.0)) or 0.0
            )
            prior_ask_size_event = float(
                quote.get("last_ask_size_event_seen", quote.get("last_ask_size_seen", 0.0)) or 0.0
            )
            if bid_size_present and (prior_bid_size_event <= 0.0 or event_seen_at >= prior_bid_size_event):
                quote["last_bid_size_event_seen"] = event_seen_at
                self._observe_top_size(quote, "bid", float(bid_size), event_seen_at)
            if ask_size_present and (prior_ask_size_event <= 0.0 or event_seen_at >= prior_ask_size_event):
                quote["last_ask_size_event_seen"] = event_seen_at
                self._observe_top_size(quote, "ask", float(ask_size), event_seen_at)

            current_bid_size = self._current_top_size(quote, "bid", now)
            current_ask_size = self._current_top_size(quote, "ask", now)
            bid_size_drop_pct = self._current_size_drop(quote, "bid", now)
            ask_size_drop_pct = self._current_size_drop(quote, "ask", now)
            bid_size_history_samples = int(quote.get("top_bid_history_samples", 0))
            ask_size_history_samples = int(quote.get("top_ask_history_samples", 0))
            if day_volume > 0:
                quote["day_volume"] = float(day_volume)
            quote["last_seen"] = now
            quote["tick_times"].append(now)
            price_events = quote["price_events"]
            if not price_events or abs(float(price_events[-1][1]) - float(price)) > 1e-12:
                price_events.append((now, float(price)))

            bid_level_seen = float(quote.get("last_bid_seen", 0.0) or 0.0)
            ask_level_seen = float(quote.get("last_ask_seen", 0.0) or 0.0)
            bid_confirmed_seen = float(quote.get("last_bid_confirmed_seen", bid_level_seen) or 0.0)
            ask_confirmed_seen = float(quote.get("last_ask_confirmed_seen", ask_level_seen) or 0.0)
            bid_age = now - bid_confirmed_seen if bid_confirmed_seen > 0.0 else float("inf")
            ask_age = now - ask_confirmed_seen if ask_confirmed_seen > 0.0 else float("inf")
            last_bbo_seen = min(bid_confirmed_seen, ask_confirmed_seen)
            quote["last_bbo_seen"] = last_bbo_seen if last_bbo_seen > 0.0 else 0.0
            bbo_age = now - last_bbo_seen if last_bbo_seen > 0.0 else float("inf")
            bbo_fresh = (
                float(quote["bid"]) > 0 and float(quote["ask"]) > 0
                and float(quote["ask"]) >= float(quote["bid"])
                and 0.0 <= bid_age <= self.bbo_max_age_sec
                and 0.0 <= ask_age <= self.bbo_max_age_sec
            )

            # Направленный импульс относительно цены до текущего тика.
            lookback = min(5, int(history_prices.size))
            reference_price = float(history_prices[-lookback]) if lookback > 0 else 0.0
            price_change_pct = (
                (float(price) - reference_price) / reference_price * 100.0
                if reference_price > 0 else 0.0
            )
            if self.trade_data_mode:
                price_change_pct = float(trade_metrics.get("price_change_pct", 0.0) or 0.0)
            price_vs_vwap_pct = (float(price) - float(vwap)) / float(vwap) * 100.0 if vwap > 0 else 0.0
            current_bid = float(quote["bid"])
            current_ask = float(quote["ask"])
            mid = (current_bid + current_ask) / 2.0 if bbo_fresh else 0.0
            spread_pct_current = (current_ask - current_bid) / mid * 100.0 if mid > 0 else -1.0

            should_refresh = (
                self._last_selection_update <= 0.0
                or now - self._last_selection_update >= self.selection_interval
            )
            if should_refresh:
                self._refresh_selection(now)

            selected_set = set(self.selected_tickers)
            selected = ticker in selected_set
            feature = self._ranking.get(ticker, {})
            bid_size_seen = float(quote.get("last_bid_size_seen", 0.0) or 0.0)
            ask_size_seen = float(quote.get("last_ask_size_seen", 0.0) or 0.0)
            bid_size_age = now - bid_size_seen if bid_size_seen > 0.0 else float("inf")
            ask_size_age = now - ask_size_seen if ask_size_seen > 0.0 else float("inf")
            bid_drop_at = float(quote.get("bid_size_drop_at", 0.0) or 0.0)
            ask_drop_at = float(quote.get("ask_size_drop_at", 0.0) or 0.0)
            bid_drop_age = now - bid_drop_at if bid_drop_at > 0.0 else -1.0
            ask_drop_age = now - ask_drop_at if ask_drop_at > 0.0 else -1.0
            queue_size_total = current_bid_size + current_ask_size
            queue_imbalance_available = bool(
                quote.get("order_book_valid", False)
                and current_bid_size > 0.0
                and current_ask_size > 0.0
                and queue_size_total > 0.0
            )
            queue_imbalance = (
                (current_bid_size - current_ask_size) / queue_size_total
                if queue_imbalance_available else 0.0
            )
            queue_imbalance_age = max(bid_size_age, ask_size_age)
            metrics: dict[str, Any] = {
                "current_price": float(price),
                "vwap": float(vwap),
                "trade_data_mode": float(self.trade_data_mode),
                "trade_data_ready": float(trade_metrics.get("trade_data_ready", 0.0)) if self.trade_data_mode else 0.0,
                "trade_vwap_60s": float(trade_metrics.get("trade_vwap_60s", 0.0)) if self.trade_data_mode else 0.0,
                "trade_flow_imbalance_30s": float(trade_metrics.get("trade_flow_imbalance_30s", 0.0)) if self.trade_data_mode else 0.0,
                "trade_buy_volume_30s": float(trade_metrics.get("trade_buy_volume_30s", 0.0)) if self.trade_data_mode else 0.0,
                "trade_sell_volume_30s": float(trade_metrics.get("trade_sell_volume_30s", 0.0)) if self.trade_data_mode else 0.0,
                "trade_count_60s": int(trade_metrics.get("trade_count_60s", 0)) if self.trade_data_mode else 0,
                "trade_metrics_age_sec": float(trade_metrics.get("trade_metrics_age_sec", -1.0)) if self.trade_data_mode else -1.0,
                "std_dev": float(std_dev),
                "volume_spike": float(volume_spike),
                "mean_price": float(mean_price),
                "price_change_pct": float(price_change_pct),
                "price_vs_vwap_pct": float(price_vs_vwap_pct),
                "bid_size_drop_pct": float(bid_size_drop_pct),
                "ask_size_drop_pct": float(ask_size_drop_pct),
                "bid_size_baseline": float(quote.get("bid_size_baseline", 0.0) or 0.0),
                "ask_size_baseline": float(quote.get("ask_size_baseline", 0.0) or 0.0),
                "bid_size_current": float(quote.get("bid_size_current", current_bid_size) or 0.0),
                "ask_size_current": float(quote.get("ask_size_current", current_ask_size) or 0.0),
                "top_bid_history_samples": int(bid_size_history_samples),
                "top_ask_history_samples": int(ask_size_history_samples),
                "bid_size_drop_age_sec": round(bid_drop_age, 3) if bid_drop_age >= 0.0 else -1.0,
                "ask_size_drop_age_sec": round(ask_drop_age, 3) if ask_drop_age >= 0.0 else -1.0,
                "bid_size_age_sec": round(bid_size_age, 3) if math.isfinite(bid_size_age) else -1.0,
                "ask_size_age_sec": round(ask_size_age, 3) if math.isfinite(ask_size_age) else -1.0,
                "bid_size_available": float(current_bid_size > 0),
                "ask_size_available": float(current_ask_size > 0),
                "order_book_valid": float(bool(quote.get("order_book_valid", False))),
                "queue_imbalance_available": float(queue_imbalance_available),
                "queue_imbalance": float(queue_imbalance),
                "queue_imbalance_age_sec": round(queue_imbalance_age, 3) if math.isfinite(queue_imbalance_age) else -1.0,
                "bid_ask_available": float(bbo_fresh),
                "bid_quote_age_sec": round(bid_age, 3) if math.isfinite(bid_age) else -1.0,
                "ask_quote_age_sec": round(ask_age, 3) if math.isfinite(ask_age) else -1.0,
                "bid_level_change_age_sec": round(now - bid_level_seen, 3) if bid_level_seen > 0.0 else -1.0,
                "ask_level_change_age_sec": round(now - ask_level_seen, 3) if ask_level_seen > 0.0 else -1.0,
                "order_book_age_sec": round(now - float(quote.get("last_order_book_seen", 0.0) or 0.0), 3) if float(quote.get("last_order_book_seen", 0.0) or 0.0) > 0.0 else -1.0,
                "bid_ask_age_sec": round(bbo_age, 3) if math.isfinite(bbo_age) else -1.0,
                "bbo_age_sec": round(bbo_age, 3) if math.isfinite(bbo_age) else -1.0,
                "bbo_max_age_sec": float(self.bbo_max_age_sec),
                "bid": current_bid,
                "ask": current_ask,
                "bid_size": float(current_bid_size),
                "ask_size": float(current_ask_size),
                "day_volume": float(quote["day_volume"]),
                "funnel_score": float(feature.get("score", 0.0)),
                "funnel_rank": int(feature.get("rank", 0)),
                "volatility_pct": float(feature.get("volatility_pct", 0.0)),
                "momentum_pct": float(feature.get("momentum_pct", 0.0)),
                "spread_pct": float(spread_pct_current) if spread_pct_current >= 0.0 else 0.0,
                "spread_available": float(spread_pct_current >= 0.0),
                "selected_for_ai": float(selected),
                "funnel_ready": float(self.selection_ready),
                **bar_pattern_metrics,
            }
            if float(bar_pattern_metrics.get("bar_pattern_confirmed", 0.0)) >= 0.5:
                logger.info(
                    "📈 [M1 PATTERN] %s: %s; ATR=%.4f%%, gap=%.2f ATR, body=%.0f%%, close_pos=%.0f%%.",
                    ticker,
                    str(bar_pattern_metrics.get("bar_pattern_name", "")),
                    float(bar_pattern_metrics.get("bar_atr_pct", 0.0)),
                    float(bar_pattern_metrics.get("bar_signal_gap_atr", 0.0)),
                    float(bar_pattern_metrics.get("bar_body_ratio", 0.0)) * 100.0,
                    float(bar_pattern_metrics.get("bar_close_position", 0.5)) * 100.0,
                )
            return {
                "is_anomaly": bool(is_anomaly),
                "selected": selected,
                "selection_ready": self.selection_ready,
                "eligible_count": self.eligible_count,
                "selected_tickers": list(self.selected_tickers),
                "metrics": metrics,
            }

    def enable_trade_data_mode(self) -> None:
        """Require unique INSTRUMENT_TRADES for live volume/VWAP evidence."""
        with self._lock:
            self.trade_data_mode = True
            for state in self.history.values():
                state["volumes"].fill(0.0)
            logger.info("📈 Trade-flow mode enabled: QUOTES.lastSize excluded from volume/VWAP.")

    def reset_trade_history(self, ticker: str) -> None:
        """Invalidate rolling trade-flow evidence without erasing the independent QUOTES M1 history."""
        symbol = self.normalize_ticker(ticker)
        if not symbol:
            return
        with self._lock:
            state = self.history.get(symbol)
            if isinstance(state, dict):
                state.get("trade_events", deque()).clear()
                state.get("trade_seen_ids", deque()).clear()
                state.get("trade_seen_set", set()).clear()
            quote = self.quotes.get(symbol)
            if isinstance(quote, dict):
                quote["trade_metrics"] = {}
                quote["last_trade_event_seen"] = 0.0
                quote.pop("last_trade_price", None)

    def reset_trade_subscription(self, ticker: str) -> None:
        """Reset rolling trade-flow on subscription changes without erasing closed M1 bars."""
        symbol = self.normalize_ticker(ticker)
        if not symbol:
            return
        with self._lock:
            state = self.history.get(symbol)
            if isinstance(state, dict):
                state.get("trade_events", deque()).clear()
                state.get("trade_seen_ids", deque()).clear()
                state.get("trade_seen_set", set()).clear()
            quote = self.quotes.get(symbol)
            if isinstance(quote, dict):
                quote["trade_metrics"] = {}
                quote["last_trade_event_seen"] = 0.0
                quote.pop("last_trade_price", None)

    def push_trade(
        self, ticker: str, price: float, size: float, trade_id: object = None,
        side: object = None, event_timestamp: object = None,
        received_monotonic: float | None = None,
    ) -> dict[str, Any]:
        """Record one unique exchange trade and refresh rolling trade-flow metrics."""
        symbol = self.normalize_ticker(ticker)
        try:
            px, qty = float(price), float(size)
        except (TypeError, ValueError, OverflowError):
            return {"accepted": False, "reason": "invalid_numeric"}
        if not symbol or not math.isfinite(px) or not math.isfinite(qty) or px <= 0.0 or qty <= 0.0:
            return {"accepted": False, "reason": "invalid_trade"}
        now = time.monotonic()
        received = now
        if received_monotonic is not None:
            try:
                candidate = float(received_monotonic)
                if math.isfinite(candidate) and 0.0 < candidate <= now + 0.05:
                    received = min(now, candidate)
            except (TypeError, ValueError, OverflowError):
                pass
        event_seen = _event_timestamp_to_monotonic(event_timestamp, received)
        if event_seen < now - 120.0 or event_seen > now + 0.05:
            return {"accepted": False, "reason": "stale_or_future_trade"}
        raw_id = str(trade_id or "").strip()
        side_text = str(side or "").strip().upper()
        side_norm = "BUY" if ("BUY" in side_text or side_text == "1") else "SELL" if ("SELL" in side_text or side_text == "2") else "UNKNOWN"
        fingerprint = raw_id or f"{event_seen:.6f}|{px:.8f}|{qty:.8f}|{side_norm}"
        with self._lock:
            self.register_ticker(symbol)
            state = self.history[symbol]
            seen = state["trade_seen_set"]
            if fingerprint in seen:
                return {"accepted": False, "reason": "duplicate_trade"}
            ids = state["trade_seen_ids"]
            if len(ids) == ids.maxlen:
                seen.discard(ids[0])
            ids.append(fingerprint)
            seen.add(fingerprint)
            events = state["trade_events"]
            if events and event_seen < events[-1][0] - 0.001:
                return {"accepted": False, "reason": "out_of_order_trade"}
            events.append((event_seen, px, qty, side_norm))
            bar_metrics = self.minute_bar_patterns.update(
                symbol, px, now=event_seen, event_timestamp=event_timestamp
            )
            metrics = self._trade_metrics_locked(state, now)
            self.quotes[symbol]["trade_metrics"] = metrics
            self.quotes[symbol]["bar_pattern_metrics"] = bar_metrics
            self.quotes[symbol]["last_trade_event_seen"] = event_seen
            self.quotes[symbol]["last_trade_price"] = px
            return {"accepted": True, **metrics}

    def refresh_trade_metrics(self, ticker: str, now: float | None = None) -> dict[str, Any]:
        """Revalidate trade-flow freshness immediately before a post-LLM decision."""
        symbol = self.normalize_ticker(ticker)
        if not symbol:
            return {"trade_data_ready": 0.0, "trade_metrics_age_sec": -1.0}
        current = time.monotonic() if now is None else float(now)
        with self._lock:
            state = self.history.get(symbol)
            quote = self.quotes.get(symbol)
            if not state or not quote:
                return {"trade_data_ready": 0.0, "trade_metrics_age_sec": -1.0}
            metrics = self._trade_metrics_locked(state, current)
            quote["trade_metrics"] = metrics
            latest = state["trade_events"][-1] if state.get("trade_events") else None
            last_price = float(latest[1]) if latest else 0.0
            vwap = float(metrics.get("trade_vwap_60s", 0.0) or 0.0)
            return {
                **metrics,
                "current_price": last_price,
                "vwap": vwap,
                "price_vs_vwap_pct": ((last_price - vwap) / vwap * 100.0) if last_price > 0 and vwap > 0 else 0.0,
            }

    @staticmethod
    def _trade_metrics_locked(state: dict[str, Any], now: float) -> dict[str, Any]:
        events = state.get("trade_events")
        if not isinstance(events, deque):
            return {"trade_data_ready": 0.0, "volume_spike": 0.0}
        while events and now - events[0][0] > 120.0:
            events.popleft()
        recent60 = [item for item in events if 0.0 <= now - item[0] <= 60.0]
        recent30 = [item for item in recent60 if now - item[0] <= 30.0]
        recent5 = [item for item in recent60 if now - item[0] <= 5.0]
        prior55 = [item for item in recent60 if 5.0 < now - item[0] <= 60.0]
        total60 = sum(item[2] for item in recent60)
        total5 = sum(item[2] for item in recent5)
        baseline5 = sum(item[2] for item in prior55) / 11.0
        vwap60 = sum(item[1] * item[2] for item in recent60) / total60 if total60 > 0 else 0.0
        latest = recent60[-1] if recent60 else None
        reference = next((item for item in reversed(recent60) if latest and item[0] <= latest[0] - 15.0), None)
        price_change15 = (latest[1] - reference[1]) / reference[1] * 100.0 if latest and reference and reference[1] > 0 else 0.0
        reference30 = next((item for item in reversed(recent60) if latest and item[0] <= latest[0] - 30.0), None)
        price_change30 = (latest[1] - reference30[1]) / reference30[1] * 100.0 if latest and reference30 and reference30[1] > 0 else 0.0
        trade_returns = [
            (recent60[i][1] - recent60[i - 1][1]) / recent60[i - 1][1] * 100.0
            for i in range(1, len(recent60)) if recent60[i - 1][1] > 0
        ]
        realized_volatility60 = float(np.std(trade_returns)) if len(trade_returns) >= 2 else 0.0
        buy30 = sum(item[2] for item in recent30 if item[3] == "BUY")
        sell30 = sum(item[2] for item in recent30 if item[3] == "SELL")
        known30 = buy30 + sell30
        imbalance30 = (buy30 - sell30) / known30 if known30 > 0 else 0.0
        last_age = max(0.0, now - latest[0]) if latest else math.inf
        coverage_sec = max(0.0, now - events[0][0]) if events else 0.0
        # The 5s-vs-prior-55s baseline is valid only after a complete 60s
        # observation window; otherwise the denominator would be understated.
        ready = bool(
            len(recent60) >= 3 and reference is not None and coverage_sec >= 60.0
            and baseline5 > 0 and last_age <= 5.0 and vwap60 > 0
        )
        return {
            "trade_data_ready": float(ready), "trade_vwap_60s": float(vwap60),
            "trade_volume_5s": float(total5), "trade_volume_baseline_5s": float(baseline5),
            "volume_spike": float(total5 / baseline5) if ready and baseline5 > 0 else 0.0,
            "price_change_pct": float(price_change15) if ready else 0.0,
            "trade_price_change_30s_pct": float(price_change30) if ready and reference30 else 0.0,
            "trade_realized_volatility_60s_pct": float(realized_volatility60) if ready else 0.0,
            "trade_flow_imbalance_30s": float(imbalance30),
            "trade_buy_volume_30s": float(buy30), "trade_sell_volume_30s": float(sell30),
            "trade_count_60s": int(len(recent60)),
            "trade_history_coverage_sec": float(coverage_sec),
            "trade_metrics_age_sec": float(last_age) if math.isfinite(last_age) else -1.0,
        }

    async def push_tick(
        self,
        ticker: str,
        price: float,
        volume: float,
        bid: float = 0.0,
        ask: float = 0.0,
        bid_size: float = 0.0,
        ask_size: float = 0.0,
        day_volume: float = 0.0,
        bid_present: bool | None = None,
        ask_present: bool | None = None,
        bid_size_present: bool | None = None,
        ask_size_present: bool | None = None,
        event_timestamp: object | None = None,
        received_monotonic: float | None = None,
    ) -> dict:
        """Обновляет короткую историю и рейтинг; расчёты массива малы и локальны."""
        # Функция работает в одном event loop; короткий расчёт 30 тиков x 88 буферов
        # не требует создавать задачу в executor для каждого входящего сообщения.
        return self._sync_push_tick(
            ticker, float(price), float(volume), float(bid), float(ask),
            float(bid_size), float(ask_size), float(day_volume),
            bid_present=bid_present, ask_present=ask_present,
            bid_size_present=bid_size_present, ask_size_present=ask_size_present,
            event_timestamp=event_timestamp, received_monotonic=received_monotonic,
        )

    def get_status(self) -> dict[str, Any]:
        with self._lock:
            return {
                "ready": bool(self.selection_ready),
                "universe_count": len(self.universe) if self.universe else len(self.history),
                "eligible_count": int(self.eligible_count),
                "observed_count": sum(1 for ticker in self.universe if int(self.history.get(ticker, {}).get("count", 0)) > 0),
                "selected_count": len(self.selected_tickers),
                "selected_tickers": list(self.selected_tickers),
                "top_n": self.top_n,
            }

    def get_status_text(self) -> str:
        status = self.get_status()
        if status["ready"] and status["selected_count"]:
            names = "/".join(status["selected_tickers"][:3])
            return f"🎯 Funnel {status['universe_count']}→{status['selected_count']} {names}"
        return (
            f"🎯 Funnel warmup {status['eligible_count']}/{status['universe_count']} "
            f"→{status['top_n']}"
        )
