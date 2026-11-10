"""Causal 1-minute OHLC pattern features for PILUITUS.

Bars are built from observed last-price updates. No synthetic bars are inserted
for missing minutes, and the module does not aggregate QUOTES.lastSize into bar
volume because Finam may repeat the last trade size on quote updates.

The two enabled patterns are candidates, not claims of guaranteed profitability:
1. five-closed-bar range breakout with a strong close;
2. EMA(9)/EMA(21) trend pullback with a confirming close.
A signal is emitted only on the tick that closes the qualifying candle (first
observed tick of the next minute) and only if the new price has not gapped away.
"""
from __future__ import annotations

import math
import os
import time
from datetime import datetime, timezone
from collections import deque
from dataclasses import dataclass
from typing import Any
from collections.abc import Mapping


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


@dataclass
class MinuteBar:
    bucket: int
    open: float
    high: float
    low: float
    close: float
    ticks: int = 1

    def update(self, price: float) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.ticks += 1


def _event_epoch_seconds(value: object) -> float | None:
    """Parse an exchange event timestamp into epoch seconds when available.

    Supports numeric seconds/milliseconds and ISO-like timestamps. Unknown or
    invalid values return None so the caller can use its monotonic fallback.
    """
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, Mapping):
        # Protobuf JSON Timestamp: seconds may be encoded as a decimal string.
        raw_seconds = value.get("seconds")
        raw_nanos = value.get("nanos", 0)
        if isinstance(raw_seconds, bool):
            return None
        if isinstance(raw_seconds, str):
            seconds_raw = raw_seconds
        elif isinstance(raw_seconds, (int, float)):
            seconds_raw = raw_seconds
        else:
            return None
        if raw_nanos is None:
            nanos_raw: int | float | str = 0
        elif isinstance(raw_nanos, bool):
            return None
        elif isinstance(raw_nanos, str):
            nanos_raw = raw_nanos
        elif isinstance(raw_nanos, (int, float)):
            nanos_raw = raw_nanos
        else:
            return None
        try:
            seconds = float(seconds_raw)
            nanos = float(nanos_raw)
        except (TypeError, ValueError):
            return None
        if not math.isfinite(seconds) or not math.isfinite(nanos):
            return None
        number = seconds + nanos / 1_000_000_000.0
        return number if number >= 1e8 else None
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        if not math.isfinite(number) or number <= 0.0:
            return None
        # Unix timestamps in milliseconds (or microseconds) are common in feeds.
        if number >= 1e15:
            number /= 1e6
        elif number >= 1e12:
            number /= 1e3
        return number if number >= 1e8 else None
    if isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            return _event_epoch_seconds(float(raw))
        except ValueError:
            pass
        normalized = raw.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(normalized)
        except ValueError:
            return None
        if parsed.tzinfo is None:
            # Minute boundaries are timezone-offset invariant for whole-hour MOEX
            # offsets; explicitly assume UTC rather than machine-local time.
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    if isinstance(value, datetime):
        parsed = value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    return None


class MinuteBarPatternEngine:
    """Build per-ticker observed 1m bars and emit sparse, measurable patterns."""

    def __init__(
        self,
        bar_seconds: int = 60,
        history_bars: int = 180,
        min_ticks_per_bar: int | None = None,
        max_signal_gap_atr: float | None = None,
    ) -> None:
        self.bar_seconds = max(10, int(bar_seconds))
        self.history_bars = max(30, int(history_bars))
        self.min_ticks_per_bar = max(
            2,
            int(min_ticks_per_bar if min_ticks_per_bar is not None else _env_int("PILUITUS_PATTERN_MIN_TICKS_PER_BAR", 3)),
        )
        self.max_signal_gap_atr = max(
            0.05,
            float(max_signal_gap_atr if max_signal_gap_atr is not None else _env_float("PILUITUS_PATTERN_MAX_GAP_ATR", 0.35)),
        )
        self._states: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _ema(values: list[float], period: int) -> float:
        if not values:
            return 0.0
        alpha = 2.0 / (period + 1.0)
        value = values[0]
        for item in values[1:]:
            value = alpha * item + (1.0 - alpha) * value
        return float(value)

    @staticmethod
    def _atr(bars: list[MinuteBar], period: int = 14) -> float:
        if len(bars) < 2:
            return 0.0
        recent = bars[-(period + 1):]
        ranges: list[float] = []
        previous_close = recent[0].close
        for bar in recent[1:]:
            ranges.append(max(
                bar.high - bar.low,
                abs(bar.high - previous_close),
                abs(bar.low - previous_close),
            ))
            previous_close = bar.close
        return float(sum(ranges) / len(ranges)) if ranges else 0.0

    @staticmethod
    def _empty() -> dict[str, Any]:
        return {
            "bar_count": 0.0,
            "bar_pattern_confirmed": 0.0,
            "bar_pattern_direction_code": 0.0,  # +1=BUY, -1=SELL, 0=none
            "bar_breakout_up": 0.0,
            "bar_breakout_down": 0.0,
            "bar_pullback_buy": 0.0,
            "bar_pullback_sell": 0.0,
            "bar_trend_up": 0.0,
            "bar_trend_down": 0.0,
            "bar_ema9": 0.0,
            "bar_ema21": 0.0,
            "bar_atr": 0.0,
            "bar_atr_pct": 0.0,
            "bar_atr_periods": 0.0,
            "bar_atr_age_sec": -1.0,
            "bar_body_ratio": 0.0,
            "bar_close_position": 0.5,
            "bar_signal_gap_atr": 0.0,
            "bar_pattern_name": "",
            "_bar_pattern_signal_at_monotonic": -1.0,
        }

    def reset_partial(self, ticker: str) -> None:
        """Discard only the in-progress M1 candle; retain completed observed bars."""
        symbol = str(ticker or "").strip().upper().split("@", 1)[0]
        if not symbol:
            return
        state = self._states.get(symbol)
        if isinstance(state, dict):
            state["current"] = None

    def reset(self, ticker: str) -> None:
        """Fully discard M1 history after a confirmed clock-domain/feed reset."""
        symbol = str(ticker or "").strip().upper().split("@", 1)[0]
        if symbol:
            self._states.pop(symbol, None)

    def _state(self, ticker: str) -> dict[str, Any]:
        state = self._states.get(ticker)
        if state is None:
            state = {
                "current": None,
                "bars": deque(maxlen=self.history_bars),
                "last_closed_bar_monotonic": 0.0,
            }
            self._states[ticker] = state
        return state

    def _summarize(self, state: dict[str, Any], price: float, now: float) -> dict[str, Any]:
        result = self._empty()
        bars = list(state["bars"])
        result["bar_count"] = float(len(bars))
        if not bars:
            return result
        closes = [bar.close for bar in bars]
        result["bar_ema9"] = self._ema(closes[-min(len(closes), 36):], 9)
        result["bar_ema21"] = self._ema(closes[-min(len(closes), 63):], 21) if len(closes) >= 21 else 0.0
        atr_snapshot = self.get_atr_snapshot_from_bars(bars)
        atr = float(atr_snapshot["atr"])
        result["bar_atr"] = atr
        result["bar_atr_pct"] = (atr / price * 100.0) if price > 0 else 0.0
        result["bar_atr_periods"] = float(atr_snapshot["periods"])
        last_closed_monotonic = float(state.get("last_closed_bar_monotonic", 0.0) or 0.0)
        result["bar_atr_age_sec"] = (
            max(0.0, now - last_closed_monotonic)
            if last_closed_monotonic > 0.0 else -1.0
        )
        if len(closes) >= 21:
            result["bar_trend_up"] = float(result["bar_ema9"] > result["bar_ema21"])
            result["bar_trend_down"] = float(result["bar_ema9"] < result["bar_ema21"])
        return result

    @staticmethod
    def get_atr_snapshot_from_bars(
        bars: list[MinuteBar], period: int = 14
    ) -> dict[str, float]:
        """Return a simple-average true-range ATR over the latest contiguous M1 bars.

        No missing minute is synthesized. A gap breaks the usable suffix, so old
        volatility does not leak across a data outage. At most ``period`` true
        ranges are used; the caller decides the minimum warm-up sample count.
        """
        period = max(1, int(period))
        if len(bars) < 2:
            return {"atr": 0.0, "periods": 0.0, "bars": float(len(bars))}
        contiguous = [bars[-1]]
        for prior in reversed(bars[:-1]):
            following = contiguous[-1]
            if prior.bucket + 1 != following.bucket:
                break
            contiguous.append(prior)
            if len(contiguous) >= period + 1:
                break
        contiguous.reverse()
        periods = max(0, len(contiguous) - 1)
        atr = MinuteBarPatternEngine._atr(contiguous, period=period)
        if not math.isfinite(atr) or atr <= 0.0:
            atr = 0.0
        return {"atr": float(atr), "periods": float(min(period, periods)), "bars": float(len(contiguous))}

    def get_atr_snapshot(self, ticker: str, period: int = 14) -> dict[str, float]:
        """Public read-only ATR snapshot for the execution/risk layer."""
        symbol = str(ticker or "").strip().upper().split("@")[0]
        state = self._states.get(symbol)
        if not isinstance(state, dict):
            return {"atr": 0.0, "periods": 0.0, "bars": 0.0}
        bars = list(state.get("bars", []))
        return self.get_atr_snapshot_from_bars(bars, period=period)

    def _pattern_for_closed_bar(
        self,
        state: dict[str, Any],
        closed_bar: MinuteBar,
        entry_price: float,
        now: float,
    ) -> dict[str, Any]:
        result = self._summarize(state, entry_price, now)
        bars = list(state["bars"])
        result["bar_count"] = float(len(bars))
        if len(bars) < 6 or closed_bar.ticks < self.min_ticks_per_bar:
            return result

        # Only consecutive observed minute bars may form a pattern. Missing bars
        # are not interpolated and invalidate this setup.
        recent_six = bars[-6:]
        buckets = [bar.bucket for bar in recent_six]
        if any(buckets[i] + 1 != buckets[i + 1] for i in range(len(buckets) - 1)):
            return result
        if any(bar.ticks < self.min_ticks_per_bar for bar in recent_six):
            return result

        previous_five = recent_six[:-1]
        bar = recent_six[-1]
        atr = self._atr(bars)
        candle_range = bar.high - bar.low
        if atr <= 0 or candle_range <= 0:
            return result

        body_ratio = abs(bar.close - bar.open) / candle_range
        close_position = (bar.close - bar.low) / candle_range
        prior_high = max(item.high for item in previous_five)
        prior_low = min(item.low for item in previous_five)
        breakout_up = (
            bar.close > prior_high + 0.05 * atr
            and bar.close > bar.open
            and body_ratio >= 0.55
            and close_position >= 0.75
        )
        breakout_down = (
            bar.close < prior_low - 0.05 * atr
            and bar.close < bar.open
            and body_ratio >= 0.55
            and close_position <= 0.25
        )

        closes = [item.close for item in bars]
        ema9 = self._ema(closes[-min(len(closes), 36):], 9)
        ema21 = self._ema(closes[-min(len(closes), 63):], 21) if len(closes) >= 21 else 0.0
        pullback_buy = False
        pullback_sell = False
        if len(bars) >= 21:
            pullback_buy = (
                ema9 > ema21
                and ema9 - 0.35 * atr <= bar.low <= ema9 + 0.15 * atr
                and bar.close > ema9
                and bar.close > bar.open
                and close_position >= 0.60
            )
            pullback_sell = (
                ema9 < ema21
                and ema9 - 0.15 * atr <= bar.high <= ema9 + 0.35 * atr
                and bar.close < ema9
                and bar.close < bar.open
                and close_position <= 0.40
            )

        # Reject a setup if the first observed price of the new minute has already
        # run too far from the signal candle's close. This is a gap/chase guard.
        gap_atr = abs(entry_price - bar.close) / atr
        result.update({
            "bar_ema9": ema9,
            "bar_ema21": ema21,
            "bar_atr": atr,
            "bar_atr_pct": (atr / entry_price * 100.0) if entry_price > 0 else 0.0,
            "bar_body_ratio": body_ratio,
            "bar_close_position": close_position,
            "bar_signal_gap_atr": gap_atr,
            "bar_breakout_up": float(breakout_up),
            "bar_breakout_down": float(breakout_down),
            "bar_pullback_buy": float(pullback_buy),
            "bar_pullback_sell": float(pullback_sell),
            "bar_trend_up": float(ema9 > ema21) if ema21 > 0 else 0.0,
            "bar_trend_down": float(ema9 < ema21) if ema21 > 0 else 0.0,
        })

        direction = 0
        name = ""
        # Breakout has priority over a pullback if the same close satisfies both.
        if breakout_up:
            direction, name = 1, "M1_RANGE_BREAKOUT_BUY"
        elif breakout_down:
            direction, name = -1, "M1_RANGE_BREAKOUT_SELL"
        elif pullback_buy:
            direction, name = 1, "M1_EMA_PULLBACK_BUY"
        elif pullback_sell:
            direction, name = -1, "M1_EMA_PULLBACK_SELL"

        if direction and gap_atr <= self.max_signal_gap_atr:
            result["bar_pattern_confirmed"] = 1.0
            result["bar_pattern_direction_code"] = float(direction)
            result["bar_pattern_name"] = name
            result["_bar_pattern_signal_at_monotonic"] = float(now)
        return result

    def update(
        self, ticker: str, price: float, now: float | None = None, event_timestamp: object | None = None
    ) -> dict[str, Any]:
        """Observe a price update; return measured features and an optional fresh pattern.

        `now` is monotonic seconds so signal freshness is immune to wall-clock jumps.
        The signal is emitted only on a minute boundary that closes a valid candle.
        """
        result = self._empty()
        symbol = str(ticker or "").strip().upper().split("@")[0]
        try:
            value = float(price)
        except (TypeError, ValueError):
            return result
        if not symbol or not math.isfinite(value) or value <= 0:
            return result
        stamp = time.monotonic() if now is None else float(now)
        if not math.isfinite(stamp) or stamp < 0:
            return result
        event_epoch = _event_epoch_seconds(event_timestamp)
        clock_source = "exchange" if event_epoch is not None else "monotonic"
        bucket_time = event_epoch if event_epoch is not None else stamp
        bucket = int(bucket_time // self.bar_seconds)
        state = self._state(symbol)
        prior_source = state.get("clock_source")
        if prior_source is not None and prior_source != clock_source:
            # Never combine monotonic-relative buckets with exchange-time buckets.
            state["bars"].clear()
            state["current"] = None
            state["last_closed_bar_monotonic"] = 0.0
        state["clock_source"] = clock_source
        state["last_event_time"] = bucket_time
        current: MinuteBar | None = state.get("current")
        if current is None:
            state["current"] = MinuteBar(bucket, value, value, value, value)
            return self._summarize(state, value, stamp)
        if bucket < current.bucket:
            # Out-of-order clock/test data must not corrupt a candle.
            return self._summarize(state, value, stamp)
        if bucket == current.bucket:
            current.update(value)
            return self._summarize(state, value, stamp)

        # Finalize the just-finished observed bar. Do not fabricate empty bars.
        closed = current
        gap_minutes = bucket - closed.bucket
        if gap_minutes >= 5:
            # Do not carry EMA/ATR regime across a long data outage or overnight.
            state["bars"].clear()
            state["last_closed_bar_monotonic"] = 0.0
            state["current"] = MinuteBar(bucket, value, value, value, value)
            return self._summarize(state, value, stamp)
        state["bars"].append(closed)
        # The prior candle ended before the current event when minute buckets
        # were skipped. Estimate its close-time on the local monotonic clock so
        # the ATR freshness guard does not treat a delayed close as a fresh bar.
        state["last_closed_bar_monotonic"] = (
            stamp - max(0, gap_minutes - 1) * self.bar_seconds
        )
        state["current"] = MinuteBar(bucket, value, value, value, value)
        if gap_minutes != 1:
            return self._summarize(state, value, stamp)
        return self._pattern_for_closed_bar(state, closed, value, stamp)
