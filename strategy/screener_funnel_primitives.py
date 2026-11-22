"""Shared numeric, timestamp and flow-metric primitives for the screener funnel."""
from __future__ import annotations

import logging
import math
import os
from datetime import datetime, timezone


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



def _best_quote_ofi_delta(
    previous_bid: float, previous_bid_size: float,
    previous_ask: float, previous_ask_size: float,
    current_bid: float, current_bid_size: float,
    current_ask: float, current_ask_size: float,
) -> float | None:
    """Cont-Kukanov-Stoikov best-quote OFI increment for one ordered event."""
    values = (
        previous_bid, previous_bid_size, previous_ask, previous_ask_size,
        current_bid, current_bid_size, current_ask, current_ask_size,
    )
    if not all(math.isfinite(float(value)) for value in values):
        return None
    if min(values) < 0.0 or previous_bid <= 0.0 or previous_ask <= 0.0:
        return None
    if current_bid <= 0.0 or current_ask <= 0.0:
        return None
    if previous_ask < previous_bid or current_ask < current_bid:
        return None
    if min(previous_bid_size, previous_ask_size, current_bid_size, current_ask_size) <= 0.0:
        return None

    bid_flow = (
        (current_bid_size if current_bid >= previous_bid else 0.0)
        - (previous_bid_size if current_bid <= previous_bid else 0.0)
    )
    ask_flow = (
        -(current_ask_size if current_ask <= previous_ask else 0.0)
        + (previous_ask_size if current_ask >= previous_ask else 0.0)
    )
    return float(bid_flow + ask_flow)


def _ofi_window(history, now, window_sec=5.0, min_samples=3, min_coverage_sec=1.0, max_age_sec=2.0):
    """Return depth-normalized event OFI; no fabricated zero during warmup."""
    rows = []
    try:
        source = list(history or ())
    except (TypeError, ValueError):
        source = []
    for item in source:
        if not isinstance(item, (tuple, list)) or len(item) < 3:
            continue
        try:
            ts, delta, depth = float(item[0]), float(item[1]), float(item[2])
        except (TypeError, ValueError, OverflowError):
            continue
        if (
            math.isfinite(ts) and math.isfinite(delta) and math.isfinite(depth)
            and ts <= now and now - ts <= window_sec and depth > 0.0
        ):
            rows.append((ts, delta, depth))
    rows.sort(key=lambda item: item[0])
    if not rows:
        return {
            "value": 0.0, "raw": 0.0, "available": 0.0,
            "samples": 0, "coverage_sec": 0.0, "age_sec": -1.0,
        }
    # Keep only strictly ordered events; duplicates must not manufacture coverage.
    ordered = []
    for row in rows:
        if ordered and row[0] <= ordered[-1][0]:
            continue
        ordered.append(row)
    age = max(0.0, now - ordered[-1][0])
    coverage = max(0.0, ordered[-1][0] - ordered[0][0])
    raw = sum(row[1] for row in ordered)
    depth_scale = sum(row[2] for row in ordered)
    available = (
        len(ordered) >= max(1, int(min_samples))
        and coverage >= max(0.0, float(min_coverage_sec))
        and age <= max(0.0, float(max_age_sec))
        and depth_scale > 0.0
    )
    return {
        "value": float(raw / depth_scale) if available else 0.0,
        "raw": float(raw) if available else 0.0,
        "available": float(available),
        "samples": len(ordered),
        "coverage_sec": float(coverage),
        "age_sec": float(age),
    }


def _queue_imbalance_window(history, now, window_sec=5.0, min_samples=3, min_coverage_sec=1.0):
    """Time-weighted average of accepted best-queue imbalance observations."""
    rows = []
    try:
        source = list(history or ())
    except (TypeError, ValueError):
        source = []
    for item in source:
        if not isinstance(item, (tuple, list)) or len(item) < 2:
            continue
        try:
            ts, value = float(item[0]), float(item[1])
        except (TypeError, ValueError, OverflowError):
            continue
        if math.isfinite(ts) and math.isfinite(value) and ts <= now and now - ts <= window_sec and -1.0 <= value <= 1.0:
            rows.append((ts, value))
    rows.sort(key=lambda item: item[0])
    if not rows:
        return {"value": 0.0, "available": 0.0, "coverage_sec": 0.0, "samples": 0, "age_sec": -1.0, "positive_time_pct": 0.0}
    collapsed = []
    for ts, value in rows:
        if collapsed and ts <= collapsed[-1][0]:
            collapsed[-1] = (ts, value)
        else:
            collapsed.append((ts, value))
    start_window = max(now - max(0.1, window_sec), collapsed[0][0])
    weighted = positive_duration = covered = 0.0
    for index, (ts, value) in enumerate(collapsed):
        start = max(start_window, ts)
        end = min(now, collapsed[index + 1][0] if index + 1 < len(collapsed) else now)
        duration = max(0.0, end - start)
        weighted += value * duration
        covered += duration
        if value > 0.0:
            positive_duration += duration
    coverage = max(0.0, now - start_window)
    age = max(0.0, now - collapsed[-1][0])
    average = weighted / covered if covered > 0.0 else collapsed[-1][1]
    available = len(collapsed) >= max(1, min_samples) and coverage >= max(0.0, min_coverage_sec)
    return {
        "value": max(-1.0, min(1.0, average)),
        "available": float(available),
        "coverage_sec": coverage,
        "samples": len(collapsed),
        "age_sec": age,
        "positive_time_pct": positive_duration / covered * 100.0 if covered > 0.0 else 0.0,
    }
