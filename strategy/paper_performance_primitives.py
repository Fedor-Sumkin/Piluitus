"""Shared value normalization, timestamps and cohort bucketing for PAPER analytics."""
from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

PAPER_CLOSED_TRADES_STORAGE_KEY = "piluitus:paper:closed_trades"
PAPER_CLOSED_TRADES_STORAGE_LIMIT = 20000

def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _text(value: object, default: str = "UNKNOWN") -> str:
    text = str(value or "").strip()
    return text[:120] if text else default


def _confidence_bucket(value: object) -> str:
    confidence = _number(value)
    if confidence is None or not 0.0 <= confidence <= 100.0:
        return "UNKNOWN"
    lower = int(confidence // 10) * 10
    upper = min(100, lower + 9)
    return f"{lower:02d}-{upper:02d}"

def _exit_timestamp_sort_key(value: object) -> tuple[int, float | str]:
    """Sort ISO timestamps by their actual UTC instant, including mixed offsets."""
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                return (0, parsed.astimezone(timezone.utc).timestamp())
        except (TypeError, ValueError, OverflowError):
            pass
    return (1, str(value or ""))



def _aware_timestamp_epoch(value: object) -> float | None:
    """Return an absolute timestamp only for valid timezone-aware ISO values."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc).timestamp()
    except (TypeError, ValueError, OverflowError):
        return None


def _reentry_gap_bucket(value: object) -> str:
    seconds = _number(value)
    if seconds is None or seconds < 0.0:
        return "UNKNOWN"
    if seconds < 60.0:
        return "LT_60S"
    if seconds < 180.0:
        return "60_179S"
    if seconds < 600.0:
        return "180_599S"
    return "600S_PLUS"

def _holding_time_bucket(value: object) -> str:
    seconds = _number(value)
    if seconds is None or seconds < 0.0:
        return "UNKNOWN"
    if seconds < 60.0:
        return "LT_60S"
    if seconds < 180.0:
        return "60_179S"
    if seconds < 300.0:
        return "180_299S"
    if seconds < 600.0:
        return "300_599S"
    return "600S_PLUS"



def _numeric_bucket(value: object, *, prefix: str, thresholds: tuple[float, ...], suffixes: tuple[str, ...]) -> str:
    number = _number(value)
    if number is None or number < 0.0:
        return f"{prefix}_UNKNOWN"
    for threshold, suffix in zip(thresholds, suffixes):
        if number < threshold:
            return f"{prefix}_{suffix}"
    return f"{prefix}_{suffixes[-1]}"


def _entry_spread_bucket(value: object) -> str:
    return _numeric_bucket(
        value, prefix="SPREAD_PCT", thresholds=(0.02, 0.05, 0.10),
        suffixes=("LT_0_02", "0_02_0_05", "0_05_0_10", "GE_0_10"),
    )


def _entry_bbo_age_bucket(value: object) -> str:
    return _numeric_bucket(
        value, prefix="BBO_AGE_SEC", thresholds=(0.25, 0.50, 1.0),
        suffixes=("LT_0_25", "0_25_0_50", "0_50_1_00", "GE_1_00"),
    )


def _entry_shortfall_bucket(value: object) -> str:
    number = _number(value)
    if number is not None and number < 0.0:
        return "SHORTFALL_BETTER_THAN_BBO"
    return _numeric_bucket(
        value, prefix="SHORTFALL_BPS", thresholds=(1.0, 3.0, 5.0),
        suffixes=("LT_1", "1_3", "3_5", "GE_5"),
    )


def _entry_spread_atr_bucket(value: object) -> str:
    return _numeric_bucket(
        value, prefix="SPREAD_ATR", thresholds=(0.10, 0.25, 0.50),
        suffixes=("LT_0_10", "0_10_0_25", "0_25_0_50", "GE_0_50"),
    )


def _exit_shortfall_bucket(value: object) -> str:
    number = _number(value)
    if number is not None and number < 0.0:
        return "EXIT_SHORTFALL_BETTER_THAN_BBO"
    return _numeric_bucket(
        value, prefix="EXIT_SHORTFALL_BPS", thresholds=(1.0, 3.0, 5.0),
        suffixes=("LT_1", "1_3", "3_5", "GE_5"),
    )


def _exit_depth_impact_bucket(value: object) -> str:
    number = _number(value)
    if number is not None and number < 0.0:
        return "EXIT_DEPTH_BETTER_THAN_TOP"
    return _numeric_bucket(
        value, prefix="EXIT_DEPTH_IMPACT_RUB", thresholds=(1.0, 3.0, 5.0),
        suffixes=("LT_1", "1_3", "3_5", "GE_5"),
    )

def _entry_signal_drift_bucket(value: object) -> str:
    number = _number(value)
    if number is None:
        return "SIGNAL_DRIFT_UNKNOWN"
    if number < -0.10:
        return "FAVORABLE_LT_MINUS_0_10"
    if number < -0.05:
        return "FAVORABLE_MINUS_0_10_TO_0_05"
    if number <= 0.05:
        return "NEAR_FLAT"
    if number <= 0.10:
        return "ADVERSE_0_05_TO_0_10"
    return "ADVERSE_GT_0_10"

def _moscow_exit_date(row: Mapping[str, Any]) -> str:
    """Return the Moscow calendar date for an aware exit timestamp only."""
    raw = row.get("exit_timestamp")
    if not isinstance(raw, str) or not raw.strip():
        return "UNKNOWN"
    try:
        stamp = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
        if stamp.tzinfo is None or stamp.utcoffset() is None:
            return "UNKNOWN"
        return stamp.astimezone(timezone(timedelta(hours=3))).date().isoformat()
    except (TypeError, ValueError, OverflowError):
        return "UNKNOWN"
