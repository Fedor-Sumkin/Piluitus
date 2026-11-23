"""Deterministic evidence gate for LLM-generated scalping decisions.

This module intentionally treats the LLM as a candidate signal proposer, not as
an authority on facts which were not measured by the quote stream.
"""
from __future__ import annotations

import math
import time
import os
from typing import Any


MIN_VOLUME_SPIKE = 1.5
MIN_TOP_SIZE_DROP_PCT = 50.0
MIN_SIZE_HISTORY_SAMPLES = 3
MAX_SPREAD_PCT = 0.50
MAX_SIZE_SIGNAL_AGE_SEC = 2.0
try:
    MAX_BAR_PATTERN_SIGNAL_AGE_SEC = max(2.0, float(os.getenv("PILUITUS_PATTERN_SIGNAL_MAX_AGE_SEC", "15")))
except (TypeError, ValueError):
    MAX_BAR_PATTERN_SIGNAL_AGE_SEC = 15.0
ALLOWED_BAR_PATTERNS = {
    "M1_RANGE_BREAKOUT_BUY", "M1_RANGE_BREAKOUT_SELL",
    "M1_EMA_PULLBACK_BUY", "M1_EMA_PULLBACK_SELL",
}


def _num(metrics: dict[str, Any], key: str, default: float = 0.0) -> float:
    try:
        value = float(metrics.get(key, default))
        return value if math.isfinite(value) else default
    except (TypeError, ValueError):
        return default


def _fresh_bar_pattern_direction(metrics: dict[str, Any]) -> str | None:
    """Return a supported 1m-pattern direction only while its trigger is fresh."""
    if _num(metrics, "bar_pattern_confirmed") < 0.5:
        return None
    name = str(metrics.get("bar_pattern_name", "") or "").strip().upper()
    if name not in ALLOWED_BAR_PATTERNS:
        return None
    required_flag = {
        "M1_RANGE_BREAKOUT_BUY": "bar_breakout_up",
        "M1_RANGE_BREAKOUT_SELL": "bar_breakout_down",
        "M1_EMA_PULLBACK_BUY": "bar_pullback_buy",
        "M1_EMA_PULLBACK_SELL": "bar_pullback_sell",
    }[name]
    if _num(metrics, required_flag) < 0.5:
        return None
    signal_at = _num(metrics, "_bar_pattern_signal_at_monotonic", -1.0)
    if signal_at <= 0.0:
        return None
    age = time.monotonic() - signal_at
    if age < -0.25 or age > MAX_BAR_PATTERN_SIGNAL_AGE_SEC:
        return None
    code = _num(metrics, "bar_pattern_direction_code")
    expected_direction = "BUY" if name.endswith("_BUY") else "SELL"
    if expected_direction == "BUY" and code >= 0.5:
        return "BUY"
    if expected_direction == "SELL" and code <= -0.5:
        return "SELL"
    return None

def fresh_bar_pattern_direction(metrics: dict[str, Any]) -> str | None:
    """Expose the same freshness-checked M1 pattern used by the entry guard."""
    if not isinstance(metrics, dict):
        return None
    return _fresh_bar_pattern_direction(metrics)


def classify_m1_evidence_state(metrics: object, direction: str) -> str:
    """Classify only confirmed fresh M1 patterns or a warmed closed-bar EMA trend."""
    side = str(direction or "").strip().upper()
    if side not in {"BUY", "SELL"} or not isinstance(metrics, dict):
        return "UNAVAILABLE"

    atr_periods = _num(metrics, "bar_atr_periods", 0.0)
    atr_age = _num(metrics, "bar_atr_age_sec", -1.0)
    if atr_periods < 5.0 or atr_age < 0.0 or atr_age > 180.0:
        return "UNAVAILABLE"

    pattern_direction = _fresh_bar_pattern_direction(metrics)
    if pattern_direction is not None:
        return "ALIGNED" if pattern_direction == side else "OPPOSITE"

    if _num(metrics, "bar_count", 0.0) < 21.0:
        return "UNAVAILABLE"
    trend_up = _num(metrics, "bar_trend_up", 0.0) >= 0.5
    trend_down = _num(metrics, "bar_trend_down", 0.0) >= 0.5
    if trend_up and trend_down:
        return "UNAVAILABLE"
    if side == "BUY":
        if trend_down:
            return "OPPOSITE"
        if trend_up:
            return "ALIGNED"
    else:
        if trend_up:
            return "OPPOSITE"
        if trend_down:
            return "ALIGNED"
    return "NEUTRAL"


def validate_atr_readiness(metrics: dict[str, Any]) -> tuple[bool, str]:
    """Check that a valid completed-M1 ATR exists before spending an LLM call."""
    try:
        min_periods = min(14, max(2, int(os.getenv("PILUITUS_ATR_MIN_PERIODS", "5"))))
    except (TypeError, ValueError):
        min_periods = 5
    atr = _num(metrics, "bar_atr")
    periods = max(0, int(_num(metrics, "bar_atr_periods")))
    age = _num(metrics, "bar_atr_age_sec", -1.0)
    try:
        max_age = float(os.getenv("PILUITUS_ATR_MAX_AGE_SEC", "120"))
    except (TypeError, ValueError, OverflowError):
        max_age = 120.0
    if not math.isfinite(max_age) or max_age <= 0.0:
        max_age = 120.0
    if not math.isfinite(atr) or atr <= 0.0 or periods < min_periods:
        return False, (
            f"Недостаточно закрытых M1-диапазонов для ATR: {periods}/{min_periods}; "
            "кандидат не отправлен в LLM."
        )
    if age < 0.0 or age > max_age:
        age_label = "неизвестен" if age < 0.0 else f"{age:.1f}s"
        return False, (
            f"История ATR устарела: возраст последней закрытой M1-свечи={age_label}; "
            f"лимит {max_age:.0f}s. Кандидат не отправлен в LLM."
        )
    return True, "Достаточная и актуальная M1-история для расчёта ATR подтверждена."


def classify_direction_evidence(direction: str, metrics: dict[str, Any]) -> str:
    """Classify price-momentum/VWAP evidence without treating neutral VWAP as a veto."""
    side = str(direction or "").strip().upper()
    if side not in {"BUY", "SELL"} or not isinstance(metrics, dict):
        return "UNAVAILABLE"
    if "price_change_pct" not in metrics or "price_vs_vwap_pct" not in metrics:
        return "UNAVAILABLE"
    momentum = _num(metrics, "price_change_pct", float("nan"))
    vs_vwap = _num(metrics, "price_vs_vwap_pct", float("nan"))
    if not math.isfinite(momentum) or not math.isfinite(vs_vwap):
        return "UNAVAILABLE"
    try:
        momentum_band = max(0.0, float(os.getenv("PILUITUS_DIRECTION_MOMENTUM_NEUTRAL_PCT", "0.01")))
        vwap_band = max(0.0, float(os.getenv("PILUITUS_DIRECTION_VWAP_NEUTRAL_PCT", "0.03")))
    except (TypeError, ValueError, OverflowError):
        momentum_band, vwap_band = 0.01, 0.03
    sign = 1.0 if side == "BUY" else -1.0
    signed_momentum = momentum * sign
    signed_vwap = vs_vwap * sign
    if signed_momentum < -momentum_band or signed_vwap < -vwap_band:
        return "OPPOSITE"
    if signed_momentum > momentum_band and signed_vwap > vwap_band:
        return "ALIGNED"
    if signed_momentum >= -momentum_band and signed_vwap >= -vwap_band:
        return "NEUTRAL"
    return "UNAVAILABLE"


def infer_candidate_direction(metrics: dict[str, Any]) -> str | None:
    """Use a fresh, closed 1m pattern first; otherwise use momentum + VWAP.

    This stage proposes a direction only. Spread, quote freshness, relative
    last-trade size, risk, capital and execution guards remain independent.
    """
    if not isinstance(metrics, dict):
        return None
    pattern_direction = _fresh_bar_pattern_direction(metrics)
    change = _num(metrics, "price_change_pct", 0.0)
    if pattern_direction in {"BUY", "SELL"}:
        return (
            pattern_direction
            if classify_direction_evidence(pattern_direction, metrics) in {"ALIGNED", "NEUTRAL"}
            else None
        )
    try:
        momentum_band = max(
            0.0, float(os.getenv("PILUITUS_DIRECTION_MOMENTUM_NEUTRAL_PCT", "0.01"))
        )
    except (TypeError, ValueError, OverflowError):
        momentum_band = 0.01
    if change > momentum_band:
        return "BUY" if classify_direction_evidence("BUY", metrics) in {"ALIGNED", "NEUTRAL"} else None
    if change < -momentum_band:
        return "SELL" if classify_direction_evidence("SELL", metrics) in {"ALIGNED", "NEUTRAL"} else None
    return None
