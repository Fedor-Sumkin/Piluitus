"""Shadow-only, deterministic microstructure strategy hypothesis for PILUITUS.

The evaluator deliberately does not place orders. Its score is an auditable
research feature set, not a calibrated probability of profit. Thresholds are
starting hypotheses for MOEX PAPER observation and must be validated
out-of-sample before they can influence execution.
"""
from __future__ import annotations

import math
import os
import time
from collections.abc import Mapping
from typing import Any


def _number(metrics: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    value = metrics.get(key, default)
    if isinstance(value, bool):
        return float(value)
    if not isinstance(value, (int, float, str)):
        return default
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return number if math.isfinite(number) else default


def _minimum_atr_periods() -> int:
    try:
        return min(14, max(2, int(os.getenv("PILUITUS_ATR_MIN_PERIODS", "5"))))
    except (TypeError, ValueError, OverflowError):
        return 5


def evaluate_scalping_candidate(
    direction: str,
    metrics: Mapping[str, Any] | None,
) -> dict[str, Any]:
    """Score a BUY/SELL candidate without changing the active trading decision.

    Components:
      * price/VWAP directional agreement (25);
      * M1 pattern/trend agreement (20);
      * best-queue imbalance (20);
      * trade-size impulse (10);
      * spread relative to M1 ATR (15);
      * quote freshness (10).

    Queue imbalance uses only the visible best Bid/Ask sizes. Finam's size unit
    is not assumed; the normalized ratio is dimensionless. This is not OFI:
    true OFI requires signed changes in best bid/ask prices and quantities over
    time, which is a separate future data-engineering task.
    """
    side = str(direction or "").strip().upper()
    empty = {
        "schema_version": 1,
        "side": side,
        "score": 0.0,
        "eligible": False,
        "regime": "UNKNOWN",
        "components": {},
        "reasons": [],
        "hard_blocks": [],
    }
    if side not in {"BUY", "SELL"}:
        empty["hard_blocks"] = ["invalid_direction"]
        empty["reasons"] = ["Направление не BUY/SELL."]
        return empty
    if not isinstance(metrics, Mapping):
        empty["hard_blocks"] = ["missing_metrics"]
        empty["reasons"] = ["Нет измеренных рыночных метрик."]
        return empty

    reasons: list[str] = []
    hard_blocks: list[str] = []
    components: dict[str, float] = {}

    # Hard data-integrity checks are intentionally stricter in this shadow
    # hypothesis than in the active baseline; they do not block real entries.
    if _number(metrics, "bid_ask_available") < 0.5 or _number(metrics, "spread_available") < 0.5:
        hard_blocks.append("bbo_unavailable")
    if _number(metrics, "order_book_valid") < 0.5:
        hard_blocks.append("order_book_not_confirmed")
    if _number(metrics, "spread_pct", math.inf) > 0.50:
        hard_blocks.append("spread_above_0.50_pct")

    atr = _number(metrics, "bar_atr")
    atr_periods = int(max(0.0, _number(metrics, "bar_atr_periods")))
    min_periods = _minimum_atr_periods()
    if atr <= 0.0 or atr_periods < min_periods:
        hard_blocks.append("atr_not_ready")

    bid_age = _number(metrics, "bid_quote_age_sec", math.inf)
    ask_age = _number(metrics, "ask_quote_age_sec", math.inf)
    max_age = min(5.0, max(0.25, _number(metrics, "bbo_max_age_sec", 2.0)))
    if (
        bid_age < 0.0 or ask_age < 0.0
        or bid_age > max_age or ask_age > max_age
    ):
        hard_blocks.append("bbo_stale")

    price_change = _number(metrics, "price_change_pct")
    price_vs_vwap = _number(metrics, "price_vs_vwap_pct")
    direction_aligned = (
        price_change > 0.0 and price_vs_vwap > 0.0
        if side == "BUY"
        else price_change < 0.0 and price_vs_vwap < 0.0
    )
    components["direction"] = 25.0 if direction_aligned else 0.0
    if direction_aligned:
        reasons.append("price_momentum_and_vwap_aligned")
    else:
        reasons.append("price_momentum_or_vwap_not_aligned")

    pattern_confirmed = _number(metrics, "bar_pattern_confirmed") >= 0.5
    pattern_name = str(metrics.get("bar_pattern_name") or "").strip().upper()
    if pattern_confirmed:
        signal_at = _number(metrics, "_bar_pattern_signal_at_monotonic", -1.0)
        try:
            max_pattern_age = max(2.0, float(os.getenv("PILUITUS_PATTERN_SIGNAL_MAX_AGE_SEC", "15")))
        except (TypeError, ValueError, OverflowError):
            max_pattern_age = 15.0
        pattern_age = time.monotonic() - signal_at if signal_at > 0.0 else math.inf
        pattern_confirmed = -0.25 <= pattern_age <= max_pattern_age
    pattern_side = "BUY" if pattern_name.endswith("_BUY") else "SELL" if pattern_name.endswith("_SELL") else ""
    trend_aligned = (
        _number(metrics, "bar_trend_up") >= 0.5
        if side == "BUY"
        else _number(metrics, "bar_trend_down") >= 0.5
    )
    if pattern_confirmed and pattern_side == side:
        components["m1_pattern"] = 20.0
        reasons.append("fresh_matching_m1_pattern")
    elif pattern_confirmed and pattern_side and pattern_side != side:
        components["m1_pattern"] = 0.0
        hard_blocks.append("m1_pattern_conflicts_with_candidate")
        reasons.append("fresh_m1_pattern_opposes_candidate")
    elif trend_aligned:
        components["m1_pattern"] = 12.0
        reasons.append("m1_ema_trend_aligned_without_fresh_pattern")
    else:
        components["m1_pattern"] = 5.0
        reasons.append("m1_trend_not_confirmed")

    queue_available = (
        _number(metrics, "queue_imbalance_available") >= 0.5
        and _number(metrics, "order_book_valid") >= 0.5
    )
    queue_age = _number(metrics, "queue_imbalance_age_sec", math.inf)
    queue_imbalance = _number(metrics, "queue_imbalance")
    if not queue_available or queue_age < 0.0 or queue_age > max_age:
        components["queue_imbalance"] = 0.0
        reasons.append("best_queue_imbalance_unavailable_or_stale")
    else:
        signed_imbalance = queue_imbalance if side == "BUY" else -queue_imbalance
        if signed_imbalance >= 0.25:
            components["queue_imbalance"] = 20.0
            reasons.append("strong_best_queue_imbalance_supports_side")
        elif signed_imbalance >= 0.10:
            components["queue_imbalance"] = 15.0
            reasons.append("best_queue_imbalance_supports_side")
        elif signed_imbalance > 0.0:
            components["queue_imbalance"] = 10.0
            reasons.append("weak_best_queue_imbalance_support")
        elif signed_imbalance >= -0.10:
            components["queue_imbalance"] = 5.0
            reasons.append("best_queue_imbalance_neutral")
        else:
            components["queue_imbalance"] = 0.0
            reasons.append("best_queue_imbalance_opposes_side")

    volume_spike = _number(metrics, "volume_spike")
    components["volume"] = 10.0 if volume_spike >= 2.0 else 7.0 if volume_spike >= 1.5 else 0.0
    if components["volume"]:
        reasons.append("trade_size_impulse_confirmed")
    else:
        reasons.append("trade_size_impulse_weak")

    atr_pct = _number(metrics, "bar_atr_pct")
    spread_pct = _number(metrics, "spread_pct")
    spread_atr_ratio = spread_pct / atr_pct if atr_pct > 0.0 else math.inf
    if spread_atr_ratio <= 0.10:
        components["spread_vs_atr"] = 15.0
    elif spread_atr_ratio <= 0.20:
        components["spread_vs_atr"] = 12.0
    elif spread_atr_ratio <= 0.35:
        components["spread_vs_atr"] = 8.0
    elif spread_atr_ratio <= 0.50:
        components["spread_vs_atr"] = 4.0
    else:
        components["spread_vs_atr"] = 0.0
    if math.isfinite(spread_atr_ratio):
        reasons.append("spread_to_atr_ratio=%.3f" % spread_atr_ratio)
    else:
        reasons.append("spread_to_atr_ratio_unavailable")

    max_quote_age = max(bid_age, ask_age)
    if math.isfinite(max_quote_age) and max_quote_age <= max_age * 0.5:
        components["freshness"] = 10.0
    elif math.isfinite(max_quote_age) and max_quote_age <= max_age:
        components["freshness"] = 7.0
    else:
        components["freshness"] = 0.0
    reasons.append("bbo_age=%.3fs/max=%.3fs" % (max_quote_age, max_age))

    if trend_aligned:
        regime = "TREND_ALIGNED"
    elif pattern_confirmed and pattern_side == side:
        regime = "M1_PATTERN_TRANSITION"
    elif pattern_confirmed and pattern_side and pattern_side != side:
        regime = "COUNTER_PATTERN"
    else:
        regime = "TRANSITION_OR_RANGE"

    score = round(sum(components.values()), 2)
    eligible = not hard_blocks and score >= 85.0
    return {
        "schema_version": 1,
        "side": side,
        "score": score,
        "eligible": eligible,
        "regime": regime,
        "components": components,
        "reasons": reasons,
        "hard_blocks": hard_blocks,
        "queue_imbalance": round(queue_imbalance, 4) if queue_available else None,
        "spread_atr_ratio": round(spread_atr_ratio, 4) if math.isfinite(spread_atr_ratio) else None,
        "threshold": 85.0,
        "mode": "SHADOW_ONLY",
    }
