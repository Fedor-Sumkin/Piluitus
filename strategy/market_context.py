"""Point-in-time market regime and executable entry-cost helpers.

Market-regime classification remains descriptive. The cost-to-ATR validator
is used as a conservative PAPER-only entry-quality gate.
"""
from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any


def _finite(metrics: Mapping[str, Any], key: str) -> float | None:
    value = metrics.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def classify_market_regime(
    metrics: Mapping[str, Any],
    *,
    atr_reference_pct: float | None = None,
    atr_reference_samples: int = 0,
) -> dict[str, Any]:
    """Classify conditions using only metrics available at candidate time.

    Volatility is relative to the median ATR% observed *before* this candidate
    for the same ticker. Until 20 prior observations exist it is explicitly
    unclassified; no cross-ticker absolute ATR threshold is assumed.
    """
    atr_pct = _finite(metrics, "bar_atr_pct")
    spread_atr = _finite(metrics, "spread_atr_ratio")
    if spread_atr is None:
        spread_pct = _finite(metrics, "spread_pct")
        atr_pct_for_spread = atr_pct
        if spread_pct is not None and atr_pct_for_spread is not None and atr_pct_for_spread > 0:
            spread_atr = spread_pct / atr_pct_for_spread

    trade_count = _finite(metrics, "trade_count_60s")
    trade_coverage = _finite(metrics, "trade_history_coverage_sec")
    if spread_atr is not None and spread_atr > 0.50:
        liquidity = "LOW_LIQUIDITY"
    elif (
        trade_count is not None and trade_coverage is not None
        and trade_coverage >= 60.0 and trade_count < 3.0
    ):
        liquidity = "LOW_LIQUIDITY"
    elif spread_atr is not None and spread_atr >= 0.0:
        liquidity = "NORMAL_OR_UNKNOWN"
    else:
        liquidity = "LIQUIDITY_UNAVAILABLE"

    volatility = "VOLATILITY_UNCLASSIFIED"
    atr_ratio = None
    reference = (
        float(atr_reference_pct)
        if atr_reference_pct is not None and math.isfinite(float(atr_reference_pct))
        and float(atr_reference_pct) > 0.0 else None
    )
    if (
        atr_pct is not None and atr_pct > 0.0 and reference is not None
        and int(atr_reference_samples) >= 20
    ):
        atr_ratio = atr_pct / reference
        # Ratios such as 0.30 / 0.20 can be represented as
        # 1.4999999999999998 in binary floating point. Treat values within
        # a tiny numerical tolerance as being on the documented boundary.
        if atr_ratio >= 1.50 - 1e-9:
            volatility = "HIGH_VOLATILITY"
        elif atr_ratio <= 0.65 + 1e-9:
            volatility = "LOW_VOLATILITY"
        else:
            volatility = "NORMAL_VOLATILITY"

    up_votes = down_votes = 0
    evidence_count = 0
    trade_ready_flag = _finite(metrics, "trade_data_ready")
    trade_age = _finite(metrics, "trade_metrics_age_sec")
    trade_coverage = _finite(metrics, "trade_history_coverage_sec")
    trade_ready = bool(
        trade_ready_flag is not None and trade_ready_flag >= 0.5
        and trade_age is not None and 0.0 <= trade_age <= 5.0
        and trade_coverage is not None and trade_coverage >= 60.0
    )
    if trade_ready:
        for key in ("price_change_pct", "price_vs_vwap_pct"):
            value = _finite(metrics, key)
            if value is not None:
                evidence_count += 1
                up_votes += int(value > 0.0)
                down_votes += int(value < 0.0)
    up = _finite(metrics, "bar_trend_up")
    down = _finite(metrics, "bar_trend_down")
    if up is not None and down is not None and (up >= 0.5 or down >= 0.5):
        evidence_count += 1
        up_votes += int(up >= 0.5 and down < 0.5)
        down_votes += int(down >= 0.5 and up < 0.5)
    flow = _finite(metrics, "trade_flow_imbalance_30s") if trade_ready else None
    if flow is not None and abs(flow) >= 0.10:
        evidence_count += 1
        up_votes += int(flow >= 0.10)
        down_votes += int(flow <= -0.10)

    if evidence_count >= 2 and up_votes >= 2 and down_votes == 0:
        directional = "TREND_UP"
    elif evidence_count >= 2 and down_votes >= 2 and up_votes == 0:
        directional = "TREND_DOWN"
    elif evidence_count >= 2:
        directional = "RANGE_OR_MIXED"
    else:
        directional = "DIRECTION_UNAVAILABLE"

    if liquidity == "LOW_LIQUIDITY":
        overall = "LOW_LIQUIDITY"
    elif volatility == "HIGH_VOLATILITY":
        overall = "HIGH_VOLATILITY"
    elif directional in {"TREND_UP", "TREND_DOWN"}:
        overall = "TREND"
    elif directional == "RANGE_OR_MIXED":
        overall = "RANGE_OR_MIXED"
    else:
        overall = "INSUFFICIENT_DATA"

    return {
        "market_regime": overall,
        "volatility_regime": volatility,
        "liquidity_regime": liquidity,
        "directional_regime": directional,
        "atr_pct": atr_pct,
        "atr_reference_pct": reference,
        "atr_reference_samples": max(0, int(atr_reference_samples)),
        "atr_vs_reference_ratio": atr_ratio,
        "spread_atr_ratio": spread_atr,
        "trade_count_60s": trade_count,
        "direction_evidence_count": evidence_count,
    }


def estimate_entry_economics(
    *,
    side: str,
    bid: float,
    ask: float,
    commission_pct_per_side: float,
    slippage_bps_per_side: float,
    atr_pct: float | None = None,
) -> dict[str, Any]:
    """Compute exact break-even future executable quote under stated costs.

    Entry starts at Ask for BUY and Bid for SELL. The opposite executable quote
    is adjusted for adverse slippage on exit. Commission is charged on both
    entry and exit notionals. The returned required move is measured from the
    entry executable quote to the future opposite-side quote.
    """
    direction = str(side or "").strip().upper()
    values = (bid, ask, commission_pct_per_side, slippage_bps_per_side)
    try:
        bid_value, ask_value, commission_pct, slippage_bps = map(float, values)
    except (TypeError, ValueError, OverflowError):
        return {"available": False, "reason": "INVALID_INPUT"}
    if (
        direction not in {"BUY", "SELL"}
        or not all(math.isfinite(value) for value in (bid_value, ask_value, commission_pct, slippage_bps))
        or bid_value <= 0.0 or ask_value < bid_value
        or commission_pct < 0.0 or commission_pct >= 100.0
        or slippage_bps < 0.0 or slippage_bps >= 10000.0
    ):
        return {"available": False, "reason": "INVALID_INPUT"}

    commission_rate = commission_pct / 100.0
    slippage_rate = slippage_bps / 10000.0
    entry_quote = ask_value if direction == "BUY" else bid_value
    entry_fill = entry_quote * (1.0 + slippage_rate if direction == "BUY" else 1.0 - slippage_rate)

    if direction == "BUY":
        required_exit_fill = entry_fill * (1.0 + commission_rate) / (1.0 - commission_rate)
        required_exit_quote = required_exit_fill / (1.0 - slippage_rate)
        move_from_executable_pct = (required_exit_quote / entry_quote - 1.0) * 100.0
    else:
        required_exit_fill = entry_fill * (1.0 - commission_rate) / (1.0 + commission_rate)
        required_exit_quote = required_exit_fill / (1.0 + slippage_rate)
        move_from_executable_pct = (entry_quote / required_exit_quote - 1.0) * 100.0

    mid = (bid_value + ask_value) / 2.0
    spread_pct = (ask_value - bid_value) / mid * 100.0 if mid > 0.0 else math.nan
    # Measure the required move from the current mid: this includes crossing
    # the entry spread as well as both-side commission and slippage.
    required_move_pct = (
        (required_exit_quote / mid - 1.0) * 100.0
        if direction == "BUY"
        else (mid / required_exit_quote - 1.0) * 100.0
    )
    atr_value = None
    try:
        candidate_atr = float(atr_pct) if atr_pct is not None else math.nan
        if math.isfinite(candidate_atr) and candidate_atr > 0.0:
            atr_value = candidate_atr
    except (TypeError, ValueError, OverflowError):
        pass
    cost_to_atr = required_move_pct / atr_value if atr_value else None
    return {
        "available": True,
        "side": direction,
        "entry_executable_price": entry_quote,
        "entry_fill_price": entry_fill,
        "breakeven_exit_executable_price": required_exit_quote,
        "minimum_favorable_move_pct": max(0.0, required_move_pct),
        "move_beyond_entry_executable_pct": max(0.0, move_from_executable_pct),
        "entry_spread_pct": spread_pct,
        "commission_pct_per_side": commission_pct,
        "slippage_bps_per_side": slippage_bps,
        "cost_to_atr_ratio": cost_to_atr,
        "cost_model_status": (
            "CONFIGURED" if commission_pct > 0.0 and slippage_bps > 0.0
            else "INCOMPLETE_OR_ZERO_ASSUMPTION"
        ),
    }



def validate_entry_economics(
    *,
    side: str,
    bid: float,
    ask: float,
    commission_pct_per_side: float,
    slippage_bps_per_side: float,
    atr_pct: float,
    max_cost_to_atr_ratio: float = 0.50,
) -> dict[str, Any]:
    """Gate an entry when known round-trip execution costs consume too much M1 ATR.

    The calculation includes crossing the current spread, commission on both
    sides, and configured adverse slippage. A zero slippage assumption is still
    reported as incomplete; the resulting cost/ATR ratio is then a lower bound.
    """
    try:
        limit = float(max_cost_to_atr_ratio)
        atr_value = float(atr_pct)
    except (TypeError, ValueError, OverflowError):
        return {
            "available": False,
            "eligible": False,
            "reason": "INVALID_COST_GATE_INPUT",
        }
    if not math.isfinite(limit) or limit <= 0.0 or not math.isfinite(atr_value) or atr_value <= 0.0:
        return {
            "available": False,
            "eligible": False,
            "reason": "ATR_OR_COST_LIMIT_UNAVAILABLE",
        }
    limit = min(2.0, max(0.05, limit))
    result = estimate_entry_economics(
        side=side,
        bid=bid,
        ask=ask,
        commission_pct_per_side=commission_pct_per_side,
        slippage_bps_per_side=slippage_bps_per_side,
        atr_pct=atr_value,
    )
    if not result.get("available"):
        return {**result, "eligible": False, "reason": "ENTRY_ECONOMICS_UNAVAILABLE"}
    try:
        ratio_value = float(result.get("cost_to_atr_ratio"))
    except (TypeError, ValueError, OverflowError):
        ratio_value = math.nan
    ratio = ratio_value if math.isfinite(ratio_value) else None
    if ratio is None:
        return {
            **result,
            "eligible": False,
            "max_cost_to_atr_ratio": limit,
            "reason": "COST_TO_ATR_UNAVAILABLE",
        }
    eligible = ratio <= limit + 1e-12
    return {
        **result,
        "eligible": eligible,
        "max_cost_to_atr_ratio": limit,
        "reason": (
            "ENTRY_COST_TO_ATR_OK" if eligible else "ENTRY_COST_TO_ATR_EXCEEDED"
        ),
    }
