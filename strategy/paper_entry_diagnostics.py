"""Pure entry-fill diagnostics for PAPER execution research.

All shortfall values are adverse-cost measures: positive means the fill was
worse than the executable top-of-book reference for the selected side.
"""
from __future__ import annotations

import math
from typing import Any


def _finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def build_entry_execution_diagnostics(
    *,
    side: object,
    quantity: object,
    fill_price: object,
    executable_bbo_price: object,
    bid: object,
    ask: object,
    bid_age_sec: object = None,
    ask_age_sec: object = None,
    atr: object = None,
    signal_reference_price: object = None,
    depth_vwap: object = None,
) -> dict[str, Any]:
    """Calculate observed entry spread, depth impact, slippage and signal drift.

    Missing/invalid inputs remain None. This helper never fabricates a zero
    shortfall when the executable BBO or fill reference is unavailable.
    """
    keys = (
        "entry_mid_price",
        "entry_executable_bbo_price",
        "entry_bid_age_sec",
        "entry_ask_age_sec",
        "entry_spread_to_atr_ratio",
        "entry_depth_impact_rub",
        "entry_configured_slippage_rub",
        "entry_execution_shortfall_rub",
        "entry_execution_shortfall_bps",
        "entry_signal_adverse_drift_pct",
    )
    result: dict[str, Any] = {key: None for key in keys}
    direction = str(side or "").strip().upper()
    qty = _finite(quantity)
    fill = _finite(fill_price)
    executable = _finite(executable_bbo_price)
    bid_value = _finite(bid)
    ask_value = _finite(ask)
    if direction not in {"BUY", "SELL"} or qty is None or qty <= 0:
        return result

    for field, raw in (("entry_bid_age_sec", bid_age_sec), ("entry_ask_age_sec", ask_age_sec)):
        value = _finite(raw)
        result[field] = round(max(0.0, value), 6) if value is not None and value >= 0.0 else None

    mid = None
    if bid_value is not None and ask_value is not None and bid_value > 0.0 and ask_value >= bid_value:
        mid = (bid_value + ask_value) / 2.0
        result["entry_mid_price"] = round(mid, 8)
        atr_value = _finite(atr)
        if atr_value is not None and atr_value > 0.0:
            result["entry_spread_to_atr_ratio"] = round((ask_value - bid_value) / atr_value, 6)

    if executable is None or executable <= 0.0 or fill is None or fill <= 0.0:
        return result

    result["entry_executable_bbo_price"] = round(executable, 8)
    sign = 1.0 if direction == "BUY" else -1.0
    depth_reference = _finite(depth_vwap)
    if depth_reference is None or depth_reference <= 0.0:
        depth_reference = executable

    depth_impact = (depth_reference - executable) * qty * sign
    configured_slippage = (fill - depth_reference) * qty * sign
    total_shortfall = (fill - executable) * qty * sign
    result["entry_depth_impact_rub"] = round(depth_impact, 6)
    result["entry_configured_slippage_rub"] = round(configured_slippage, 6)
    result["entry_execution_shortfall_rub"] = round(total_shortfall, 6)
    result["entry_execution_shortfall_bps"] = round(
        total_shortfall / (executable * qty) * 10_000.0, 6
    )

    signal_reference = _finite(signal_reference_price)
    if signal_reference is not None and signal_reference > 0.0:
        result["entry_signal_adverse_drift_pct"] = round(
            (fill - signal_reference) / signal_reference * 100.0 * sign, 6
        )
    return result
