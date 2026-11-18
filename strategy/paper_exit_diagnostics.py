"""Pure execution-cost attribution for PAPER exits.

Positive cost values always mean an adverse fill relative to the executable
closing-side BBO. Missing depth references remain unknown, not zero.
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


def build_exit_execution_diagnostics(
    *,
    position_side: object,
    quantity: object,
    fill_price: object,
    executable_bbo_price: object,
    bid: object = None,
    ask: object = None,
    bid_age_sec: object = None,
    ask_age_sec: object = None,
    order_book_age_sec: object = None,
    depth_vwap: object = None,
    depth_fill_ratio: object = None,
    depth_levels_used: object = None,
) -> dict[str, Any]:
    """Separate depth impact from configured slippage for a closing fill.

    BUY positions close by selling at Bid; SELL positions close by buying at
    Ask. Cost signs are therefore based on the closing transaction side.
    """
    fields = (
        "exit_executable_bbo_price",
        "exit_depth_vwap",
        "exit_bid_age_sec",
        "exit_ask_age_sec",
        "exit_order_book_age_sec",
        "exit_depth_impact_rub",
        "exit_configured_slippage_rub",
        "exit_execution_shortfall_rub",
        "exit_execution_shortfall_bps",
        "exit_depth_fill_ratio",
        "exit_depth_levels_used",
        "exit_diagnostics_status",
    )
    result: dict[str, Any] = {key: None for key in fields}
    side = str(position_side or "").strip().upper()
    qty = _finite(quantity)
    fill = _finite(fill_price)
    executable = _finite(executable_bbo_price)
    if side not in {"BUY", "SELL"} or qty is None or qty <= 0:
        return result

    for field, raw in (
        ("exit_bid_age_sec", bid_age_sec),
        ("exit_ask_age_sec", ask_age_sec),
        ("exit_order_book_age_sec", order_book_age_sec),
    ):
        value = _finite(raw)
        result[field] = round(value, 6) if value is not None and value >= 0 else None

    bid_value, ask_value = _finite(bid), _finite(ask)
    if (
        bid_value is not None and ask_value is not None
        and bid_value > 0 and ask_value >= bid_value
    ):
        result["exit_bid"] = round(bid_value, 8)
        result["exit_ask"] = round(ask_value, 8)

    if executable is None or executable <= 0 or fill is None or fill <= 0:
        result["exit_diagnostics_status"] = "MISSING_EXECUTABLE_REFERENCE"
        return result

    result["exit_executable_bbo_price"] = round(executable, 8)
    transaction_sign = -1.0 if side == "BUY" else 1.0
    depth = _finite(depth_vwap)
    has_depth = depth is not None and depth > 0
    reference = depth if has_depth else executable
    if has_depth:
        result["exit_depth_vwap"] = round(depth, 8)
        result["exit_depth_impact_rub"] = round(
            (reference - executable) * qty * transaction_sign, 6
        )
    result["exit_configured_slippage_rub"] = round(
        (fill - reference) * qty * transaction_sign, 6
    )
    shortfall = (fill - executable) * qty * transaction_sign
    result["exit_execution_shortfall_rub"] = round(shortfall, 6)
    result["exit_execution_shortfall_bps"] = round(
        shortfall / (executable * qty) * 10_000.0, 6
    )

    ratio = _finite(depth_fill_ratio)
    result["exit_depth_fill_ratio"] = (
        round(min(1.0, max(0.0, ratio)), 6) if ratio is not None else None
    )
    levels = _finite(depth_levels_used)
    result["exit_depth_levels_used"] = (
        max(0, int(levels)) if levels is not None and levels >= 0 else None
    )
    result["exit_diagnostics_status"] = "DEPTH_REFERENCE_AVAILABLE" if has_depth else "BBO_ONLY"
    return result
