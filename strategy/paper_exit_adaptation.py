"""Adaptive PAPER trailing calculations using comparable market observables.

Liquidity is deliberately treated as a *relative raw-size ratio*. Finam's public
ORDER_BOOK schema does not define whether buy_size/sell_size are shares or lots,
so this module never converts those values into executable quantities.
"""
from __future__ import annotations

import math
from typing import Any


def _finite(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def calculate_adaptive_trailing_gap(
    *,
    initial_risk_distance: object,
    base_gap_r: object,
    entry_atr: object = None,
    current_atr: object = None,
    entry_spread: object = None,
    current_spread: object = None,
    entry_exit_top_size_raw: object = None,
    current_exit_top_size_raw: object = None,
) -> dict[str, Any]:
    """Return a volatility/spread/liquidity adjusted trailing distance.

    ATR expansion widens the gap modestly to reduce noise exits. Spread expansion
    adds a capped microstructure buffer. A deterioration in same-side displayed
    top-level size tightens the gap, protecting profit when executable liquidity
    thins. The final distance is bounded to 0.25R..1.5R and never affects an
    already tighter stop (the caller enforces monotonic tightening).
    """
    risk = _finite(initial_risk_distance)
    gap_r = _finite(base_gap_r)
    if risk is None or risk <= 0.0 or gap_r is None or gap_r <= 0.0:
        return {"available": False, "reason": "INVALID_RISK_OR_GAP"}

    entry_atr_value = _finite(entry_atr)
    current_atr_value = _finite(current_atr)
    atr_ratio = 1.0
    if (
        entry_atr_value is not None and entry_atr_value > 0.0
        and current_atr_value is not None and current_atr_value > 0.0
    ):
        atr_ratio = min(1.5, max(0.75, current_atr_value / entry_atr_value))

    entry_spread_value = _finite(entry_spread)
    current_spread_value = _finite(current_spread)
    spread_expansion = 0.0
    if (
        entry_spread_value is not None and entry_spread_value >= 0.0
        and current_spread_value is not None and current_spread_value >= 0.0
    ):
        spread_expansion = max(0.0, current_spread_value - entry_spread_value)
    spread_buffer = min(risk * 0.25, spread_expansion * 0.5)

    entry_size = _finite(entry_exit_top_size_raw)
    current_size = _finite(current_exit_top_size_raw)
    liquidity_ratio = 1.0
    liquidity_available = bool(
        entry_size is not None and entry_size > 0.0
        and current_size is not None and current_size >= 0.0
    )
    if liquidity_available:
        liquidity_ratio = min(1.0, max(0.0, current_size / entry_size))
    # Relative only: no shares/lots conversion or size-unit assumption.
    liquidity_factor = (
        0.85 + 0.15 * liquidity_ratio if liquidity_available else 1.0
    )

    base_gap = risk * gap_r
    unbounded_gap = (base_gap * atr_ratio + spread_buffer) * liquidity_factor
    minimum_gap = risk * 0.25
    maximum_gap = risk * 1.50
    gap = min(maximum_gap, max(minimum_gap, unbounded_gap))
    return {
        "available": True,
        "gap_distance": gap,
        "base_gap_distance": base_gap,
        "atr_ratio": atr_ratio,
        "spread_expansion": spread_expansion,
        "spread_buffer": spread_buffer,
        "liquidity_ratio": liquidity_ratio if liquidity_available else None,
        "liquidity_factor": liquidity_factor,
        "liquidity_basis": "RELATIVE_RAW_TOP_SIZE_NO_UNIT_CONVERSION",
        "gap_clamped": abs(gap - unbounded_gap) > 1e-12,
    }
