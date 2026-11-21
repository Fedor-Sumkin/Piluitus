"""PAPER portfolio accounting helpers.

These functions operate on in-memory position snapshots and deliberately avoid
market-data or broker dependencies.
"""
from __future__ import annotations

import math
from collections.abc import Iterable
from typing import Any


def parse_finite_float(value: object) -> float | None:
    """Convert an untrusted JSON scalar to a finite float."""
    if isinstance(value, bool):
        return None
    if isinstance(value, str):
        raw = value
    elif isinstance(value, (int, float)):
        raw = value
    else:
        return None
    try:
        number = float(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def calculate_open_pnl(positions: Iterable[object], *, paper_mode: bool = True) -> float:
    """Sum floating PnL for OPEN in-memory PAPER positions only."""
    if not paper_mode:
        return 0.0

    total = 0.0
    for position in positions:
        if not isinstance(position, dict):
            continue
        if str(position.get("status", "OPEN")).upper() != "OPEN":
            continue
        value = parse_finite_float(position.get("pnl", 0.0))
        if value is not None:
            total += value
    return total
