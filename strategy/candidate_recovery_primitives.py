"""Shared safe numeric parsing for deferred-candidate recovery."""
from __future__ import annotations

import math
from typing import Any


def _finite(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    if isinstance(value, str):
        raw = value
    elif isinstance(value, (int, float)):
        raw = value
    else:
        return default
    try:
        number = float(raw)
    except (TypeError, ValueError, OverflowError):
        return default
    return number if math.isfinite(number) else default
