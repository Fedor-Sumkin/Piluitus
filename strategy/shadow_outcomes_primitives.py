"""Numeric and cost primitives shared by forward Shadow outcome labelling."""
from __future__ import annotations

import logging
import math
import os
from collections.abc import Mapping
from datetime import timedelta, timezone
from typing import Any

logger = logging.getLogger("Piluitus.StrategyShadow")
_MOSCOW_TZ = timezone(timedelta(hours=3), name="MSK")

def _number(metrics: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    value = metrics.get(key, default)
    if isinstance(value, bool):
        return float(value)
    if not isinstance(value, (int, float, str)):
        return default
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return numeric if math.isfinite(numeric) else default


def _cost_settings() -> tuple[float, float]:
    try:
        commission_pct = min(1.0, max(0.0, float(os.getenv("PILUITUS_PAPER_COMMISSION_PCT", "0"))))
    except (TypeError, ValueError, OverflowError):
        commission_pct = 0.0
    try:
        slippage_bps = min(100.0, max(0.0, float(os.getenv("PILUITUS_PAPER_SLIPPAGE_BPS", "0"))))
    except (TypeError, ValueError, OverflowError):
        slippage_bps = 0.0
    if not math.isfinite(commission_pct):
        commission_pct = 0.0
    if not math.isfinite(slippage_bps):
        slippage_bps = 0.0
    return commission_pct, slippage_bps


def _apply_adverse_slippage(price: float, transaction_side: str, slippage_bps: float) -> float:
    rate = max(0.0, slippage_bps) / 10_000.0
    adjusted = price * (1.0 + rate if transaction_side == "BUY" else 1.0 - rate)
    return adjusted if math.isfinite(adjusted) and adjusted > 0.0 else price
