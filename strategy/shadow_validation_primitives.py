"""Shared numeric, timestamp and label-quality primitives for Shadow validation."""
from __future__ import annotations

import math
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from strategy.shadow_label_quality import shadow_label_quality_rank


def _finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _timestamp(row: Mapping[str, Any]) -> datetime | None:
    for key in ("entry_time_utc", "entry_time_msk", "entry_time"):
        raw = row.get(key)
        if not isinstance(raw, str) or not raw.strip():
            continue
        try:
            parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
        except (TypeError, ValueError, OverflowError):
            continue
        if parsed.tzinfo is None:
            continue
        return parsed.astimezone(timezone.utc)
    return None


def _quality_rank(row: Mapping[str, Any]) -> tuple[int, int, int, float]:
    return shadow_label_quality_rank(row)


def _confidence_band(value: object) -> str:
    confidence = _finite(value)
    if confidence is None or not 0.0 <= confidence <= 100.0:
        return "UNKNOWN"
    lower = 90 if confidence >= 90.0 else int(confidence // 10) * 10
    upper = 100 if lower == 90 else lower + 9
    return f"{lower:02d}-{upper:02d}"


def _wilson_interval_pct(wins: int, total: int) -> list[float] | None:
    if total <= 0:
        return None
    z = 1.959963984540054
    proportion = wins / total
    denominator = 1.0 + z * z / total
    center = (proportion + z * z / (2.0 * total)) / denominator
    margin = z * math.sqrt(
        proportion * (1.0 - proportion) / total
        + z * z / (4.0 * total * total)
    ) / denominator
    return [
        round(max(0.0, center - margin) * 100.0, 3),
        round(min(1.0, center + margin) * 100.0, 3),
    ]
