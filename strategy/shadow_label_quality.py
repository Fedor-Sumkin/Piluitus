"""Shared deterministic quality ranking for duplicate Shadow horizon labels."""
from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from typing import Any


def finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def shadow_label_quality_rank(row: Mapping[str, Any]) -> tuple[int, int, int, float]:
    """Prefer usable on-time labels; never let an invalid duplicate hide a valid one."""
    result = str(row.get("result") or "").strip().upper()
    quality = str(row.get("label_quality") or "").strip().upper()
    result_rank = {"OK": 3, "LABEL_LATE": 2, "NO_FRESH_BBO": 1}.get(result, 0)
    has_valid_return = int(
        result == "OK"
        and finite_number(row.get("net_executable_return_pct")) is not None
    )
    quality_rank = {"ON_TIME": 2, "DELAYED": 1}.get(quality, 0)
    lag = finite_number(row.get("label_lag_sec"))
    lag_rank = -(lag if lag is not None else math.inf)
    return result_rank, has_valid_return, quality_rank, lag_rank


def deduplicate_shadow_labels(labels: Iterable[object]) -> list[dict[str, Any]]:
    """Keep one best-quality row per exact candidate_id + horizon_sec pair."""
    selected: dict[tuple[str, int], dict[str, Any]] = {}
    for raw in labels:
        if not isinstance(raw, Mapping):
            continue
        candidate_id = str(raw.get("candidate_id") or "").strip()
        horizon = finite_number(raw.get("horizon_sec"))
        if not candidate_id or horizon is None or horizon <= 0.0:
            continue
        horizon_int = int(horizon)
        if horizon_int <= 0:
            continue
        key = (candidate_id, horizon_int)
        row = dict(raw)
        previous = selected.get(key)
        if previous is None or shadow_label_quality_rank(row) > shadow_label_quality_rank(previous):
            selected[key] = row
    return list(selected.values())
