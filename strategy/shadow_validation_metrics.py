"""Cohort and return metrics used by Shadow validation reports."""
from __future__ import annotations

from collections import defaultdict
from typing import Any

from .shadow_validation_primitives import (
    _confidence_band,
    _finite,
    _wilson_interval_pct,
)


def _metrics_by_horizon_side_dimensions(
    selected: list[dict[str, Any]],
    dimensions: tuple[str, ...],
) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    group_candidates: dict[str, set[str]] = defaultdict(set)
    for row in selected:
        identifier = str(row.get("candidate_id") or "")
        horizon = _finite(row.get("horizon_sec"))
        side = str(row.get("side") or "").upper()
        if not identifier or horizon is None or horizon <= 0.0 or side not in {"BUY", "SELL"}:
            continue
        key = f"{int(horizon)}s|{side}"
        for dimension in dimensions:
            if dimension == "llm_confidence":
                value = _confidence_band(row.get(dimension))
            else:
                value = str(row.get(dimension) or "UNKNOWN").strip().upper() or "UNKNOWN"
            key += f"|{dimension}={value}"
        groups[key].append(row)
        group_candidates[key].add(identifier)
    return {
        key: _summarize(rows, group_candidates[key])
        for key, rows in sorted(groups.items())
    }


def _summarize(rows: list[dict[str, Any]], candidate_ids: set[str]) -> dict[str, Any]:
    values = [
        value for row in rows
        if (value := _finite(row.get("net_executable_return_pct"))) is not None
        and str(row.get("result") or "").upper() == "OK"
    ]
    status_counts: dict[str, int] = defaultdict(int)
    for row in rows:
        status_counts[str(row.get("result") or "UNKNOWN").upper()] += 1
    wins = [value for value in values if value > 0.0]
    losses = [abs(value) for value in values if value < 0.0]
    positive_sum = sum(wins)
    negative_sum = sum(losses)
    count = len(values)
    confidence_values = [
        value for row in rows
        if (value := _finite(row.get("llm_confidence"))) is not None
        and 0.0 <= value <= 100.0
    ]
    return {
        "candidate_count": len(candidate_ids),
        "observed_label_rows": len(rows),
        "label_status_counts": dict(sorted(status_counts.items())),
        "rows_without_valid_net_return": len(rows) - count,
        "valid_labels": count,
        "positive_labels": len(wins),
        "negative_labels": len(losses),
        "flat_labels": count - len(wins) - len(losses),
        "positive_rate_pct": round(len(wins) / count * 100.0, 2) if count else None,
        "positive_rate_wilson_95_pct": _wilson_interval_pct(len(wins), count),
        "mean_model_confidence_pct": (
            round(sum(confidence_values) / len(confidence_values), 3)
            if confidence_values else None
        ),
        "confidence_observations": len(confidence_values),
        "expectancy_net_pct": round(sum(values) / count, 6) if count else None,
        "average_win_pct": round(positive_sum / len(wins), 6) if wins else None,
        "average_loss_pct": round(negative_sum / len(losses), 6) if losses else None,
        "profit_factor": round(positive_sum / negative_sum, 4) if negative_sum > 0.0 else None,
        "sample_quality": (
            "INSUFFICIENT_LT_30" if count < 30 else
            "PRELIMINARY_30_99" if count < 100 else
            "DEVELOPING_100_299" if count < 300 else "LARGE_300_PLUS"
        ),
    }


def _metrics_by_horizon_side(
    selected: list[dict[str, Any]],
    candidate_ids: set[str],
) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    group_candidates: dict[str, set[str]] = defaultdict(set)
    for row in selected:
        identifier = str(row.get("candidate_id") or "")
        horizon = _finite(row.get("horizon_sec"))
        side = str(row.get("side") or "").upper()
        if not identifier or horizon is None or horizon <= 0.0 or side not in {"BUY", "SELL"}:
            continue
        key = f"{int(horizon)}s|{side}"
        groups[key].append(row)
        group_candidates[key].add(identifier)
    return {
        key: _summarize(rows, group_candidates[key])
        for key, rows in sorted(groups.items())
    }


def _metrics_by_regime(
    selected: list[dict[str, Any]],
    candidate_ids: set[str],
    dimension: str,
    *,
    include_horizon_side: bool = False,
) -> dict[str, dict[str, Any]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    group_candidates: dict[str, set[str]] = defaultdict(set)
    for row in selected:
        identifier = str(row.get("candidate_id") or "")
        if not identifier:
            continue
        value = str(row.get(dimension) or "UNKNOWN").upper()
        if include_horizon_side:
            horizon = _finite(row.get("horizon_sec"))
            side = str(row.get("side") or "").upper()
            if horizon is None or horizon <= 0.0 or side not in {"BUY", "SELL"}:
                continue
            key = f"{int(horizon)}s|{side}|{dimension}={value}"
        else:
            key = f"{dimension}={value}"
        groups[key].append(row)
        group_candidates[key].add(identifier)
    return {
        key: _summarize(rows, group_candidates[key])
        for key, rows in sorted(groups.items())
    }
