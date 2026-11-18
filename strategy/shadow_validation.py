"""Chronological purged walk-forward diagnostics for Shadow forward labels.

This is a descriptive validation report, not a fitted model or a strategy
optimizer. Candidate IDs are the sampling unit; horizon rows from one
candidate are never split across train/test partitions.
"""
from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any


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


def _quality_rank(row: Mapping[str, Any]) -> tuple[int, float]:
    result = str(row.get("result") or "").upper()
    quality = str(row.get("label_quality") or "").upper()
    result_rank = {"OK": 3, "LABEL_LATE": 2, "NO_FRESH_BBO": 1}.get(result, 0)
    quality_rank = {"ON_TIME": 2, "DELAYED": 1}.get(quality, 0)
    lag = _finite(row.get("label_lag_sec"))
    return result_rank * 10 + quality_rank, -(lag if lag is not None else math.inf)


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


def build_shadow_walk_forward_validation(
    labels: Any,
    *,
    folds: int = 5,
    embargo_seconds: float = 30.0,
    max_horizon_seconds: int = 600,
    limit: int = 20000,
) -> dict[str, Any]:
    """Build expanding-window temporal folds with candidate-level purge/embargo."""
    try:
        fold_count = int(folds)
        embargo = float(embargo_seconds)
        max_horizon = int(max_horizon_seconds)
        row_limit = int(limit)
    except (TypeError, ValueError, OverflowError):
        return {"available": False, "reason": "INVALID_PARAMETERS"}
    if (
        fold_count < 3 or fold_count > 10
        or not math.isfinite(embargo) or embargo < 0.0 or embargo > 3600.0
        or max_horizon < 1 or max_horizon > 86400
        or row_limit < 1 or row_limit > 20000
    ):
        return {"available": False, "reason": "INVALID_PARAMETERS"}

    source = list(labels) if isinstance(labels, (list, tuple)) else []
    source = source[-row_limit:]
    deduplicated: dict[tuple[str, int], dict[str, Any]] = {}
    candidate_times: dict[str, datetime] = {}
    malformed = 0
    for raw in source:
        if not isinstance(raw, Mapping):
            malformed += 1
            continue
        identifier = str(raw.get("candidate_id") or "").strip()
        side = str(raw.get("side") or "").upper()
        horizon = _finite(raw.get("horizon_sec"))
        timestamp = _timestamp(raw)
        if not identifier or side not in {"BUY", "SELL"} or horizon is None or horizon <= 0.0 or timestamp is None:
            malformed += 1
            continue
        horizon_int = int(horizon)
        previous_time = candidate_times.get(identifier)
        if previous_time is not None and previous_time != timestamp:
            malformed += 1
            continue
        candidate_times[identifier] = timestamp
        row = dict(raw)
        key = (identifier, horizon_int)
        existing = deduplicated.get(key)
        if existing is None or _quality_rank(row) > _quality_rank(existing):
            deduplicated[key] = row

    if not candidate_times:
        return {
            "available": False,
            "reason": "NO_TIMESTAMPED_CANDIDATES",
            "input_rows": len(source),
            "malformed_rows": malformed,
            "unique_candidates": 0,
        }

    candidate_ids_by_time: dict[datetime, list[str]] = defaultdict(list)
    for identifier, timestamp in candidate_times.items():
        candidate_ids_by_time[timestamp].append(identifier)
    ordered_times = sorted(candidate_ids_by_time)
    if len(ordered_times) < 3:
        return {
            "available": False,
            "reason": "INSUFFICIENT_DISTINCT_TIME_POINTS",
            "input_rows": len(source),
            "malformed_rows": malformed,
            "unique_candidates": len(candidate_times),
            "distinct_entry_times": len(ordered_times),
            "minimum_distinct_entry_times": 3,
        }

    block_count = min(fold_count, len(ordered_times))
    time_blocks = [
        ordered_times[index * len(ordered_times) // block_count:
                      (index + 1) * len(ordered_times) // block_count]
        for index in range(block_count)
    ]
    rows_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in deduplicated.values():
        rows_by_candidate[str(row["candidate_id"])].append(row)

    purge_window = float(max_horizon) + embargo
    fold_results: list[dict[str, Any]] = []
    all_test_rows: list[dict[str, Any]] = []
    all_test_ids: set[str] = set()
    for fold_index, test_times in enumerate(time_blocks[1:], start=1):
        if not test_times:
            continue
        test_start = test_times[0]
        test_end = test_times[-1]
        cutoff = test_start.timestamp() - purge_window
        test_ids = {
            identifier
            for moment in test_times
            for identifier in candidate_ids_by_time[moment]
        }
        train_candidates = {
            identifier for identifier, moment in candidate_times.items()
            if moment.timestamp() < cutoff
        }
        prior_candidates = {
            identifier for identifier, moment in candidate_times.items()
            if moment < test_start
        }
        purged_ids = prior_candidates - train_candidates
        train_rows = [
            row for identifier in train_candidates
            for row in rows_by_candidate.get(identifier, [])
        ]
        test_rows = [
            row for identifier in test_ids
            for row in rows_by_candidate.get(identifier, [])
        ]
        all_test_rows.extend(test_rows)
        all_test_ids.update(test_ids)
        fold_results.append({
            "fold": fold_index,
            "train_start_utc": min(
                (candidate_times[item] for item in train_candidates), default=None
            ).isoformat() if train_candidates else None,
            "train_end_utc": max(
                (candidate_times[item] for item in train_candidates), default=None
            ).isoformat() if train_candidates else None,
            "test_start_utc": test_start.isoformat(),
            "test_end_utc": test_end.isoformat(),
            "train_candidates": len(train_candidates),
            "purged_candidates": len(purged_ids),
            "test_candidates": len(test_ids),
            "train_by_horizon_side": _metrics_by_horizon_side(train_rows, train_candidates),
            "test_by_horizon_side": _metrics_by_horizon_side(test_rows, test_ids),
            "train_by_horizon_side_market_regime": _metrics_by_regime(
                train_rows, train_candidates, "market_regime", include_horizon_side=True
            ),
            "test_by_horizon_side_market_regime": _metrics_by_regime(
                test_rows, test_ids, "market_regime", include_horizon_side=True
            ),
            "test_by_volatility_regime": _metrics_by_regime(test_rows, test_ids, "volatility_regime"),
            "test_by_liquidity_regime": _metrics_by_regime(test_rows, test_ids, "liquidity_regime"),
            "test_by_directional_regime": _metrics_by_regime(test_rows, test_ids, "directional_regime"),
        })

    return {
        "available": bool(fold_results),
        "method": "EXPANDING_WINDOW_CANDIDATE_GROUPED_PURGED_WALK_FORWARD",
        "input_rows": len(source),
        "deduplicated_candidate_horizon_rows": len(deduplicated),
        "unique_candidates": len(candidate_times),
        "distinct_entry_times": len(ordered_times),
        "malformed_rows": malformed,
        "folds_requested": fold_count,
        "folds_evaluated": len(fold_results),
        "max_horizon_seconds": max_horizon,
        "embargo_seconds": embargo,
        "purge_window_seconds": purge_window,
        "sample_unit": "unique_candidate_id; each candidate and all its horizons stay in one time block",
        "return_basis": "net_executable_return_pct; only result=OK rows enter return metrics",
        "folds": fold_results,
        "out_of_sample_by_horizon_side": _metrics_by_horizon_side(all_test_rows, all_test_ids),
        "out_of_sample_by_horizon_side_market_regime": _metrics_by_regime(
            all_test_rows, all_test_ids, "market_regime", include_horizon_side=True
        ),
        "out_of_sample_by_volatility_regime": _metrics_by_regime(
            all_test_rows, all_test_ids, "volatility_regime"
        ),
        "out_of_sample_by_liquidity_regime": _metrics_by_regime(
            all_test_rows, all_test_ids, "liquidity_regime"
        ),
        "out_of_sample_by_directional_regime": _metrics_by_regime(
            all_test_rows, all_test_ids, "directional_regime"
        ),
        "warning": (
            "Descriptive temporal validation only; no model is fitted and no thresholds are selected. "
            "Forward labels overlap within a candidate, so candidate IDs—not horizon rows—are the "
            "sampling unit. Results below 30 valid labels are insufficient for conclusions. "
            "This is not a substitute for an untouched final holdout or multiple-testing controls."
        ),
    }
