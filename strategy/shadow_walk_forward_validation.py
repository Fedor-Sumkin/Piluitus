"""Chronological candidate-grouped purged walk-forward validation."""
from __future__ import annotations

import math

from collections import defaultdict
from collections.abc import Mapping
from datetime import datetime
from typing import Any

from .shadow_validation_metrics import (
    _metrics_by_horizon_side,
    _metrics_by_horizon_side_dimensions,
    _metrics_by_regime,
)
from .shadow_validation_primitives import _finite, _quality_rank, _timestamp


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
            "test_by_horizon_side_confidence": _metrics_by_horizon_side_dimensions(
                test_rows, ("llm_confidence",)
            ),
            "test_by_horizon_side_decision_confidence": _metrics_by_horizon_side_dimensions(
                test_rows, ("llm_model_decision", "llm_confidence")
            ),
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
        "out_of_sample_by_horizon_side_confidence": _metrics_by_horizon_side_dimensions(
            all_test_rows, ("llm_confidence",)
        ),
        "out_of_sample_by_horizon_side_decision_confidence": _metrics_by_horizon_side_dimensions(
            all_test_rows, ("llm_model_decision", "llm_confidence")
        ),
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
