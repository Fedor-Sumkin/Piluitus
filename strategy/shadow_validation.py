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

from strategy.shadow_label_quality import deduplicate_shadow_labels, shadow_label_quality_rank


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


def build_shadow_cost_sensitivity(
    labels: Any,
    *,
    scenarios_bps: tuple[float, ...] = (0.0, 1.0, 2.0, 5.0),
    limit: int = 20000,
) -> dict[str, Any]:
    """Reprice Shadow outcomes exactly under alternative per-side slippage assumptions.

    Each scenario rebuilds entry and exit fills from the recorded executable BBO
    prices, then reapplies the recorded per-side commission. The observed BBO
    spread is already reflected by the executable entry/exit prices. No linear
    approximation of the return impact is used.
    """
    try:
        row_limit = int(limit)
        scenarios = sorted({float(value) for value in scenarios_bps})
    except (TypeError, ValueError, OverflowError):
        return {"available": False, "reason": "INVALID_PARAMETERS"}
    if (
        row_limit < 1 or row_limit > 20000
        or not scenarios
        or any(not math.isfinite(value) or value < 0.0 or value > 100.0 for value in scenarios)
    ):
        return {"available": False, "reason": "INVALID_PARAMETERS"}

    source = labels if isinstance(labels, (list, tuple)) else []
    deduplicated = deduplicate_shadow_labels(source)[-row_limit:]
    valid_rows: list[dict[str, Any]] = []
    rejected_missing_prices = 0
    for row in deduplicated:
        if str(row.get("result") or "").strip().upper() != "OK":
            continue
        side = str(row.get("side") or "").strip().upper()
        candidate_id = str(row.get("candidate_id") or "").strip()
        horizon = _finite(row.get("horizon_sec"))
        entry_raw = _finite(row.get("entry_executable_price"))
        exit_raw = _finite(row.get("exit_executable_price"))
        commission = _finite(row.get("commission_pct_per_side"))
        if (
            side not in {"BUY", "SELL"} or not candidate_id
            or horizon is None or horizon <= 0.0
            or entry_raw is None or exit_raw is None
            or entry_raw <= 0.0 or exit_raw <= 0.0
            or commission is None or commission < 0.0
        ):
            rejected_missing_prices += 1
            continue
        valid_rows.append({
            "candidate_id": candidate_id,
            "side": side,
            "horizon_sec": int(horizon),
            "entry_raw": entry_raw,
            "exit_raw": exit_raw,
            "commission_pct_per_side": commission,
            "recorded_slippage_bps": max(0.0, _finite(row.get("slippage_bps")) or 0.0),
        })

    def reprice(row: dict[str, Any], scenario_bps: float) -> float:
        rate = scenario_bps / 10000.0
        side = row["side"]
        entry_transaction = "BUY" if side == "BUY" else "SELL"
        exit_transaction = "SELL" if side == "BUY" else "BUY"
        entry_fill = row["entry_raw"] * (
            1.0 + rate if entry_transaction == "BUY" else 1.0 - rate
        )
        exit_fill = row["exit_raw"] * (
            1.0 + rate if exit_transaction == "BUY" else 1.0 - rate
        )
        if entry_fill <= 0.0 or exit_fill <= 0.0:
            return math.nan
        sign = 1.0 if side == "BUY" else -1.0
        gross_per_share = (exit_fill - entry_fill) * sign
        fees_per_share = (
            entry_fill + exit_fill
        ) * row["commission_pct_per_side"] / 100.0
        return (gross_per_share - fees_per_share) / entry_fill * 100.0

    def summarize(rows: list[dict[str, Any]], scenario_bps: float) -> dict[str, Any]:
        adjusted = [
            value for row in rows
            if math.isfinite(value := reprice(row, scenario_bps))
        ]
        wins = [value for value in adjusted if value > 0.0]
        losses = [abs(value) for value in adjusted if value < 0.0]
        gross_profit = sum(wins)
        gross_loss = sum(losses)
        return {
            "candidate_count": len({row["candidate_id"] for row in rows}),
            "label_rows": len(adjusted),
            "positive_labels": len(wins),
            "positive_rate_pct": round(len(wins) / len(adjusted) * 100.0, 3) if adjusted else None,
            "average_net_return_pct": round(sum(adjusted) / len(adjusted), 6) if adjusted else None,
            "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0.0 else None,
        }

    by_scenario: dict[str, Any] = {}
    for scenario in scenarios:
        by_side = {
            side: summarize([row for row in valid_rows if row["side"] == side], scenario)
            for side in ("BUY", "SELL")
        }
        horizons = sorted({row["horizon_sec"] for row in valid_rows})
        by_horizon_side = {
            f"{horizon}s|{side}": summarize(
                [row for row in valid_rows if row["horizon_sec"] == horizon and row["side"] == side],
                scenario,
            )
            for horizon in horizons
            for side in ("BUY", "SELL")
            if any(row["horizon_sec"] == horizon and row["side"] == side for row in valid_rows)
        }
        by_scenario[f"{scenario:g}"] = {
            "slippage_bps_per_side": scenario,
            "additional_round_trip_cost_bps": round(2.0 * scenario, 4),
            "overall": summarize(valid_rows, scenario),
            "by_side": by_side,
            "by_horizon_side": by_horizon_side,
        }

    return {
        "available": bool(valid_rows),
        "method": "DEDUPLICATED_CANDIDATE_HORIZON_EXACT_BBO_REPRICING",
        "input_label_rows": len(source),
        "deduplicated_label_rows": len(deduplicated),
        "valid_label_rows": len(valid_rows),
        "rejected_ok_labels_missing_executable_prices_or_commission": rejected_missing_prices,
        "unique_candidates": len({row["candidate_id"] for row in valid_rows}),
        "scenarios": by_scenario,
        "warning": (
            "Stress scenarios are assumptions, not measured slippage. Every scenario reprices both "
            "transactions from the recorded executable BBO and reapplies the recorded commission. "
            "Horizon rows from one candidate are correlated and must not be interpreted as independent trades."
        ),
    }


def build_shadow_cost_atr_gate_sensitivity(
    labels: Any,
    *,
    thresholds: tuple[float, ...] = (0.25, 0.35, 0.50, 0.65, 0.80, 1.00, 1.25),
    required_horizons: tuple[int, ...] = (60, 180, 300, 600),
    limit: int = 20000,
) -> dict[str, Any]:
    """Compare historical cost/ATR thresholds without changing active gates."""
    try:
        row_limit = int(limit)
        scenario_values = sorted({float(value) for value in thresholds})
        horizons = sorted({int(value) for value in required_horizons})
    except (TypeError, ValueError, OverflowError):
        return {"available": False, "reason": "INVALID_PARAMETERS"}
    if (
        row_limit < 1 or row_limit > 20000 or not scenario_values or not horizons
        or any(not math.isfinite(value) or value <= 0.0 or value > 2.0 for value in scenario_values)
        or any(value not in {60, 180, 300, 600} for value in horizons)
    ):
        return {"available": False, "reason": "INVALID_PARAMETERS"}

    source = labels if isinstance(labels, (list, tuple)) else []
    deduplicated = deduplicate_shadow_labels(source)[-row_limit:]
    valid: list[dict[str, Any]] = []
    missing_cost = 0
    for row in deduplicated:
        ratio = _finite(row.get("cost_to_atr_ratio"))
        horizon = _finite(row.get("horizon_sec"))
        side = str(row.get("side") or "").upper()
        candidate_id = str(row.get("candidate_id") or "").strip()
        if ratio is None or ratio < 0.0:
            missing_cost += 1
            continue
        if not candidate_id or side not in {"BUY", "SELL"} or horizon is None or int(horizon) not in horizons:
            continue
        valid.append(dict(row, _cost_to_atr=ratio, horizon_sec=int(horizon)))

    by_threshold: dict[str, Any] = {}
    valid_candidates = {str(row.get("candidate_id")) for row in valid}
    for threshold in scenario_values:
        included = [row for row in valid if row["_cost_to_atr"] <= threshold]
        candidate_ids = {str(row.get("candidate_id")) for row in included}
        observed_by_horizon: dict[str, Any] = {}
        for horizon in horizons:
            horizon_rows = [row for row in included if int(row["horizon_sec"]) == horizon]
            observed_by_horizon[f"{horizon}s"] = {
                "candidate_count": len({str(row.get("candidate_id")) for row in horizon_rows}),
                "label_rows": len(horizon_rows),
                "by_side": {
                    side: _summarize(
                        [row for row in horizon_rows if str(row.get("side")).upper() == side],
                        {str(row.get("candidate_id")) for row in horizon_rows if str(row.get("side")).upper() == side},
                    )
                    for side in ("BUY", "SELL")
                },
                "overall": _summarize(horizon_rows, {str(row.get("candidate_id")) for row in horizon_rows}),
            }
        by_threshold[f"{threshold:.2f}"] = {
            "max_cost_to_atr_ratio": threshold,
            "included_candidates": len(candidate_ids),
            "included_label_rows": len(included),
            "coverage_pct": round(len(candidate_ids) / max(1, len(valid_candidates)) * 100.0, 3),
            "by_horizon": observed_by_horizon,
        }
    return {
        "available": bool(valid),
        "method": "OBSERVED_SHADOW_LABELS_COST_ATR_THRESHOLD_SWEEP",
        "active_gate_unchanged": True,
        "input_label_rows": len(source),
        "deduplicated_label_rows": len(deduplicated),
        "valid_cost_labeled_rows": len(valid),
        "rows_missing_cost_ratio": missing_cost,
        "unique_cost_labeled_candidates": len(valid_candidates),
        "thresholds": by_threshold,
        "interpretation": "Descriptive historical sensitivity only; not a recommendation to change the execution gate.",
    }
