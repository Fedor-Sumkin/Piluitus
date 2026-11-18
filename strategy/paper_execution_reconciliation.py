"""Exact-ID comparison of realized PAPER fills with BBO replay outcomes.

This is descriptive execution-model reconciliation, not a strategy optimizer.
A match requires both the exact trade ID and exact BBO replay session ID.
"""
from __future__ import annotations

import math
from datetime import datetime
from collections import Counter
from statistics import median
from typing import Any, Iterable, Mapping


def _finite(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _aware_timestamp_epoch(value: object) -> float | None:
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except (TypeError, ValueError, OverflowError):
        return None
    # Never guess a timezone for legacy naive timestamps.
    if parsed.tzinfo is None:
        return None
    epoch = parsed.timestamp()
    return epoch if math.isfinite(epoch) else None


def _identity(row: Mapping[str, Any], *, replay: bool) -> tuple[str, str] | None:
    if replay:
        trade_id = str(row.get("position_id") or row.get("id") or "").strip()
        session_id = str(row.get("session_id") or "").strip()
    else:
        trade_id = str(row.get("id") or row.get("position_id") or "").strip()
        # Do not fall back to the broader process session: replay has its own
        # event-journal identity and must be joined against that exact session.
        session_id = str(row.get("bbo_replay_session_id") or "").strip()
    return (session_id, trade_id) if session_id and trade_id else None


def _scenario_exit(replay_row: Mapping[str, Any], scenario_name: str) -> Mapping[str, Any] | None:
    scenarios = replay_row.get("scenarios")
    if not isinstance(scenarios, list):
        return None
    for scenario in scenarios:
        if not isinstance(scenario, Mapping):
            continue
        if str(scenario.get("name") or "").strip() == scenario_name:
            exit_row = scenario.get("exit")
            return exit_row if isinstance(exit_row, Mapping) else None
    return None


def build_actual_vs_replay_reconciliation(
    actual_trades: Iterable[Mapping[str, Any]],
    replay_batch: Mapping[str, Any],
    *,
    scenario_name: str = "BASELINE_SL_TP",
    limit: int = 100,
) -> dict[str, Any]:
    """Compare actual PAPER results to replay only on exact (session, trade) identity."""
    scenario = str(scenario_name or "BASELINE_SL_TP").strip() or "BASELINE_SL_TP"
    actual_rows = []
    for raw in actual_trades:
        if not isinstance(raw, Mapping):
            continue
        row = dict(raw)
        mode = str(row.get("mode") or "").strip().upper()
        trade_id = str(row.get("id") or row.get("position_id") or "").strip()
        is_paper = mode == "PAPER" or (
            mode != "LIVE" and trade_id.startswith(("PAPER_", "TRADE_"))
        )
        if str(row.get("status") or "").strip().upper() == "CLOSED" and is_paper:
            actual_rows.append(row)
    replay_rows_raw = replay_batch.get("trades", [])
    replay_rows = [
        dict(row) for row in replay_rows_raw
        if isinstance(row, Mapping)
    ] if isinstance(replay_rows_raw, list) else []

    actual_keys = [_identity(row, replay=False) for row in actual_rows]
    replay_keys = [_identity(row, replay=True) for row in replay_rows]
    actual_counts = Counter(key for key in actual_keys if key is not None)
    replay_counts = Counter(key for key in replay_keys if key is not None)
    duplicate_actual = sum(count - 1 for count in actual_counts.values() if count > 1)
    duplicate_replay = sum(count - 1 for count in replay_counts.values() if count > 1)

    actual_by_key = {
        key: row for key, row in zip(actual_keys, actual_rows)
        if key is not None and actual_counts[key] == 1
    }
    replay_by_key = {
        key: row for key, row in zip(replay_keys, replay_rows)
        if key is not None and replay_counts[key] == 1
    }
    matched_keys = set(actual_by_key) & set(replay_by_key)
    rejected_by_key: dict[tuple[str, str], str] = {}
    rejected_rows = replay_batch.get("rejected_positions", [])
    if isinstance(rejected_rows, list):
        for rejected in rejected_rows:
            if not isinstance(rejected, Mapping):
                continue
            rejected_id = str(rejected.get("position_id") or "").strip()
            rejected_session = str(rejected.get("session_id") or "").strip()
            if rejected_id and rejected_session:
                rejected_by_key[(rejected_session, rejected_id)] = str(
                    rejected.get("reason") or "REPLAY_POSITION_REJECTED"
                )

    counts = {
        "MATCHED_EXACT_ID_AND_SESSION": 0,
        "MISSING_ACTUAL_REPLAY_IDENTITY": 0,
        "MISSING_REPLAY_IDENTITY": sum(1 for key in replay_keys if key is None),
        "DUPLICATE_ACTUAL_IDENTITY": 0,
        "DUPLICATE_REPLAY_IDENTITY": 0,
        "NO_REPLAY_POSITION": 0,
        "REPLAY_POSITION_REJECTED": 0,
        "NO_REPLAY_EXIT": 0,
        "MISSING_ACTUAL_PNL": 0,
    }
    rows: list[dict[str, Any]] = []
    pnl_deltas: list[float] = []
    exit_advantages: list[float] = []
    actual_better = replay_better = equal = 0

    for actual, key in zip(actual_rows, actual_keys):
        trade_id = str(actual.get("id") or actual.get("position_id") or "")
        session_id = str(actual.get("bbo_replay_session_id") or "")
        if key is None:
            status = "MISSING_ACTUAL_REPLAY_IDENTITY"
            counts[status] += 1
            rows.append({
                "trade_id": trade_id,
                "bbo_replay_session_id": session_id or None,
                "status": status,
            })
            continue
        if actual_counts[key] > 1:
            status = "DUPLICATE_ACTUAL_IDENTITY"
            counts[status] += 1
            rows.append({
                "trade_id": trade_id,
                "bbo_replay_session_id": session_id,
                "status": status,
            })
            continue
        if replay_counts.get(key, 0) > 1:
            status = "DUPLICATE_REPLAY_IDENTITY"
            counts[status] += 1
            rows.append({
                "trade_id": trade_id,
                "bbo_replay_session_id": session_id,
                "status": status,
            })
            continue
        replay_row = replay_by_key.get(key)
        if replay_row is None:
            rejected_reason = rejected_by_key.get(key)
            status = "REPLAY_POSITION_REJECTED" if rejected_reason else "NO_REPLAY_POSITION"
            counts[status] += 1
            row = {
                "trade_id": trade_id,
                "bbo_replay_session_id": session_id,
                "ticker": str(actual.get("ticker") or ""),
                "status": status,
            }
            if rejected_reason:
                row["replay_rejection_reason"] = rejected_reason
            rows.append(row)
            continue

        replay_exit = _scenario_exit(replay_row, scenario)
        if replay_exit is None:
            status = "NO_REPLAY_EXIT"
            counts[status] += 1
            rows.append({
                "trade_id": trade_id,
                "bbo_replay_session_id": session_id,
                "ticker": str(actual.get("ticker") or ""),
                "scenario": scenario,
                "status": status,
                "actual_exit_reason": actual.get("exit_reason"),
            })
            continue

        actual_pnl = _finite(actual.get("pnl"))
        replay_pnl = _finite(replay_exit.get("net_pnl_rub"))
        pnl_delta = (
            round(actual_pnl - replay_pnl, 6)
            if actual_pnl is not None and replay_pnl is not None else None
        )
        actual_exit_price = _finite(actual.get("exit_price"))
        replay_exit_price = _finite(replay_exit.get("fill_price_after_slippage"))
        quantity = _finite(actual.get("volume"))
        side = str(actual.get("side") or "").strip().upper()
        sign = 1.0 if side == "BUY" else -1.0 if side == "SELL" else 0.0
        exit_advantage = None
        price_delta = None
        if actual_exit_price is not None and replay_exit_price is not None:
            price_delta = round(actual_exit_price - replay_exit_price, 8)
            if quantity is not None and quantity > 0.0 and sign:
                exit_advantage = round(price_delta * quantity * sign, 6)
                exit_advantages.append(exit_advantage)
        if pnl_delta is not None:
            pnl_deltas.append(pnl_delta)
            if pnl_delta > 0.005:
                actual_better += 1
            elif pnl_delta < -0.005:
                replay_better += 1
            else:
                equal += 1

        actual_commission = _finite(actual.get("total_commission_rub"))
        replay_commission = _finite(replay_exit.get("total_commission_rub"))
        commission_delta = (
            round(actual_commission - replay_commission, 6)
            if actual_commission is not None and replay_commission is not None else None
        )
        actual_exit_epoch = _aware_timestamp_epoch(actual.get("exit_timestamp"))
        replay_exit_epoch = _finite(replay_exit.get("event_time_epoch"))
        exit_time_delta = (
            round(actual_exit_epoch - replay_exit_epoch, 6)
            if actual_exit_epoch is not None and replay_exit_epoch is not None else None
        )
        status = "MATCHED_EXACT_ID_AND_SESSION"
        counts[status] += 1
        if actual_pnl is None:
            counts["MISSING_ACTUAL_PNL"] += 1
        partial_count = _finite(actual.get("exit_partial_fill_count"))
        rows.append({
            "trade_id": trade_id,
            "bbo_replay_session_id": session_id,
            "ticker": str(actual.get("ticker") or ""),
            "side": side,
            "scenario": scenario,
            "status": status,
            "actual_exit_reason": actual.get("exit_reason"),
            "replay_exit_reason": replay_exit.get("reason"),
            "actual_exit_timestamp": actual.get("exit_timestamp"),
            "replay_exit_event_time_epoch": replay_exit_epoch,
            "exit_time_delta_actual_minus_replay_sec": exit_time_delta,
            "actual_net_pnl_rub": actual_pnl,
            "replay_net_pnl_rub": replay_pnl,
            "net_pnl_delta_actual_minus_replay_rub": pnl_delta,
            "actual_exit_price": actual_exit_price,
            "replay_exit_price": replay_exit_price,
            "exit_price_delta_actual_minus_replay_rub": price_delta,
            "actual_exit_advantage_rub": exit_advantage,
            "actual_total_commission_rub": actual_commission,
            "replay_total_commission_rub": replay_commission,
            "commission_delta_actual_minus_replay_rub": commission_delta,
            "actual_entry_shortfall_rub": _finite(actual.get("entry_execution_shortfall_rub")),
            "actual_exit_shortfall_rub": _finite(actual.get("exit_execution_shortfall_rub")),
            "actual_exit_depth_impact_rub": _finite(actual.get("exit_depth_impact_rub")),
            "actual_exit_partial_fill_count": int(partial_count) if partial_count is not None else None,
            "actual_exit_fill_model": actual.get("exit_fill_model"),
            "replay_event_sequence": replay_exit.get("event_sequence"),
            "replay_bbo_age_sec": replay_exit.get("bbo_age_sec"),
        })

    actual_identity_keys = set(actual_by_key)
    replay_identity_keys = set(replay_by_key)
    return {
        "scenario": scenario,
        "actual_closed_paper_trades": len(actual_rows),
        "replay_positions": len(replay_rows),
        "matched_exact_identity": len(matched_keys),
        "unmatched_actual_identities": len(actual_identity_keys - replay_identity_keys),
        "unmatched_replay_identities": len(replay_identity_keys - actual_identity_keys),
        "duplicate_actual_identities": duplicate_actual,
        "duplicate_replay_identities": duplicate_replay,
        "counts_by_status": counts,
        "pnl_delta": {
            "observations": len(pnl_deltas),
            "total_rub": round(sum(pnl_deltas), 6) if pnl_deltas else None,
            "mean_rub": round(sum(pnl_deltas) / len(pnl_deltas), 6) if pnl_deltas else None,
            "median_rub": round(median(pnl_deltas), 6) if pnl_deltas else None,
            "actual_better": actual_better,
            "replay_better": replay_better,
            "equal_within_0_005_rub": equal,
        },
        "actual_exit_advantage": {
            "observations": len(exit_advantages),
            "total_rub": round(sum(exit_advantages), 6) if exit_advantages else None,
            "mean_rub": round(sum(exit_advantages) / len(exit_advantages), 6) if exit_advantages else None,
        },
        "trades": rows[:max(0, int(limit))],
        "note": (
            "Exact bbo_replay_session_id + trade ID matches only. Actual partial/depth fills "
            "are compared with a top-of-book replay model; differences are execution-model "
            "diagnostics, not evidence that a replay strategy would have produced the same fills."
        ),
    }
