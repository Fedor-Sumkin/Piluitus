"""Exact-identifier reconciliation between realized PAPER trades and Shadow labels."""
from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Iterable, Mapping


def _horizon_values(labels: Iterable[Mapping[str, Any]]) -> list[int | float]:
    values: set[int | float] = set()
    for label in labels:
        try:
            value = float(label.get("horizon_sec"))
        except (TypeError, ValueError, OverflowError):
            continue
        if not math.isfinite(value) or value <= 0.0:
            continue
        values.add(int(value) if value.is_integer() else value)
    return sorted(values)


def build_shadow_reconciliation(
    trades: Iterable[Mapping[str, Any]],
    labels: Iterable[Mapping[str, Any]],
    *,
    limit: int = 100,
) -> dict[str, Any]:
    """Join only exact shadow_candidate_id/candidate_id values; never infer matches."""
    labels_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for label in labels:
        if not isinstance(label, Mapping):
            continue
        candidate_id = str(label.get("candidate_id") or "").strip()
        if candidate_id:
            labels_by_candidate[candidate_id].append(dict(label))

    records = [dict(row) for row in trades if isinstance(row, Mapping)]
    records.sort(
        key=lambda row: str(row.get("exit_timestamp") or ""),
        reverse=True,
    )
    counts = {
        "MATCHED_EXACT_ID": 0,
        "MISSING_TRADE_CANDIDATE_ID": 0,
        "NO_SHADOW_LABEL_FOR_CANDIDATE": 0,
    }
    reconciled: list[dict[str, Any]] = []
    for trade in records:
        candidate_id = str(trade.get("shadow_candidate_id") or "").strip()
        if not candidate_id:
            status = "MISSING_TRADE_CANDIDATE_ID"
            matched_labels: list[dict[str, Any]] = []
        else:
            matched_labels = labels_by_candidate.get(candidate_id, [])
            status = (
                "MATCHED_EXACT_ID"
                if matched_labels else "NO_SHADOW_LABEL_FOR_CANDIDATE"
            )
        counts[status] += 1
        reconciled.append({
            "trade_id": str(trade.get("id") or ""),
            "ticker": str(trade.get("ticker") or ""),
            "side": str(trade.get("side") or ""),
            "net_pnl_rub": trade.get("pnl"),
            "max_favorable_bbo_mfe_gross_rub": trade.get("max_favorable_bbo_mfe_gross_rub"),
            "max_favorable_bbo_mfe_net_estimate_rub": trade.get("max_favorable_bbo_mfe_net_estimate_rub"),
            "realized_capture_of_net_mfe_pct": trade.get("realized_capture_of_net_mfe_pct"),
            "net_mfe_minus_realized_pnl_rub": trade.get("net_mfe_minus_realized_pnl_rub"),
            "mfe_reconciliation_status": trade.get("mfe_reconciliation_status"),
            "exit_reason": trade.get("exit_reason"),
            "shadow_candidate_id": candidate_id or None,
            "status": status,
            "shadow_label_count": len(matched_labels),
            "shadow_horizons_sec": _horizon_values(matched_labels),
        })

    matched_ids = {
        row["shadow_candidate_id"] for row in reconciled
        if row["status"] == "MATCHED_EXACT_ID"
    }
    label_candidate_ids = set(labels_by_candidate)
    return {
        "realized_closed_trades": len(records),
        "shadow_candidates_with_labels": len(label_candidate_ids),
        "matched_trades": counts["MATCHED_EXACT_ID"],
        "unmatched_trades": len(records) - counts["MATCHED_EXACT_ID"],
        "counts_by_status": counts,
        "shadow_candidates_without_realized_trade": len(label_candidate_ids - matched_ids),
        "trades": reconciled[:max(0, int(limit))],
        "note": (
            "Matches use exact shadow_candidate_id/candidate_id only; "
            "ticker or timestamp proximity is never treated as a link."
        ),
    }
