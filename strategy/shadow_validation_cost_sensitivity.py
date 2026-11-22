"""Exact BBO-based cost sensitivity scenarios for Shadow labels."""
from __future__ import annotations

import math
from typing import Any

from strategy.shadow_label_quality import deduplicate_shadow_labels

from .shadow_validation_primitives import _finite


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
