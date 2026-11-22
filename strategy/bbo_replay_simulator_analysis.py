"""Trade-level replay summaries and paired scenario cohort comparisons."""
from __future__ import annotations

import math
from typing import Any, Mapping

from .bbo_replay_simulator_primitives import _number

def _sample_quality(sample_size: int) -> str:
    if sample_size < 30:
        return "INSUFFICIENT_LT_30"
    if sample_size < 100:
        return "PRELIMINARY_30_99"
    if sample_size < 300:
        return "DEVELOPING_100_299"
    return "LARGE_300_PLUS"


def _summarize_replay_trades(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    closed = [row for row in rows if isinstance(row.get("exit"), Mapping)]
    closed.sort(key=lambda row: (
        _number(row["exit"].get("event_time_epoch"), 0.0),
        str(row.get("position_id") or ""),
    ))
    pnls = [_number(row["exit"].get("net_pnl_rub"), 0.0) for row in closed]
    wins = [value for value in pnls if value > 0.0]
    losses = [value for value in pnls if value < 0.0]
    risk_values = [
        _number(row.get("risk_rub"), math.nan) for row in closed
    ]
    r_values = [
        pnl / risk for pnl, risk in zip(pnls, risk_values)
        if math.isfinite(risk) and risk > 0.0
    ]
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        cumulative += pnl
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return {
        "trades": len(rows),
        "closed": len(closed),
        "open_at_end": len(rows) - len(closed),
        "sample_quality": _sample_quality(len(closed)),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(len(wins) / len(closed) * 100.0, 3) if closed else None,
        "net_pnl_rub": round(sum(pnls), 4) if closed else None,
        "expectancy_rub": round(sum(pnls) / len(pnls), 4) if pnls else None,
        "average_win_rub": round(sum(wins) / len(wins), 4) if wins else None,
        "average_loss_rub": round(sum(losses) / len(losses), 4) if losses else None,
        "profit_factor": (
            round(sum(wins) / abs(sum(losses)), 4)
            if losses else None
        ),
        "expectancy_r": round(sum(r_values) / len(r_values), 4) if r_values else None,
        "max_drawdown_rub": round(max_drawdown, 4) if closed else None,
        "drawdown_basis": "SEQUENTIAL_CLOSED_TRADE_PNL_NOT_PORTFOLIO_DRAWDOWN",
        "average_holding_sec": round(sum(
            _number(row["exit"].get("elapsed_sec"), 0.0) for row in closed
        ) / len(closed), 3) if closed else None,
    }



def _paired_scenario_comparison(
    trade_results: list[Mapping[str, Any]],
) -> dict[str, Any]:
    """Compare exit hypotheses only on identical position cohorts."""
    scenario_names: list[str] = []
    baseline_name: str | None = None
    by_scenario: dict[str, dict[int, Mapping[str, Any]]] = {}

    for trade in trade_results:
        trade_index = int(_number(trade.get("trade_index"), -1))
        for scenario in trade.get("scenarios", []):
            if not isinstance(scenario, Mapping):
                continue
            name = str(scenario.get("name") or "")
            if not name:
                continue
            if name not in scenario_names:
                scenario_names.append(name)
            by_scenario.setdefault(name, {})[trade_index] = {
                "position_id": str(trade.get("position_id") or ""),
                "ticker": str(trade.get("ticker") or ""),
                "side": str(trade.get("side") or ""),
                "session_id": str(trade.get("session_id") or ""),
                "risk_rub": _number(trade.get("risk_rub"), 0.0),
                "exit": scenario.get("exit") if isinstance(scenario.get("exit"), Mapping) else None,
            }
            if baseline_name is None and str(scenario.get("kind") or "").lower() == "baseline":
                baseline_name = name

    if baseline_name is None:
        return {
            "available": False,
            "reason": "NO_BASELINE_SCENARIO",
            "comparisons_vs_baseline": {},
            "all_scenarios_common_cohort": None,
        }

    baseline_rows = by_scenario.get(baseline_name, {})
    comparisons: dict[str, Any] = {}
    for alternative_name in scenario_names:
        if alternative_name == baseline_name:
            continue
        alternative_rows = by_scenario.get(alternative_name, {})
        shared_positions = sorted(set(baseline_rows) & set(alternative_rows))
        baseline_closed = {
            index: row for index, row in baseline_rows.items() if row.get("exit") is not None
        }
        alternative_closed = {
            index: row for index, row in alternative_rows.items() if row.get("exit") is not None
        }
        paired = sorted(set(baseline_closed) & set(alternative_closed))
        baseline_only = sorted(set(baseline_closed) - set(alternative_closed))
        alternative_only = sorted(set(alternative_closed) - set(baseline_closed))

        baseline_metrics_rows: list[Mapping[str, Any]] = []
        alternative_metrics_rows: list[Mapping[str, Any]] = []
        deltas: list[float] = []
        better = equal = worse = 0
        baseline_wins = alternative_wins = 0
        details: list[dict[str, Any]] = []
        for index in paired:
            base = baseline_closed[index]
            alt = alternative_closed[index]
            base_exit = base["exit"]
            alt_exit = alt["exit"]
            base_pnl = _number(base_exit.get("net_pnl_rub"))
            alt_pnl = _number(alt_exit.get("net_pnl_rub"))
            if not math.isfinite(base_pnl) or not math.isfinite(alt_pnl):
                continue
            delta = alt_pnl - base_pnl
            deltas.append(delta)
            better += int(delta > 1e-9)
            worse += int(delta < -1e-9)
            equal += int(abs(delta) <= 1e-9)
            baseline_wins += int(base_pnl > 0.0)
            alternative_wins += int(alt_pnl > 0.0)
            common = {
                "position_id": base["position_id"],
                "ticker": base["ticker"],
                "side": base["side"],
                "session_id": base["session_id"],
                "risk_rub": base["risk_rub"],
            }
            baseline_metrics_rows.append({**common, "exit": base_exit})
            alternative_metrics_rows.append({**common, "exit": alt_exit})
            if len(details) < 200:
                details.append({
                    **common,
                    "baseline_net_pnl_rub": round(base_pnl, 4),
                    "alternative_net_pnl_rub": round(alt_pnl, 4),
                    "delta_net_pnl_rub": round(delta, 4),
                    "baseline_exit_reason": base_exit.get("reason"),
                    "alternative_exit_reason": alt_exit.get("reason"),
                })

        count = len(deltas)
        comparisons[alternative_name] = {
            "baseline_scenario": baseline_name,
            "alternative_scenario": alternative_name,
            "cohort": {
                "replayed_positions": len(shared_positions),
                "baseline_closed": len(baseline_closed),
                "alternative_closed": len(alternative_closed),
                "paired_closed": count,
                "baseline_closed_alternative_open": len(baseline_only),
                "alternative_closed_baseline_open": len(alternative_only),
                "sample_quality": _sample_quality(count),
            },
            "baseline_on_paired_cohort": _summarize_replay_trades(baseline_metrics_rows),
            "alternative_on_paired_cohort": _summarize_replay_trades(alternative_metrics_rows),
            "paired_difference": {
                "alternative_minus_baseline_net_pnl_rub": round(sum(deltas), 4) if deltas else None,
                "mean_delta_net_pnl_rub": round(sum(deltas) / count, 4) if count else None,
                "median_delta_net_pnl_rub": round(sorted(deltas)[count // 2], 4) if count and count % 2 else (
                    round((sorted(deltas)[count // 2 - 1] + sorted(deltas)[count // 2]) / 2.0, 4)
                    if count else None
                ),
                "alternative_better": better,
                "equal_within_1e_9_rub": equal,
                "alternative_worse": worse,
                "baseline_wins_on_paired_cohort": baseline_wins,
                "alternative_wins_on_paired_cohort": alternative_wins,
                "win_rate_delta_percentage_points": round(
                    (alternative_wins - baseline_wins) / count * 100.0, 3
                ) if count else None,
            },
            "paired_trades": details,
            "note": (
                "Both scenarios are evaluated on exactly the same positions that closed in both. "
                "Scenario-only closures are reported separately and are not assigned zero PnL. "
                "This is trade-level paired replay, not a portfolio simulation."
            ),
        }

    common_indexes: set[int] | None = None
    for name in scenario_names:
        closed_indexes = {
            index for index, row in by_scenario.get(name, {}).items()
            if row.get("exit") is not None
        }
        common_indexes = closed_indexes if common_indexes is None else common_indexes & closed_indexes
    common_indexes = common_indexes or set()
    all_common: dict[str, Any] = {
        "positions_closed_in_every_scenario": len(common_indexes),
        "sample_quality": _sample_quality(len(common_indexes)),
        "summary_by_scenario": {},
    }
    for name in scenario_names:
        rows = []
        for index in sorted(common_indexes):
            source = by_scenario[name][index]
            rows.append({
                "position_id": source["position_id"],
                "ticker": source["ticker"],
                "side": source["side"],
                "session_id": source["session_id"],
                "risk_rub": source["risk_rub"],
                "exit": source["exit"],
            })
        all_common["summary_by_scenario"][name] = _summarize_replay_trades(rows)

    return {
        "available": True,
        "baseline_scenario": baseline_name,
        "comparisons_vs_baseline": comparisons,
        "all_scenarios_common_cohort": all_common,
        "note": (
            "Use paired comparisons as the primary scenario comparison. "
            "The all-scenarios cohort is stricter and may be small; open-at-end outcomes "
            "are not converted to zero-return trades."
        ),
    }
