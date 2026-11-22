"""Multi-position replay orchestration and grouped reporting."""
from __future__ import annotations

import math
from typing import Any, Iterable, Mapping

from .bbo_replay_simulator_analysis import (
    _paired_scenario_comparison,
    _summarize_replay_trades,
)
from .bbo_replay_simulator_exit import replay_exit_scenarios
from .bbo_replay_simulator_portfolio import _portfolio_capacity_simulation
from .bbo_replay_simulator_primitives import _epoch, _number

def replay_exit_batch(
    events: Iterable[Mapping[str, Any]],
    positions: Iterable[Mapping[str, Any]],
    *,
    max_bbo_age_sec: float = 2.0,
    scenarios: Iterable[Mapping[str, Any]] | None = None,
    portfolio_capital_rub: float | None = None,
    max_allocation_pct: float = 15.0,
) -> dict[str, Any]:
    """Replay many PAPER positions and summarize each exit hypothesis.

    This is an offline research helper. It does not read Memurai or alter positions.
    """
    event_rows = list(events)
    position_rows = list(positions)
    if not position_rows:
        return {
            "schema_version": 1,
            "engine": "PAPER_EXIT_REPLAY_V1",
            "positions": 0,
            "rejected_positions": [],
            "trades": [],
            "summary_by_scenario": {},
            "matched_cohort_comparison": {
                "available": False,
                "reason": "NO_REPLAYED_POSITIONS",
                "comparisons_vs_baseline": {},
                "all_scenarios_common_cohort": None,
            },
        }

    scenario_specs = list(scenarios) if scenarios is not None else None
    trade_results: list[dict[str, Any]] = []
    rejected_positions: list[dict[str, str]] = []
    grouped: dict[str, list[dict[str, Any]]] = {}

    # Index once by session and ticker; precompute session-wide sequence diagnostics.
    events_by_session_ticker: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    session_rows: dict[str, list[Mapping[str, Any]]] = {}
    for event in event_rows:
        if not isinstance(event, Mapping):
            continue
        event_session = str(event.get("session_id") or "").strip()
        if not event_session:
            continue
        session_rows.setdefault(event_session, []).append(event)

    session_metadata: dict[str, dict[str, int]] = {}
    for event_session, rows in session_rows.items():
        sequence_counts: dict[int, int] = {}
        valid_rows: list[Mapping[str, Any]] = []
        for event in rows:
            sequence = _number(event.get("sequence"), math.nan)
            if not math.isfinite(sequence) or sequence <= 0 or int(sequence) != sequence:
                continue
            sequence_int = int(sequence)
            sequence_counts[sequence_int] = sequence_counts.get(sequence_int, 0) + 1
            if sequence_counts[sequence_int] == 1:
                valid_rows.append(event)
        valid_rows.sort(key=lambda item: int(_number(item.get("sequence"), 0)))
        for row in valid_rows:
            event_ticker = str(row.get("ticker") or "").strip().upper().split("@")[0]
            if event_ticker:
                events_by_session_ticker.setdefault(
                    (event_session, event_ticker), []
                ).append(row)
        sequences = [int(_number(row.get("sequence"), 0)) for row in valid_rows]
        gaps = max(0, sequences[0] - 1) if sequences else 0
        if len(sequences) > 1:
            gaps += sum(
                max(0, right - left - 1)
                for left, right in zip(sequences, sequences[1:])
            )
        previous_epoch_by_ticker: dict[str, float] = {}
        missing_timestamps = regressions = 0
        for row in valid_rows:
            stamp = _epoch(row.get("event_time_epoch"))
            if not math.isfinite(stamp):
                missing_timestamps += 1
                continue
            symbol = str(row.get("ticker") or "").strip().upper().split("@")[0]
            previous_epoch = previous_epoch_by_ticker.get(symbol, math.nan)
            if math.isfinite(previous_epoch) and stamp < previous_epoch:
                regressions += 1
            previous_epoch_by_ticker[symbol] = stamp
        session_metadata[event_session] = {
            "session_events": len(rows),
            "duplicate_sequences": sum(count - 1 for count in sequence_counts.values()),
            "sequence_gaps": gaps,
            "missing_sequence_prefix": max(0, sequences[0] - 1) if sequences else 0,
            "first_sequence": sequences[0] if sequences else 0,
            "last_sequence": sequences[-1] if sequences else 0,
            "missing_event_timestamps": missing_timestamps,
            "event_time_regressions": regressions,
        }

    for index, position in enumerate(position_rows):
        if not isinstance(position, Mapping):
            continue
        position_session = str(
            position.get("bbo_replay_session_id") or position.get("session_id") or ""
        ).strip()
        position_ticker = str(position.get("ticker") or "").strip().upper().split("@")[0]
        relevant_events = events_by_session_ticker.get(
            (position_session, position_ticker), []
        )
        try:
            result = replay_exit_scenarios(
                relevant_events,
                position,
                max_bbo_age_sec=max_bbo_age_sec,
                scenarios=scenario_specs,
                _session_metadata=session_metadata.get(position_session),
            )
        except ValueError as exc:
            rejected_positions.append({
                "position_id": str(position.get("id") or position.get("position_id") or index),
                "session_id": position_session,
                "reason": str(exc),
            })
            continue
        risk_rub = result["risk_per_unit"] * result["quantity"]
        result["risk_rub"] = round(risk_rub, 4)
        result["trade_index"] = index
        trade_results.append(result)
        for scenario in result["scenarios"]:
            grouped.setdefault(str(scenario["name"]), []).append({
                "position_id": result["position_id"],
                "ticker": result["ticker"],
                "side": result["side"],
                "session_id": result["session_id"],
                "risk_rub": result["risk_rub"],
                "exit": scenario["exit"],
            })

    summaries: dict[str, Any] = {}
    for name, rows in grouped.items():
        summaries[name] = {
            "overall": _summarize_replay_trades(rows),
            "by_ticker": {},
            "by_side": {},
            "by_session": {},
        }
        for field, target in (
            ("ticker", "by_ticker"),
            ("side", "by_side"),
            ("session_id", "by_session"),
        ):
            buckets: dict[str, list[Mapping[str, Any]]] = {}
            for row in rows:
                key = str(row.get(field) or "UNKNOWN")
                buckets.setdefault(key, []).append(row)
            summaries[name][target] = {
                key: _summarize_replay_trades(bucket)
                for key, bucket in sorted(buckets.items())
            }

    result = {
        "schema_version": 1,
        "engine": "PAPER_EXIT_REPLAY_V1",
        "positions": len(trade_results),
        "rejected_positions": rejected_positions,
        "trades": trade_results,
        "summary_by_scenario": summaries,
        "matched_cohort_comparison": _paired_scenario_comparison(trade_results),
        "limitations": [
            "BASELINE uses actual latched EXIT_TRIGGER events when the journal contains them; legacy sessions without trigger events use a BBO-threshold proxy.",
            "Open positions at the end of the replay are excluded from realized scenario PnL.",
            "Portfolio capacity is modeled only when portfolio_capital_rub is explicitly supplied.",
        ],
    }
    if portfolio_capital_rub is not None:
        result["portfolio_capacity"] = {
            name: _portfolio_capacity_simulation(
                trade_results, name, portfolio_capital_rub, max_allocation_pct,
                events=event_rows, max_bbo_age_sec=max_bbo_age_sec,
            )
            for name in summaries
        }
    return result
