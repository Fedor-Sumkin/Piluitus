"""Descriptive entry, exit, loss, re-entry and session diagnostics for PAPER."""
from __future__ import annotations

from collections import defaultdict
from typing import Any, Mapping

from .paper_performance_metrics import _metrics, _wilson_interval_pct
from .paper_performance_primitives import (
    _aware_timestamp_epoch,
    _exit_timestamp_sort_key,
    _number,
    _reentry_gap_bucket,
    _text,
)


def _reentry_diagnostics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Analyze sequential same-ticker/same-session re-entries without guessing."""
    import heapq

    groups: dict[tuple[str, str], list[tuple[float, float, dict[str, Any]]]] = defaultdict(list)
    invalid_timestamps = 0
    missing_identity = 0
    eligible = 0

    for row in rows:
        entry_epoch = _aware_timestamp_epoch(row.get("entry_timestamp"))
        exit_epoch = _aware_timestamp_epoch(row.get("exit_timestamp"))
        if entry_epoch is None or exit_epoch is None or exit_epoch < entry_epoch:
            invalid_timestamps += 1
            continue
        ticker = _text(row.get("ticker"), "")
        session = _text(row.get("session_id"), "")
        if not ticker or ticker == "UNKNOWN" or not session or session == "UNKNOWN":
            missing_identity += 1
            continue
        eligible += 1
        groups[(ticker, session)].append((entry_epoch, exit_epoch, row))

    pairs: list[dict[str, Any]] = []
    by_gap: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_previous_exit: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_previous_outcome: dict[str, list[dict[str, Any]]] = defaultdict(list)
    by_direction: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for (ticker, session), items in groups.items():
        ordered = sorted(items, key=lambda item: item[0])
        pending_exits: list[tuple[float, int, dict[str, Any]]] = []
        next_prior = 0
        latest_closed: tuple[float, dict[str, Any]] | None = None

        for entry_epoch, _exit_epoch, current in ordered:
            while next_prior < len(ordered) and ordered[next_prior][0] < entry_epoch:
                prior_entry, prior_exit, prior_row = ordered[next_prior]
                if prior_exit >= prior_entry:
                    heapq.heappush(pending_exits, (prior_exit, next_prior, prior_row))
                next_prior += 1

            while pending_exits and pending_exits[0][0] <= entry_epoch:
                closed_epoch, _sequence, prior_row = heapq.heappop(pending_exits)
                if latest_closed is None or closed_epoch >= latest_closed[0]:
                    latest_closed = (closed_epoch, prior_row)

            if latest_closed is None:
                continue

            previous_exit_epoch, previous = latest_closed
            gap_sec = max(0.0, entry_epoch - previous_exit_epoch)
            previous_pnl = _number(previous.get("pnl"))
            previous_outcome = (
                "LOSS" if previous_pnl is not None and previous_pnl < 0.0
                else "WIN" if previous_pnl is not None and previous_pnl > 0.0
                else "BREAKEVEN" if previous_pnl == 0.0
                else "UNKNOWN"
            )
            previous_reason = _text(previous.get("exit_reason"))
            same_side = str(previous.get("side") or "").upper() == str(current.get("side") or "").upper()
            pair = dict(current)
            pair.update({
                "reentry_gap_sec": round(gap_sec, 3),
                "previous_trade_id": _text(previous.get("id"), ""),
                "previous_trade_pnl_rub": previous_pnl,
                "previous_exit_reason": previous_reason,
                "previous_trade_outcome": previous_outcome,
                "same_direction_as_previous": same_side,
            })
            pairs.append({
                "ticker": ticker,
                "session_id": session,
                "previous_trade_id": pair["previous_trade_id"],
                "trade_id": _text(current.get("id"), ""),
                "side": _text(current.get("side")),
                "previous_side": _text(previous.get("side")),
                "gap_sec": round(gap_sec, 3),
                "gap_bucket": _reentry_gap_bucket(gap_sec),
                "previous_exit_reason": previous_reason,
                "previous_trade_outcome": previous_outcome,
                "previous_trade_pnl_rub": previous_pnl,
                "current_trade_pnl_rub": _number(current.get("pnl")),
            })
            by_gap[_reentry_gap_bucket(gap_sec)].append(pair)
            by_previous_exit[previous_reason].append(pair)
            by_previous_outcome[previous_outcome].append(pair)
            by_direction["SAME_SIDE" if same_side else "OPPOSITE_SIDE"].append(pair)

    def summarize(source: Mapping[str, list[dict[str, Any]]]) -> dict[str, Any]:
        return {
            key: _metrics(sorted(
                bucket,
                key=lambda item: _exit_timestamp_sort_key(item.get("exit_timestamp")),
            ))
            for key, bucket in sorted(source.items())
        }

    return {
        "paired_reentries": len(pairs),
        "eligible_trades": eligible,
        "trades_without_valid_aware_entry_exit_timestamps": invalid_timestamps,
        "trades_without_ticker_or_session_identity": missing_identity,
        "sample_quality": (
            "INSUFFICIENT_LT_30" if len(pairs) < 30
            else "PRELIMINARY_30_99" if len(pairs) < 100
            else "DEVELOPING_100_299" if len(pairs) < 300
            else "LARGE_300_PLUS"
        ),
        "by_gap": summarize(by_gap),
        "by_previous_exit_reason": summarize(by_previous_exit),
        "by_previous_trade_outcome": summarize(by_previous_outcome),
        "by_direction": summarize(by_direction),
        "pairs": pairs[-200:],
        "note": (
            "A re-entry is paired only when a prior trade for the same ticker and "
            "known PAPER session has an aware entry/exit timestamp and closed no "
            "later than the next entry. Unknown sessions, naive timestamps and "
            "overlapping positions are not inferred. Cohorts are descriptive and "
            "do not establish that the prior exit caused the next outcome."
        ),
    }

def _exit_diagnostics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    fields = {
        "exit_execution_shortfall_rub": "execution_shortfall_rub",
        "exit_execution_shortfall_bps": "execution_shortfall_bps",
        "exit_depth_impact_rub": "depth_impact_rub",
        "trigger_to_executable_bbo_delay_sec": "trigger_to_bbo_delay_sec",
        "exit_depth_observation_count": "depth_observation_count",
    }
    coverage: dict[str, int] = {}
    averages: dict[str, float | None] = {}
    for field, label in fields.items():
        values = [value for row in rows if (value := _number(row.get(field))) is not None]
        coverage[label] = len(values)
        averages[label] = round(sum(values) / len(values), 6) if values else None
    return {
        "trades": len(rows),
        "coverage": coverage,
        "average": averages,
        "interpretation": (
            "Positive exit shortfall and depth impact are adverse. "
            "Depth impact is UNKNOWN when no confirmed depth VWAP was available; "
            "BBO-only fills are not treated as zero-impact depth observations."
        ),
    }

def _entry_diagnostics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    fields = {
        "entry_spread_pct": "spread_pct",
        "entry_bbo_age_sec": "bbo_age_sec",
        "entry_execution_shortfall_rub": "execution_shortfall_rub",
        "entry_execution_shortfall_bps": "execution_shortfall_bps",
        "entry_signal_adverse_drift_pct": "signal_adverse_drift_pct",
        "entry_spread_to_atr_ratio": "spread_to_atr_ratio",
    }
    coverage: dict[str, int] = {}
    averages: dict[str, float | None] = {}
    for field, label in fields.items():
        values = [value for row in rows if (value := _number(row.get(field))) is not None]
        coverage[label] = len(values)
        averages[label] = round(sum(values) / len(values), 6) if values else None
    return {
        "trades": len(rows),
        "coverage": coverage,
        "average": averages,
        "interpretation": (
            "Positive execution shortfall and positive signal drift are adverse. "
            "Entry spread-to-ATR is the full quoted spread divided by ATR. "
            "Buckets are descriptive cohorts, not recommended trading thresholds."
        ),
    }

def _loss_diagnostics(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Describe observable loss contributors without asserting unsupported causality."""
    losses = [row for row in rows if (_number(row.get("pnl")) or 0.0) < 0.0]
    flags = {
        "GROSS_LOSS_BEFORE_COSTS": 0,
        "COSTS_TURNED_GROSS_WIN_INTO_NET_LOSS": 0,
        "EXIT_BBO_GAP_PRESENT": 0,
        "TRIGGER_TO_FILL_DELAY_PRESENT": 0,
        "FAVORABLE_EXCURSION_WITH_NET_LOSS": 0,
        "EXIT_SPREAD_RECORDED": 0,
        "INSUFFICIENT_ATTRIBUTION_DATA": 0,
    }
    gross_coverage = commission_coverage = excursion_coverage = 0
    by_reason: dict[str, dict[str, int]] = defaultdict(lambda: {
        key: 0 for key in flags
    })
    by_side: dict[str, dict[str, int]] = defaultdict(lambda: {
        key: 0 for key in flags
    })

    for row in losses:
        gross = _number(row.get("gross_pnl"))
        commission = _number(row.get("total_commission_rub"))
        mfe = _number(row.get("max_favorable_excursion_pct"))
        gap_count = _number(row.get("exit_bbo_gap_count"))
        gap_sec = _number(row.get("total_exit_bbo_gap_sec"))
        current_gap = _number(row.get("current_exit_bbo_gap_sec"))
        delay = _number(row.get("trigger_to_executable_bbo_delay_sec"))
        spread = _number(row.get("exit_spread_pct"))
        trade_flags: list[str] = []

        if gross is not None:
            gross_coverage += 1
            if gross < 0.0:
                trade_flags.append("GROSS_LOSS_BEFORE_COSTS")
            elif gross > 0.0:
                trade_flags.append("COSTS_TURNED_GROSS_WIN_INTO_NET_LOSS")
        if commission is not None:
            commission_coverage += 1
        if mfe is not None:
            excursion_coverage += 1
            if mfe > 0.0:
                trade_flags.append("FAVORABLE_EXCURSION_WITH_NET_LOSS")
        if max(0.0, gap_count or 0.0) > 0 or max(0.0, gap_sec or 0.0) > 0 or max(0.0, current_gap or 0.0) > 0:
            trade_flags.append("EXIT_BBO_GAP_PRESENT")
        if delay is not None and delay > 0.0:
            trade_flags.append("TRIGGER_TO_FILL_DELAY_PRESENT")
        if spread is not None and spread > 0.0:
            trade_flags.append("EXIT_SPREAD_RECORDED")
        if gross is None and commission is None and mfe is None and gap_count is None and gap_sec is None and delay is None and spread is None:
            trade_flags.append("INSUFFICIENT_ATTRIBUTION_DATA")

        reason = _text(row.get("exit_reason"))
        side = _text(row.get("side"))
        for flag in trade_flags:
            flags[flag] += 1
            by_reason[reason][flag] += 1
            by_side[side][flag] += 1

    def _counts(source: Mapping[str, Mapping[str, int]]) -> dict[str, dict[str, int]]:
        return {key: dict(value) for key, value in sorted(source.items())}

    count = len(losses)
    return {
        "losing_trades": count,
        "sample_quality": (
            "INSUFFICIENT_LT_30" if count < 30
            else "PRELIMINARY_30_99" if count < 100
            else "DEVELOPING_100_299" if count < 300
            else "LARGE_300_PLUS"
        ),
        "data_coverage": {
            "gross_pnl_records": gross_coverage,
            "commission_records": commission_coverage,
            "favorable_excursion_records": excursion_coverage,
        },
        "observable_flags": flags,
        "by_exit_reason": _counts(by_reason),
        "by_side": _counts(by_side),
        "interpretation": (
            "Flags are overlapping observations, not mutually exclusive causes. "
            "A positive favorable excursion does not prove that a different exit "
            "would have improved realized PnL. Missing fields are not inferred."
        ),
    }

def _session_stability(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Summarize independent PAPER sessions rather than counting trades as independent."""
    buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
    missing_identity_trades = 0
    invalid_pnl_trades = 0
    for row in rows:
        session_id = _text(row.get("session_id"), "")
        pnl = _number(row.get("pnl"))
        if not session_id or session_id == "UNKNOWN":
            missing_identity_trades += 1
            continue
        if pnl is None:
            invalid_pnl_trades += 1
            continue
        buckets[session_id].append(row)

    session_pnls = {
        session_id: sum(float(_number(row.get("pnl")) or 0.0) for row in session_rows)
        for session_id, session_rows in buckets.items()
    }
    values = list(session_pnls.values())
    positive = sum(value > 0.0 for value in values)
    negative = sum(value < 0.0 for value in values)
    flat = len(values) - positive - negative
    ordered_values = sorted(values)
    median = None
    if ordered_values:
        middle = len(ordered_values) // 2
        median = (
            ordered_values[middle] if len(ordered_values) % 2
            else (ordered_values[middle - 1] + ordered_values[middle]) / 2.0
        )

    return {
        "sessions": len(values),
        "trades_in_identified_sessions": sum(len(items) for items in buckets.values()),
        "trades_missing_session_identity": missing_identity_trades,
        "trades_with_invalid_pnl": invalid_pnl_trades,
        "profitable_sessions": positive,
        "losing_sessions": negative,
        "flat_sessions": flat,
        "profitable_session_rate_pct": (
            round(positive / len(values) * 100.0, 3) if values else None
        ),
        "profitable_session_rate_wilson_95_pct": (
            _wilson_interval_pct(positive, len(values)) if values else None
        ),
        "total_session_net_pnl_rub": round(sum(values), 2) if values else None,
        "mean_session_net_pnl_rub": round(sum(values) / len(values), 2) if values else None,
        "median_session_net_pnl_rub": round(median, 2) if median is not None else None,
        "best_session_net_pnl_rub": round(max(values), 2) if values else None,
        "worst_session_net_pnl_rub": round(min(values), 2) if values else None,
        "session_results": {
            session_id: {
                "trades": len(buckets[session_id]),
                "net_pnl_rub": round(session_pnls[session_id], 2),
                "outcome": (
                    "PROFITABLE" if session_pnls[session_id] > 0.0
                    else "LOSING" if session_pnls[session_id] < 0.0 else "FLAT"
                ),
            }
            for session_id in sorted(buckets)
        },
    }
