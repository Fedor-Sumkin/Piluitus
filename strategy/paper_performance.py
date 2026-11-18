"""Realized PAPER trade journal analytics.

Only final CLOSED trade records are accepted. Shadow labels are joined by the
stable candidate_id carried on the trade; ticker/time proximity is never used.
"""
from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Mapping
from datetime import datetime, timezone, timedelta

from strategy.shadow_label_quality import (
    deduplicate_shadow_labels,
)

PAPER_CLOSED_TRADES_STORAGE_KEY = "piluitus:paper:closed_trades"
PAPER_CLOSED_TRADES_STORAGE_LIMIT = 20000


def _number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def _text(value: object, default: str = "UNKNOWN") -> str:
    text = str(value or "").strip()
    return text[:120] if text else default


def _confidence_bucket(value: object) -> str:
    confidence = _number(value)
    if confidence is None or not 0.0 <= confidence <= 100.0:
        return "UNKNOWN"
    lower = int(confidence // 10) * 10
    upper = min(100, lower + 9)
    return f"{lower:02d}-{upper:02d}"


def _shadow_segment_metrics(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    """Summarize unique candidate/horizon labels without treating them as fills."""
    returns = [
        value for row in rows
        if (value := _number(row.get("net_executable_return_pct"))) is not None
    ]
    wins = [value for value in returns if value > 0.0]
    losses = [value for value in returns if value < 0.0]
    mfe = [
        value for row in rows
        if (value := _number(row.get("mfe_executable_pct"))) is not None
    ]
    mae = [
        value for row in rows
        if (value := _number(row.get("mae_executable_pct"))) is not None
    ]
    count = len(returns)
    confidence_values = [
        value for row in rows
        if (value := _number(row.get("llm_confidence"))) is not None
        and 0.0 <= value <= 100.0
    ]
    positive_rate = len(wins) / count * 100.0 if count else None
    return {
        "labels": count,
        "sample_quality": (
            "INSUFFICIENT_LT_30" if count < 30
            else "PRELIMINARY_30_99" if count < 100
            else "DEVELOPING_100_299" if count < 300
            else "LARGE_300_PLUS"
        ),
        "mean_net_return_pct": round(sum(returns) / count, 6) if count else None,
        "positive_rate_pct": round(positive_rate, 3) if positive_rate is not None else None,
        "positive_rate_wilson_95_pct": _wilson_interval_pct(len(wins), count),
        "profit_factor": (
            round(sum(wins) / abs(sum(losses)), 4) if losses else None
        ),
        "average_win_pct": round(sum(wins) / len(wins), 6) if wins else None,
        "average_loss_pct": round(sum(losses) / len(losses), 6) if losses else None,
        "average_mfe_net_pct": round(sum(mfe) / len(mfe), 6) if mfe else None,
        "average_mae_net_pct": round(sum(mae) / len(mae), 6) if mae else None,
        "mean_model_confidence_pct": (
            round(sum(confidence_values) / len(confidence_values), 3)
            if confidence_values else None
        ),
        "confidence_observations": len(confidence_values),
    }


def _exit_timestamp_sort_key(value: object) -> tuple[int, float | str]:
    """Sort ISO timestamps by their actual UTC instant, including mixed offsets."""
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                return (0, parsed.astimezone(timezone.utc).timestamp())
        except (TypeError, ValueError, OverflowError):
            pass
    return (1, str(value or ""))



def _aware_timestamp_epoch(value: object) -> float | None:
    """Return an absolute timestamp only for valid timezone-aware ISO values."""
    if not isinstance(value, str) or not value.strip():
        return None
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            return None
        return parsed.astimezone(timezone.utc).timestamp()
    except (TypeError, ValueError, OverflowError):
        return None


def _reentry_gap_bucket(value: object) -> str:
    seconds = _number(value)
    if seconds is None or seconds < 0.0:
        return "UNKNOWN"
    if seconds < 60.0:
        return "LT_60S"
    if seconds < 180.0:
        return "60_179S"
    if seconds < 600.0:
        return "180_599S"
    return "600S_PLUS"


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

def _wilson_interval_pct(wins: int, total: int) -> list[float] | None:
    """95% Wilson interval for a descriptive positive-outcome rate."""
    if total <= 0:
        return None
    z = 1.959963984540054
    proportion = wins / total
    denominator = 1.0 + (z * z / total)
    center = (proportion + z * z / (2.0 * total)) / denominator
    margin = (
        z * math.sqrt(
            proportion * (1.0 - proportion) / total
            + z * z / (4.0 * total * total)
        ) / denominator
    )
    return [
        round(max(0.0, center - margin) * 100.0, 3),
        round(min(1.0, center + margin) * 100.0, 3),
    ]


def _metrics(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    pnls = [_number(row.get("pnl")) for row in rows]
    valid = [value for value in pnls if value is not None]
    wins = [value for value in valid if value > 0.0]
    losses = [value for value in valid if value < 0.0]
    gross_profit = sum(wins)
    gross_loss = abs(sum(losses))

    equity = 0.0
    peak = 0.0
    max_drawdown = 0.0
    max_drawdown_pct = 0.0
    has_positive_peak = False
    max_loss_streak = 0
    current_loss_streak = 0
    ordered = sorted(
        (row for row in rows if _number(row.get("pnl")) is not None),
        key=lambda row: _exit_timestamp_sort_key(row.get("exit_timestamp")),
    )
    for row in ordered:
        realized = float(_number(row.get("pnl")) or 0.0)
        equity += realized
        peak = max(peak, equity)
        drawdown = peak - equity
        if drawdown > max_drawdown:
            max_drawdown = drawdown
        if peak > 0.0:
            has_positive_peak = True
            max_drawdown_pct = max(max_drawdown_pct, drawdown / peak * 100.0)
        current_loss_streak = current_loss_streak + 1 if realized < 0.0 else 0
        max_loss_streak = max(max_loss_streak, current_loss_streak)

    holding = [
        value for row in rows
        if (value := _number(row.get("time_in_trade_sec"))) is not None and value >= 0.0
    ]
    commissions = [
        value for row in rows
        if (value := _number(row.get("total_commission_rub"))) is not None and value >= 0.0
    ]
    gross_values = [
        value for row in rows
        if (value := _number(row.get("gross_pnl"))) is not None
    ]

    # Exit-BBO gaps are collected by the execution monitor while a position is
    # open. Closed trades carry the completed episode count and durations.
    # Include a non-zero current duration defensively for older/interrupted rows.
    gap_episode_count = 0
    trades_with_gap = 0
    total_gap_sec = 0.0
    max_gap_sec = 0.0
    for row in rows:
        raw_count = _number(row.get("exit_bbo_gap_count"))
        episodes = max(0, int(raw_count or 0))
        total = max(0.0, _number(row.get("total_exit_bbo_gap_sec")) or 0.0)
        current = max(0.0, _number(row.get("current_exit_bbo_gap_sec")) or 0.0)
        last = max(0.0, _number(row.get("last_exit_bbo_gap_sec")) or 0.0)
        maximum = max(
            0.0,
            _number(row.get("max_exit_bbo_gap_sec")) or 0.0,
            current,
            last,
        )
        # An interrupted or legacy row may have a current gap but no completed
        # episode count. Count that observation once rather than losing its time.
        if current > 0.0 and episodes == 0:
            episodes = 1
        gap_episode_count += episodes
        total_gap_sec += total + current
        max_gap_sec = max(max_gap_sec, maximum)
        if episodes > 0 or total > 0.0 or current > 0.0 or last > 0.0 or maximum > 0.0:
            trades_with_gap += 1

    return {
        "trades": len(valid),
        "sample_quality": (
            "INSUFFICIENT_LT_30" if len(valid) < 30
            else "PRELIMINARY_30_99" if len(valid) < 100
            else "DEVELOPING_100_299" if len(valid) < 300
            else "LARGE_300_PLUS"
        ),
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": len(valid) - len(wins) - len(losses),
        "win_rate_pct": round(len(wins) / len(valid) * 100.0, 3) if valid else None,
        "win_rate_95_wilson_pct": _wilson_interval_pct(len(wins), len(valid)),
        "net_pnl_rub": round(sum(valid), 2),
        "gross_pnl_rub": round(sum(gross_values), 2),
        "gross_profit_rub": round(gross_profit, 2),
        "gross_loss_rub": round(gross_loss, 2),
        "expectancy_rub": round(sum(valid) / len(valid), 2) if valid else None,
        "average_win_rub": round(sum(wins) / len(wins), 2) if wins else None,
        "average_loss_rub": round(sum(losses) / len(losses), 2) if losses else None,
        "profit_factor": round(gross_profit / gross_loss, 4) if gross_loss > 0.0 else None,
        "max_drawdown_rub": round(max_drawdown, 2),
        "max_drawdown_pct_of_prior_peak": (
            round(max_drawdown_pct, 3) if has_positive_peak else None
        ),
        "max_consecutive_losses": max_loss_streak,
        "current_loss_streak": current_loss_streak,
        "average_holding_sec": round(sum(holding) / len(holding), 2) if holding else None,
        "total_commission_rub": round(sum(commissions), 2),
        "trades_with_exit_bbo_gap": trades_with_gap,
        "exit_bbo_gap_trade_rate_pct": (
            round(trades_with_gap / len(valid) * 100.0, 3) if valid else None
        ),
        "exit_bbo_gap_episodes": gap_episode_count,
        "total_exit_bbo_gap_sec": round(total_gap_sec, 3),
        "average_exit_bbo_gap_sec": (
            round(total_gap_sec / gap_episode_count, 3) if gap_episode_count else None
        ),
        "max_exit_bbo_gap_sec": round(max_gap_sec, 3),
    }



def _holding_time_bucket(value: object) -> str:
    seconds = _number(value)
    if seconds is None or seconds < 0.0:
        return "UNKNOWN"
    if seconds < 60.0:
        return "LT_60S"
    if seconds < 180.0:
        return "60_179S"
    if seconds < 300.0:
        return "180_299S"
    if seconds < 600.0:
        return "300_599S"
    return "600S_PLUS"



def _numeric_bucket(value: object, *, prefix: str, thresholds: tuple[float, ...], suffixes: tuple[str, ...]) -> str:
    number = _number(value)
    if number is None or number < 0.0:
        return f"{prefix}_UNKNOWN"
    for threshold, suffix in zip(thresholds, suffixes):
        if number < threshold:
            return f"{prefix}_{suffix}"
    return f"{prefix}_{suffixes[-1]}"


def _entry_spread_bucket(value: object) -> str:
    return _numeric_bucket(
        value, prefix="SPREAD_PCT", thresholds=(0.02, 0.05, 0.10),
        suffixes=("LT_0_02", "0_02_0_05", "0_05_0_10", "GE_0_10"),
    )


def _entry_bbo_age_bucket(value: object) -> str:
    return _numeric_bucket(
        value, prefix="BBO_AGE_SEC", thresholds=(0.25, 0.50, 1.0),
        suffixes=("LT_0_25", "0_25_0_50", "0_50_1_00", "GE_1_00"),
    )


def _entry_shortfall_bucket(value: object) -> str:
    number = _number(value)
    if number is not None and number < 0.0:
        return "SHORTFALL_BETTER_THAN_BBO"
    return _numeric_bucket(
        value, prefix="SHORTFALL_BPS", thresholds=(1.0, 3.0, 5.0),
        suffixes=("LT_1", "1_3", "3_5", "GE_5"),
    )


def _entry_spread_atr_bucket(value: object) -> str:
    return _numeric_bucket(
        value, prefix="SPREAD_ATR", thresholds=(0.10, 0.25, 0.50),
        suffixes=("LT_0_10", "0_10_0_25", "0_25_0_50", "GE_0_50"),
    )


def _exit_shortfall_bucket(value: object) -> str:
    number = _number(value)
    if number is not None and number < 0.0:
        return "EXIT_SHORTFALL_BETTER_THAN_BBO"
    return _numeric_bucket(
        value, prefix="EXIT_SHORTFALL_BPS", thresholds=(1.0, 3.0, 5.0),
        suffixes=("LT_1", "1_3", "3_5", "GE_5"),
    )


def _exit_depth_impact_bucket(value: object) -> str:
    number = _number(value)
    if number is not None and number < 0.0:
        return "EXIT_DEPTH_BETTER_THAN_TOP"
    return _numeric_bucket(
        value, prefix="EXIT_DEPTH_IMPACT_RUB", thresholds=(1.0, 3.0, 5.0),
        suffixes=("LT_1", "1_3", "3_5", "GE_5"),
    )


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


def _entry_signal_drift_bucket(value: object) -> str:
    number = _number(value)
    if number is None:
        return "SIGNAL_DRIFT_UNKNOWN"
    if number < -0.10:
        return "FAVORABLE_LT_MINUS_0_10"
    if number < -0.05:
        return "FAVORABLE_MINUS_0_10_TO_0_05"
    if number <= 0.05:
        return "NEAR_FLAT"
    if number <= 0.10:
        return "ADVERSE_0_05_TO_0_10"
    return "ADVERSE_GT_0_10"


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


class PaperPerformanceTracker:
    """Bounded, idempotent in-memory view of the durable closed-trade journal."""

    def __init__(self, max_records: int = PAPER_CLOSED_TRADES_STORAGE_LIMIT) -> None:
        self.max_records = max(1, int(max_records))
        self._by_id: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _valid_trade(trade: object) -> dict[str, Any] | None:
        if not isinstance(trade, Mapping):
            return None
        if str(trade.get("status") or "").upper() != "CLOSED":
            return None
        trade_id = _text(trade.get("id"), "")
        mode = str(trade.get("mode") or "").strip().upper()
        if mode == "LIVE" or (mode != "PAPER" and not trade_id.startswith(("PAPER_", "TRADE_"))):
            return None
        pnl = _number(trade.get("pnl"))
        side = str(trade.get("side") or "").upper()
        if not trade_id or pnl is None or side not in {"BUY", "SELL"}:
            return None
        row = dict(trade)
        row["id"] = trade_id
        row["side"] = side
        row["ticker"] = _text(trade.get("ticker"))
        row["pnl"] = round(pnl, 2)
        return row

    def record(self, trade: object) -> bool:
        row = self._valid_trade(trade)
        if row is None or row["id"] in self._by_id:
            return False
        self._by_id[row["id"]] = row
        while len(self._by_id) > self.max_records:
            oldest_id = next(iter(self._by_id))
            self._by_id.pop(oldest_id, None)
        return True

    def restore(self, trades: object) -> int:
        if not isinstance(trades, (list, tuple)):
            return 0
        # LPUSH journals are newest-first. Keep the newest copy of a duplicate ID,
        # then replay unique rows oldest-first so bounded eviction retains the newest.
        newest_unique: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for trade in trades:
            row = self._valid_trade(trade)
            if row is None or row["id"] in seen_ids:
                continue
            seen_ids.add(row["id"])
            newest_unique.append(row)
        restored = 0
        for row in reversed(newest_unique):
            restored += int(self.record(row))
        return restored

    def clear(self) -> int:
        """Clear the in-memory view when the operator explicitly resets PAPER."""
        removed = len(self._by_id)
        self._by_id.clear()
        return removed

    @property
    def records(self) -> list[dict[str, Any]]:
        return list(self._by_id.values())

    @staticmethod
    def _group(rows: list[dict[str, Any]], field: str, transform=None) -> dict[str, Any]:
        buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            raw = row.get(field)
            key = transform(raw) if transform else _text(raw)
            buckets[str(key)].append(row)
        return {
            key: _metrics(sorted(
                bucket,
                key=lambda item: _text(item.get("exit_timestamp"), ""),
            ))
            for key, bucket in sorted(buckets.items())
        }

    @staticmethod
    def _group_rows(
        rows: list[dict[str, Any]], key_for_row
    ) -> dict[str, Any]:
        buckets: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            buckets[str(key_for_row(row) or "UNKNOWN")].append(row)
        return {
            key: _metrics(sorted(
                bucket,
                key=lambda item: _text(item.get("exit_timestamp"), ""),
            ))
            for key, bucket in sorted(buckets.items())
        }

    @staticmethod
    def _moscow_entry_time_bucket(row: Mapping[str, Any]) -> str:
        raw = row.get("entry_timestamp")
        if not isinstance(raw, str) or not raw.strip():
            return "UNKNOWN"
        try:
            stamp = datetime.fromisoformat(raw.strip())
            if stamp.tzinfo is None or stamp.utcoffset() is None:
                return "UNKNOWN"
            local = stamp.astimezone(timezone(timedelta(hours=3)))
        except (TypeError, ValueError, OverflowError):
            return "UNKNOWN"
        minute = local.hour * 60 + local.minute
        if 10 * 60 <= minute < 11 * 60:
            return "OPEN_10_11"
        if 11 * 60 <= minute < 14 * 60:
            return "MORNING_11_14"
        if 14 * 60 <= minute < 18 * 60:
            return "AFTERNOON_14_18"
        if 18 * 60 <= minute < 23 * 60:
            return "EVENING_18_23"
        return "OUTSIDE_10_23"

    def scorecard(self, shadow_labels: object = None) -> dict[str, Any]:
        rows = sorted(
            self.records,
            key=lambda row: _text(row.get("exit_timestamp"), ""),
        )
        result: dict[str, Any] = {
            "sample_quality": (
                "INSUFFICIENT_LT_30" if len(rows) < 30
                else "PRELIMINARY_30_99" if len(rows) < 100
                else "DEVELOPING_100_299" if len(rows) < 300
                else "LARGE_300_PLUS"
            ),
            "overall": _metrics(rows),
            "by_side": self._group(rows, "side"),
            "by_ticker": self._group(rows, "ticker"),
            "by_ticker_side": self._group_rows(
                rows, lambda row: f"{_text(row.get('ticker'))}|{_text(row.get('side'))}"
            ),
            "by_moscow_entry_time": self._group_rows(
                rows, self._moscow_entry_time_bucket
            ),
            "by_market_regime": self._group(rows, "market_regime"),
            "by_volatility_regime": self._group(rows, "volatility_regime"),
            "by_liquidity_regime": self._group(rows, "liquidity_regime"),
            "by_directional_regime": self._group(rows, "directional_regime"),
            "by_session": self._group(rows, "session_id"),
            "by_llm_model_decision": self._group(rows, "llm_model_decision"),
            "by_llm_final_decision": self._group(rows, "llm_final_decision"),
            "by_confidence": self._group(rows, "llm_confidence_pct", _confidence_bucket),
            "by_llm_reason": self._group(rows, "llm_reason_code"),
            "by_exit_reason": self._group(rows, "exit_reason"),
            "by_holding_time": self._group(rows, "time_in_trade_sec", _holding_time_bucket),
            "by_entry_spread": self._group(rows, "entry_spread_pct", _entry_spread_bucket),
            "by_entry_bbo_age": self._group(rows, "entry_bbo_age_sec", _entry_bbo_age_bucket),
            "by_entry_execution_shortfall": self._group(
                rows, "entry_execution_shortfall_bps", _entry_shortfall_bucket
            ),
            "by_entry_spread_to_atr": self._group(
                rows, "entry_spread_to_atr_ratio", _entry_spread_atr_bucket
            ),
            "by_entry_signal_drift": self._group(
                rows, "entry_signal_adverse_drift_pct", _entry_signal_drift_bucket
            ),
            "by_exit_execution_shortfall": self._group(
                rows, "exit_execution_shortfall_bps", _exit_shortfall_bucket
            ),
            "by_exit_depth_impact": self._group(
                rows, "exit_depth_impact_rub", _exit_depth_impact_bucket
            ),
            "entry_diagnostics": _entry_diagnostics(rows),
            "exit_diagnostics": _exit_diagnostics(rows),
            "loss_diagnostics": _loss_diagnostics(rows),
            "reentry_diagnostics": _reentry_diagnostics(rows),
        }
        result["shadow_comparison"] = self._compare_shadow(rows, shadow_labels)
        return result

    @staticmethod
    def _compare_shadow(
        trades: list[dict[str, Any]], labels: object
    ) -> dict[str, Any]:
        label_rows = labels if isinstance(labels, (list, tuple)) else []
        labels_by_candidate: dict[str, list[dict[str, Any]]] = defaultdict(list)
        by_candidate: dict[str, dict[int, dict[str, Any]]] = defaultdict(dict)
        deduplicated_labels = deduplicate_shadow_labels(label_rows)
        for label in deduplicated_labels:
            candidate_id = _text(label.get("candidate_id"), "")
            if not candidate_id:
                continue
            labels_by_candidate[candidate_id].append(dict(label))
            horizon = _number(label.get("horizon_sec"))
            if horizon is not None and horizon > 0:
                by_candidate[candidate_id][int(horizon)] = dict(label)

        matched: list[dict[str, Any]] = []
        unmatched_reasons: dict[str, int] = defaultdict(int)
        horizon_returns: dict[int, list[float]] = defaultdict(list)
        horizon_observations: dict[int, list[dict[str, Any]]] = defaultdict(list)
        counted_candidate_horizons: set[tuple[str, int]] = set()
        label_coverage: dict[int, dict[str, Any]] = defaultdict(lambda: {
            "observed_rows": 0, "valid_ok_labels": 0,
            "status_counts": defaultdict(int), "rows_without_valid_net_return": 0,
        })
        for label in deduplicated_labels:
            horizon_value = _number(label.get("horizon_sec"))
            if horizon_value is None or horizon_value <= 0:
                continue
            horizon_key = int(horizon_value)
            coverage = label_coverage[horizon_key]
            coverage["observed_rows"] += 1
            status = str(label.get("result") or "UNKNOWN").upper()
            coverage["status_counts"][status] += 1
            net_value = _number(label.get("net_executable_return_pct"))
            if status == "OK" and net_value is not None:
                coverage["valid_ok_labels"] += 1
            else:
                coverage["rows_without_valid_net_return"] += 1
        matched_realized_net = 0.0
        matched_details: list[dict[str, Any]] = []
        horizon_segments: dict[str, dict[str, list[Mapping[str, Any]]]] = {
            "by_side": defaultdict(list),
            "by_model_decision": defaultdict(list),
            "by_final_decision": defaultdict(list),
            "by_confidence": defaultdict(list),
            "by_model_decision_confidence": defaultdict(list),
            "by_execution_status": defaultdict(list),
            "by_horizon_side_confidence": defaultdict(list),
            "by_horizon_decision_confidence": defaultdict(list),
        }
        for trade in trades:
            candidate_id = _text(trade.get("shadow_candidate_id"), "")
            if not candidate_id:
                unmatched_reasons["NO_SHADOW_CANDIDATE_ID"] += 1
                continue
            all_candidate_labels = labels_by_candidate.get(candidate_id, [])
            candidate_labels = by_candidate.get(candidate_id, {})
            if not all_candidate_labels:
                unmatched_reasons["CANDIDATE_NO_LABEL_RECORD"] += 1
                continue
            valid_candidate_labels = {
                horizon: label for horizon, label in candidate_labels.items()
                if str(label.get("result") or "").upper() == "OK"
                and _number(label.get("net_executable_return_pct")) is not None
            }
            if not valid_candidate_labels:
                results = {
                    str(label.get("result") or "UNKNOWN").upper()
                    for label in all_candidate_labels
                }
                if results == {"NO_FRESH_BBO"}:
                    reason = "CANDIDATE_LABEL_NO_FRESH_BBO"
                elif results == {"LABEL_LATE"}:
                    reason = "CANDIDATE_LABEL_LATE"
                else:
                    reason = "CANDIDATE_HAS_NO_VALID_OK_LABEL"
                unmatched_reasons[reason] += 1
                continue

            matched.append(trade)
            realized_net = _number(trade.get("pnl")) or 0.0
            matched_realized_net += realized_net
            matched_horizons: dict[str, float] = {}
            for horizon, label in valid_candidate_labels.items():
                value = _number(label.get("net_executable_return_pct"))
                if value is not None:
                    if (candidate_id, horizon) not in counted_candidate_horizons:
                        horizon_returns[horizon].append(value)
                        horizon_observations[horizon].append(label)
                        counted_candidate_horizons.add((candidate_id, horizon))
                        side = _text(label.get("side") or trade.get("side"))
                        model_decision = _text(label.get("llm_model_decision"))
                        final_decision = _text(label.get("llm_final_decision"))
                        confidence = _confidence_bucket(label.get("llm_confidence"))
                        execution_status = _text(label.get("llm_execution_status"))
                        segment_prefix = f"{horizon}s|"
                        horizon_segments["by_side"][segment_prefix + side].append(label)
                        horizon_segments["by_model_decision"][segment_prefix + model_decision].append(label)
                        horizon_segments["by_final_decision"][segment_prefix + final_decision].append(label)
                        horizon_segments["by_confidence"][segment_prefix + confidence].append(label)
                        horizon_segments["by_model_decision_confidence"][
                            segment_prefix + model_decision + "|" + confidence
                        ].append(label)
                        horizon_segments["by_execution_status"][
                            segment_prefix + execution_status
                        ].append(label)
                        horizon_segments["by_horizon_side_confidence"][
                            segment_prefix + side + "|" + confidence
                        ].append(label)
                        horizon_segments["by_horizon_decision_confidence"][
                            segment_prefix + model_decision + "|" + confidence
                        ].append(label)
                    matched_horizons[str(horizon)] = round(value, 6)
            matched_details.append({
                "trade_id": _text(trade.get("id"), ""),
                "candidate_id": candidate_id,
                "ticker": _text(trade.get("ticker")),
                "side": _text(trade.get("side")),
                "realized_net_pnl_rub": round(realized_net, 2),
                "shadow_net_return_pct_by_horizon": matched_horizons,
            })

        horizons = {}
        for horizon, values in sorted(horizon_returns.items()):
            positive = [value for value in values if value > 0.0]
            negative = [value for value in values if value < 0.0]
            observations = horizon_observations[horizon]

            def average_field(field: str, digits: int = 3) -> float | None:
                field_values = [
                    value for label in observations
                    if (value := _number(label.get(field))) is not None
                ]
                return round(sum(field_values) / len(field_values), digits) if field_values else None

            horizons[str(horizon)] = {
                "labels": len(values),
                "mean_net_return_pct": round(sum(values) / len(values), 6),
                "positive_rate_pct": round(len(positive) / len(values) * 100.0, 3),
                "profit_factor": (
                    round(sum(positive) / abs(sum(negative)), 4)
                    if sum(negative) < 0.0 else None
                ),
                "average_mfe_net_pct": average_field("mfe_executable_pct", 6),
                "average_mae_net_pct": average_field("mae_executable_pct", 6),
                "average_time_to_mfe_sec": average_field("time_to_mfe_sec"),
                "average_time_to_mae_sec": average_field("time_to_mae_sec"),
                "average_valid_bbo_observations": average_field("valid_bbo_observations", 2),
                "average_bbo_observation_span_sec": average_field("bbo_observation_span_sec"),
            }
        unmatched = sum(unmatched_reasons.values())
        total = len(trades)
        summarized_segments = {
            group_name: {
                key: _shadow_segment_metrics(rows)
                for key, rows in sorted(groups.items())
            }
            for group_name, groups in horizon_segments.items()
        }
        return {
            "matched_trades": len(matched),
            "unmatched_trades": unmatched,
            "matched_trade_rate_pct": round(len(matched) / total * 100.0, 3) if total else None,
            "unmatched_by_reason": dict(sorted(unmatched_reasons.items())),
            "matched_realized_net_pnl_rub": round(matched_realized_net, 2),
            "join_key": "shadow_candidate_id",
            "matched_trade_details": matched_details[-200:],
            "horizons": horizons,
            "horizon_segments": summarized_segments,
            "label_coverage_by_horizon": {
                str(horizon): {
                    "observed_rows": item["observed_rows"],
                    "valid_ok_labels": item["valid_ok_labels"],
                    "rows_without_valid_net_return": item["rows_without_valid_net_return"],
                    "status_counts": dict(sorted(item["status_counts"].items())),
                }
                for horizon, item in sorted(label_coverage.items())
            },
            "horizon_segment_note": (
                "Groups contain deduplicated candidate_id+horizon labels joined to closed PAPER trades; "
                "returns are hypothetical Shadow outcomes, not realized trade PnL. "
                "Wilson intervals describe sampling uncertainty in positive rates; model confidence "
                "is a score and is not assumed to be a calibrated probability."
            ),
        }


paper_performance_tracker = PaperPerformanceTracker()
