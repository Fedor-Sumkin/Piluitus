"""Pure realized-trade metrics and Shadow label aggregation for PAPER."""
from __future__ import annotations

import math
from typing import Any, Mapping

from .paper_performance_primitives import _exit_timestamp_sort_key, _number


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
