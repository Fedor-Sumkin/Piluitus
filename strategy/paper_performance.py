"""Realized PAPER trade journal analytics.

Only final CLOSED trade records are accepted. Shadow labels are joined by the
stable candidate_id carried on the trade; ticker/time proximity is never used.
"""
from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Mapping

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
    return {
        "labels": count,
        "sample_quality": (
            "INSUFFICIENT_LT_30" if count < 30
            else "PRELIMINARY_30_99" if count < 100
            else "DEVELOPING_100_299" if count < 300
            else "LARGE_300_PLUS"
        ),
        "mean_net_return_pct": round(sum(returns) / count, 6) if count else None,
        "positive_rate_pct": round(len(wins) / count * 100.0, 3) if count else None,
        "profit_factor": (
            round(sum(wins) / abs(sum(losses)), 4) if losses else None
        ),
        "average_win_pct": round(sum(wins) / len(wins), 6) if wins else None,
        "average_loss_pct": round(sum(losses) / len(losses), 6) if losses else None,
        "average_mfe_net_pct": round(sum(mfe) / len(mfe), 6) if mfe else None,
        "average_mae_net_pct": round(sum(mae) / len(mae), 6) if mae else None,
    }


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
        key=lambda row: _text(row.get("exit_timestamp"), ""),
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
        "wins": len(wins),
        "losses": len(losses),
        "breakeven": len(valid) - len(wins) - len(losses),
        "win_rate_pct": round(len(wins) / len(valid) * 100.0, 3) if valid else None,
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
            "entry_diagnostics": _entry_diagnostics(rows),
            "loss_diagnostics": _loss_diagnostics(rows),
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
        for label in label_rows:
            if not isinstance(label, Mapping):
                continue
            candidate_id = _text(label.get("candidate_id"), "")
            if not candidate_id:
                continue
            labels_by_candidate[candidate_id].append(dict(label))
            if str(label.get("result") or "") != "OK":
                continue
            horizon = _number(label.get("horizon_sec"))
            net_return = _number(label.get("net_executable_return_pct"))
            if horizon is None or net_return is None or horizon <= 0:
                continue
            # A candidate has at most one observation per horizon; ignore duplicate
            # persisted rows rather than inflating the matched sample.
            by_candidate[candidate_id].setdefault(int(horizon), dict(label))

        matched: list[dict[str, Any]] = []
        unmatched_reasons: dict[str, int] = defaultdict(int)
        horizon_returns: dict[int, list[float]] = defaultdict(list)
        horizon_observations: dict[int, list[dict[str, Any]]] = defaultdict(list)
        counted_candidate_horizons: set[tuple[str, int]] = set()
        matched_realized_net = 0.0
        matched_details: list[dict[str, Any]] = []
        horizon_segments: dict[str, dict[str, list[Mapping[str, Any]]]] = {
            "by_side": defaultdict(list),
            "by_model_decision": defaultdict(list),
            "by_final_decision": defaultdict(list),
            "by_confidence": defaultdict(list),
            "by_model_decision_confidence": defaultdict(list),
            "by_execution_status": defaultdict(list),
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
            if not candidate_labels:
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
            for horizon, label in candidate_labels.items():
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
            "horizon_segment_note": (
                "Groups contain unique candidate_id+horizon labels joined to closed PAPER trades; "
                "returns are hypothetical Shadow outcomes, not realized trade PnL."
            ),
        }


paper_performance_tracker = PaperPerformanceTracker()
