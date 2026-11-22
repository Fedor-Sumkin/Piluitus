"""Grouping helpers and scorecard assembly for realized PAPER results."""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from .paper_performance_metrics import (
    _entry_diagnostics,
    _exit_diagnostics,
    _loss_diagnostics,
    _metrics,
    _reentry_diagnostics,
    _session_stability,
)
from .paper_performance_primitives import (
    _confidence_bucket,
    _entry_bbo_age_bucket,
    _entry_shortfall_bucket,
    _entry_signal_drift_bucket,
    _entry_spread_atr_bucket,
    _entry_spread_bucket,
    _exit_depth_impact_bucket,
    _exit_shortfall_bucket,
    _holding_time_bucket,
    _moscow_exit_date,
    _number,
    _text,
)


class PaperPerformanceScorecardMixin:
    """Build the established grouped scorecard without owning record state."""

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
            "by_moscow_exit_date": self._group_rows(rows, _moscow_exit_date),
            "session_stability": _session_stability(rows),
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
