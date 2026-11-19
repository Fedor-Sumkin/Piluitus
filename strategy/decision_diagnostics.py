"""Deterministic diagnostics helpers for the LLM decision funnel."""
from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any


_NUMERIC_FIELDS = {
    "price_change_pct": "price_change_pct",
    "price_vs_vwap_pct": "price_vs_vwap_pct",
    "volume_spike": "volume_spike",
    "volume_spike_raw": "volume_spike_raw",
    "trade_data_mode": "trade_data_mode",
    "trade_data_ready": "trade_data_ready",
    "trade_vwap_60s": "trade_vwap_60s",
    "trade_volume_5s": "trade_volume_5s",
    "trade_volume_baseline_5s": "trade_volume_baseline_5s",
    "trade_flow_imbalance_30s": "trade_flow_imbalance_30s",
    "trade_buy_volume_30s": "trade_buy_volume_30s",
    "trade_sell_volume_30s": "trade_sell_volume_30s",
    "trade_count_60s": "trade_count_60s",
    "trade_count_5s": "trade_count_5s",
    "current_price": "current_price",
    "bid": "bid",
    "ask": "ask",
    "bbo_age_sec": "bbo_age_sec",
    "bid_ask_age_sec": "bid_ask_age_sec",
    "bbo_max_age_sec": "bbo_max_age_sec",
    "order_book_age_sec": "order_book_age_sec",
    "order_book_valid": "order_book_valid",
    "post_llm_bbo_refresh_valid": "post_llm_bbo_refresh_valid",
    "post_llm_trade_refresh_valid": "post_llm_trade_refresh_valid",
    "trade_count_5s": "trade_count_5s",
    "trade_baseline_bucket_count": "trade_baseline_bucket_count",
    "trade_baseline_trade_count": "trade_baseline_trade_count",
    "trade_baseline_min_bucket_count": "trade_baseline_min_bucket_count",
    "trade_history_coverage_sec": "trade_history_coverage_sec",
    "trade_metrics_age_sec": "trade_metrics_age_sec",
    "trade_price_change_30s_pct": "trade_price_change_30s_pct",
    "trade_realized_volatility_60s_pct": "trade_realized_volatility_60s_pct",
    "spread_pct": "spread_pct",
    "m1_pattern_confirmed": "bar_pattern_confirmed",
    "m1_pattern_direction_code": "bar_pattern_direction_code",
    "m1_breakout_up": "bar_breakout_up",
    "m1_breakout_down": "bar_breakout_down",
    "m1_pullback_buy": "bar_pullback_buy",
    "m1_pullback_sell": "bar_pullback_sell",
    "m1_trend_up": "bar_trend_up",
    "m1_trend_down": "bar_trend_down",
    "m1_signal_gap_atr": "bar_signal_gap_atr",
    "bid_size_drop_pct": "bid_size_drop_pct",
    "ask_size_drop_pct": "ask_size_drop_pct",
    "top_bid_history_samples": "top_bid_history_samples",
    "top_ask_history_samples": "top_ask_history_samples",
    "bid_size_drop_age_sec": "bid_size_drop_age_sec",
    "ask_size_drop_age_sec": "ask_size_drop_age_sec",
    "bid_size_available": "bid_size_available",
    "ask_size_available": "ask_size_available",
    "bid_ask_available": "bid_ask_available",
    "spread_available": "spread_available",
    "m1_atr": "bar_atr",
    "m1_atr_periods": "bar_atr_periods",
    "m1_close_position": "bar_close_position",
    "m1_body_ratio": "bar_body_ratio",
    "queue_imbalance": "queue_imbalance",
    "queue_imbalance_available": "queue_imbalance_available",
    "queue_imbalance_age_sec": "queue_imbalance_age_sec",
    "bid_quote_age_sec": "bid_quote_age_sec",
    "ask_quote_age_sec": "ask_quote_age_sec",
}


def _finite_number(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def build_llm_feature_snapshot(metrics: object) -> dict[str, Any]:
    """Return a stable, JSON-safe snapshot of the metric mapping used at a stage."""
    if not isinstance(metrics, Mapping):
        return {}
    snapshot = {
        public_name: _finite_number(metrics.get(source_name))
        for public_name, source_name in _NUMERIC_FIELDS.items()
    }
    snapshot["direction_evidence_state"] = str(
        metrics.get("direction_evidence_state") or "UNAVAILABLE"
    )
    snapshot["m1_pattern"] = str(metrics.get("bar_pattern_name") or "")
    return snapshot


def classify_hold_reason_bucket(reason_code: object) -> str:
    """Map model reason codes to bounded buckets for side-by-side diagnostics."""
    reason = str(reason_code or "").strip().upper()
    if reason.startswith("HOLD_M1_CONTRADICTION"):
        return "m1"
    if reason.startswith("HOLD_INSUFFICIENT_SUPPORT"):
        return "support"
    if reason.startswith("HOLD_RISK_REWARD"):
        return "risk_reward"
    if reason.startswith("HOLD_WEAK_DIRECTION"):
        return "direction"
    if reason.startswith("HOLD_WEAK_VOLUME"):
        return "volume"
    if reason.startswith("HOLD_WIDE_SPREAD"):
        return "spread"
    if reason.startswith("HOLD_STALE_BBO"):
        return "stale"
    return "other"
