"""Post-inference refresh of executable BBO and trade-flow evidence."""
from __future__ import annotations

import time
from collections.abc import Mapping
from typing import Any

from .candidate_bbo_recovery import refresh_candidate_bbo_metrics
from .candidate_recovery_primitives import _finite


_POST_LLM_TRADE_METRIC_FIELDS = (
    "trade_data_ready",
    "trade_vwap_60s",
    "trade_volume_5s",
    "trade_volume_baseline_5s",
    "volume_spike",
    "volume_spike_raw",
    "trade_baseline_bucket_count",
    "trade_baseline_min_bucket_count",
    "trade_baseline_trade_count",
    "trade_count_5s",
    "price_change_pct",
    "trade_price_change_30s_pct",
    "trade_realized_volatility_60s_pct",
    "trade_flow_imbalance_30s",
    "trade_buy_volume_30s",
    "trade_sell_volume_30s",
    "trade_count_60s",
    "trade_history_coverage_sec",
    "trade_metrics_age_sec",
    "current_price",
    "vwap",
    "price_vs_vwap_pct",
)


def refresh_post_llm_evidence(
    screener: Any,
    ticker: str,
    metrics: Mapping[str, Any],
    *,
    now: float | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Rebuild the executable BBO and trade-flow evidence immediately after LLM.

    The original pre-LLM signal is retained for attribution, but it is never
    allowed to stand in for the current book or current trade-flow window at the
    final entry gate. Failed refreshes explicitly invalidate the corresponding
    evidence so stale values cannot accidentally pass validation.
    """
    evidence = dict(metrics) if isinstance(metrics, Mapping) else {}
    symbol = str(ticker or "").strip().upper().split("@")[0]
    refreshed_at = time.monotonic() if now is None else _finite(now, time.monotonic())

    try:
        bbo_ready, bbo_reason = refresh_candidate_bbo_metrics(
            screener, symbol, evidence, now=refreshed_at
        )
    except Exception as exc:
        bbo_ready = False
        bbo_reason = f"ORDER_BOOK refresh failed ({type(exc).__name__})."
    if not bbo_ready:
        evidence.update({
            "bid_ask_available": 0.0,
            "spread_available": 0.0,
            "order_book_valid": 0.0,
            "bbo_age_sec": -1.0,
            "bid_ask_age_sec": -1.0,
        })

    diagnostics: dict[str, Any] = {
        "bbo_refresh_status": "PASS" if bbo_ready else "BLOCKED",
        "bbo_refresh_reason": str(bbo_reason)[:240],
        "trade_refresh_status": "NOT_REQUIRED",
        "trade_refresh_reason": "Trade-flow refresh is not enabled for this candidate.",
        "refreshed_at_monotonic": round(refreshed_at, 6),
        "price_refreshed": False,
    }

    trade_mode = _finite(evidence.get("trade_data_mode"))
    if trade_mode >= 0.5:
        try:
            refresh_method = getattr(screener, "refresh_trade_metrics", None)
            if not callable(refresh_method):
                raise RuntimeError("refresh_trade_metrics is unavailable")
            try:
                fresh_trade = refresh_method(symbol, now=refreshed_at)
            except TypeError:
                # Compatibility with test doubles and older screener adapters.
                fresh_trade = refresh_method(symbol)
            if not isinstance(fresh_trade, Mapping):
                raise TypeError("refresh_trade_metrics returned a non-mapping value")
            for field in _POST_LLM_TRADE_METRIC_FIELDS:
                if field in fresh_trade:
                    evidence[field] = fresh_trade[field]
            trade_ready = _finite(evidence.get("trade_data_ready")) >= 0.5
            trade_age = _finite(evidence.get("trade_metrics_age_sec"), -1.0)
            trade_fresh = trade_ready and 0.0 <= trade_age <= 5.0
            diagnostics["trade_refresh_status"] = "PASS" if trade_fresh else (
                "STALE" if trade_ready else "NOT_READY"
            )
            diagnostics["trade_refresh_reason"] = (
                "Fresh INSTRUMENT_TRADES window restored."
                if trade_fresh else
                f"INSTRUMENT_TRADES is not ready/fresh (ready={int(trade_ready)}, age={trade_age:.3f}s)."
            )
            diagnostics["price_refreshed"] = bool(
                trade_fresh and _finite(evidence.get("current_price")) > 0.0
            )
        except Exception as exc:
            evidence.update({
                "trade_data_ready": 0.0,
                "trade_metrics_age_sec": -1.0,
                "volume_spike": 0.0,
                "trade_volume_5s": 0.0,
                "trade_volume_baseline_5s": 0.0,
            })
            diagnostics["trade_refresh_status"] = "ERROR"
            diagnostics["trade_refresh_reason"] = f"INSTRUMENT_TRADES refresh failed ({type(exc).__name__})."

    evidence["post_llm_bbo_refresh_valid"] = float(bbo_ready)
    evidence["post_llm_trade_refresh_valid"] = float(
        diagnostics["trade_refresh_status"] == "PASS"
        if diagnostics["trade_refresh_status"] != "NOT_REQUIRED" else True
    )
    evidence["post_llm_refresh_monotonic"] = refreshed_at
    return evidence, diagnostics
