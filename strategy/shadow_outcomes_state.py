"""Shared state helpers for the forward Shadow outcome tracker."""
from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from .shadow_outcomes_primitives import _number


class ShadowOutcomeStateMixin:
    """Provide bounded aggregate buckets and executable-BBO validation."""

    @staticmethod
    def _new_stats_bucket() -> dict[str, float | int]:
        return {
            "labels": 0, "ok": 0, "late": 0, "no_fresh_bbo": 0,
            "positive": 0, "negative": 0, "net_sum_pct": 0.0, "gross_sum_pct": 0.0,
            "net_positive_sum_pct": 0.0, "net_negative_sum_pct": 0.0,
            "mfe_sum_pct": 0.0, "mae_sum_pct": 0.0,
            "mfe_time_sum_sec": 0.0, "mae_time_sum_sec": 0.0,
            "valid_bbo_observations_sum": 0, "bbo_observation_span_sum_sec": 0.0,
        }

    @staticmethod
    def _accumulate_stats(bucket: dict[str, float | int], label: Mapping[str, Any]) -> None:
        bucket["labels"] = int(bucket["labels"]) + 1
        result = str(label.get("result") or "")
        if result == "OK":
            bucket["ok"] = int(bucket["ok"]) + 1
            net = _number(label, "net_executable_return_pct", math.nan)
            gross = _number(label, "gross_executable_return_pct", math.nan)
            if math.isfinite(net):
                bucket["net_sum_pct"] = float(bucket["net_sum_pct"]) + net
                if net > 0.0:
                    bucket["positive"] = int(bucket["positive"]) + 1
                    bucket["net_positive_sum_pct"] = float(bucket["net_positive_sum_pct"]) + net
                elif net < 0.0:
                    bucket["negative"] = int(bucket["negative"]) + 1
                    bucket["net_negative_sum_pct"] = float(bucket["net_negative_sum_pct"]) + abs(net)
            if math.isfinite(gross):
                bucket["gross_sum_pct"] = float(bucket["gross_sum_pct"]) + gross
            bucket["mfe_sum_pct"] = float(bucket["mfe_sum_pct"]) + _number(label, "mfe_executable_pct")
            bucket["mae_sum_pct"] = float(bucket["mae_sum_pct"]) + _number(label, "mae_executable_pct")
            bucket["mfe_time_sum_sec"] = float(bucket["mfe_time_sum_sec"]) + _number(label, "time_to_mfe_sec")
            bucket["mae_time_sum_sec"] = float(bucket["mae_time_sum_sec"]) + _number(label, "time_to_mae_sec")
            bucket["valid_bbo_observations_sum"] = int(bucket["valid_bbo_observations_sum"]) + int(max(0.0, _number(label, "valid_bbo_observations")))
            bucket["bbo_observation_span_sum_sec"] = float(bucket["bbo_observation_span_sum_sec"]) + max(0.0, _number(label, "bbo_observation_span_sec"))
        elif result == "LABEL_LATE":
            bucket["late"] = int(bucket["late"]) + 1
        elif result == "NO_FRESH_BBO":
            bucket["no_fresh_bbo"] = int(bucket["no_fresh_bbo"]) + 1

    @staticmethod
    def _score_band(score: float) -> str:
        value = min(100.0, max(0.0, score))
        lower = 90 if value >= 90 else int(value // 10) * 10
        upper = 100 if lower == 90 else lower + 9
        return f"{lower:02d}-{upper:02d}"

    @staticmethod
    def _fresh_bbo(metrics: Mapping[str, Any]) -> tuple[bool, dict[str, Any]]:
        bid = _number(metrics, "bid")
        ask = _number(metrics, "ask")
        bid_age = _number(metrics, "bid_quote_age_sec", math.inf)
        ask_age = _number(metrics, "ask_quote_age_sec", math.inf)
        max_age = min(5.0, max(0.25, _number(metrics, "bbo_max_age_sec", 2.0)))
        spread_pct = _number(metrics, "spread_pct", math.nan)
        if "bid_ask_available" not in metrics or "spread_available" not in metrics:
            unavailable_reason = "MISSING_BBO_FLAGS"
        elif bid <= 0.0:
            unavailable_reason = "MISSING_BID"
        elif ask <= 0.0:
            unavailable_reason = "MISSING_ASK"
        elif ask < bid:
            unavailable_reason = "CROSSED_BBO"
        elif not math.isfinite(bid_age) or bid_age < 0.0 or bid_age > max_age:
            unavailable_reason = "BID_STALE"
        elif not math.isfinite(ask_age) or ask_age < 0.0 or ask_age > max_age:
            unavailable_reason = "ASK_STALE"
        elif _number(metrics, "bid_ask_available") < 0.5:
            unavailable_reason = "BBO_UNCONFIRMED"
        elif _number(metrics, "spread_available") < 0.5:
            unavailable_reason = "SPREAD_UNAVAILABLE"
        else:
            if not math.isfinite(spread_pct) and bid > 0.0 and ask >= bid:
                midpoint = (bid + ask) / 2.0
                spread_pct = (ask - bid) / midpoint * 100.0 if midpoint > 0.0 else math.nan
            unavailable_reason = (
                "INVALID_SPREAD"
                if not math.isfinite(spread_pct) or spread_pct < 0.0
                else "OK"
            )
        valid = unavailable_reason == "OK"
        return valid, {
            "bid": bid,
            "ask": ask,
            "bid_age_sec": bid_age,
            "ask_age_sec": ask_age,
            "max_age_sec": max_age,
            "bid_ask_available": _number(metrics, "bid_ask_available", math.nan),
            "spread_available": _number(metrics, "spread_available", math.nan),
            "spread_pct": spread_pct,
            "unavailable_reason": unavailable_reason,
        }
