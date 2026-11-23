"""Parallel REGIME_ADAPTIVE_V2 Shadow evaluator.

This module is research-only: it never changes the baseline decision or submits
orders. V2 reuses V1's score and hard blocks, applies fixed documented
penalties, and records only point-in-time evidence.
"""
from __future__ import annotations

import math
import os
import time
from collections import defaultdict, deque
from collections.abc import Mapping
from statistics import median
from typing import Any

from strategy.market_context import classify_market_regime


def _finite(value: object) -> float | None:
    if isinstance(value, bool) or not isinstance(value, (int, float, str)):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def tri_state_bool(mapping: Mapping[str, Any], key: str) -> bool | str:
    """Preserve an explicit False; use UNKNOWN only for absent/invalid evidence."""
    if key not in mapping:
        return "UNKNOWN"
    value = mapping.get(key)
    if isinstance(value, bool):
        return value
    number = _finite(value)
    if number in (0.0, 1.0):
        return bool(number)
    return "UNKNOWN"


class RegimeAdaptiveV2:
    """Per-ticker point-in-time regime classifier and Shadow-only score modifier."""

    def __init__(self, *, history_size: int = 300, min_reference_samples: int = 20) -> None:
        self.history_size = max(20, int(history_size))
        self.min_reference_samples = max(20, int(min_reference_samples))
        self._atr_history: dict[str, deque[float]] = defaultdict(
            lambda: deque(maxlen=self.history_size)
        )

    def clear(self) -> None:
        """Reset point-in-time ATR references at the beginning of a new session."""
        self._atr_history.clear()

    @staticmethod
    def _m1_freshness(metrics: Mapping[str, Any]) -> bool | str:
        confirmed = tri_state_bool(metrics, "bar_pattern_confirmed")
        if confirmed is not True:
            return confirmed
        signal_at = _finite(metrics.get("_bar_pattern_signal_at_monotonic"))
        if signal_at is None or signal_at <= 0.0:
            return "UNKNOWN"
        try:
            max_age = float(os.getenv("PILUITUS_PATTERN_SIGNAL_MAX_AGE_SEC", "15"))
        except (TypeError, ValueError, OverflowError):
            max_age = 15.0
        if not math.isfinite(max_age) or max_age <= 0.0:
            max_age = 15.0
        age = time.monotonic() - signal_at
        return -0.25 <= age <= max_age

    def evaluate(
        self,
        direction: str,
        metrics: Mapping[str, Any] | None,
        baseline_result: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Return V2 score/attribution while retaining V1's exact hard-block set."""
        side = str(direction or "").strip().upper()
        if not isinstance(metrics, Mapping):
            metrics = {}
        ticker = str(metrics.get("ticker") or metrics.get("security_code") or "").strip().upper()
        history = self._atr_history[ticker] if ticker else deque()
        prior_reference = (
            median(history) if len(history) >= self.min_reference_samples else None
        )
        context = classify_market_regime(
            metrics,
            atr_reference_pct=prior_reference,
            atr_reference_samples=len(history),
        )

        base_score = _finite(baseline_result.get("score"))
        if base_score is None:
            base_score = 0.0
        base_score = min(100.0, max(0.0, base_score))
        threshold = _finite(baseline_result.get("threshold"))
        if threshold is None or not 0.0 <= threshold <= 100.0:
            threshold = 85.0

        hard_blocks_raw = baseline_result.get("hard_blocks", [])
        hard_blocks = (
            [str(item) for item in hard_blocks_raw if str(item).strip()]
            if isinstance(hard_blocks_raw, (list, tuple, set)) else []
        )
        penalties: dict[str, float] = {}
        penalty_reasons: list[str] = []

        liquidity = str(context.get("liquidity_regime") or "UNKNOWN").upper()
        volatility = str(context.get("volatility_regime") or "UNKNOWN").upper()
        if liquidity == "LOW_LIQUIDITY":
            penalties["LOW_LIQUIDITY"] = 12.0
            penalty_reasons.append("LOW_LIQUIDITY:-12")
        if volatility == "HIGH_VOLATILITY":
            penalties["HIGH_VOLATILITY"] = 8.0
            penalty_reasons.append("HIGH_VOLATILITY:-8")

        m1_fresh = self._m1_freshness(metrics)
        baseline_regime = str(baseline_result.get("regime") or "UNKNOWN").upper()
        if baseline_regime == "TREND_ALIGNED" and m1_fresh is False:
            penalties["TREND_WITHOUT_FRESH_M1"] = 8.0
            penalty_reasons.append("TREND_WITHOUT_FRESH_M1:-8")

        total_penalty = sum(penalties.values())
        score = round(max(0.0, min(100.0, base_score - total_penalty)), 2)
        eligible = not hard_blocks and score >= threshold

        # Update the reference only after classifying the current candidate,
        # so the candidate never influences its own volatility baseline.
        atr_pct = _finite(metrics.get("bar_atr_pct"))
        atr_value = _finite(metrics.get("bar_atr"))
        atr_periods = _finite(metrics.get("bar_atr_periods"))
        try:
            min_atr_periods = min(14, max(2, int(os.getenv("PILUITUS_ATR_MIN_PERIODS", "5"))))
        except (TypeError, ValueError, OverflowError):
            min_atr_periods = 5
        atr_added = bool(
            ticker and atr_pct is not None and atr_pct > 0.0
            and atr_value is not None and atr_value > 0.0
            and atr_periods is not None and atr_periods >= min_atr_periods
        )
        if atr_added:
            history.append(atr_pct)

        return {
            "schema_version": 2,
            "strategy": "REGIME_ADAPTIVE_V2",
            "side": side,
            "base_v1_score": round(base_score, 2),
            "v2_score": score,
            "threshold": threshold,
            "eligible": eligible,
            "hard_blocks": hard_blocks,
            "penalties": penalties,
            "total_penalty": total_penalty,
            "penalty_reasons": penalty_reasons,
            "liquidity_regime": liquidity,
            "volatility_regime": volatility,
            "directional_regime": str(context.get("directional_regime") or "UNKNOWN"),
            "m1_freshness": m1_fresh,
            "atr_reference_samples_before_candidate": max(0, len(history) - int(atr_added)),
            "reasons": penalty_reasons,
            "mode": "SHADOW_ONLY",
        }
