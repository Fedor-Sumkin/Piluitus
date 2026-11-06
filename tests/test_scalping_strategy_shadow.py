from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from scalping_strategy import evaluate_scalping_candidate


def strong_buy_metrics() -> dict[str, float | str]:
    return {
        "price_change_pct": 0.20,
        "price_vs_vwap_pct": 0.12,
        "volume_spike": 2.4,
        "bid_ask_available": 1.0,
        "spread_available": 1.0,
        "spread_pct": 0.025,
        "bbo_max_age_sec": 2.0,
        "bid_quote_age_sec": 0.2,
        "ask_quote_age_sec": 0.3,
        "order_book_valid": 1.0,
        "queue_imbalance_available": 1.0,
        "queue_imbalance": 0.32,
        "queue_imbalance_age_sec": 0.2,
        "bar_atr": 0.20,
        "bar_atr_pct": 0.20,
        "bar_atr_periods": 8.0,
        "bar_pattern_confirmed": 1.0,
        "bar_pattern_name": "M1_RANGE_BREAKOUT_BUY",
        "bar_trend_up": 1.0,
        "bar_trend_down": 0.0,
    }


class ScalpingShadowStrategyTests(unittest.TestCase):
    def test_aligned_m1_momentum_queue_and_costs_score_as_shadow_eligible(self) -> None:
        with patch.dict(os.environ, {"PILUITUS_ATR_MIN_PERIODS": "5"}):
            result = evaluate_scalping_candidate("BUY", strong_buy_metrics())
        self.assertTrue(result["eligible"], result)
        self.assertGreaterEqual(result["score"], 70.0)
        self.assertEqual(result["mode"], "SHADOW_ONLY")
        self.assertEqual(result["regime"], "TREND_ALIGNED")

    def test_opposing_queue_imbalance_reduces_score_without_inverting_side(self) -> None:
        metrics = strong_buy_metrics()
        metrics["queue_imbalance"] = -0.45
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertFalse(result["eligible"])
        self.assertEqual(result["side"], "BUY")
        self.assertIn("best_queue_imbalance_opposes_side", result["reasons"])

    def test_stale_or_invalid_book_is_a_shadow_hard_block(self) -> None:
        metrics = strong_buy_metrics()
        metrics["order_book_valid"] = 0.0
        metrics["bid_quote_age_sec"] = 3.0
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertFalse(result["eligible"])
        self.assertIn("order_book_not_confirmed", result["hard_blocks"])
        self.assertIn("bbo_stale", result["hard_blocks"])

    def test_insufficient_atr_history_is_a_shadow_hard_block(self) -> None:
        metrics = strong_buy_metrics()
        metrics["bar_atr_periods"] = 2.0
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertFalse(result["eligible"])
        self.assertIn("atr_not_ready", result["hard_blocks"])

    def test_conflicting_closed_m1_pattern_blocks_shadow_approval(self) -> None:
        metrics = strong_buy_metrics()
        metrics["bar_pattern_name"] = "M1_RANGE_BREAKOUT_SELL"
        metrics["bar_trend_up"] = 0.0
        metrics["bar_trend_down"] = 1.0
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertFalse(result["eligible"])
        self.assertIn("m1_pattern_conflicts_with_candidate", result["hard_blocks"])

    def test_missing_queue_measurement_is_explicit_not_fabricated(self) -> None:
        metrics = strong_buy_metrics()
        metrics["queue_imbalance_available"] = 0.0
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertIsNone(result["queue_imbalance"])
        self.assertIn("best_queue_imbalance_unavailable_or_stale", result["reasons"])


if __name__ == "__main__":
    unittest.main()
