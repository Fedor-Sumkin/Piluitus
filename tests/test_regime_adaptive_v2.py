from __future__ import annotations

import unittest
from unittest.mock import patch

from strategy.regime_adaptive_v2 import RegimeAdaptiveV2, tri_state_bool


class RegimeAdaptiveV2Tests(unittest.TestCase):
    def setUp(self):
        self.evaluator = RegimeAdaptiveV2()
        self.baseline = {
            "score": 92.0,
            "threshold": 85.0,
            "eligible": True,
            "regime": "TREND_ALIGNED",
            "hard_blocks": [],
        }

    @staticmethod
    def metrics(**overrides):
        values = {
            "bar_atr": 1.0,
            "bar_atr_pct": 0.20,
            "bar_atr_periods": 5,
            "spread_pct": 0.01,
            "spread_atr_ratio": 0.10,
            "trade_count_60s": 40,
            "trade_history_coverage_sec": 60.0,
            "bar_pattern_confirmed": True,
            "_bar_pattern_signal_at_monotonic": 100.0,
        }
        values.update(overrides)
        return values

    def test_explicit_false_is_not_collapsed_into_unknown(self):
        self.assertIs(tri_state_bool({"flag": False}, "flag"), False)
        self.assertIs(tri_state_bool({"flag": 0}, "flag"), False)
        self.assertEqual(tri_state_bool({}, "flag"), "UNKNOWN")
        self.assertEqual(tri_state_bool({"flag": None}, "flag"), "UNKNOWN")

    def test_low_liquidity_penalty_is_exactly_twelve_and_shadow_only(self):
        metrics = self.metrics(spread_atr_ratio=0.60)
        with patch("strategy.regime_adaptive_v2.time.monotonic", return_value=100.0):
            result = self.evaluator.evaluate("BUY", metrics, self.baseline)
        self.assertEqual(result["penalties"], {"LOW_LIQUIDITY": 12.0})
        self.assertEqual(result["v2_score"], 80.0)
        self.assertFalse(result["eligible"])
        self.assertEqual(result["mode"], "SHADOW_ONLY")

    def test_high_volatility_penalty_uses_prior_ticker_history(self):
        with patch("strategy.regime_adaptive_v2.time.monotonic", return_value=100.0):
            for _ in range(20):
                self.evaluator.evaluate(
                    "BUY",
                    self.metrics(ticker="SBER", bar_atr_pct=0.20),
                    {**self.baseline, "regime": "M1_PATTERN_TRANSITION"},
                )
            result = self.evaluator.evaluate(
                "BUY",
                self.metrics(ticker="SBER", bar_atr_pct=0.40),
                {**self.baseline, "regime": "M1_PATTERN_TRANSITION"},
            )
        self.assertEqual(result["volatility_regime"], "HIGH_VOLATILITY")
        self.assertEqual(result["penalties"], {"HIGH_VOLATILITY": 8.0})
        self.assertEqual(result["v2_score"], 84.0)

    def test_trend_without_fresh_m1_penalty_is_eight(self):
        with patch("strategy.regime_adaptive_v2.time.monotonic", return_value=100.0):
            result = self.evaluator.evaluate(
                "BUY", self.metrics(bar_pattern_confirmed=False), self.baseline
            )
        self.assertEqual(result["penalties"], {"TREND_WITHOUT_FRESH_M1": 8.0})
        self.assertEqual(result["v2_score"], 84.0)

    def test_unknown_m1_evidence_is_not_treated_as_false(self):
        metrics = self.metrics()
        metrics.pop("bar_pattern_confirmed")
        with patch("strategy.regime_adaptive_v2.time.monotonic", return_value=100.0):
            result = self.evaluator.evaluate("BUY", metrics, self.baseline)
        self.assertEqual(result["m1_freshness"], "UNKNOWN")
        self.assertNotIn("TREND_WITHOUT_FRESH_M1", result["penalties"])

    def test_v1_hard_blocks_are_preserved_without_relaxation(self):
        blocks = ["bbo_stale", "atr_not_ready"]
        result = self.evaluator.evaluate(
            "BUY", self.metrics(), {**self.baseline, "hard_blocks": blocks}
        )
        self.assertEqual(result["hard_blocks"], blocks)
        self.assertFalse(result["eligible"])

    def test_v2_does_not_lower_the_v1_threshold(self):
        with patch("strategy.regime_adaptive_v2.time.monotonic", return_value=100.0):
            result = self.evaluator.evaluate(
                "BUY", self.metrics(), {**self.baseline, "score": 88.0, "threshold": 90.0}
            )
        self.assertEqual(result["threshold"], 90.0)
        self.assertFalse(result["eligible"])


if __name__ == "__main__":
    unittest.main()
