from __future__ import annotations

import unittest
from unittest.mock import patch

from strategy.shadow_outcomes import ShadowForwardTracker


class ShadowV2ScorecardTests(unittest.TestCase):
    def test_v1_and_v2_are_attributed_to_separate_scorecards(self):
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        metrics = {
            "bid": 100.0, "ask": 100.1,
            "bid_quote_age_sec": 0.1, "ask_quote_age_sec": 0.1,
            "bbo_max_age_sec": 2.0, "bid_ask_available": 1,
            "spread_available": 1, "spread_pct": 0.1,
        }
        result = {
            "score": 92.0,
            "eligible": True,
            "regime": "TREND_ALIGNED",
            "hard_blocks": [],
            "regime_adaptive_v2": {
                "v2_score": 80.0,
                "eligible": False,
                "threshold": 85.0,
                "hard_blocks": [],
                "penalties": {"LOW_LIQUIDITY": 12.0},
                "penalty_reasons": ["LOW_LIQUIDITY:-12"],
                "liquidity_regime": "LOW_LIQUIDITY",
                "volatility_regime": "VOLATILITY_UNCLASSIFIED",
                "directional_regime": "DIRECTION_UNAVAILABLE",
                "m1_freshness": True,
            },
        }
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=(100.0, 160.0)):
            candidate_id = tracker.record_candidate("SBER", "BUY", metrics, result)
            self.assertIsNotNone(candidate_id)
            labels = tracker.observe_market_tick("SBER", {
                **metrics, "bid": 100.2, "ask": 100.3,
            })

        self.assertEqual(len(labels), 1)
        self.assertEqual(labels[0]["v2_score"], 80.0)
        self.assertIs(labels[0]["v2_eligible"], False)
        snapshot = tracker.statistics_snapshot()
        self.assertEqual(snapshot["eligible"]["labels"], 1)
        self.assertEqual(snapshot["strategy_v2"]["eligible"]["labels"], 0)
        self.assertEqual(snapshot["strategy_v2"]["ineligible"]["labels"], 1)
        self.assertEqual(
            snapshot["strategy_v2"]["segments"]["v2_penalty=LOW_LIQUIDITY"]["labels"],
            1,
        )

    def test_missing_v2_attribution_is_not_counted_as_v2_ineligible(self):
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        metrics = {
            "bid": 100.0, "ask": 100.1,
            "bid_quote_age_sec": 0.1, "ask_quote_age_sec": 0.1,
            "bbo_max_age_sec": 2.0, "bid_ask_available": 1,
            "spread_available": 1, "spread_pct": 0.1,
        }
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=(100.0, 160.0)):
            tracker.record_candidate(
                "SBER", "BUY", metrics,
                {"score": 90.0, "eligible": True, "regime": "TREND_ALIGNED"},
            )
            tracker.observe_market_tick("SBER", {**metrics, "bid": 100.2, "ask": 100.3})
        snapshot = tracker.statistics_snapshot()
        self.assertEqual(snapshot["strategy_v2"]["eligible"]["labels"], 0)
        self.assertEqual(snapshot["strategy_v2"]["ineligible"]["labels"], 0)
        self.assertEqual(snapshot["strategy_v2"]["unknown_labels"], 1)


if __name__ == "__main__":
    unittest.main()
