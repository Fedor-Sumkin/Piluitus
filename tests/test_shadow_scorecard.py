"""Regression tests for the bounded Strategy Shadow scorecard."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from shadow_outcomes import ShadowForwardTracker


class ShadowScorecardTests(unittest.TestCase):
    def test_completed_labels_are_aggregated_by_eligibility(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        metrics = {
            "bid": 100.0,
            "ask": 100.1,
            "bid_quote_age_sec": 0.1,
            "ask_quote_age_sec": 0.1,
            "bbo_max_age_sec": 2.0,
            "bid_ask_available": 1,
            "spread_available": 1,
            "spread_pct": 0.1,
        }
        with patch("shadow_outcomes.time.monotonic", side_effect=(1000.0, 1061.0)):
            candidate_id = tracker.record_candidate(
                "TEST", "BUY", metrics, {"eligible": True, "score": 90.0, "regime": "TREND"}
            )
            self.assertIsNotNone(candidate_id)
            completed = tracker.observe_market_tick("TEST", {
                **metrics,
                "bid": 100.3,
                "ask": 100.4,
                "bid_quote_age_sec": 0.1,
                "ask_quote_age_sec": 0.1,
            })

        self.assertEqual(len(completed), 1)
        self.assertEqual(completed[0]["result"], "OK")
        stats = tracker.statistics_snapshot()
        self.assertEqual(stats["eligible"]["labels"], 1)
        self.assertEqual(stats["eligible"]["ok"], 1)
        self.assertEqual(stats["eligible"]["positive"], 1)
        self.assertGreater(stats["eligible"]["average_net_return_pct"], 0.0)
        self.assertEqual(stats["ineligible"]["labels"], 0)


if __name__ == "__main__":
    unittest.main()
