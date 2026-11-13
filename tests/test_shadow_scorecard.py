"""Regression tests for the bounded Strategy Shadow scorecard."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from shadow_outcomes import ShadowForwardTracker


class ShadowScorecardTests(unittest.TestCase):
    def test_segments_compare_side_ticker_regime_score_band_and_moscow_hour(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        base = {
            "bid": 100.0, "ask": 100.1,
            "bid_quote_age_sec": 0.1, "ask_quote_age_sec": 0.1,
            "bbo_max_age_sec": 2.0, "bid_ask_available": 1,
            "spread_available": 1, "spread_pct": 0.1,
        }
        with patch("shadow_outcomes.time.monotonic", side_effect=(100.0, 160.0, 200.0, 260.0)):
            tracker.record_candidate(
                "SBER", "BUY", base,
                {"eligible": True, "score": 92.0, "regime": "TREND_ALIGNED"},
            )
            tracker.observe_market_tick("SBER", {**base, "bid": 100.3, "ask": 100.4})
            tracker.record_candidate(
                "GAZP", "SELL", base,
                {
                    "eligible": False, "score": 64.0,
                    "regime": "TRANSITION_OR_RANGE",
                    "hard_blocks": ["atr_not_ready"],
                },
            )
            tracker.observe_market_tick("GAZP", {**base, "bid": 99.8, "ask": 99.9})

        snapshot = tracker.statistics_snapshot()
        segments = snapshot["segments"]
        self.assertEqual(segments["side=BUY"]["ok"], 1)
        self.assertEqual(segments["side=SELL"]["ok"], 1)
        self.assertEqual(segments["ticker=SBER"]["ok"], 1)
        self.assertEqual(segments["regime=TREND_ALIGNED"]["ok"], 1)
        self.assertEqual(segments["score_band=90-100"]["ok"], 1)
        self.assertEqual(segments["score_band=60-69"]["ok"], 1)
        hour_keys = [key for key in segments if key.startswith("hour_msk=")]
        self.assertEqual(sum(int(segments[key]["ok"]) for key in hour_keys), 2)
        self.assertEqual(segments["side_eligibility=BUY|eligible"]["ok"], 1)
        self.assertEqual(segments["ticker_eligibility=GAZP|ineligible"]["ok"], 1)
        self.assertEqual(segments["side_score_band=BUY|90-100"]["ok"], 1)
        self.assertEqual(segments["side_score_band=SELL|60-69"]["ok"], 1)
        self.assertEqual(segments["side_m1_pattern=BUY|NONE"]["ok"], 1)
        self.assertEqual(segments["side_m1_pattern=SELL|NONE"]["ok"], 1)
        self.assertEqual(segments["hard_block=atr_not_ready"]["ok"], 1)

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
