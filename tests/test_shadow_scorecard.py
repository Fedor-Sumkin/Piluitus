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
            "queue_imbalance_available": 1.0,
            "queue_imbalance": 0.2,
            "queue_imbalance_5s_available": 1.0,
            "queue_imbalance_5s": 0.2,
            "queue_imbalance_5s_coverage_sec": 3.0,
            "queue_imbalance_5s_samples": 10,
            "queue_imbalance_5s_positive_time_pct": 80.0,
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
        self.assertEqual(segments["side_queue_alignment_5s=BUY|aligned"]["ok"], 1)
        self.assertEqual(segments["side_m1_pattern=SELL|NONE"]["ok"], 1)
        self.assertEqual(segments["hard_block=atr_not_ready"]["ok"], 1)

    def test_component_score_points_are_persisted_and_segmented(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        metrics = {
            "bid": 100.0, "ask": 100.1,
            "bid_quote_age_sec": 0.1, "ask_quote_age_sec": 0.1,
            "bbo_max_age_sec": 2.0, "bid_ask_available": 1,
            "spread_available": 1, "spread_pct": 0.1,
        }
        components = {
            "direction": 25.0, "m1_pattern": 20.0, "queue_imbalance": 12.0,
            "trade_flow_confirmation": 6.0, "volume": 0.0,
            "spread_vs_atr": 8.0, "freshness": 10.0, "unexpected_field": 999.0,
        }
        with patch("shadow_outcomes.time.monotonic", side_effect=(1000.0, 1061.0)):
            tracker.record_candidate(
                "TEST", "BUY", metrics,
                {
                    "eligible": True, "score": 81.0, "regime": "TREND_ALIGNED",
                    "components": components,
                },
            )
            labels = tracker.observe_market_tick("TEST", {
                **metrics, "bid": 100.3, "ask": 100.4,
            })

        self.assertEqual(len(labels), 1)
        self.assertEqual(labels[0]["component_points"], {
            "direction": 25.0, "m1_pattern": 20.0, "queue_imbalance": 12.0,
            "trade_flow_confirmation": 6.0, "volume": 0.0,
            "spread_vs_atr": 8.0, "freshness": 10.0,
        })
        segments = tracker.statistics_snapshot()["segments"]
        self.assertEqual(segments["component=direction|gt_20"]["ok"], 1)
        self.assertEqual(segments["component=queue_imbalance|gt_10_le_20"]["ok"], 1)
        self.assertEqual(segments["component=volume|zero"]["ok"], 1)

    def test_clear_resets_pending_labels_and_session_aggregates(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        metrics = {
            "bid": 100.0, "ask": 100.1,
            "bid_quote_age_sec": 0.1, "ask_quote_age_sec": 0.1,
            "bbo_max_age_sec": 2.0, "bid_ask_available": 1,
            "spread_available": 1, "spread_pct": 0.1,
        }
        with patch("shadow_outcomes.time.monotonic", return_value=1000.0):
            tracker.record_candidate(
                "TEST", "BUY", metrics,
                {"eligible": True, "score": 90.0, "regime": "TREND"},
            )
        self.assertEqual(tracker.pending_count(), 1)
        self.assertEqual(tracker.clear(), 1)
        snapshot = tracker.statistics_snapshot()
        self.assertEqual(tracker.pending_count(), 0)
        self.assertEqual(snapshot["eligible"]["labels"], 0)
        self.assertEqual(snapshot["ineligible"]["labels"], 0)
        self.assertEqual(snapshot["segments"], {})
        self.assertEqual(snapshot["by_horizon"], {})

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
