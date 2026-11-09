import unittest
from unittest.mock import patch

from shadow_outcomes import ShadowForwardTracker


def fresh_metrics():
    return {
        "bid": 99.0,
        "ask": 101.0,
        "bid_quote_age_sec": 0.1,
        "ask_quote_age_sec": 0.1,
        "bid_ask_available": 1.0,
        "spread_available": 1.0,
        "bbo_max_age_sec": 2.0,
        "price_change_pct": 0.25,
        "price_vs_vwap_pct": 0.12,
        "bar_pattern_confirmed": 1.0,
        "bar_pattern_name": "M1_RANGE_BREAKOUT_BUY",
    }


class ShadowLabelQualityTests(unittest.TestCase):
    def test_valid_label_with_lag_over_ten_seconds_is_separated_as_delayed(self):
        tracker = ShadowForwardTracker(
            horizons_seconds=(1,),
            max_label_lag_sec=5.0,
            on_time_label_lag_sec=0.1,
        )
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[100.0, 101.2]):
            tracker.record_candidate(
                "TEST", "BUY", fresh_metrics(), {"score": 90, "eligible": True}
            )
            labels = tracker.observe_market_tick("TEST", fresh_metrics())

        self.assertEqual(len(labels), 1)
        self.assertEqual(labels[0]["result"], "OK")
        self.assertEqual(labels[0]["label_quality"], "DELAYED")
        self.assertEqual(labels[0]["price_change_pct"], 0.25)
        self.assertEqual(labels[0]["price_vs_vwap_pct"], 0.12)

    def test_missing_fresh_bbo_is_not_mixed_with_valid_delayed_labels(self):
        tracker = ShadowForwardTracker(
            horizons_seconds=(1,),
            max_label_lag_sec=1.0,
            on_time_label_lag_sec=0.1,
        )
        stale = fresh_metrics()
        stale["bid_quote_age_sec"] = 5.0
        stale["ask_quote_age_sec"] = 5.0
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[200.0, 202.1]):
            tracker.record_candidate(
                "TEST", "SELL", fresh_metrics(), {"score": 88, "eligible": True}
            )
            labels = tracker.observe_market_tick("TEST", stale)

        self.assertEqual(len(labels), 1)
        self.assertEqual(labels[0]["result"], "NO_FRESH_BBO")
        self.assertEqual(labels[0]["label_quality"], "NO_FRESH_BBO")


if __name__ == "__main__":
    unittest.main()
