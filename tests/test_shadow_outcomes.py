from __future__ import annotations

import unittest
from unittest.mock import patch

from shadow_outcomes import ShadowForwardTracker


def quote_metrics(bid: float, ask: float) -> dict[str, float]:
    return {
        "bid": bid,
        "ask": ask,
        "bid_ask_available": 1.0,
        "spread_available": 1.0,
        "order_book_valid": 1.0,
        "bid_quote_age_sec": 0.1,
        "ask_quote_age_sec": 0.1,
        "bbo_max_age_sec": 2.0,
        "spread_pct": (ask - bid) / ((ask + bid) / 2.0) * 100.0,
    }


class ShadowForwardTrackerTests(unittest.TestCase):
    def test_buy_labels_use_ask_entry_and_bid_exit_after_one_minute(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,), max_label_lag_sec=30)
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[100.0, 160.0]):
            candidate_id = tracker.record_candidate(
                "SBER", "BUY", quote_metrics(100.0, 100.1),
                {"score": 90.0, "eligible": True, "regime": "TREND_ALIGNED"},
            )
            self.assertIsNotNone(candidate_id)
            outcomes = tracker.observe_market_tick("SBER", quote_metrics(101.0, 101.1))

        self.assertEqual(len(outcomes), 1)
        row = outcomes[0]
        self.assertEqual(row["result"], "OK")
        self.assertEqual(row["entry_executable_price"], 100.1)
        self.assertEqual(row["exit_executable_price"], 101.0)
        self.assertGreater(row["net_executable_return_pct"], 0.0)
        self.assertEqual(tracker.pending_count(), 0)

    def test_sell_labels_use_bid_entry_and_ask_exit(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[10.0, 70.0]):
            tracker.record_candidate(
                "SBER", "SELL", quote_metrics(100.0, 100.1),
                {"score": 88.0, "eligible": True, "regime": "TREND_ALIGNED"},
            )
            outcomes = tracker.observe_market_tick("SBER", quote_metrics(99.8, 99.9))
        self.assertEqual(outcomes[0]["entry_executable_price"], 100.0)
        self.assertEqual(outcomes[0]["exit_executable_price"], 99.9)
        self.assertGreater(outcomes[0]["net_executable_return_pct"], 0.0)

    def test_horizon_without_fresh_bbo_is_labelled_not_filled_at_stale_price(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,), max_label_lag_sec=30)
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[100.0, 191.0]):
            tracker.record_candidate(
                "SBER", "BUY", quote_metrics(100.0, 100.1),
                {"score": 80.0, "eligible": False, "regime": "TRANSITION_OR_RANGE"},
            )
            stale = quote_metrics(101.0, 101.1)
            stale["bid_quote_age_sec"] = 5.0
            outcomes = tracker.observe_market_tick("SBER", stale)
        self.assertEqual(outcomes[0]["result"], "NO_FRESH_BBO")
        self.assertNotIn("net_executable_return_pct", outcomes[0])

    def test_fresh_quote_after_allowed_label_lag_is_not_mislabelled_as_on_time(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,), max_label_lag_sec=30)
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[100.0, 191.0]):
            tracker.record_candidate(
                "SBER", "BUY", quote_metrics(100.0, 100.1),
                {"score": 90.0, "eligible": True, "regime": "TREND_ALIGNED"},
            )
            outcomes = tracker.observe_market_tick("SBER", quote_metrics(101.0, 101.1))
        self.assertEqual(outcomes[0]["result"], "NO_FRESH_BBO")
        self.assertNotIn("net_executable_return_pct", outcomes[0])

    def test_configured_costs_reduce_net_executable_return(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_COMMISSION_PCT": "0.10",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "1.0",
        }), patch("strategy.shadow_outcomes.time.monotonic", side_effect=[100.0, 160.0]):
            tracker.record_candidate(
                "SBER", "BUY", quote_metrics(100.0, 100.1),
                {"score": 90.0, "eligible": True, "regime": "TREND_ALIGNED"},
            )
            outcomes = tracker.observe_market_tick("SBER", quote_metrics(101.0, 101.1))
        self.assertLess(
            outcomes[0]["net_executable_return_pct"],
            outcomes[0]["gross_executable_return_pct"],
        )
        self.assertEqual(outcomes[0]["commission_pct_per_side"], 0.10)
        self.assertEqual(outcomes[0]["slippage_bps"], 1.0)

    def test_fresh_bbo_can_label_candidate_even_without_full_book_confirmation(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        metrics = quote_metrics(100.0, 100.1)
        metrics["order_book_valid"] = 0.0
        candidate_id = tracker.record_candidate(
            "SBER", "BUY", metrics,
            {"score": 0.0, "eligible": False, "regime": "UNKNOWN"},
        )
        self.assertIsNotNone(candidate_id)
        self.assertEqual(tracker.pending_count(), 1)

    def test_stale_bbo_is_not_recorded_as_a_candidate_entry(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        metrics = quote_metrics(100.0, 100.1)
        metrics["bid_quote_age_sec"] = 3.0
        candidate_id = tracker.record_candidate(
            "SBER", "BUY", metrics,
            {"score": 90.0, "eligible": True, "regime": "TREND_ALIGNED"},
        )
        self.assertIsNone(candidate_id)
        self.assertEqual(tracker.pending_count(), 0)

    def test_each_horizon_is_emitted_once_and_tracker_is_bounded(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60, 180), max_pending_per_ticker=1)
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[0.0, 1.0, 61.0, 181.0]):
            tracker.record_candidate("SBER", "BUY", quote_metrics(100.0, 100.1), {"score": 90.0})
            tracker.record_candidate("SBER", "BUY", quote_metrics(100.0, 100.1), {"score": 90.0})
            self.assertEqual(tracker.pending_count("SBER"), 1)
            first = tracker.observe_market_tick("SBER", quote_metrics(100.2, 100.3))
            second = tracker.observe_market_tick("SBER", quote_metrics(100.4, 100.5))
        self.assertEqual(len(first), 1)
        self.assertEqual(first[0]["horizon_sec"], 60)
        self.assertEqual(len(second), 1)
        self.assertEqual(second[0]["horizon_sec"], 180)
        self.assertEqual(tracker.pending_count(), 0)


if __name__ == "__main__":
    unittest.main()
