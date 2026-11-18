"""Tests for the time-weighted best-queue imbalance window."""
import asyncio
import unittest
from unittest.mock import patch

from strategy.screener_funnel import (
    HFTMemoryScreener,
    _best_quote_ofi_delta,
    _ofi_window,
    _queue_imbalance_window,
)


class QueueImbalanceWindowTests(unittest.TestCase):
    def test_average_is_time_weighted_not_message_count_weighted(self):
        result = _queue_imbalance_window(
            [(0.0, 0.5), (0.1, 0.5), (2.0, -0.5)],
            3.0,
            window_sec=5.0,
        )
        self.assertAlmostEqual(result["value"], 1.0 / 6.0, places=6)
        self.assertEqual(result["samples"], 3)
        self.assertEqual(result["available"], 1.0)
        self.assertAlmostEqual(result["positive_time_pct"], 200.0 / 3.0, places=4)

    def test_short_history_is_not_marked_available(self):
        result = _queue_imbalance_window([(9.0, 0.8)], 10.0)
        self.assertEqual(result["available"], 0.0)
        self.assertEqual(result["coverage_sec"], 1.0)
        self.assertEqual(result["samples"], 1)

    def test_stale_and_invalid_observations_are_ignored(self):
        result = _queue_imbalance_window(
            [(1.0, 0.4), (9.0, 2.0), (9.5, float("nan"))],
            10.0,
        )
        self.assertEqual(result["samples"], 0)
        self.assertEqual(result["available"], 0.0)

    def test_observations_outside_window_do_not_extend_coverage(self):
        result = _queue_imbalance_window(
            [(1.0, 0.9), (6.0, 0.2), (8.0, 0.2)],
            10.0,
            window_sec=5.0,
        )
        self.assertEqual(result["samples"], 2)
        self.assertEqual(result["available"], 0.0)
        self.assertAlmostEqual(result["coverage_sec"], 4.0)

    def test_best_quote_ofi_delta_captures_bid_addition_and_ask_cancellation(self):
        delta = _best_quote_ofi_delta(
            100.0, 100.0, 100.1, 80.0,
            100.0, 130.0, 100.1, 60.0,
        )
        self.assertEqual(delta, 50.0)

    def test_best_quote_ofi_delta_handles_price_improvement_and_worsening(self):
        bid_improvement = _best_quote_ofi_delta(
            100.0, 100.0, 100.2, 80.0,
            100.1, 70.0, 100.2, 80.0,
        )
        ask_improvement = _best_quote_ofi_delta(
            100.0, 100.0, 100.2, 80.0,
            100.0, 100.0, 100.1, 60.0,
        )
        self.assertEqual(bid_improvement, 70.0)
        self.assertEqual(ask_improvement, -60.0)

    def test_ofi_window_is_depth_normalized_and_requires_fresh_coverage(self):
        history = [
            (1.0, 20.0, 100.0),
            (1.5, -10.0, 100.0),
            (2.0, 30.0, 100.0),
        ]
        result = _ofi_window(history, 2.2, min_samples=3, min_coverage_sec=1.0)
        self.assertEqual(result["available"], 1.0)
        self.assertAlmostEqual(result["value"], 40.0 / 300.0)
        self.assertEqual(result["raw"], 40.0)
        stale = _ofi_window(history, 5.0, min_samples=3, min_coverage_sec=1.0)
        self.assertEqual(stale["available"], 0.0)
        self.assertEqual(stale["value"], 0.0)

    def test_ofi_window_rejects_duplicate_timestamps_for_coverage(self):
        result = _ofi_window(
            [(1.0, 10.0, 100.0), (1.0, 20.0, 100.0), (1.0, 30.0, 100.0)],
            1.1, min_samples=3, min_coverage_sec=0.5,
        )
        self.assertEqual(result["samples"], 1)
        self.assertEqual(result["available"], 0.0)

    def test_order_book_stream_publishes_ofi_only_after_ordered_depth_changes(self):
        screener = HFTMemoryScreener()
        snapshots = [
            (100.0, 100.0, 100.1, 100.0, 100.0),
            (100.5, 100.0, 100.1, 120.0, 100.0),
            (101.0, 100.0, 100.1, 120.0, 80.0),
            (101.5, 100.01, 100.1, 120.0, 80.0),
        ]
        for event_time, bid, ask, bid_size, ask_size in snapshots:
            with patch("strategy.screener_funnel.time.monotonic", return_value=event_time):
                result = asyncio.run(screener.push_order_book(
                    "SBER", bid, ask, bid_size, ask_size,
                ))
                self.assertTrue(result.get("valid"))

        with patch("strategy.screener_funnel.time.monotonic", return_value=101.5):
            tick = asyncio.run(screener.push_tick("SBER", 100.05, 0.0))
        metrics = tick["metrics"]
        self.assertEqual(metrics["ofi_5s_available"], 1.0)
        self.assertEqual(metrics["ofi_5s_samples"], 3)
        self.assertGreater(metrics["ofi_5s"], 0.0)

        with patch("strategy.screener_funnel.time.monotonic", return_value=102.0):
            asyncio.run(screener.push_order_book(
                "SBER", 100.01, 100.1, 120.0, 80.0,
                book_valid=False, book_reason="TEST_INVALID",
            ))
        self.assertEqual(len(screener.quotes["SBER"]["ofi_history"]), 0)

    def test_order_book_updates_publish_rolling_metric_and_invalid_book_clears_it(self):
        screener = HFTMemoryScreener()

        # With no exchange timestamp, ORDER_BOOK uses the captured local
        # monotonic receive clock. This keeps event ordering, freshness and the
        # rolling-window clock in one domain without fabricating Unix timestamps.
        for event_time in (100.0, 101.0, 102.0):
            with patch("strategy.screener_funnel.time.monotonic", return_value=event_time):
                result = asyncio.run(screener.push_order_book(
                    "SBER", 100.0, 100.1, 120.0, 80.0,
                ))
                self.assertTrue(result.get("valid"))

        with patch("strategy.screener_funnel.time.monotonic", return_value=102.0):
            result = asyncio.run(screener.push_tick("SBER", 100.05, 0.0))
        metrics = result["metrics"]
        self.assertEqual(metrics["queue_imbalance_5s_available"], 1.0)
        self.assertAlmostEqual(metrics["queue_imbalance_5s"], 0.2, places=6)
        self.assertGreaterEqual(metrics["queue_imbalance_5s_samples"], 3)

        with patch("strategy.screener_funnel.time.monotonic", return_value=103.0):
            asyncio.run(screener.push_order_book(
                "SBER", 100.0, 100.1, 120.0, 80.0,
                book_valid=False, book_reason="TEST_INVALID",
            ))
        with patch("strategy.screener_funnel.time.monotonic", return_value=103.0):
            invalidated = asyncio.run(screener.push_tick("SBER", 100.05, 0.0))
        self.assertEqual(invalidated["metrics"]["queue_imbalance_5s_available"], 0.0)


if __name__ == "__main__":
    unittest.main()
