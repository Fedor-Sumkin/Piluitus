"""Tests for the time-weighted best-queue imbalance window."""
import asyncio
import unittest
from unittest.mock import patch

from strategy.screener_funnel import HFTMemoryScreener, _queue_imbalance_window


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

    def test_order_book_updates_publish_rolling_metric_and_invalid_book_clears_it(self):
        screener = HFTMemoryScreener()

        # Keep provider event time and the mocked monotonic receive clock in
        # the same domain. Backdating events while processing them "now"
        # correctly makes their size observations stale and tests a different case.
        for event_time in (100.0, 101.0, 102.0):
            with patch("strategy.screener_funnel.time.monotonic", return_value=event_time):
                result = asyncio.run(screener.push_order_book(
                    "SBER", 100.0, 100.1, 120.0, 80.0,
                    order_book_timestamp=event_time,
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
                order_book_timestamp=103.0,
            ))
        with patch("strategy.screener_funnel.time.monotonic", return_value=103.0):
            invalidated = asyncio.run(screener.push_tick("SBER", 100.05, 0.0))
        self.assertEqual(invalidated["metrics"]["queue_imbalance_5s_available"], 0.0)


if __name__ == "__main__":
    unittest.main()
