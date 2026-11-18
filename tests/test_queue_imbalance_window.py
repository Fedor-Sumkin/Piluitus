"""Tests for the time-weighted best-queue imbalance window."""
import unittest

from strategy.screener_funnel import _queue_imbalance_window


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


if __name__ == "__main__":
    unittest.main()
