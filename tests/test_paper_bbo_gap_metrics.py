"""Regression tests for PAPER executable-BBO gap telemetry."""
from __future__ import annotations

import unittest

from execution_layer import BaseExecutionEngine


class PaperBboGapMetricsTests(unittest.TestCase):
    def test_gap_episode_tracks_count_last_max_and_total_duration(self) -> None:
        trade: dict[str, object] = {}

        self.assertIsNone(BaseExecutionEngine._track_exit_bbo_gap(trade, 100.0, fresh=False))
        self.assertIsNone(BaseExecutionEngine._track_exit_bbo_gap(trade, 102.5, fresh=False))
        recovered = BaseExecutionEngine._track_exit_bbo_gap(trade, 106.0, fresh=True)

        self.assertEqual(recovered, 6.0)
        self.assertEqual(trade["exit_bbo_gap_count"], 1)
        self.assertEqual(trade["last_exit_bbo_gap_sec"], 6.0)
        self.assertEqual(trade["max_exit_bbo_gap_sec"], 6.0)
        self.assertEqual(trade["total_exit_bbo_gap_sec"], 6.0)
        self.assertEqual(trade["current_exit_bbo_gap_sec"], 0.0)
        self.assertNotIn("exit_bbo_gap_started_monotonic", trade)

        BaseExecutionEngine._track_exit_bbo_gap(trade, 110.0, fresh=False)
        BaseExecutionEngine._track_exit_bbo_gap(trade, 112.0, fresh=True)

        self.assertEqual(trade["exit_bbo_gap_count"], 2)
        self.assertEqual(trade["last_exit_bbo_gap_sec"], 2.0)
        self.assertEqual(trade["max_exit_bbo_gap_sec"], 6.0)
        self.assertEqual(trade["total_exit_bbo_gap_sec"], 8.0)


if __name__ == "__main__":
    unittest.main()
