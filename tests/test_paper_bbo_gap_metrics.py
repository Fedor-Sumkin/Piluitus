"""Regression tests for PAPER executable-BBO gap telemetry."""
from __future__ import annotations

import unittest

from execution_layer import BaseExecutionEngine


class PaperBboGapMetricsTests(unittest.TestCase):
    def test_close_log_exposes_bbo_gap_and_execution_model(self) -> None:
        from pathlib import Path

        source = (
            Path(__file__).resolve().parents[1] / "execution" / "execution_layer.py"
        ).read_text(encoding="utf-8")
        self.assertIn("fill_model=%s depth_unit=%s; BBO_gap=%d episodes", source)
        self.assertIn("trigger_to_fresh_BBO=%.3fs", source)
        self.assertIn("FULL_FILL_AT_EXECUTABLE_BBO_DEPTH_UNVERIFIED", source)

    def test_exit_status_log_throttle_is_per_order_and_event_type(self) -> None:
        engine = BaseExecutionEngine.__new__(BaseExecutionEngine)
        engine.paper_exit_status_log_interval_sec = 300.0
        engine._last_exit_status_log_at = {}

        self.assertTrue(engine._should_log_paper_exit_status("PAPER_A", "wait", 100.0))
        self.assertFalse(engine._should_log_paper_exit_status("PAPER_A", "wait", 399.9))
        self.assertTrue(engine._should_log_paper_exit_status("PAPER_A", "wait", 400.0))
        self.assertTrue(engine._should_log_paper_exit_status("PAPER_A", "recovery", 101.0))
        self.assertTrue(engine._should_log_paper_exit_status("PAPER_B", "wait", 101.0))

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
