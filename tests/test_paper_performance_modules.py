"""Architecture checks for the modular realized PAPER performance tracker."""
from __future__ import annotations

import unittest

from strategy import paper_performance as facade
from strategy import paper_performance_diagnostics as diagnostics
from strategy import paper_performance_metrics as metrics
from strategy import paper_performance_primitives as primitives
from strategy import paper_performance_records as records
from strategy import paper_performance_scorecard as scorecard
from strategy import paper_performance_shadow as shadow


class PaperPerformanceModuleTests(unittest.TestCase):
    def test_legacy_constants_and_helper_exports_remain_available(self):
        for name in (
            "PAPER_CLOSED_TRADES_STORAGE_KEY",
            "PAPER_CLOSED_TRADES_STORAGE_LIMIT",
            "_number",
            "_text",
            "_confidence_bucket",
            "_exit_timestamp_sort_key",
            "_aware_timestamp_epoch",
            "_reentry_gap_bucket",
            "_holding_time_bucket",
            "_numeric_bucket",
            "_moscow_exit_date",
        ):
            with self.subTest(name=name):
                self.assertIs(
                    getattr(facade, name),
                    getattr(primitives, name),
                )

    def test_legacy_metric_exports_remain_available(self):
        for name in (
            "_shadow_segment_metrics",
            "_wilson_interval_pct",
            "_metrics",
        ):
            with self.subTest(name=name):
                self.assertIs(getattr(facade, name), getattr(metrics, name))
        for name in (
            "_reentry_diagnostics",
            "_exit_diagnostics",
            "_entry_diagnostics",
            "_loss_diagnostics",
            "_session_stability",
        ):
            with self.subTest(name=name):
                self.assertIs(getattr(facade, name), getattr(diagnostics, name))
        self.assertIsNotNone(facade.deduplicate_shadow_labels)


    def test_tracker_methods_are_owned_by_focused_mixins(self):
        tracker = facade.PaperPerformanceTracker
        self.assertEqual(tracker._valid_trade.__module__, records.__name__)
        self.assertEqual(tracker.record.__module__, records.__name__)
        self.assertEqual(tracker.restore.__module__, records.__name__)
        self.assertEqual(tracker._group.__module__, scorecard.__name__)
        self.assertEqual(tracker.scorecard.__module__, scorecard.__name__)
        self.assertEqual(tracker._compare_shadow.__module__, shadow.__name__)

    def test_application_singleton_keeps_the_legacy_tracker_type(self):
        self.assertIsInstance(
            facade.paper_performance_tracker,
            facade.PaperPerformanceTracker,
        )


if __name__ == "__main__":
    unittest.main()
