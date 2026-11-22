"""Architecture checks for the modular forward Shadow outcome tracker."""
from __future__ import annotations

import unittest

from strategy import shadow_outcomes as facade
from strategy import shadow_outcomes_candidates as candidates
from strategy import shadow_outcomes_observation as observation
from strategy import shadow_outcomes_primitives as primitives
from strategy import shadow_outcomes_state as state
from strategy import shadow_outcomes_statistics as statistics


class ShadowOutcomeModuleTests(unittest.TestCase):
    def test_legacy_helpers_and_logger_remain_available_from_facade(self):
        self.assertIs(facade._number, primitives._number)
        self.assertIs(facade._cost_settings, primitives._cost_settings)
        self.assertIs(facade._apply_adverse_slippage, primitives._apply_adverse_slippage)
        self.assertIs(facade.logger, primitives.logger)
        self.assertIs(facade._MOSCOW_TZ, primitives._MOSCOW_TZ)

    def test_tracker_methods_are_owned_by_focused_mixins(self):
        tracker = facade.ShadowForwardTracker
        self.assertEqual(tracker.__init__.__module__, facade.__name__)
        self.assertEqual(tracker.clear.__module__, facade.__name__)
        self.assertEqual(tracker._fresh_bbo.__module__, state.__name__)
        self.assertEqual(tracker._new_stats_bucket.__module__, state.__name__)
        self.assertEqual(tracker.record_candidate.__module__, candidates.__name__)
        self.assertEqual(tracker.attach_llm_verdict.__module__, candidates.__name__)
        self.assertEqual(tracker.observe_market_tick.__module__, observation.__name__)
        self.assertEqual(tracker.restore_completed_labels.__module__, statistics.__name__)
        self.assertEqual(tracker.statistics_snapshot.__module__, statistics.__name__)

    def test_time_module_is_shared_for_historical_monotonic_patches(self):
        self.assertIs(facade.time, candidates.time)
        self.assertIs(facade.time, observation.time)

    def test_tracker_keeps_its_original_public_class_name(self):
        tracker = facade.ShadowForwardTracker(horizons_seconds=(60,))
        self.assertEqual(tracker.horizons_seconds, (60,))
        self.assertEqual(tracker.pending_count(), 0)


if __name__ == "__main__":
    unittest.main()
