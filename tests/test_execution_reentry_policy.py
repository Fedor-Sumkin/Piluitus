"""Regression tests for side-specific stop cooldowns and shadow break-even math."""
from __future__ import annotations

import unittest

from execution_layer import BaseExecutionEngine


class ExecutionReentryPolicyTests(unittest.TestCase):
    def setUp(self) -> None:
        self.engine = object.__new__(BaseExecutionEngine)
        self.engine.stop_loss_cooldown_sec = 180.0
        self.engine.repeat_stop_window_sec = 1800.0
        self.engine.repeat_stop_limit = 2
        self.engine.repeat_stop_cooldown_sec = 900.0
        self.engine._recent_stop_losses = {}
        self.engine._ticker_cooldowns = {}
        self.engine.paper_mode = True

    def test_first_stop_sets_short_cooldown_for_same_side(self) -> None:
        cooldown, count = self.engine._register_stop_loss_cooldown("ALRS", "SELL", now=1000.0)
        self.assertEqual(cooldown, 180.0)
        self.assertEqual(count, 1)
        self.assertIn(("ALRS", "SELL"), self.engine._ticker_cooldowns)
        self.assertNotIn(("ALRS", "BUY"), self.engine._ticker_cooldowns)

    def test_second_same_side_stop_extends_pause_to_fifteen_minutes(self) -> None:
        self.engine._register_stop_loss_cooldown("ALRS", "SELL", now=1000.0)
        cooldown, count = self.engine._register_stop_loss_cooldown("ALRS", "SELL", now=1100.0)
        self.assertEqual(cooldown, 900.0)
        self.assertEqual(count, 2)
        self.assertNotIn(("ALRS", "BUY"), self.engine._ticker_cooldowns)

    def test_stop_outside_window_does_not_count_as_repeat(self) -> None:
        self.engine._register_stop_loss_cooldown("ALRS", "SELL", now=1000.0)
        cooldown, count = self.engine._register_stop_loss_cooldown("ALRS", "SELL", now=2801.0)
        self.assertEqual(cooldown, 180.0)
        self.assertEqual(count, 1)

    def test_shadow_break_even_threshold_has_correct_side_and_costs(self) -> None:
        long_free = BaseExecutionEngine._calculate_shadow_be_raw_trigger_price(
            100.0, "BUY", 10, 0.0, 0.0, 0.0
        )
        short_free = BaseExecutionEngine._calculate_shadow_be_raw_trigger_price(
            100.0, "SELL", 10, 0.0, 0.0, 0.0
        )
        long_cost = BaseExecutionEngine._calculate_shadow_be_raw_trigger_price(
            100.0, "BUY", 10, 1.0, 0.1, 10.0
        )
        short_cost = BaseExecutionEngine._calculate_shadow_be_raw_trigger_price(
            100.0, "SELL", 10, 1.0, 0.1, 10.0
        )
        self.assertAlmostEqual(long_free, 100.0)
        self.assertAlmostEqual(short_free, 100.0)
        self.assertGreater(long_cost, 100.0)
        self.assertLess(short_cost, 100.0)


if __name__ == "__main__":
    unittest.main()
