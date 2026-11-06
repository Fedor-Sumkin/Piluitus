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

    def test_lot_multiplier_is_available_before_execution_diagnostics(self) -> None:
        self.assertGreater(BaseExecutionEngine._lot_multiplier_for("IRKT"), 0)
        self.assertEqual(BaseExecutionEngine._lot_multiplier_for("__UNKNOWN__"), 1)

    def test_paper_commission_is_charged_on_entry_and_exit_for_long(self) -> None:
        gross, exit_fee, total_fee, net = BaseExecutionEngine._calculate_paper_pnl(
            100.0, 101.0, 10, "BUY", 0.1, entry_commission=1.0
        )
        self.assertAlmostEqual(gross, 10.0)
        self.assertAlmostEqual(exit_fee, 1.01)
        self.assertAlmostEqual(total_fee, 2.01)
        self.assertAlmostEqual(net, 7.99)

    def test_paper_commission_is_charged_on_entry_and_exit_for_short(self) -> None:
        gross, exit_fee, total_fee, net = BaseExecutionEngine._calculate_paper_pnl(
            100.0, 99.0, 10, "SELL", 0.1, entry_commission=1.0
        )
        self.assertAlmostEqual(gross, 10.0)
        self.assertAlmostEqual(exit_fee, 0.99)
        self.assertAlmostEqual(total_fee, 1.99)
        self.assertAlmostEqual(net, 8.01)

    def test_paper_commission_rejects_invalid_inputs(self) -> None:
        with self.assertRaises(ValueError):
            BaseExecutionEngine._calculate_paper_pnl(100.0, 99.0, 10, "BUY", -0.1)

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
