"""Regression tests for side-specific stop cooldowns and shadow break-even math."""
from __future__ import annotations

import unittest
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from execution_layer import BaseExecutionEngine


class ExecutionReentryPolicyTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.engine = object.__new__(BaseExecutionEngine)
        self.engine.stop_loss_cooldown_sec = 180.0
        self.engine.repeat_stop_window_sec = 1800.0
        self.engine.repeat_stop_limit = 2
        self.engine.repeat_stop_cooldown_sec = 900.0
        self.engine._recent_stop_losses = {}
        self.engine._ticker_cooldowns = {}
        self.engine.paper_mode = True
        self.engine._hydrated_stop_loss_keys = set()

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


    async def test_recent_same_side_stops_are_restored_after_restart(self) -> None:
        now_wall = datetime.now(timezone(timedelta(hours=3)))
        rows = {
            "old": {
                "status": "CLOSED", "mode": "PAPER", "ticker": "ALRS", "side": "SELL",
                "exit_reason": "STOP_LOSS",
                "exit_timestamp": (now_wall - timedelta(seconds=600)).isoformat(),
            },
            "latest": {
                "status": "CLOSED", "mode": "PAPER", "ticker": "ALRS", "side": "SELL",
                "exit_reason": "STOP_LOSS",
                "exit_timestamp": (now_wall - timedelta(seconds=120)).isoformat(),
            },
            "other-side": {
                "status": "CLOSED", "mode": "PAPER", "ticker": "ALRS", "side": "BUY",
                "exit_reason": "STOP_LOSS",
                "exit_timestamp": (now_wall - timedelta(seconds=30)).isoformat(),
            },
        }
        db = SimpleNamespace(get_all_daily_trades=AsyncMock(return_value=rows))
        mono_now = 5000.0
        count, remaining = await self.engine._hydrate_stop_loss_cooldown(
            db, "ALRS", "SELL", now_monotonic=mono_now
        )

        self.assertEqual(count, 2)
        self.assertAlmostEqual(remaining, 780.0, places=2)
        self.assertAlmostEqual(self.engine._ticker_cooldowns[("ALRS", "SELL")], mono_now + 780.0, places=2)
        self.assertEqual(len(self.engine._recent_stop_losses[("ALRS", "SELL")]), 2)
        self.assertNotIn(("ALRS", "BUY"), self.engine._ticker_cooldowns)

        await self.engine._hydrate_stop_loss_cooldown(
            db, "ALRS", "SELL", now_monotonic=mono_now + 1.0
        )
        db.get_all_daily_trades.assert_awaited_once()

    async def test_previous_moscow_date_stop_does_not_carry_into_new_session(self) -> None:
        now_wall = datetime(2026, 11, 10, 0, 2, tzinfo=timezone(timedelta(hours=3)))
        previous_session = datetime(2026, 11, 9, 23, 59, tzinfo=timezone(timedelta(hours=3)))
        db = SimpleNamespace(get_all_daily_trades=AsyncMock(return_value={
            "previous-day": {
                "status": "CLOSED", "mode": "PAPER", "ticker": "ALRS", "side": "BUY",
                "exit_reason": "STOP_LOSS", "exit_timestamp": previous_session.isoformat(),
            },
        }))
        # Pin wall-clock conversion while preserving the injected monotonic clock.
        self.engine._hydrated_stop_loss_keys.clear()
        count, remaining = await self.engine._hydrate_stop_loss_cooldown(
            db, "ALRS", "BUY", now_monotonic=6000.0, now_wall=now_wall
        )
        self.assertEqual(count, 0)
        self.assertEqual(remaining, 0.0)

    async def test_hydration_does_not_erase_stop_registered_before_journal_write(self) -> None:
        self.engine._register_stop_loss_cooldown("ALRS", "BUY", now=1000.0)
        db = SimpleNamespace(get_all_daily_trades=AsyncMock(return_value={}))

        count, remaining = await self.engine._hydrate_stop_loss_cooldown(
            db, "ALRS", "BUY", now_monotonic=1000.0
        )

        self.assertEqual(count, 1)
        self.assertEqual(remaining, 180.0)
        self.assertEqual(self.engine._ticker_cooldowns[("ALRS", "BUY")], 1180.0)

    async def test_invalid_stop_timestamp_fails_closed(self) -> None:
        db = SimpleNamespace(get_all_daily_trades=AsyncMock(return_value={
            "broken": {
                "status": "CLOSED", "mode": "PAPER", "ticker": "ALRS", "side": "BUY",
                "exit_reason": "STOP_LOSS", "exit_timestamp": "not-a-timestamp",
            },
        }))
        with self.assertRaises(RuntimeError):
            await self.engine._hydrate_stop_loss_cooldown(db, "ALRS", "BUY", now_monotonic=10.0)

    def test_lot_multiplier_requires_confirmed_account_metadata(self) -> None:
        from config import FINAM_TRADE_LOT_SIZES

        # Static MOEX reference data must not authorize a new order.
        self.assertEqual(BaseExecutionEngine._lot_multiplier_for("IRKT"), 0)
        self.assertEqual(BaseExecutionEngine._lot_multiplier_for("__UNKNOWN__"), 0)

        # A confirmed account-specific Finam value is available immediately.
        with patch.dict(FINAM_TRADE_LOT_SIZES, {"IRKT": 10}):
            self.assertEqual(BaseExecutionEngine._lot_multiplier_for("IRKT"), 10)

    def test_entry_bbo_rejects_spread_above_final_execution_limit(self) -> None:
        now = time.monotonic()
        quote = {
            "bid": 100.0,
            "ask": 101.0,
            "last_bid_confirmed_seen": now - 0.1,
            "last_ask_confirmed_seen": now - 0.1,
            "bid_levels": [],
            "ask_levels": [],
        }
        self.engine.screener = SimpleNamespace(
            quotes={"SBER": quote}, bbo_max_age_sec=2.0
        )
        with patch.dict("os.environ", {"PILUITUS_MAX_ENTRY_SPREAD_PCT": "0.50"}):
            self.assertIsNone(self.engine._latest_fresh_entry_quote("SBER", "BUY"))

    def test_entry_bbo_accepts_fresh_spread_within_final_execution_limit(self) -> None:
        now = time.monotonic()
        quote = {
            "bid": 100.0,
            "ask": 100.2,
            "last_bid_confirmed_seen": now - 0.1,
            "last_ask_confirmed_seen": now - 0.1,
            "bid_levels": [],
            "ask_levels": [],
        }
        self.engine.screener = SimpleNamespace(
            quotes={"SBER": quote}, bbo_max_age_sec=2.0
        )
        with patch.dict("os.environ", {"PILUITUS_MAX_ENTRY_SPREAD_PCT": "0.50"}):
            result = self.engine._latest_fresh_entry_quote("SBER", "BUY")
        self.assertIsNotNone(result)
        self.assertAlmostEqual(result["spread_pct"], (0.2 / 100.1) * 100.0)

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
