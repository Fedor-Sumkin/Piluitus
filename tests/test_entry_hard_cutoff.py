from __future__ import annotations

import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from execution.execution_layer import ExecutionLayer


MSK = timezone(timedelta(hours=3))


class _ScheduleBroker:
    async def get_asset_schedule(self, ticker: str):
        return {
            "sessions": [{
                "type": "evening",
                "interval": {
                    "start_time": "2026-11-20T10:00:00+03:00",
                    "end_time": "2026-11-20T23:50:00+03:00",
                },
            }]
        }


def _fixed_datetime(value: datetime):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            return value.astimezone(tz) if tz else value.replace(tzinfo=None)
    return FixedDateTime


class EntryHardCutoffTests(unittest.IsolatedAsyncioTestCase):
    async def _status_at(self, clock: str):
        fixed = datetime.fromisoformat(f"2026-11-20T{clock}:00+03:00")
        layer = ExecutionLayer.__new__(ExecutionLayer)
        layer.broker = _ScheduleBroker()
        with patch("execution.execution_layer.datetime", _fixed_datetime(fixed)), patch.dict(
            os.environ,
            {
                "PILUITUS_ENTRY_HARD_CUTOFF_MSK": "22:00",
                "PILUITUS_ENTRY_CUTOFF_MINUTES": "5",
            },
        ):
            return await layer._market_session_status("SBER")

    async def test_entry_is_blocked_in_final_five_minutes_before_hard_cutoff(self):
        state = await self._status_at("21:54")
        self.assertTrue(state["entry_allowed"])
        state = await self._status_at("21:56")
        self.assertFalse(state["entry_allowed"])
        self.assertEqual(state["reason"], "entry_cutoff_reached")

    async def test_entry_is_blocked_at_and_after_2200_even_if_provider_schedule_is_open(self):
        for clock in ("22:00", "22:01", "23:00"):
            with self.subTest(clock=clock):
                state = await self._status_at(clock)
                self.assertTrue(state["is_open"])
                self.assertFalse(state["entry_allowed"])
                self.assertEqual(state["reason"], "hard_entry_cutoff_reached")

    def test_finam_fee_model_uses_percentage_per_side(self):
        gross, exit_fee, total_fee, net = ExecutionLayer._calculate_paper_pnl(
            entry_price=100.0,
            exit_price=101.0,
            share_qty=100,
            side="BUY",
            commission_pct_per_side=0.0354,
            entry_commission=100.0 * 100 * 0.0354 / 100.0,
        )
        self.assertAlmostEqual(gross, 100.0)
        self.assertAlmostEqual(exit_fee, 101.0 * 100 * 0.0354 / 100.0)
        self.assertAlmostEqual(total_fee, 71.154)
        self.assertAlmostEqual(net, 28.846)


if __name__ == "__main__":
    unittest.main()
