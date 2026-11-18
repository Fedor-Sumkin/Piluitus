from __future__ import annotations

import unittest
from unittest.mock import AsyncMock

from execution_layer import BaseExecutionEngine


class PaperRestartQuarantineTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.engine = object.__new__(BaseExecutionEngine)
        self.engine.paper_mode = True
        self.engine._unreconciled_paper_positions = {}

    async def test_full_journal_read_failure_keeps_entry_lock_and_never_falls_back(self) -> None:
        db = type("DB", (), {})()
        db.get_all_daily_trades = AsyncMock(side_effect=RuntimeError("storage unavailable"))
        db.get_active_positions = AsyncMock(return_value={})

        with self.assertRaises(RuntimeError):
            await self.engine.quarantine_unreconciled_paper_positions(db)

        db.get_active_positions.assert_not_awaited()
        self.assertTrue(self.engine._unreconciled_paper_positions)
        self.assertIn("__PAPER_JOURNAL_UNAVAILABLE__", self.engine._unreconciled_paper_positions)

    async def test_missing_full_journal_api_fails_closed(self) -> None:
        db = type("DB", (), {"get_active_positions": AsyncMock(return_value={})})()

        with self.assertRaises(RuntimeError):
            await self.engine.quarantine_unreconciled_paper_positions(db)

        db.get_active_positions.assert_not_awaited()
        self.assertTrue(self.engine._unreconciled_paper_positions)

    async def test_non_mapping_full_journal_fails_closed(self) -> None:
        db = type("DB", (), {
            "get_all_daily_trades": AsyncMock(return_value=[]),
            "get_active_positions": AsyncMock(return_value={}),
        })()

        with self.assertRaises(RuntimeError):
            await self.engine.quarantine_unreconciled_paper_positions(db)

        db.get_active_positions.assert_not_awaited()
        self.assertTrue(self.engine._unreconciled_paper_positions)

    async def test_failed_quarantine_write_keeps_discovered_position_locked_in_memory(self) -> None:
        db = type("DB", (), {
            "get_all_daily_trades": AsyncMock(return_value={
                "PAPER_OLD": {
                    "status": "OPEN",
                    "mode": "PAPER",
                    "ticker": "SBER",
                    "side": "BUY",
                    "entry_price": 100.0,
                    "stop_loss": 99.0,
                    "take_profit": 102.0,
                }
            }),
            "set_json": AsyncMock(side_effect=RuntimeError("write failed")),
        })()

        with self.assertRaises(RuntimeError):
            await self.engine.quarantine_unreconciled_paper_positions(db)

        self.assertIn("PAPER_OLD", self.engine._unreconciled_paper_positions)
        self.assertEqual(
            self.engine._unreconciled_paper_positions["PAPER_OLD"]["reconciliation_status"],
            "REVIEW_REQUIRED",
        )

    async def test_existing_stale_review_is_preserved_across_restart(self) -> None:
        stale = {
            "status": "STALE_REVIEW",
            "mode": "PAPER",
            "reconciliation_status": "REVIEW_REQUIRED",
            "ticker": "SBER",
            "side": "BUY",
            "entry_price": 100.0,
            "stop_loss": 99.0,
            "take_profit": 102.0,
        }
        db = type("DB", (), {
            "get_all_daily_trades": AsyncMock(return_value={"PAPER_OLD": stale}),
        })()

        quarantined = await self.engine.quarantine_unreconciled_paper_positions(db)

        self.assertEqual(quarantined, 0)
        self.assertEqual(self.engine._unreconciled_paper_positions["PAPER_OLD"], stale)


if __name__ == "__main__":
    unittest.main()
