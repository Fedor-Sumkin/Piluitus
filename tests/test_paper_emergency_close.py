"""Emergency PAPER close must latch maintenance before iterating positions."""
from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock

from execution_layer import BaseExecutionEngine


def make_engine() -> BaseExecutionEngine:
    engine = object.__new__(BaseExecutionEngine)
    engine.paper_mode = True
    engine._portfolio_lock = asyncio.Lock()
    engine._maintenance_active = False
    engine._maintenance_reason = ""
    engine._lock_closing = set()
    engine.paper_positions = {
        "SBER": {
            "id": "PAPER_EMERGENCY_1",
            "ticker": "SBER",
            "side": "BUY",
            "status": "OPEN",
            "exit_partial_fill_count": 0,
        }
    }
    engine._latest_fresh_exit_quote = lambda ticker, side: {
        "executable_side": "Bid",
        "executable_price": 99.9,
        "age_sec": 0.1,
    }
    engine._latest_price = lambda ticker, position: 100.0
    return engine


class PaperEmergencyCloseTests(unittest.IsolatedAsyncioTestCase):
    async def test_emergency_close_blocks_new_entries_until_close_attempt_finishes(self) -> None:
        engine = make_engine()
        database = object()
        reservation_during_close: dict[str, object] = {}

        async def finalize(ticker, position, db, bbo, **kwargs):
            self.assertTrue(engine._maintenance_active)
            approved, reason = await engine._reserve_order_size(
                "NEW", 1, 10.0, database, side="BUY", stop_loss=9.0
            )
            reservation_during_close["approved"] = approved
            reservation_during_close["reason"] = reason
            return {"status": "CLOSED"}

        engine._finalize_paper_close = AsyncMock(side_effect=finalize)

        result = await engine.close_all_positions(database)

        self.assertEqual(result["closed"], 1)
        self.assertEqual(result["blocked"], 0)
        self.assertEqual(reservation_during_close["approved"], 0)
        self.assertIn("Экстренное закрытие", str(reservation_during_close["reason"]))
        self.assertFalse(engine._maintenance_active)
        self.assertEqual(engine._maintenance_reason, "")
        self.assertEqual(engine._lock_closing, set())
        engine._finalize_paper_close.assert_awaited_once()

    async def test_existing_maintenance_is_not_overridden_by_emergency_close(self) -> None:
        engine = make_engine()
        engine._maintenance_active = True
        engine._maintenance_reason = "Проверка PAPER-журнала"
        engine._finalize_paper_close = AsyncMock()

        result = await engine.close_all_positions(object())

        self.assertEqual(result["closed"], 0)
        self.assertEqual(result["blocked"], 1)
        self.assertIn("Проверка PAPER-журнала", result["message"])
        self.assertTrue(engine._maintenance_active)
        self.assertEqual(engine._maintenance_reason, "Проверка PAPER-журнала")
        engine._finalize_paper_close.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
