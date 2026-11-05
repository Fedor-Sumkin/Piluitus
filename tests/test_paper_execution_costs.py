import os
import unittest
from unittest.mock import patch

from execution_layer import BaseExecutionEngine


class _FakeDB:
    def __init__(self):
        self.saved = {}
        self.history = []

    async def set_json(self, key, value):
        self.saved[key] = value

    async def push_to_list(self, key, item, max_len=100):
        self.history.insert(0, item)
        del self.history[max_len:]


class PaperExecutionCostTests(unittest.IsolatedAsyncioTestCase):
    async def test_exit_charges_entry_and_exit_commission_plus_adverse_slippage(self):
        env = {
            "PAPER_TRADING": "True",
            "PILUITUS_PAPER_COMMISSION_PCT": "0.1",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "10",
        }
        with patch.dict(os.environ, env, clear=False):
            engine = BaseExecutionEngine(initial_capital=20000.0)
        db = _FakeDB()
        trade = {
            "id": "PAPER_cost_test", "mode": "PAPER", "status": "OPEN",
            "ticker": "TEST", "side": "BUY", "entry_price": 100.0,
            "volume": 10, "lots": 1, "lot_size": 10,
            "notional_entry_rub": 1000.0, "entry_commission_rub": 1.0,
            "commission_pct_per_side": 0.1,
            "entry_timestamp": "2026-11-05T10:00:00+03:00",
        }
        bbo = {
            "executable_price": 100.0, "bid": 100.0, "ask": 100.01,
            "bid_size": 50.0, "ask_size": 60.0, "age_sec": 0.1,
            "spread_pct": 0.01,
        }

        closed = await engine._finalize_paper_close(
            "TEST", trade, db, bbo, exit_reason="STOP_LOSS"
        )

        self.assertAlmostEqual(closed["exit_price"], 99.9, places=6)
        self.assertAlmostEqual(closed["entry_commission_rub"], 1.0, places=4)
        self.assertAlmostEqual(closed["exit_commission_rub"], 0.999, places=4)
        self.assertAlmostEqual(closed["gross_pnl"], -1.0, places=4)
        self.assertAlmostEqual(closed["pnl"], -2.999, places=3)
        self.assertEqual(closed["exit_slippage_bps"], 10.0)
        self.assertEqual(len(db.history), 1)


if __name__ == "__main__":
    unittest.main()
