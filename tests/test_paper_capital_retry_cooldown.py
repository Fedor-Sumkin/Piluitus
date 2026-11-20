from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from execution.execution_layer import BaseExecutionEngine


class _ReservationHarness:
    def __init__(self):
        self._portfolio_lock = asyncio.Lock()
        self._maintenance_active = False
        self._maintenance_reason = ""
        self.paper_mode = True
        self._ticker_cooldowns = {}
        self._capital_reject_cooldowns = {}
        self._opening_tickers = set()
        self._unreconciled_paper_positions = {}
        self.paper_positions = {}
        self._reserved_notional = {}
        self._reserved_stop_risk = {}
        self.initial_capital = 40000.0
        self.max_gross_exposure_pct = 0.10
        self.max_single_position_pct = 0.03
        self.max_open_stop_risk_pct = 0.01

    @staticmethod
    def _gross_exposure(_positions):
        return 0.0

    @staticmethod
    def _open_stop_risk(_positions):
        return 0.0

    @staticmethod
    def _stop_risk_with_costs(_price, _stop, _quantity, _side):
        return 1.0


class PaperCapitalRetryCooldownTests(unittest.IsolatedAsyncioTestCase):
    async def test_repeated_capital_rejection_is_throttled_per_ticker_and_side(self):
        engine = _ReservationHarness()
        with (
            patch("execution.execution_layer.FINAM_TRADE_LOT_SIZES", {"TEST": 1}),
            patch("execution.execution_layer.time.monotonic", return_value=100.0),
            patch.dict("os.environ", {
                "PILUITUS_PAPER_CAPITAL_REJECT_COOLDOWN_SEC": "10",
            }, clear=False),
        ):
            first_lots, first_reason = await BaseExecutionEngine._reserve_order_size(
                engine, "TEST", 1, 2000.0, None, side="BUY", stop_loss=1990.0
            )
            second_lots, second_reason = await BaseExecutionEngine._reserve_order_size(
                engine, "TEST", 1, 2000.0, None, side="BUY", stop_loss=1990.0
            )

        self.assertEqual(first_lots, 0)
        self.assertIn("Лимиты портфеля", first_reason)
        self.assertEqual(second_lots, 0)
        self.assertIn("capital retry cooldown", second_reason)
        self.assertIn(("TEST", "BUY"), engine._capital_reject_cooldowns)

    async def test_cooldown_is_scoped_to_ticker_and_side(self):
        engine = _ReservationHarness()
        engine._capital_reject_cooldowns[("TEST", "BUY")] = 110.0
        with (
            patch("execution.execution_layer.FINAM_TRADE_LOT_SIZES", {"TEST": 1}),
            patch("execution.execution_layer.time.monotonic", return_value=101.0),
            patch.dict("os.environ", {
                "PILUITUS_PAPER_CAPITAL_REJECT_COOLDOWN_SEC": "10",
            }, clear=False),
        ):
            sell_lots, sell_reason = await BaseExecutionEngine._reserve_order_size(
                engine, "TEST", 1, 2000.0, None, side="SELL", stop_loss=2010.0
            )
        self.assertEqual(sell_lots, 0)
        self.assertIn("Лимиты портфеля", sell_reason)
        self.assertIn(("TEST", "BUY"), engine._capital_reject_cooldowns)
        self.assertIn(("TEST", "SELL"), engine._capital_reject_cooldowns)


if __name__ == "__main__":
    unittest.main()
