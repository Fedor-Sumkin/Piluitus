from __future__ import annotations

import unittest
from types import SimpleNamespace

from config import FINAM_TRADE_LOT_SIZES, LOT_SIZES
from execution_layer import BaseExecutionEngine
from risk_manager import RiskManager


class LotSizeMetadataGuardTests(unittest.TestCase):
    def test_static_reference_does_not_authorize_order_without_finam_trade_lot(self):
        ticker = "__STATIC_ONLY_TEST__"
        old_static = LOT_SIZES.get(ticker)
        old_trade = FINAM_TRADE_LOT_SIZES.get(ticker)
        try:
            LOT_SIZES[ticker] = 10
            FINAM_TRADE_LOT_SIZES.pop(ticker, None)
            risk = SimpleNamespace(initial_deposit=40000.0, max_risk_per_trade=0.01)
            self.assertEqual(RiskManager.calculate_lot_size(risk, ticker, 100.0, 99.0), 0)
        finally:
            if old_static is None:
                LOT_SIZES.pop(ticker, None)
            else:
                LOT_SIZES[ticker] = old_static
            if old_trade is None:
                FINAM_TRADE_LOT_SIZES.pop(ticker, None)
            else:
                FINAM_TRADE_LOT_SIZES[ticker] = old_trade

    def test_execution_multiplier_requires_account_specific_finam_value(self):
        ticker = "__STATIC_ONLY_EXECUTION_TEST__"
        old_static = LOT_SIZES.get(ticker)
        old_trade = FINAM_TRADE_LOT_SIZES.get(ticker)
        try:
            LOT_SIZES[ticker] = 10
            FINAM_TRADE_LOT_SIZES.pop(ticker, None)
            self.assertEqual(BaseExecutionEngine._lot_multiplier_for(ticker), 0)
            FINAM_TRADE_LOT_SIZES[ticker] = 7
            self.assertEqual(BaseExecutionEngine._lot_multiplier_for(ticker), 7)
        finally:
            if old_static is None:
                LOT_SIZES.pop(ticker, None)
            else:
                LOT_SIZES[ticker] = old_static
            if old_trade is None:
                FINAM_TRADE_LOT_SIZES.pop(ticker, None)
            else:
                FINAM_TRADE_LOT_SIZES[ticker] = old_trade

    def test_unknown_instrument_never_defaults_to_one_share_lot(self):
        risk = SimpleNamespace(initial_deposit=40000.0, max_risk_per_trade=0.01)
        self.assertEqual(
            RiskManager.calculate_lot_size(risk, "__UNKNOWN_TEST_TICKER__", 100.0, 99.0),
            0,
        )


if __name__ == "__main__":
    unittest.main()
