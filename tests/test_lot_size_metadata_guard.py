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

    def test_large_minimum_lot_is_rejected_with_single_position_cap_reason(self):
        ticker = "__LARGE_LOT_TEST__"
        old_trade = FINAM_TRADE_LOT_SIZES.get(ticker)
        try:
            FINAM_TRADE_LOT_SIZES[ticker] = 10_000
            risk = SimpleNamespace(initial_deposit=40_000.0, max_risk_per_trade=0.005)
            with patch.dict("os.environ", {"PILUITUS_MAX_SINGLE_POSITION_PCT": "0.03"}, clear=False):
                lots, reason = RiskManager.calculate_lot_size_with_reason(risk, ticker, 57.30, 56.80)
            self.assertEqual(lots, 0)
            self.assertIn("MIN_LOT_NOTIONAL_EXCEEDS_SINGLE_CAP", reason)
            self.assertIn("573000.00 ₽", reason)
            self.assertIn("1200.00 ₽", reason)
        finally:
            if old_trade is None:
                FINAM_TRADE_LOT_SIZES.pop(ticker, None)
            else:
                FINAM_TRADE_LOT_SIZES[ticker] = old_trade

    def test_minimum_lot_stop_risk_is_reported_separately_from_notional(self):
        ticker = "__RISK_ONLY_BLOCK_TEST__"
        old_trade = FINAM_TRADE_LOT_SIZES.get(ticker)
        try:
            FINAM_TRADE_LOT_SIZES[ticker] = 3
            risk = SimpleNamespace(initial_deposit=40_000.0, max_risk_per_trade=0.005)
            with patch.dict("os.environ", {"PILUITUS_MAX_SINGLE_POSITION_PCT": "0.03"}, clear=False):
                lots, reason = RiskManager.calculate_lot_size_with_reason(risk, ticker, 100.0, 1.0)
            self.assertEqual(lots, 0)
            self.assertIn("MIN_LOT_RISK_EXCEEDS_TRADE_RISK", reason)
            self.assertIn("297.00 ₽", reason)
            self.assertIn("200.00 ₽", reason)
        finally:
            if old_trade is None:
                FINAM_TRADE_LOT_SIZES.pop(ticker, None)
            else:
                FINAM_TRADE_LOT_SIZES[ticker] = old_trade

    def test_affordable_minimum_lot_returns_allowed_lot_count_and_diagnostics(self):
        ticker = "__AFFORDABLE_LOT_TEST__"
        old_trade = FINAM_TRADE_LOT_SIZES.get(ticker)
        try:
            FINAM_TRADE_LOT_SIZES[ticker] = 1
            risk = SimpleNamespace(initial_deposit=40_000.0, max_risk_per_trade=0.005)
            with patch.dict("os.environ", {"PILUITUS_MAX_SINGLE_POSITION_PCT": "0.03"}, clear=False):
                lots, reason = RiskManager.calculate_lot_size_with_reason(risk, ticker, 100.0, 99.0)
            self.assertEqual(lots, 12)
            self.assertIn("SIZE_OK", reason)
        finally:
            if old_trade is None:
                FINAM_TRADE_LOT_SIZES.pop(ticker, None)
            else:
                FINAM_TRADE_LOT_SIZES[ticker] = old_trade

    def test_missing_metadata_reports_reason_and_does_not_use_static_reference(self):
        ticker = "__STATIC_ONLY_DIAGNOSTIC_TEST__"
        old_static = LOT_SIZES.get(ticker)
        old_trade = FINAM_TRADE_LOT_SIZES.get(ticker)
        try:
            LOT_SIZES[ticker] = 1
            FINAM_TRADE_LOT_SIZES.pop(ticker, None)
            risk = SimpleNamespace(initial_deposit=40_000.0, max_risk_per_trade=0.005)
            lots, reason = RiskManager.calculate_lot_size_with_reason(risk, ticker, 100.0, 99.0)
            self.assertEqual(lots, 0)
            self.assertIn("LOT_SIZE_METADATA_MISSING", reason)
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
