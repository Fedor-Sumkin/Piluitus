"""Composition contract for the modular risk manager implementation."""
from __future__ import annotations

import unittest

from risk.risk_manager import RiskManager, RealMarketSimulator
from risk.risk_lot_metadata import RiskLotMetadataMixin
from risk.risk_capital_sync import RiskCapitalSyncMixin
from risk.risk_trading_limits import RiskTradingLimitsMixin
from risk.risk_lot_sizing import RiskLotSizingMixin


class RiskManagerModuleBoundaryTests(unittest.TestCase):
    def test_public_risk_manager_composes_all_policy_responsibilities(self):
        for mixin in (
            RiskLotMetadataMixin,
            RiskCapitalSyncMixin,
            RiskTradingLimitsMixin,
            RiskLotSizingMixin,
        ):
            self.assertTrue(issubclass(RiskManager, mixin))

    def test_stable_public_classes_and_methods_remain_available(self):
        self.assertTrue(callable(RealMarketSimulator))
        self.assertTrue(callable(getattr(RiskManager, "update_finam_lot_sizes")))
        self.assertTrue(callable(getattr(RiskManager, "is_trading_allowed")))
        self.assertTrue(callable(getattr(RiskManager, "calculate_lot_size")))


if __name__ == "__main__":
    unittest.main()
