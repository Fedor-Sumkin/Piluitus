from __future__ import annotations

import unittest
from types import SimpleNamespace

from risk_manager import RiskManager


class LotSizeMetadataGuardTests(unittest.TestCase):
    def test_unknown_instrument_never_defaults_to_one_share_lot(self):
        risk = SimpleNamespace(initial_deposit=40000.0, max_risk_per_trade=0.01)
        self.assertEqual(
            RiskManager.calculate_lot_size(risk, "__UNKNOWN_TEST_TICKER__", 100.0, 99.0),
            0,
        )


if __name__ == "__main__":
    unittest.main()
