from __future__ import annotations

import unittest
from unittest.mock import patch
from execution_layer import BaseExecutionEngine
from config import ASSET_METADATA


class AssetMetadataTickRoundingTests(unittest.TestCase):
    def setUp(self):
        self.previous = ASSET_METADATA.get("TEST")
        ASSET_METADATA["TEST"] = {
            "price_tick": 0.05,
            "decimals": 2,
            "min_step": 5,
        }

    def tearDown(self):
        if self.previous is None:
            ASSET_METADATA.pop("TEST", None)
        else:
            ASSET_METADATA["TEST"] = self.previous

    def test_buy_sl_rounds_down_and_tp_rounds_up_to_finams_tick(self):
        with patch.dict("os.environ", {"PILUITUS_MIN_STOP_PCT": "0.0025", "PILUITUS_TAKE_PROFIT_R_MULTIPLE": "2.0", "ATR_MULTIPLIER": "1.5"}):
            sl, tp = BaseExecutionEngine.calculate_atr_stop(
                object(), "TEST", 100.0, "BUY", atr_value=0.87, atr_periods=5
            )
        self.assertAlmostEqual(sl / 0.05, round(sl / 0.05))
        self.assertAlmostEqual(tp / 0.05, round(tp / 0.05))
        self.assertLessEqual(sl, 98.695)
        self.assertGreaterEqual(tp, 102.61)

    def test_sell_sl_rounds_up_and_tp_rounds_down_to_finams_tick(self):
        with patch.dict("os.environ", {"PILUITUS_MIN_STOP_PCT": "0.0025", "PILUITUS_TAKE_PROFIT_R_MULTIPLE": "2.0", "ATR_MULTIPLIER": "1.5"}):
            sl, tp = BaseExecutionEngine.calculate_atr_stop(
                object(), "TEST", 100.0, "SELL", atr_value=0.87, atr_periods=5
            )
        self.assertAlmostEqual(sl / 0.05, round(sl / 0.05))
        self.assertAlmostEqual(tp / 0.05, round(tp / 0.05))
        self.assertGreaterEqual(sl, 101.305)
        self.assertLessEqual(tp, 97.39)


if __name__ == "__main__":
    unittest.main()
