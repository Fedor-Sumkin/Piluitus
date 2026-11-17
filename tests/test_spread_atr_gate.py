import os
import unittest
from unittest.mock import patch

from strategy.signal_guard import validate_signal_evidence, validate_spread_to_atr


class SpreadAtrGateTests(unittest.TestCase):
    def test_ratio_at_limit_is_allowed(self):
        with patch.dict(os.environ, {"PILUITUS_MAX_SPREAD_TO_ATR_RATIO": "0.50"}):
            ok, reason = validate_spread_to_atr(
                {"bar_atr_periods": 5, "bar_atr_age_sec": 1.0},
                spread_pct=0.10, atr_value=0.20, reference_price=100.0,
                require_atr=True,
            )
        self.assertTrue(ok, reason)
        self.assertIn("ratio=0.500", reason)

    def test_ratio_above_limit_is_rejected(self):
        with patch.dict(os.environ, {"PILUITUS_MAX_SPREAD_TO_ATR_RATIO": "0.50"}):
            ok, reason = validate_spread_to_atr(
                {"bar_atr_periods": 5, "bar_atr_age_sec": 1.0},
                spread_pct=0.102, atr_value=0.20, reference_price=100.0,
                require_atr=True,
            )
        self.assertFalse(ok)
        self.assertIn("ratio=0.510", reason)

    def test_extreme_irao_like_spread_is_rejected_even_below_absolute_spread_cap(self):
        with patch.dict(os.environ, {"PILUITUS_MAX_SPREAD_TO_ATR_RATIO": "0.50"}):
            ok, reason = validate_spread_to_atr(
                {"bar_atr_periods": 14, "bar_atr_age_sec": 1.0},
                spread_pct=0.0877, atr_value=0.01, reference_price=100.0,
                require_atr=True,
            )
        self.assertFalse(ok)
        self.assertIn("Spread/ATR превышает лимит", reason)

    def test_missing_or_unready_atr_is_rejected_at_execution(self):
        cases = [
            ({"bar_atr_periods": 5, "bar_atr_age_sec": 1.0}, 0.0),
            ({"bar_atr_periods": 4, "bar_atr_age_sec": 1.0}, 0.2),
            ({"bar_atr_periods": 5, "bar_atr_age_sec": 121.0}, 0.2),
        ]
        for metadata, atr in cases:
            with self.subTest(metadata=metadata, atr=atr):
                ok, _ = validate_spread_to_atr(
                    metadata, spread_pct=0.05, atr_value=atr,
                    reference_price=100.0, require_atr=True,
                )
                self.assertFalse(ok)

    def test_pre_llm_evidence_gate_blocks_relative_spread_outlier(self):
        metrics = {
            "price_change_pct": 0.20,
            "price_vs_vwap_pct": 0.10,
            "volume_spike": 2.0,
            "spread_pct": 0.102,
            "spread_available": 1.0,
            "bid_ask_available": 1.0,
            "bar_atr": 0.20,
            "bar_atr_periods": 5,
            "bar_atr_age_sec": 1.0,
            "current_price": 100.0,
        }
        with patch.dict(os.environ, {"PILUITUS_MAX_SPREAD_TO_ATR_RATIO": "0.50"}):
            ok, reason = validate_signal_evidence("BUY", metrics)
        self.assertFalse(ok)
        self.assertIn("ratio=0.510", reason)

    def test_missing_atr_is_deferred_to_separate_readiness_gate(self):
        ok, _ = validate_spread_to_atr({"spread_pct": 0.05})
        self.assertTrue(ok)
        ok, reason = validate_spread_to_atr(
            {}, spread_pct=0.05, atr_value=0.0, reference_price=100.0,
            require_atr=True,
        )
        self.assertFalse(ok)
        self.assertIn("ATR", reason)


if __name__ == "__main__":
    unittest.main()
