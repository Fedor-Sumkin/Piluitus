import os
import unittest
from unittest.mock import patch

from strategy.signal_guard import validate_atr_readiness


class AtrReadinessPrefilterTests(unittest.TestCase):
    def test_unready_atr_is_rejected_before_llm(self):
        metrics = {"bar_atr": 0.12, "bar_atr_periods": 4}
        with patch.dict(os.environ, {"PILUITUS_ATR_MIN_PERIODS": "5"}):
            ready, reason = validate_atr_readiness(metrics)
        self.assertFalse(ready)
        self.assertIn("4/5", reason)

    def test_five_closed_ranges_make_atr_ready(self):
        metrics = {"bar_atr": 0.12, "bar_atr_periods": 5, "bar_atr_age_sec": 12.0}
        with patch.dict(os.environ, {"PILUITUS_ATR_MIN_PERIODS": "5"}):
            ready, reason = validate_atr_readiness(metrics)
        self.assertTrue(ready)
        self.assertIn("подтверждена", reason)

    def test_stale_or_unknown_atr_history_is_rejected(self):
        for age in (-1.0, 121.0, float("inf")):
            with self.subTest(age=age):
                ready, reason = validate_atr_readiness({
                    "bar_atr": 0.12, "bar_atr_periods": 5, "bar_atr_age_sec": age
                })
                self.assertFalse(ready)
                self.assertIn("История ATR устарела", reason)

    def test_zero_or_nonfinite_atr_remains_unready(self):
        for atr in (0.0, float("nan"), float("inf")):
            with self.subTest(atr=atr):
                ready, _ = validate_atr_readiness(
                    {"bar_atr": atr, "bar_atr_periods": 14}
                )
                self.assertFalse(ready)


if __name__ == "__main__":
    unittest.main()
