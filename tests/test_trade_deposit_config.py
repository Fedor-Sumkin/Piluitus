import os
import unittest
from unittest.mock import patch

from config import _resolve_trade_deposit
from execution_layer import BaseExecutionEngine


class TradeDepositConfigTests(unittest.TestCase):
    def test_canonical_and_legacy_values_must_match_when_both_are_set(self):
        with self.assertRaisesRegex(ValueError, "Конфликт стартового капитала"):
            _resolve_trade_deposit("40000", "400000")

    def test_canonical_value_is_used(self):
        self.assertEqual(_resolve_trade_deposit("400000", None), (400000.0, "TRADE_DEPOSIT"))

    def test_legacy_value_remains_supported(self):
        self.assertEqual(
            _resolve_trade_deposit(None, "400000"),
            (400000.0, "TRADING_DEPOSIT (legacy)"),
        )

    def test_default_capital_is_400000(self):
        self.assertEqual(_resolve_trade_deposit(None, None), (400000.0, "default"))

    def test_execution_fallback_uses_canonical_setting_not_legacy_environment(self):
        with patch("execution.execution_layer.Settings.TRADE_DEPOSIT", 400000.0):
            with patch.dict(os.environ, {"TRADING_DEPOSIT": "40000"}):
                engine = BaseExecutionEngine()
        self.assertEqual(engine.initial_capital, 400000.0)


if __name__ == "__main__":
    unittest.main()
