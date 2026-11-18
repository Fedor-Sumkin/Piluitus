"""Tests for PAPER entry execution-cost attribution."""
import unittest

from strategy.paper_entry_diagnostics import build_entry_execution_diagnostics


class PaperEntryDiagnosticsTests(unittest.TestCase):
    def test_buy_entry_separates_depth_impact_and_configured_slippage(self):
        result = build_entry_execution_diagnostics(
            side="BUY", quantity=10, fill_price=101.301,
            executable_bbo_price=101.0, bid=99.0, ask=101.0,
            bid_age_sec=0.1, ask_age_sec=0.2, atr=2.0,
            signal_reference_price=100.0, depth_vwap=101.2,
        )
        self.assertEqual(result["entry_mid_price"], 100.0)
        self.assertEqual(result["entry_executable_bbo_price"], 101.0)
        self.assertEqual(result["entry_spread_to_atr_ratio"], 1.0)
        self.assertAlmostEqual(result["entry_depth_impact_rub"], 2.0)
        self.assertAlmostEqual(result["entry_configured_slippage_rub"], 1.01)
        self.assertAlmostEqual(result["entry_execution_shortfall_rub"], 3.01)
        self.assertAlmostEqual(result["entry_execution_shortfall_bps"], 29.80198, places=4)
        self.assertAlmostEqual(result["entry_signal_adverse_drift_pct"], 1.301)

    def test_sell_entry_uses_adverse_sign_consistently(self):
        result = build_entry_execution_diagnostics(
            side="SELL", quantity=10, fill_price=98.7012,
            executable_bbo_price=99.0, bid=99.0, ask=101.0,
            bid_age_sec=0.15, ask_age_sec=0.25, atr=2.0,
            signal_reference_price=100.0, depth_vwap=98.8,
        )
        self.assertAlmostEqual(result["entry_depth_impact_rub"], 2.0)
        self.assertAlmostEqual(result["entry_configured_slippage_rub"], 0.988)
        self.assertAlmostEqual(result["entry_execution_shortfall_rub"], 2.988)
        self.assertAlmostEqual(result["entry_signal_adverse_drift_pct"], 1.2988)

    def test_missing_or_invalid_bbo_does_not_fabricate_execution_costs(self):
        result = build_entry_execution_diagnostics(
            side="BUY", quantity=10, fill_price=100.0,
            executable_bbo_price=None, bid=0.0, ask=0.0,
            atr=2.0, signal_reference_price=100.0,
        )
        self.assertIsNone(result["entry_mid_price"])
        self.assertIsNone(result["entry_execution_shortfall_rub"])
        self.assertIsNone(result["entry_execution_shortfall_bps"])
        self.assertIsNone(result["entry_spread_to_atr_ratio"])


if __name__ == "__main__":
    unittest.main()
