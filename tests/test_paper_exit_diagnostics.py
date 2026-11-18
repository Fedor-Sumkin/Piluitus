import unittest

from strategy.paper_exit_diagnostics import build_exit_execution_diagnostics


class PaperExitDiagnosticsTests(unittest.TestCase):
    def test_long_exit_separates_bid_depth_impact_and_slippage(self):
        result = build_exit_execution_diagnostics(
            position_side="BUY", quantity=10, fill_price=98.7012,
            executable_bbo_price=99.0, bid=99.0, ask=99.2,
            bid_age_sec=0.1, ask_age_sec=0.2, order_book_age_sec=0.15,
            depth_vwap=98.8, depth_fill_ratio=1.0, depth_levels_used=2,
        )
        self.assertAlmostEqual(result["exit_depth_impact_rub"], 2.0)
        self.assertAlmostEqual(result["exit_configured_slippage_rub"], 0.988)
        self.assertAlmostEqual(result["exit_execution_shortfall_rub"], 2.988)
        self.assertAlmostEqual(result["exit_execution_shortfall_bps"], 30.181818, places=5)
        self.assertEqual(result["exit_diagnostics_status"], "DEPTH_REFERENCE_AVAILABLE")
        self.assertEqual(result["exit_depth_levels_used"], 2)

    def test_short_exit_uses_ask_and_adverse_sign(self):
        result = build_exit_execution_diagnostics(
            position_side="SELL", quantity=5, fill_price=101.303,
            executable_bbo_price=101.0, bid=100.8, ask=101.0,
            depth_vwap=101.2,
        )
        self.assertAlmostEqual(result["exit_depth_impact_rub"], 1.0)
        self.assertAlmostEqual(result["exit_configured_slippage_rub"], 0.515)
        self.assertAlmostEqual(result["exit_execution_shortfall_rub"], 1.515)

    def test_bbo_only_keeps_depth_impact_unknown(self):
        result = build_exit_execution_diagnostics(
            position_side="BUY", quantity=10, fill_price=98.9,
            executable_bbo_price=99.0,
        )
        self.assertIsNone(result["exit_depth_impact_rub"])
        self.assertAlmostEqual(result["exit_configured_slippage_rub"], 1.0)
        self.assertAlmostEqual(result["exit_execution_shortfall_rub"], 1.0)
        self.assertEqual(result["exit_diagnostics_status"], "BBO_ONLY")

    def test_invalid_reference_does_not_fabricate_zero_costs(self):
        result = build_exit_execution_diagnostics(
            position_side="BUY", quantity=10, fill_price=99.0,
            executable_bbo_price=None, depth_vwap=98.9,
        )
        self.assertIsNone(result["exit_execution_shortfall_rub"])
        self.assertIsNone(result["exit_configured_slippage_rub"])
        self.assertEqual(result["exit_diagnostics_status"], "MISSING_EXECUTABLE_REFERENCE")


if __name__ == "__main__":
    unittest.main()
