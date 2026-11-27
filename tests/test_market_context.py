from __future__ import annotations

import unittest

from strategy.market_context import (classify_market_regime, estimate_entry_economics, estimate_planned_trade_risk_reward, validate_entry_economics)


class MarketContextTests(unittest.TestCase):
    def test_atr_regime_uses_only_prior_per_ticker_reference(self) -> None:
        metrics = {
            "bar_atr_pct": 0.30,
            "price_change_pct": 0.20,
            "price_vs_vwap_pct": 0.15,
            "bar_trend_up": 1.0,
            "bar_trend_down": 0.0,
            "trade_flow_imbalance_30s": 0.30,
            "spread_pct": 0.02,
            "trade_count_60s": 40,
            "trade_history_coverage_sec": 60.0,
            "trade_data_ready": 1.0,
            "trade_metrics_age_sec": 0.2,
        }
        warm = classify_market_regime(
            metrics, atr_reference_pct=0.20, atr_reference_samples=19
        )
        self.assertEqual(warm["volatility_regime"], "VOLATILITY_UNCLASSIFIED")
        self.assertEqual(warm["market_regime"], "TREND")

        high = classify_market_regime(
            metrics, atr_reference_pct=0.20, atr_reference_samples=20
        )
        self.assertEqual(high["volatility_regime"], "HIGH_VOLATILITY")
        self.assertEqual(high["market_regime"], "HIGH_VOLATILITY")
        self.assertAlmostEqual(high["atr_vs_reference_ratio"], 1.5)

        low = classify_market_regime(
            {**metrics, "bar_atr_pct": 0.13},
            atr_reference_pct=0.20, atr_reference_samples=20,
        )
        self.assertEqual(low["volatility_regime"], "LOW_VOLATILITY")
        self.assertAlmostEqual(low["atr_vs_reference_ratio"], 0.65)

    def test_low_liquidity_takes_precedence_over_directional_trend(self) -> None:
        result = classify_market_regime({
            "bar_atr_pct": 0.20,
            "spread_pct": 0.20,
            "price_change_pct": 0.20,
            "price_vs_vwap_pct": 0.10,
            "bar_trend_up": 1.0,
            "bar_trend_down": 0.0,
            "trade_data_ready": 1.0,
            "trade_metrics_age_sec": 0.2,
            "trade_history_coverage_sec": 60.0,
        })
        self.assertEqual(result["liquidity_regime"], "LOW_LIQUIDITY")
        self.assertEqual(result["directional_regime"], "TREND_UP")
        self.assertEqual(result["market_regime"], "LOW_LIQUIDITY")

    def test_mixed_directional_evidence_is_not_misclassified_as_trend(self) -> None:
        result = classify_market_regime({
            "price_change_pct": 0.20,
            "price_vs_vwap_pct": -0.10,
            "bar_trend_up": 0.0,
            "bar_trend_down": 0.0,
            "trade_flow_imbalance_30s": 0.20,
            "trade_data_ready": 1.0,
            "trade_metrics_age_sec": 0.2,
            "trade_history_coverage_sec": 60.0,
        })
        self.assertEqual(result["directional_regime"], "RANGE_OR_MIXED")

    def test_zero_age_trade_metrics_are_accepted_as_fresh(self) -> None:
        result = classify_market_regime({
            "price_change_pct": 0.20,
            "price_vs_vwap_pct": 0.10,
            "trade_flow_imbalance_30s": 0.20,
            "trade_data_ready": 1.0,
            "trade_metrics_age_sec": 0.0,
            "trade_history_coverage_sec": 60.0,
            "bar_trend_up": 1.0,
            "bar_trend_down": 0.0,
        })
        self.assertEqual(result["directional_regime"], "TREND_UP")

    def test_unwarmed_or_stale_trade_metrics_do_not_create_trend_votes(self) -> None:
        result = classify_market_regime({
            "price_change_pct": 0.30,
            "price_vs_vwap_pct": 0.20,
            "trade_flow_imbalance_30s": 0.40,
            "trade_data_ready": 0.0,
            "trade_metrics_age_sec": 0.2,
            "trade_history_coverage_sec": 60.0,
            "bar_trend_up": 0.0,
            "bar_trend_down": 0.0,
        })
        self.assertEqual(result["directional_regime"], "DIRECTION_UNAVAILABLE")
        self.assertEqual(result["market_regime"], "INSUFFICIENT_DATA")

    def test_buy_break_even_move_includes_spread_commission_and_slippage(self) -> None:
        result = estimate_entry_economics(
            side="BUY", bid=100.0, ask=100.2,
            commission_pct_per_side=0.05,
            slippage_bps_per_side=2.0,
            atr_pct=0.40,
        )
        self.assertTrue(result["available"])
        self.assertGreater(result["minimum_favorable_move_pct"], 0.10)
        self.assertGreater(result["move_beyond_entry_executable_pct"], 0.0)
        self.assertGreater(result["cost_to_atr_ratio"], 0.0)
        self.assertEqual(result["cost_model_status"], "CONFIGURED")
        self.assertGreater(result["breakeven_exit_executable_price"], 100.2)
        self.assertAlmostEqual(result["estimated_round_trip_spread_cost_pct"], result["entry_spread_pct"])
        self.assertAlmostEqual(result["estimated_round_trip_commission_cost_pct"], 0.10)
        self.assertAlmostEqual(result["estimated_round_trip_slippage_cost_pct"], 0.04)

    def test_sell_break_even_move_is_positive_and_directionally_correct(self) -> None:
        result = estimate_entry_economics(
            side="SELL", bid=100.0, ask=100.2,
            commission_pct_per_side=0.05,
            slippage_bps_per_side=2.0,
            atr_pct=0.40,
        )
        self.assertTrue(result["available"])
        self.assertGreater(result["minimum_favorable_move_pct"], 0.10)
        self.assertLess(result["breakeven_exit_executable_price"], 100.0)
        self.assertGreater(result["move_beyond_entry_executable_pct"], 0.0)

    def test_zero_cost_assumptions_still_include_half_spread_from_mid(self) -> None:
        result = estimate_entry_economics(
            side="BUY", bid=100.0, ask=100.2,
            commission_pct_per_side=0.0,
            slippage_bps_per_side=0.0,
        )
        self.assertAlmostEqual(result["minimum_favorable_move_pct"], (100.2 / 100.1 - 1.0) * 100.0)
        self.assertEqual(result["cost_model_status"], "INCOMPLETE_OR_ZERO_ASSUMPTION")

    def test_invalid_bbo_or_costs_are_unavailable(self) -> None:
        crossed = estimate_entry_economics(
            side="BUY", bid=100.2, ask=100.0,
            commission_pct_per_side=0.05,
            slippage_bps_per_side=2.0,
        )
        invalid_cost = estimate_entry_economics(
            side="SELL", bid=100.0, ask=100.2,
            commission_pct_per_side=100.0,
            slippage_bps_per_side=2.0,
        )
        self.assertFalse(crossed["available"])
        self.assertFalse(invalid_cost["available"])


    def test_cost_to_atr_gate_accepts_reasonable_round_trip_costs(self) -> None:
        result = validate_entry_economics(
            side="BUY", bid=99.9, ask=100.0,
            commission_pct_per_side=0.0354,
            slippage_bps_per_side=0.0,
            atr_pct=0.40,
            max_cost_to_atr_ratio=0.50,
        )
        self.assertTrue(result["available"])
        self.assertTrue(result["eligible"])
        self.assertLessEqual(result["cost_to_atr_ratio"], 0.50)
        self.assertEqual(result["cost_model_status"], "INCOMPLETE_OR_ZERO_ASSUMPTION")

    def test_cost_to_atr_gate_rejects_expensive_entry(self) -> None:
        result = validate_entry_economics(
            side="BUY", bid=99.9, ask=100.0,
            commission_pct_per_side=0.0354,
            slippage_bps_per_side=0.0,
            atr_pct=0.15,
            max_cost_to_atr_ratio=0.50,
        )
        self.assertTrue(result["available"])
        self.assertFalse(result["eligible"])
        self.assertEqual(result["reason"], "ENTRY_COST_TO_ATR_EXCEEDED")


    def test_planned_trade_risk_reward_prices_stop_and_target_after_round_trip_costs(self) -> None:
        result = estimate_planned_trade_risk_reward(
            side="BUY", bid=100.0, ask=100.1, atr_value=1.0,
            commission_pct_per_side=0.0354, slippage_bps_per_side=1.0,
            atr_multiplier=1.5, min_stop_pct=0.0025, reward_r_multiple=2.0,
            min_net_reward_to_risk=1.0,
        )
        self.assertTrue(result["available"])
        self.assertTrue(result["eligible"])
        self.assertAlmostEqual(result["gross_reward_to_risk"], 2.0)
        self.assertGreater(result["planned_take_profit"], result["reference_price"])
        self.assertLess(result["planned_stop_loss"], result["reference_price"])
        self.assertGreater(result["net_reward_to_risk"], 1.0)

    def test_planned_trade_risk_reward_rejects_unreasonable_after_cost_payoff(self) -> None:
        result = estimate_planned_trade_risk_reward(
            side="BUY", bid=100.0, ask=101.0, atr_value=0.1,
            commission_pct_per_side=0.5, slippage_bps_per_side=50.0,
            atr_multiplier=1.5, min_stop_pct=0.0025, reward_r_multiple=1.0,
            min_net_reward_to_risk=1.0,
        )
        self.assertTrue(result["available"])
        self.assertFalse(result["eligible"])
        self.assertLess(result["net_reward_pct"], 0.0)


if __name__ == "__main__":
    unittest.main()
