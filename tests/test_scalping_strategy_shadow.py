from __future__ import annotations

import os
import time
import unittest
from unittest.mock import patch

from scalping_strategy import evaluate_scalping_candidate, has_queue_trade_flow_conflict


def strong_buy_metrics() -> dict[str, float | str]:
    return {
        "price_change_pct": 0.20,
        "price_vs_vwap_pct": 0.12,
        "volume_spike": 2.4,
        "bid_ask_available": 1.0,
        "spread_available": 1.0,
        "spread_pct": 0.025,
        "bid": 100.0,
        "ask": 100.025,
        "bbo_max_age_sec": 2.0,
        "bid_quote_age_sec": 0.2,
        "ask_quote_age_sec": 0.3,
        "order_book_valid": 1.0,
        "queue_imbalance_available": 1.0,
        "queue_imbalance": 0.32,
        "queue_imbalance_age_sec": 0.2,
        "queue_imbalance_5s": 0.32,
        "queue_imbalance_5s_available": 1.0,
        "queue_imbalance_5s_coverage_sec": 3.0,
        "queue_imbalance_5s_samples": 20,
        "queue_imbalance_5s_age_sec": 0.2,
        "trade_data_ready": 1.0,
        "trade_metrics_age_sec": 0.2,
        "trade_flow_imbalance_30s": 0.35,
        "bar_atr": 0.20,
        "bar_atr_pct": 0.20,
        "bar_atr_periods": 8.0,
        "bar_pattern_confirmed": 1.0,
        "bar_pattern_name": "M1_RANGE_BREAKOUT_BUY",
        "_bar_pattern_signal_at_monotonic": time.monotonic(),
        "bar_trend_up": 1.0,
        "bar_trend_down": 0.0,
    }


class ScalpingShadowStrategyTests(unittest.TestCase):
    def test_aligned_m1_momentum_queue_and_costs_score_as_shadow_eligible(self) -> None:
        with patch.dict(os.environ, {"PILUITUS_ATR_MIN_PERIODS": "5"}):
            result = evaluate_scalping_candidate("BUY", strong_buy_metrics())
        self.assertTrue(result["eligible"], result)
        self.assertGreaterEqual(result["score"], 85.0)
        self.assertEqual(result["mode"], "SHADOW_ONLY")
        self.assertEqual(result["regime"], "TREND_ALIGNED")

    def test_opposing_queue_imbalance_reduces_score_without_inverting_side(self) -> None:
        metrics = strong_buy_metrics()
        metrics["queue_imbalance"] = -0.45
        metrics["queue_imbalance_5s"] = -0.45
        result = evaluate_scalping_candidate("BUY", metrics)
        baseline = evaluate_scalping_candidate("BUY", strong_buy_metrics())
        self.assertFalse(result["eligible"])
        self.assertEqual(result["side"], "BUY")
        self.assertLess(result["score"], baseline["score"])
        self.assertEqual(result["components"]["queue_imbalance"], -3.0)
        self.assertIn("time_weighted_queue_imbalance_opposes_side", result["reasons"])

    def test_strong_opposing_queue_reduces_sell_score_without_inverting_side(self) -> None:
        metrics = strong_buy_metrics()
        metrics["price_change_pct"] = -0.20
        metrics["price_vs_vwap_pct"] = -0.12
        metrics["queue_imbalance"] = 0.45
        metrics["queue_imbalance_5s"] = 0.45
        metrics["bar_pattern_name"] = "M1_RANGE_BREAKOUT_SELL"
        metrics["bar_trend_up"] = 0.0
        metrics["bar_trend_down"] = 1.0
        metrics["trade_flow_imbalance_30s"] = -0.35
        result = evaluate_scalping_candidate("SELL", metrics)
        self.assertFalse(result["eligible"])
        self.assertEqual(result["side"], "SELL")
        self.assertEqual(result["components"]["queue_imbalance"], -3.0)
        self.assertIn("time_weighted_queue_imbalance_opposes_side", result["reasons"])

    def test_stale_or_invalid_book_is_a_shadow_hard_block(self) -> None:
        metrics = strong_buy_metrics()
        metrics["order_book_valid"] = 0.0
        metrics["bid_quote_age_sec"] = 3.0
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertFalse(result["eligible"])
        self.assertIn("order_book_not_confirmed", result["hard_blocks"])
        self.assertIn("bbo_stale", result["hard_blocks"])

    def test_insufficient_atr_history_is_a_shadow_hard_block(self) -> None:
        metrics = strong_buy_metrics()
        metrics["bar_atr_periods"] = 2.0
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertFalse(result["eligible"])
        self.assertIn("atr_not_ready", result["hard_blocks"])

    def test_conflicting_closed_m1_pattern_blocks_shadow_approval(self) -> None:
        metrics = strong_buy_metrics()
        metrics["bar_pattern_name"] = "M1_RANGE_BREAKOUT_SELL"
        metrics["bar_trend_up"] = 0.0
        metrics["bar_trend_down"] = 1.0
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertFalse(result["eligible"])
        self.assertIn("m1_pattern_conflicts_with_candidate", result["hard_blocks"])

    def test_stale_m1_pattern_is_not_treated_as_fresh_confirmation(self) -> None:
        metrics = strong_buy_metrics()
        metrics["_bar_pattern_signal_at_monotonic"] = time.monotonic() - 30.0
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertNotIn("fresh_matching_m1_pattern", result["reasons"])

    def test_spread_to_atr_above_configured_limit_is_a_hard_block(self) -> None:
        metrics = strong_buy_metrics()
        metrics["bar_atr_pct"] = 0.01
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertGreater(result["spread_atr_ratio"], result["spread_atr_limit"])
        self.assertFalse(result["eligible"])
        self.assertIn("spread_to_atr_above_limit", result["hard_blocks"])

    def test_missing_atr_percentage_cannot_receive_shadow_approval(self) -> None:
        metrics = strong_buy_metrics()
        metrics.pop("bar_atr_pct")
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertFalse(result["eligible"])
        self.assertIn("spread_atr_unavailable", result["hard_blocks"])

    def test_crossed_or_missing_bbo_prices_are_a_shadow_hard_block(self) -> None:
        metrics = strong_buy_metrics()
        metrics["ask"] = 99.9
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertFalse(result["eligible"])
        self.assertIn("bbo_prices_invalid", result["hard_blocks"])

    def test_ofi_is_reported_as_diagnostic_without_changing_shadow_score(self) -> None:
        metrics = strong_buy_metrics()
        baseline = evaluate_scalping_candidate("BUY", metrics)
        metrics["ofi_5s_available"] = 1.0
        metrics["ofi_5s"] = -0.25
        metrics["ofi_5s_samples"] = 8
        metrics["ofi_5s_coverage_sec"] = 2.0
        with_ofi = evaluate_scalping_candidate("BUY", metrics)

        self.assertEqual(with_ofi["score"], baseline["score"])
        self.assertEqual(with_ofi["eligible"], baseline["eligible"])
        self.assertEqual(with_ofi["ofi_5s"], -0.25)
        self.assertEqual(with_ofi["ofi_directional_alignment"], "OPPOSES")
        self.assertEqual(with_ofi["ofi_5s_samples"], 8)

    def test_unavailable_ofi_is_not_reported_as_zero_signal(self) -> None:
        result = evaluate_scalping_candidate("BUY", strong_buy_metrics())
        self.assertIsNone(result["ofi_5s"])
        self.assertEqual(
            result["ofi_directional_alignment"], "NEUTRAL_OR_UNAVAILABLE"
        )

    def test_missing_queue_measurement_is_explicit_not_fabricated(self) -> None:
        metrics = strong_buy_metrics()
        metrics["queue_imbalance_available"] = 0.0
        metrics["queue_imbalance_5s_available"] = 0.0
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertIsNone(result["queue_imbalance"])
        self.assertIn("time_weighted_queue_imbalance_warmup_or_stale", result["reasons"])

    def test_queue_and_aggressor_flow_conflict_blocks_shadow_qualification(self):
        metrics = strong_buy_metrics()
        metrics["trade_flow_imbalance_30s"] = -0.30
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertFalse(result["eligible"])
        self.assertIn("queue_trade_flow_conflict", result["hard_blocks"])

    def test_fresh_queue_flow_conflict_is_directional_for_paper_guard(self):
        # Queue supports BUY while aggressive selling opposes BUY.
        metrics = strong_buy_metrics()
        metrics["trade_flow_imbalance_30s"] = -0.30
        self.assertTrue(has_queue_trade_flow_conflict("BUY", metrics))
        # The same raw measurements are not a conflict against SELL flow:
        # the queue opposes SELL, while aggressive selling confirms SELL.
        self.assertFalse(has_queue_trade_flow_conflict("SELL", metrics))

        # Mirror the case: queue supports SELL and aggressive buying opposes it.
        metrics["queue_imbalance_5s"] = -0.32
        metrics["trade_flow_imbalance_30s"] = 0.30
        self.assertTrue(has_queue_trade_flow_conflict("SELL", metrics))
        self.assertFalse(has_queue_trade_flow_conflict("BUY", metrics))

    def test_queue_flow_guard_ignores_unavailable_or_stale_measurements(self):
        metrics = strong_buy_metrics()
        metrics["trade_flow_imbalance_30s"] = -0.30
        metrics["queue_imbalance_5s_age_sec"] = 8.0
        self.assertFalse(has_queue_trade_flow_conflict("BUY", metrics))
        metrics["queue_imbalance_5s_age_sec"] = 0.2
        metrics["trade_data_ready"] = 0.0
        self.assertFalse(has_queue_trade_flow_conflict("BUY", metrics))

    def test_queue_warmup_does_not_fall_back_to_single_snapshot(self):
        metrics = strong_buy_metrics()
        metrics["queue_imbalance_5s_available"] = 0.0
        metrics["queue_imbalance_5s_coverage_sec"] = 0.4
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertFalse(result["eligible"])
        self.assertIn("time_weighted_queue_imbalance_warmup_or_stale", result["reasons"])

    def test_missing_or_stale_trade_flow_is_a_shadow_hard_block(self):
        metrics = strong_buy_metrics()
        metrics["trade_data_ready"] = 0.0
        metrics["trade_metrics_age_sec"] = -1.0
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertFalse(result["eligible"])
        self.assertIn("trade_flow_unavailable_or_stale", result["hard_blocks"])


    def test_regime_specific_shadow_variant_is_separate_from_baseline_score(self) -> None:
        metrics = strong_buy_metrics()
        metrics.update({
            "trade_history_coverage_sec": 60.0, "trade_count_60s": 40,
            "trade_baseline_bucket_count": 10, "trade_baseline_min_bucket_count": 4,
            "entry_minimum_favorable_move_pct": 0.05, "entry_target_move_pct": 0.30,
        })
        result = evaluate_scalping_candidate("BUY", metrics)
        self.assertEqual(result["mode"], "SHADOW_ONLY")
        self.assertEqual(result["market_regime"], "TREND")
        self.assertEqual(result["strategy_variant"], "TREND_CONTINUATION")
        self.assertTrue(result["volume_impulse_confirmed"])
        self.assertTrue(result["economic_edge_available"])
        self.assertGreater(result["planned_move_cost_coverage_ratio"], 1.0)
        self.assertIsInstance(result["eligible"], bool)

    def test_regime_research_track_fails_closed_when_cost_or_volume_history_missing(self) -> None:
        result = evaluate_scalping_candidate("BUY", strong_buy_metrics())
        self.assertFalse(result["volume_impulse_confirmed"])
        self.assertFalse(result["economic_edge_available"])
        self.assertFalse(result["strategy_eligible"])
        self.assertIn("planned_move_vs_full_cost_unavailable", result["strategy_hard_blocks"])


if __name__ == "__main__":
    unittest.main()
