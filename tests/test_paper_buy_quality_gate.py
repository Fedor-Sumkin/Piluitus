from __future__ import annotations

import time
import unittest
from unittest.mock import patch

from strategy.signal_guard import validate_entry_evidence


def buy_metrics() -> dict[str, float]:
    return {
        "price_change_pct": 0.20,
        "price_vs_vwap_pct": 0.12,
        "volume_spike": 2.0,
        "spread_pct": 0.08,
        "spread_available": 1.0,
        "bid_ask_available": 1.0,
        "bid_quote_age_sec": 0.2,
        "ask_quote_age_sec": 0.2,
        "bid": 99.9,
        "ask": 100.0,
        "order_book_valid": 1.0,
        "order_book_age_sec": 0.2,
        "bbo_max_age_sec": 2.0,
        "bar_atr_pct": 0.40,
        "trade_data_ready": 1.0,
        "trade_metrics_age_sec": 0.5,
        "trade_flow_imbalance_30s": 0.25,
    }


class PaperBuyQualityGateTests(unittest.TestCase):
    def _validate(
        self, metrics: dict[str, float], *, enabled: str = "true", slippage: str = "1.0"
    ):
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_BUY_STRICT_GATE": enabled,
            "PILUITUS_PAPER_BUY_MIN_FLOW_IMBALANCE": "0.10",
            "PILUITUS_PAPER_COMMISSION_PCT": "0.0354",
            "PILUITUS_PAPER_SLIPPAGE_BPS": slippage,
            "PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO": "0.50",
        }, clear=False):
            return validate_entry_evidence("BUY", metrics, paper_mode=True)

    def test_buy_accepts_neutral_vwap_when_price_impulse_and_buyer_flow_align(self) -> None:
        metrics = buy_metrics()
        metrics["price_vs_vwap_pct"] = 0.01

        valid, reason = self._validate(metrics)

        self.assertTrue(valid, reason)
        self.assertIn("нейтральном VWAP", reason)

    def test_buy_blocks_neutral_vwap_without_directional_impulse_or_fresh_m1(self) -> None:
        metrics = buy_metrics()
        metrics["price_change_pct"] = 0.005
        metrics["price_vs_vwap_pct"] = 0.01

        valid, reason = self._validate(metrics)

        self.assertFalse(valid)
        self.assertIn("свежего совпадающего M1-паттерна", reason)

    def test_buy_requires_fresh_positive_aggressor_flow(self) -> None:
        metrics = buy_metrics()
        metrics["trade_flow_imbalance_30s"] = 0.05

        valid, reason = self._validate(metrics)

        self.assertFalse(valid)
        self.assertIn("агрессивного потока покупателей", reason)
        self.assertIn("требуется >= 0.10", reason)

    def test_buy_blocks_unreliable_aggressor_side_coverage(self) -> None:
        metrics = buy_metrics()
        metrics.update({
            "trade_flow_ready": 0.0,
            "trade_flow_known_trade_count_30s": 2,
            "trade_flow_known_volume_share_30s": 0.20,
            "trade_flow_min_known_trades_30s": 3,
            "trade_flow_min_known_volume_share": 0.50,
        })
        valid, reason = self._validate(metrics)
        self.assertFalse(valid)
        self.assertIn("недостаточно данных о стороне агрессора", reason)
        self.assertIn("известных сделок=2/3", reason)

    def test_buy_blocks_stale_aggressor_flow(self) -> None:
        metrics = buy_metrics()
        metrics["trade_metrics_age_sec"] = 5.1

        valid, reason = self._validate(metrics)

        self.assertFalse(valid)
        self.assertIn("поток сделок устарел", reason)

    def test_buy_blocks_confirmed_opposite_m1_trend(self) -> None:
        metrics = buy_metrics()
        metrics["bar_trend_down"] = 1.0
        metrics["bar_trend_up"] = 0.0

        valid, reason = self._validate(metrics)

        self.assertFalse(valid)
        self.assertIn("встречный M1-тренд вниз", reason)

    def test_buy_allows_fresh_confirmed_pullback_against_m1_trend(self) -> None:
        metrics = buy_metrics()
        metrics.update({
            "bar_trend_down": 1.0,
            "bar_trend_up": 0.0,
            "bar_pattern_confirmed": 1.0,
            "bar_pattern_name": "M1_EMA_PULLBACK_BUY",
            "bar_pullback_buy": 1.0,
            "bar_pattern_direction_code": 1.0,
            "_bar_pattern_signal_at_monotonic": time.monotonic(),
        })

        valid, reason = self._validate(metrics)

        self.assertTrue(valid, reason)
        self.assertIn("PAPER BUY прошёл строгую проверку", reason)

    def test_buy_blocks_persistent_opposing_five_second_queue(self) -> None:
        metrics = buy_metrics()
        metrics.update({
            "queue_imbalance_5s_available": 1.0,
            "queue_imbalance_available": 1.0,
            "queue_imbalance_5s": -0.70,
            "queue_imbalance_5s_age_sec": 0.2,
            "queue_imbalance_5s_coverage_sec": 4.5,
            "queue_imbalance_5s_samples": 12.0,
        })

        valid, reason = self._validate(metrics)

        self.assertFalse(valid)
        self.assertIn("устойчивый встречный дисбаланс стакана", reason)

    def test_buy_ignores_short_or_stale_queue_history(self) -> None:
        metrics = buy_metrics()
        metrics.update({
            "queue_imbalance_5s_available": 1.0,
            "queue_imbalance_available": 1.0,
            "queue_imbalance_5s": -0.70,
            "queue_imbalance_5s_age_sec": 4.5,
            "queue_imbalance_5s_coverage_sec": 1.0,
            "queue_imbalance_5s_samples": 2.0,
        })

        valid, reason = self._validate(metrics)

        self.assertTrue(valid, reason)

    def test_zero_slippage_allows_pre_llm_analysis_but_keeps_cost_status_visible(self) -> None:
        metrics = buy_metrics()
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_BUY_STRICT_GATE": "true",
            "PILUITUS_PAPER_BUY_MIN_FLOW_IMBALANCE": "0.10",
            "PILUITUS_PAPER_COMMISSION_PCT": "0.0354",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "0",
            "PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO": "0.50",
        }, clear=False):
            valid, reason = validate_entry_evidence(
                "BUY", metrics, paper_mode=True,
                require_complete_cost_model=False,
            )

        self.assertTrue(valid, reason)
        self.assertEqual(
            metrics["entry_cost_model_status"], "INCOMPLETE_OR_ZERO_ASSUMPTION"
        )
        self.assertIn("INCOMPLETE_OR_ZERO_ASSUMPTION", reason)

    def test_buy_blocks_incomplete_zero_slippage_cost_model_after_llm(self) -> None:
        valid, reason = self._validate(buy_metrics(), slippage="0")

        self.assertFalse(valid)
        self.assertIn("модель издержек неполная", reason)
        self.assertIn("PILUITUS_PAPER_SLIPPAGE_BPS", reason)

    def test_buy_passes_with_aligned_direction_and_fresh_buyer_flow(self) -> None:
        valid, reason = self._validate(buy_metrics())

        self.assertTrue(valid, reason)
        self.assertIn("PAPER BUY прошёл строгую проверку", reason)

    def test_buy_strict_gate_can_be_disabled_for_controlled_ab_test(self) -> None:
        metrics = buy_metrics()
        metrics["price_vs_vwap_pct"] = 0.01
        metrics["trade_flow_imbalance_30s"] = 0.0

        valid, reason = self._validate(metrics, enabled="false")

        self.assertTrue(valid, reason)

    def test_live_buy_path_is_not_changed_by_paper_directional_gate(self) -> None:
        metrics = buy_metrics()
        metrics["price_vs_vwap_pct"] = 0.01
        metrics["trade_flow_imbalance_30s"] = -0.25

        with patch.dict("os.environ", {
            "PILUITUS_PAPER_BUY_STRICT_GATE": "true",
        }, clear=False):
            valid, reason = validate_entry_evidence("BUY", metrics, paper_mode=False)

        self.assertTrue(valid, reason)


    def test_pre_llm_analysis_can_assess_candidates_over_execution_cost_limit(self) -> None:
        metrics = buy_metrics()
        metrics["bar_atr_pct"] = 0.20
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_BUY_STRICT_GATE": "true",
            "PILUITUS_PAPER_BUY_MIN_FLOW_IMBALANCE": "0.10",
            "PILUITUS_PAPER_COMMISSION_PCT": "0.0354",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "1.0",
            "PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO": "0.50",
            "PILUITUS_MAX_PRE_LLM_COST_TO_ATR_RATIO": "1.25",
        }, clear=False):
            valid, reason = validate_entry_evidence(
                "BUY", metrics,
                paper_mode=True,
                require_complete_cost_model=False,
                pre_llm_evaluation=True,
            )

        self.assertTrue(valid, reason)
        self.assertGreater(metrics["entry_cost_to_atr_ratio"], 0.50)
        self.assertLessEqual(metrics["entry_cost_to_atr_ratio"], 1.25)
        self.assertEqual(metrics["entry_cost_evaluation_stage"], "PRE_LLM_ANALYSIS")
        self.assertIn("окончательный лимит исполнения остаётся 0.50", reason)

    def test_post_llm_gate_still_blocks_same_candidate_over_execution_cost_limit(self) -> None:
        metrics = buy_metrics()
        metrics["bar_atr_pct"] = 0.20
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_BUY_STRICT_GATE": "true",
            "PILUITUS_PAPER_BUY_MIN_FLOW_IMBALANCE": "0.10",
            "PILUITUS_PAPER_COMMISSION_PCT": "0.0354",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "1.0",
            "PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO": "0.50",
            "PILUITUS_MAX_PRE_LLM_COST_TO_ATR_RATIO": "1.25",
        }, clear=False):
            valid, reason = validate_entry_evidence(
                "BUY", metrics,
                paper_mode=True,
                require_complete_cost_model=True,
            )

        self.assertFalse(valid)
        self.assertGreater(metrics.get("entry_cost_to_atr_ratio", 0.0), 0.50)
        self.assertIn("лимит_исполнения=0.50", reason)

    def test_pre_llm_analysis_still_blocks_costs_above_analytical_ceiling(self) -> None:
        metrics = buy_metrics()
        metrics.update({
            "bar_atr_pct": 0.055,
            "spread_pct": 0.01,
            "bid": 99.99,
            "ask": 100.0,
        })
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_BUY_STRICT_GATE": "true",
            "PILUITUS_PAPER_BUY_MIN_FLOW_IMBALANCE": "0.10",
            "PILUITUS_PAPER_COMMISSION_PCT": "0.0354",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "1.0",
            "PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO": "0.50",
            "PILUITUS_MAX_PRE_LLM_COST_TO_ATR_RATIO": "1.25",
        }, clear=False):
            valid, reason = validate_entry_evidence(
                "BUY", metrics,
                paper_mode=True,
                require_complete_cost_model=False,
                pre_llm_evaluation=True,
            )

        self.assertFalse(valid)
        self.assertIn("лимит_этапа=1.25", reason)
        self.assertIn("лимит_исполнения=0.50", reason)


if __name__ == "__main__":
    unittest.main()
