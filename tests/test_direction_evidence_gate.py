import unittest
from unittest.mock import patch

from strategy.signal_guard import (
    classify_direction_evidence,
    infer_candidate_direction,
    validate_signal_evidence,
    validate_pre_llm_entry_evidence,
)


class DirectionEvidenceGateTests(unittest.TestCase):
    def test_aligned_evidence_is_classified(self):
        self.assertEqual(
            classify_direction_evidence(
                "BUY", {"price_change_pct": 0.12, "price_vs_vwap_pct": 0.08}
            ),
            "ALIGNED",
        )

    def test_neutral_vwap_does_not_block_price_momentum_candidate(self):
        metrics = {"price_change_pct": 0.12, "price_vs_vwap_pct": 0.01}
        with patch.dict("os.environ", {
            "PILUITUS_DIRECTION_MOMENTUM_NEUTRAL_PCT": "0.01",
            "PILUITUS_DIRECTION_VWAP_NEUTRAL_PCT": "0.03",
        }):
            self.assertEqual(classify_direction_evidence("BUY", metrics), "NEUTRAL")
            self.assertEqual(infer_candidate_direction(metrics), "BUY")

    def test_subthreshold_price_noise_does_not_create_candidate(self):
        metrics = {"price_change_pct": 0.005, "price_vs_vwap_pct": 0.01}
        self.assertIsNone(infer_candidate_direction(metrics))

    def test_strong_opposing_vwap_blocks_candidate(self):
        metrics = {"price_change_pct": 0.12, "price_vs_vwap_pct": -0.08}
        self.assertEqual(classify_direction_evidence("BUY", metrics), "OPPOSITE")
        self.assertIsNone(infer_candidate_direction(metrics))

    def test_neutral_vwap_passes_entry_evidence_gate(self):
        metrics = {
            "price_change_pct": 0.12,
            "price_vs_vwap_pct": 0.01,
            "volume_spike": 2.0,
            "spread_pct": 0.08,
            "spread_available": 1.0,
            "bid_ask_available": 1.0,
            "trade_data_mode": 0.0,
        }
        ok, reason = validate_signal_evidence("BUY", metrics)
        self.assertTrue(ok, reason)
        self.assertIn("VWAP нейтрален", reason)

    def test_strong_opposing_vwap_fails_entry_evidence_gate(self):
        metrics = {
            "price_change_pct": 0.12,
            "price_vs_vwap_pct": -0.08,
            "volume_spike": 2.0,
            "spread_pct": 0.08,
            "spread_available": 1.0,
            "bid_ask_available": 1.0,
            "trade_data_mode": 0.0,
        }
        ok, reason = validate_signal_evidence("BUY", metrics)
        self.assertFalse(ok)
        self.assertIn("сильное встречное", reason)

    def test_fresh_matching_m1_pattern_is_not_vetoed_by_neutral_vwap(self):
        import time
        metrics = {
            "price_change_pct": 0.12,
            "price_vs_vwap_pct": 0.01,
            "volume_spike": 2.0,
            "spread_pct": 0.08,
            "spread_available": 1.0,
            "bid_ask_available": 1.0,
            "trade_data_mode": 0.0,
            "bar_pattern_confirmed": 1.0,
            "bar_pattern_name": "M1_RANGE_BREAKOUT_BUY",
            "bar_pattern_direction_code": 1.0,
            "bar_breakout_up": 1.0,
            "_bar_pattern_signal_at_monotonic": time.monotonic(),
        }
        ok, reason = validate_signal_evidence("BUY", metrics)
        self.assertTrue(ok, reason)
        self.assertIn("VWAP нейтрален", reason)

    def test_paper_pre_llm_gate_rejects_confirmed_opposite_closed_m1_trend(self):
        metrics = {
            "bid": 99.9,
            "ask": 100.1,
            "bid_ask_available": 1.0,
            "spread_available": 1.0,
            "bid_quote_age_sec": 0.1,
            "ask_quote_age_sec": 0.1,
            "bbo_max_age_sec": 2.0,
            "bar_atr_periods": 14,
            "bar_atr_age_sec": 1.0,
            "bar_count": 30,
            "bar_trend_up": 0.0,
            "bar_trend_down": 1.0,
        }

        allowed, reason = validate_pre_llm_entry_evidence(
            "BUY", metrics, paper_mode=True
        )

        self.assertFalse(allowed)
        self.assertIn("закрытыми M1-свечами", reason)
        self.assertIn("BUY", reason)

    def test_pre_llm_m1_gate_is_paper_only_and_does_not_block_aligned_trend(self):
        metrics = {
            "bid": 99.9,
            "ask": 100.1,
            "bid_ask_available": 1.0,
            "spread_available": 1.0,
            "bid_quote_age_sec": 0.1,
            "ask_quote_age_sec": 0.1,
            "bbo_max_age_sec": 2.0,
            "bar_atr_periods": 14,
            "bar_atr_age_sec": 1.0,
            "bar_count": 30,
            "bar_trend_up": 1.0,
            "bar_trend_down": 0.0,
        }

        paper_allowed, paper_reason = validate_pre_llm_entry_evidence(
            "BUY", metrics, paper_mode=True
        )
        non_paper_allowed, non_paper_reason = validate_pre_llm_entry_evidence(
            "BUY",
            {**metrics, "bar_trend_up": 0.0, "bar_trend_down": 1.0},
            paper_mode=False,
        )

        self.assertTrue(paper_allowed, paper_reason)
        self.assertTrue(non_paper_allowed, non_paper_reason)

    def test_sell_direction_is_symmetric(self):
        neutral = {"price_change_pct": -0.10, "price_vs_vwap_pct": -0.01}
        opposite = {"price_change_pct": -0.10, "price_vs_vwap_pct": 0.08}
        self.assertEqual(classify_direction_evidence("SELL", neutral), "NEUTRAL")
        self.assertEqual(infer_candidate_direction(neutral), "SELL")
        self.assertEqual(classify_direction_evidence("SELL", opposite), "OPPOSITE")
        self.assertIsNone(infer_candidate_direction(opposite))


if __name__ == "__main__":
    unittest.main()
