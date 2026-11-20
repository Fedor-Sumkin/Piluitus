"""Tests for LLM/Shadow attribution persisted on PAPER trades."""
import unittest

from execution_layer import _decision_attribution_fields


class PaperDecisionAttributionTests(unittest.TestCase):
    def test_valid_attribution_is_normalized_and_bounded(self):
        fields = _decision_attribution_fields({
            "shadow_candidate_id": "candidate-123",
            "llm_verdict_id": "verdict-456",
            "llm_model_decision": "APPROVE",
            "llm_final_decision": "BUY",
            "llm_confidence_pct": 87.4567,
            "llm_final_confidence_pct": 0.0,
            "llm_signal_status": "APPROVED",
            "llm_reason_code": "TREND_CONFIRMED",
            "llm_local_guard": "LOCAL_EVIDENCE_OK",
            "candidate_direction": "BUY",
            "llm_shadow_linked": True,
        })

        self.assertEqual(fields["shadow_candidate_id"], "candidate-123")
        self.assertEqual(fields["llm_verdict_id"], "verdict-456")
        self.assertEqual(fields["llm_model_decision"], "APPROVE")
        self.assertEqual(fields["llm_final_decision"], "BUY")
        self.assertEqual(fields["llm_confidence_pct"], 87.457)
        self.assertEqual(fields["llm_final_confidence_pct"], 0.0)
        self.assertEqual(fields["llm_reason_code"], "TREND_CONFIRMED")
        self.assertTrue(fields["llm_shadow_linked"])

    def test_pre_and_post_llm_snapshots_are_persisted_with_the_trade(self):
        fields = _decision_attribution_fields({
            "pre_llm_features": {
                "volume_spike": 92.8,
                "bid": 100.0,
                "direction_evidence_state": "ALIGNED",
            },
            "post_llm_features": {
                "volume_spike": 0.07,
                "bid": 99.9,
                "direction_evidence_state": "OPPOSITE",
            },
            "post_llm_evidence_validation": "BLOCKED",
            "post_llm_evidence_reason": "INSTRUMENT_TRADES volume below threshold",
            "post_llm_refresh": {
                "bbo_refresh_status": "PASS",
                "trade_refresh_status": "PASS",
            },
        })

        self.assertEqual(fields["pre_llm_features"]["volume_spike"], 92.8)
        self.assertEqual(fields["post_llm_features"]["volume_spike"], 0.07)
        self.assertEqual(fields["post_llm_features"]["bid"], 99.9)
        self.assertEqual(fields["post_llm_evidence_validation"], "BLOCKED")
        self.assertEqual(fields["post_llm_evidence_reason"], "INSTRUMENT_TRADES volume below threshold")
        self.assertEqual(fields["post_llm_refresh"]["bbo_refresh_status"], "PASS")

    def test_invalid_confidence_and_non_mapping_input_are_safe(self):
        invalid = _decision_attribution_fields({
            "llm_confidence_pct": 101.0,
            "llm_final_confidence_pct": 101.0,
            "llm_model_decision": "APPROVE",
        })
        self.assertIsNone(invalid["llm_confidence_pct"])
        self.assertIsNone(invalid["llm_final_confidence_pct"])
        self.assertEqual(invalid["llm_model_decision"], "APPROVE")

        empty = _decision_attribution_fields(None)
        self.assertIsNone(empty["shadow_candidate_id"])
        self.assertFalse(empty["llm_shadow_linked"])


if __name__ == "__main__":
    unittest.main()
