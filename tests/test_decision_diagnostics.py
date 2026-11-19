from __future__ import annotations

import unittest

from strategy.decision_diagnostics import (
    build_llm_feature_snapshot,
    classify_hold_reason_bucket,
)


class DecisionDiagnosticsTests(unittest.TestCase):
    def test_pre_and_post_llm_snapshots_keep_their_own_metric_values(self):
        pre = build_llm_feature_snapshot({
            "volume_spike": 92.8,
            "trade_data_ready": 1,
            "bar_pattern_name": "M1_RANGE_BREAKOUT_BUY",
            "bar_atr": 1.25,
            "direction_evidence_state": "ALIGNED",
        })
        post = build_llm_feature_snapshot({
            "volume_spike": 0.07,
            "trade_data_ready": 1,
            "bar_pattern_name": "M1_RANGE_BREAKOUT_BUY",
            "bar_atr": 1.25,
            "direction_evidence_state": "ALIGNED",
        })
        self.assertEqual(pre["volume_spike"], 92.8)
        self.assertEqual(post["volume_spike"], 0.07)
        self.assertEqual(pre["m1_pattern"], "M1_RANGE_BREAKOUT_BUY")
        self.assertEqual(post["m1_atr"], 1.25)

    def test_invalid_metrics_are_json_safe(self):
        snapshot = build_llm_feature_snapshot({
            "volume_spike": float("nan"),
            "trade_data_ready": True,
        })
        self.assertIsNone(snapshot["volume_spike"])
        self.assertIsNone(snapshot["trade_data_ready"])
        self.assertEqual(snapshot["direction_evidence_state"], "UNAVAILABLE")

    def test_hold_reason_buckets_cover_known_families(self):
        cases = {
            "HOLD_M1_CONTRADICTION": "m1",
            "HOLD_INSUFFICIENT_SUPPORT": "support",
            "HOLD_RISK_REWARD": "risk_reward",
            "HOLD_WEAK_DIRECTION": "direction",
            "HOLD_WEAK_VOLUME": "volume",
            "HOLD_WIDE_SPREAD": "spread",
            "HOLD_STALE_BBO": "stale",
            "NOT_PROVIDED": "other",
        }
        for reason, expected in cases.items():
            with self.subTest(reason=reason):
                self.assertEqual(classify_hold_reason_bucket(reason), expected)


if __name__ == "__main__":
    unittest.main()
