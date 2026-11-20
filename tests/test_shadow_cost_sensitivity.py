from __future__ import annotations

import unittest

from strategy.shadow_validation import build_shadow_cost_sensitivity


class ShadowCostSensitivityTests(unittest.TestCase):
    @staticmethod
    def labels():
        return [
            {
                "candidate_id": "candidate-1", "side": "BUY", "horizon_sec": 60,
                "result": "OK", "label_quality": "ON_TIME",
                "net_executable_return_pct": 0.10, "slippage_bps": 0.0,
            },
            {
                "candidate_id": "candidate-1", "side": "BUY", "horizon_sec": 60,
                "result": "OK", "label_quality": "DELAYED",
                "net_executable_return_pct": -9.0, "slippage_bps": 0.0,
            },
            {
                "candidate_id": "candidate-2", "side": "SELL", "horizon_sec": 60,
                "result": "OK", "label_quality": "ON_TIME",
                "net_executable_return_pct": -0.04, "slippage_bps": 0.0,
            },
            {
                "candidate_id": "candidate-1", "side": "BUY", "horizon_sec": 180,
                "result": "OK", "label_quality": "ON_TIME",
                "net_executable_return_pct": 0.08, "slippage_bps": 0.0,
            },
            {
                "candidate_id": "candidate-late", "side": "BUY", "horizon_sec": 60,
                "result": "LABEL_LATE", "label_quality": "LABEL_LATE",
            },
        ]

    def test_deduplicates_candidate_horizon_and_reports_side_horizon_cost_stress(self):
        report = build_shadow_cost_sensitivity(self.labels())
        self.assertTrue(report["available"])
        self.assertEqual(report["unique_candidates"], 2)
        self.assertEqual(report["valid_label_rows"], 3)
        zero = report["scenarios"]["0"]
        one = report["scenarios"]["1"]
        two = report["scenarios"]["2"]
        self.assertEqual(zero["by_horizon_side"]["60s|BUY"]["label_rows"], 1)
        self.assertEqual(zero["by_horizon_side"]["60s|BUY"]["candidate_count"], 1)
        self.assertAlmostEqual(zero["overall"]["average_net_return_pct"], (0.10 - 0.04 + 0.08) / 3)
        self.assertAlmostEqual(
            one["overall"]["average_net_return_pct"],
            zero["overall"]["average_net_return_pct"] - 0.02,
        )
        self.assertAlmostEqual(
            two["overall"]["average_net_return_pct"],
            zero["overall"]["average_net_return_pct"] - 0.04,
        )
        self.assertEqual(one["slippage_bps_per_side"], 1.0)
        self.assertIn("correlated", report["warning"])

    def test_missing_or_invalid_labels_do_not_create_false_returns(self):
        report = build_shadow_cost_sensitivity([
            {"candidate_id": "bad", "side": "BUY", "horizon_sec": 60, "result": "OK"},
            {"candidate_id": "late", "side": "SELL", "horizon_sec": 60, "result": "LABEL_LATE"},
        ])
        self.assertFalse(report["available"])
        self.assertEqual(report["valid_label_rows"], 0)

    def test_invalid_scenarios_are_rejected(self):
        self.assertEqual(
            build_shadow_cost_sensitivity(self.labels(), scenarios_bps=(-1.0,))["reason"],
            "INVALID_PARAMETERS",
        )


if __name__ == "__main__":
    unittest.main()
