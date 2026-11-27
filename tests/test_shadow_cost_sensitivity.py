from __future__ import annotations

import unittest

from strategy.shadow_validation import build_shadow_cost_sensitivity, build_shadow_cost_atr_gate_sensitivity


class ShadowCostSensitivityTests(unittest.TestCase):
    @staticmethod
    def labels():
        return [
            {
                "candidate_id": "candidate-1", "side": "BUY", "horizon_sec": 60,
                "result": "OK", "label_quality": "ON_TIME",
                "net_executable_return_pct": 0.10, "slippage_bps": 0.0,
                "entry_executable_price": 100.0, "exit_executable_price": 100.2,
                "commission_pct_per_side": 0.0354,
            },
            {
                "candidate_id": "candidate-1", "side": "BUY", "horizon_sec": 60,
                "result": "OK", "label_quality": "DELAYED",
                "net_executable_return_pct": -9.0, "slippage_bps": 0.0,
                "entry_executable_price": 100.0, "exit_executable_price": 100.2,
                "commission_pct_per_side": 0.0354,
            },
            {
                "candidate_id": "candidate-2", "side": "SELL", "horizon_sec": 60,
                "result": "OK", "label_quality": "ON_TIME",
                "net_executable_return_pct": -0.04, "slippage_bps": 0.0,
                "entry_executable_price": 100.0, "exit_executable_price": 100.1,
                "commission_pct_per_side": 0.0354,
            },
            {
                "candidate_id": "candidate-1", "side": "BUY", "horizon_sec": 180,
                "result": "OK", "label_quality": "ON_TIME",
                "net_executable_return_pct": 0.08, "slippage_bps": 0.0,
                "entry_executable_price": 100.0, "exit_executable_price": 100.15,
                "commission_pct_per_side": 0.0354,
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
        entry_fill = 100.0 * (1.0 + 1.0 / 10000.0)
        exit_fill = 100.2 * (1.0 - 1.0 / 10000.0)
        fee = (entry_fill + exit_fill) * 0.0354 / 100.0
        expected_buy_net = ((exit_fill - entry_fill) - fee) / entry_fill * 100.0
        self.assertAlmostEqual(
            one["by_horizon_side"]["60s|BUY"]["average_net_return_pct"],
            expected_buy_net,
            places=6,
        )
        self.assertLess(one["overall"]["average_net_return_pct"], zero["overall"]["average_net_return_pct"])
        self.assertLess(two["overall"]["average_net_return_pct"], one["overall"]["average_net_return_pct"])
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


    def test_cost_atr_gate_sensitivity_compares_horizons_without_repricing(self) -> None:
        labels = [
            {"candidate_id": "a", "side": "BUY", "horizon_sec": horizon,
             "result": "OK", "label_quality": "ON_TIME",
             "net_executable_return_pct": 0.10, "cost_to_atr_ratio": 0.30}
            for horizon in (60, 180, 300, 600)
        ] + [
            {"candidate_id": "b", "side": "SELL", "horizon_sec": horizon,
             "result": "OK", "label_quality": "ON_TIME",
             "net_executable_return_pct": -0.05, "cost_to_atr_ratio": 0.75}
            for horizon in (60, 180, 300, 600)
        ]
        report = build_shadow_cost_atr_gate_sensitivity(labels)
        self.assertTrue(report["available"])
        self.assertTrue(report["active_gate_unchanged"])
        self.assertEqual(report["thresholds"]["0.50"]["included_candidates"], 1)
        self.assertEqual(report["thresholds"]["1.00"]["included_candidates"], 2)
        self.assertEqual(report["thresholds"]["0.50"]["by_horizon"]["60s"]["overall"]["valid_labels"], 1)
        self.assertIn("600s", report["thresholds"]["0.50"]["by_horizon"])


if __name__ == "__main__":
    unittest.main()
