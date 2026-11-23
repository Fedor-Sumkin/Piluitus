from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from strategy.shadow_validation import build_shadow_walk_forward_validation


class ShadowWalkForwardValidationTests(unittest.TestCase):
    @staticmethod
    def labels(candidate_count=20):
        start = datetime(2026, 11, 1, 9, 0, tzinfo=timezone.utc)
        rows = []
        for index in range(candidate_count):
            entry = (start + timedelta(seconds=index * 300)).isoformat()
            for horizon, result in ((60, 0.12 if index % 3 else -0.08), (600, -0.04 if index % 4 else 0.16)):
                rows.append({
                    "candidate_id": f"candidate-{index}",
                    "ticker": "SBER" if index % 2 else "GAZP",
                    "side": "BUY" if index % 2 else "SELL",
                    "entry_time_utc": entry,
                    "entry_time_msk": (start + timedelta(seconds=index * 300)).astimezone(
                        timezone(timedelta(hours=3))
                    ).isoformat(),
                    "horizon_sec": horizon,
                    "market_regime": "TREND" if index % 2 else "RANGE_OR_MIXED",
                    "volatility_regime": "HIGH_VOLATILITY" if index % 3 == 0 else "NORMAL_VOLATILITY",
                    "liquidity_regime": "LOW_LIQUIDITY" if index % 4 == 0 else "NORMAL_OR_UNKNOWN",
                    "directional_regime": "TREND_UP" if index % 2 else "RANGE_OR_MIXED",
                    "llm_confidence": 80.0 + (index % 20),
                    "shadow_eligible": index % 2 == 0,
                    "shadow_v2_eligible": index % 3 == 0,
                    "shadow_v2_score": 72.0 + (index % 25),
                    "llm_model_decision": "APPROVE" if index % 2 else "HOLD",
                    "result": "OK",
                    "label_quality": "ON_TIME",
                    "net_executable_return_pct": result,
                })
        return rows

    def test_builds_expanding_folds_and_purges_overlapping_candidates(self):
        report = build_shadow_walk_forward_validation(
            self.labels(), folds=5, embargo_seconds=30.0, max_horizon_seconds=600
        )
        self.assertTrue(report["available"])
        self.assertEqual(report["folds_evaluated"], 4)
        self.assertEqual(report["unique_candidates"], 20)
        self.assertEqual(report["purge_window_seconds"], 630.0)
        first = report["folds"][0]
        self.assertEqual(first["test_candidates"], 4)
        self.assertGreater(first["purged_candidates"], 0)
        self.assertGreater(first["train_candidates"], 0)
        self.assertEqual(
            first["test_by_horizon_side"]["60s|BUY"]["valid_labels"], 2
        )
        self.assertEqual(
            first["test_by_horizon_side"]["600s|SELL"]["valid_labels"], 2
        )
        self.assertIn("60s|BUY", report["out_of_sample_by_horizon_side"])
        self.assertIn(
            "60s|BUY|market_regime=TREND",
            report["folds"][0]["test_by_horizon_side_market_regime"],
        )
        self.assertIn("liquidity_regime=LOW_LIQUIDITY", report["folds"][0]["test_by_liquidity_regime"])

    def test_same_candidate_horizons_never_cross_time_partitions(self):
        report = build_shadow_walk_forward_validation(
            self.labels(), folds=4, embargo_seconds=0.0, max_horizon_seconds=600
        )
        for fold in report["folds"]:
            self.assertLessEqual(
                fold["train_candidates"] + fold["purged_candidates"],
                report["unique_candidates"],
            )
            self.assertGreater(fold["test_candidates"], 0)
            if fold["train_end_utc"]:
                train_end = datetime.fromisoformat(fold["train_end_utc"])
                test_start = datetime.fromisoformat(fold["test_start_utc"])
                self.assertLessEqual(
                    train_end.timestamp() + report["purge_window_seconds"],
                    test_start.timestamp(),
                )

    def test_duplicate_candidate_horizon_keeps_best_quality_row(self):
        rows = self.labels(12)
        duplicate = dict(rows[0])
        duplicate["net_executable_return_pct"] = -99.0
        duplicate["label_quality"] = "DELAYED"
        rows.append(duplicate)
        report = build_shadow_walk_forward_validation(rows, folds=3)
        self.assertEqual(
            report["deduplicated_candidate_horizon_rows"], 24
        )
        self.assertEqual(report["unique_candidates"], 12)

    def test_invalid_timestamps_and_insufficient_history_are_explicit(self):
        invalid = build_shadow_walk_forward_validation([
            {
                "candidate_id": "naive", "side": "BUY", "horizon_sec": 60,
                "entry_time_utc": "2026-11-18T10:00:00",
                "result": "OK", "net_executable_return_pct": 1.0,
            }
        ])
        self.assertFalse(invalid["available"])
        self.assertEqual(invalid["reason"], "NO_TIMESTAMPED_CANDIDATES")

        short = build_shadow_walk_forward_validation(self.labels(2))
        self.assertFalse(short["available"])
        self.assertEqual(short["reason"], "INSUFFICIENT_DISTINCT_TIME_POINTS")


    def test_walk_forward_reports_out_of_sample_confidence_cohorts(self):
        report = build_shadow_walk_forward_validation(self.labels(30), folds=5)
        self.assertTrue(report["available"])
        confidence_groups = report["out_of_sample_by_horizon_side_confidence"]
        decision_groups = report["out_of_sample_by_horizon_side_decision_confidence"]
        self.assertTrue(confidence_groups)
        self.assertTrue(decision_groups)
        sample = next(iter(confidence_groups.values()))
        self.assertIn("positive_rate_wilson_95_pct", sample)
        self.assertIn("mean_model_confidence_pct", sample)
        self.assertLess(
            sample["positive_rate_wilson_95_pct"][0],
            sample["positive_rate_wilson_95_pct"][1],
        )
        self.assertTrue(any("|llm_model_decision=" in key for key in decision_groups))
        shadow_groups = report["out_of_sample_by_horizon_side_shadow_comparison"]
        self.assertTrue(shadow_groups)
        self.assertTrue(any("|shadow_eligible=" in key for key in shadow_groups))
        self.assertTrue(any("|shadow_v2_eligible=" in key for key in shadow_groups))
        first_fold_shadow = report["folds"][0]["test_by_horizon_side_shadow_comparison"]
        self.assertTrue(first_fold_shadow)


    def test_unusable_labels_are_reported_as_coverage_not_returns(self):
        rows = self.labels(12)
        start = datetime(2026, 11, 1, 9, 0, tzinfo=timezone.utc)
        rows.append({
            "candidate_id": "candidate-no-bbo",
            "side": "BUY",
            "ticker": "SBER",
            "entry_time_utc": (start + timedelta(seconds=12 * 300)).isoformat(),
            "horizon_sec": 60,
            "result": "NO_FRESH_BBO",
            "label_quality": "NO_FRESH_BBO",
        })
        report = build_shadow_walk_forward_validation(rows, folds=3)
        coverage = report["out_of_sample_by_horizon_side"]["60s|BUY"]
        self.assertEqual(coverage["label_status_counts"]["NO_FRESH_BBO"], 1)
        self.assertEqual(coverage["rows_without_valid_net_return"], 1)
        self.assertEqual(coverage["valid_labels"], 4)

    def test_invalid_parameters_are_rejected(self):
        self.assertEqual(
            build_shadow_walk_forward_validation(self.labels(), folds=2)["reason"],
            "INVALID_PARAMETERS",
        )
        self.assertEqual(
            build_shadow_walk_forward_validation(self.labels(), embargo_seconds=-1)["reason"],
            "INVALID_PARAMETERS",
        )


if __name__ == "__main__":
    unittest.main()
