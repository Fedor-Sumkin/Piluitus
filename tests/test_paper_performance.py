"""Regression tests for realized PAPER trade analytics."""
import unittest

from strategy.paper_performance import PaperPerformanceTracker


def trade(trade_id, pnl, *, side="BUY", ticker="SBER", candidate=None,
          confidence=80.0, exit_time="2026-11-18T10:00:00"):
    return {
        "id": trade_id,
        "status": "CLOSED",
        "mode": "PAPER",
        "side": side,
        "ticker": ticker,
        "pnl": pnl,
        "gross_pnl": pnl + 2.0,
        "total_commission_rub": 2.0,
        "time_in_trade_sec": 60.0,
        "exit_timestamp": exit_time,
        "exit_reason": "TAKE_PROFIT" if pnl > 0 else "STOP_LOSS",
        "shadow_candidate_id": candidate,
        "llm_model_decision": "APPROVE",
        "llm_final_decision": side,
        "llm_confidence_pct": confidence,
        "llm_reason_code": "TREND_CONFIRMED",
    }


class PaperPerformanceTests(unittest.TestCase):
    def test_only_final_closed_records_are_accepted_and_duplicate_ids_are_ignored(self):
        tracker = PaperPerformanceTracker(max_records=10)
        self.assertFalse(tracker.record({**trade("open", 10), "status": "OPEN"}))
        self.assertFalse(tracker.record({**trade("bad", 0), "pnl": "NaN"}))
        self.assertFalse(tracker.record({**trade("live-1", 12), "mode": "LIVE"}))
        self.assertTrue(tracker.record(trade("closed-1", 12)))
        self.assertFalse(tracker.record(trade("closed-1", 999)))
        self.assertEqual(len(tracker.records), 1)
        self.assertEqual(tracker.records[0]["pnl"], 12.0)

    def test_restore_deduplicates_newest_first_records_and_respects_limit(self):
        tracker = PaperPerformanceTracker(max_records=2)
        restored = tracker.restore([
            trade("newest", 3, exit_time="2026-11-18T12:00:00"),
            trade("middle", 2, exit_time="2026-11-18T11:00:00"),
            trade("oldest", 1, exit_time="2026-11-18T10:00:00"),
            trade("middle", 200),
        ])
        self.assertEqual(restored, 3)
        self.assertEqual({row["id"] for row in tracker.records}, {"middle", "newest"})

    def test_scorecard_calculates_net_metrics_groups_and_drawdown(self):
        tracker = PaperPerformanceTracker()
        tracker.restore([
            trade("t3", 25, side="BUY", candidate=None, confidence=95,
                  exit_time="2026-11-18T12:00:00"),
            trade("t2", -50, side="SELL", candidate="c2", confidence=65,
                  exit_time="2026-11-18T11:00:00"),
            trade("t1", 100, side="BUY", candidate="c1", confidence=87.5,
                  exit_time="2026-11-18T10:00:00"),
        ])

        overall = tracker.scorecard()["overall"]
        self.assertEqual(overall["trades"], 3)
        self.assertEqual(overall["wins"], 2)
        self.assertEqual(overall["losses"], 1)
        self.assertAlmostEqual(overall["win_rate_pct"], 66.667, places=3)
        self.assertEqual(overall["net_pnl_rub"], 75.0)
        self.assertEqual(overall["gross_pnl_rub"], 81.0)
        self.assertEqual(overall["expectancy_rub"], 25.0)
        self.assertEqual(overall["profit_factor"], 2.5)
        self.assertEqual(overall["max_drawdown_rub"], 50.0)
        self.assertEqual(overall["max_drawdown_pct_of_prior_peak"], 50.0)
        self.assertEqual(overall["max_consecutive_losses"], 1)
        self.assertEqual(overall["current_loss_streak"], 0)
        scorecard = tracker.scorecard()
        self.assertEqual(scorecard["by_side"]["BUY"]["trades"], 2)
        self.assertEqual(scorecard["by_exit_reason"]["STOP_LOSS"]["trades"], 1)
        self.assertEqual(scorecard["by_confidence"]["80-89"]["trades"], 1)

    def test_shadow_join_uses_candidate_id_and_deduplicates_candidate_horizon(self):
        tracker = PaperPerformanceTracker()
        tracker.restore([
            trade("paper-1", 10, ticker="SBER", candidate="candidate-1"),
            trade("paper-2", -5, ticker="SBER", candidate="candidate-2"),
            trade("paper-3", 2, ticker="GAZP", candidate=None),
        ])
        labels = [
            {"candidate_id": "candidate-1", "ticker": "OTHER", "result": "OK",
             "horizon_sec": 60, "net_executable_return_pct": 0.2},
            {"candidate_id": "candidate-1", "ticker": "OTHER", "result": "OK",
             "horizon_sec": 60, "net_executable_return_pct": 99.0},
            {"candidate_id": "candidate-1", "result": "OK",
             "horizon_sec": 180, "net_executable_return_pct": -0.1},
            {"candidate_id": "candidate-2", "ticker": "SBER", "result": "OK",
             "horizon_sec": 60, "net_executable_return_pct": -0.3},
            {"candidate_id": "candidate-3", "ticker": "GAZP", "result": "OK",
             "horizon_sec": 60, "net_executable_return_pct": 5.0},
            {"candidate_id": "candidate-1", "result": "LABEL_LATE",
             "horizon_sec": 300, "net_executable_return_pct": 7.0},
        ]

        comparison = tracker.scorecard(labels)["shadow_comparison"]
        self.assertEqual(comparison["join_key"], "shadow_candidate_id")
        self.assertEqual(comparison["matched_trades"], 2)
        self.assertEqual(comparison["unmatched_trades"], 1)
        self.assertEqual(comparison["matched_realized_net_pnl_rub"], 5.0)
        self.assertEqual(len(comparison["matched_trade_details"]), 2)
        self.assertEqual(
            comparison["matched_trade_details"][0]["shadow_net_return_pct_by_horizon"]["60"],
            0.2,
        )
        self.assertEqual(comparison["horizons"]["60"]["labels"], 2)
        self.assertAlmostEqual(comparison["horizons"]["60"]["mean_net_return_pct"], -0.05)
        self.assertEqual(comparison["horizons"]["180"]["labels"], 1)
        self.assertNotIn("300", comparison["horizons"])


if __name__ == "__main__":
    unittest.main()
