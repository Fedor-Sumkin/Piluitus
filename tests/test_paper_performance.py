"""Regression tests for realized PAPER trade analytics."""
import unittest

from strategy.paper_performance import PaperPerformanceTracker


def trade(trade_id, pnl, *, side="BUY", ticker="SBER", candidate=None,
          confidence=80.0, exit_time="2026-11-18T10:00:00", session="session-1"):
    return {
        "id": trade_id,
        "status": "CLOSED",
        "mode": "PAPER",
        "session_id": session,
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

    def test_explicit_session_reset_clears_in_memory_performance(self):
        tracker = PaperPerformanceTracker()
        tracker.record(trade("reset-me", 10))
        self.assertEqual(tracker.clear(), 1)
        self.assertEqual(tracker.records, [])
        self.assertEqual(tracker.scorecard()["overall"]["trades"], 0)

    def test_restore_deduplicates_newest_first_records_and_respects_limit(self):
        tracker = PaperPerformanceTracker(max_records=2)
        restored = tracker.restore([
            trade("newest", 3, exit_time="2026-11-18T12:00:00"),
            trade("middle", 2, exit_time="2026-11-18T11:00:00"),
            trade("oldest", 1, exit_time="2026-11-18T10:00:00"),
            trade("middle", 200),
        ])
        self.assertEqual(restored, 3)
        self.assertEqual(tracker.restore([
            trade("newest", 3, exit_time="2026-11-18T12:00:00"),
            trade("middle", 2, exit_time="2026-11-18T11:00:00"),
        ]), 0)
        self.assertEqual({row["id"] for row in tracker.records}, {"middle", "newest"})

    def test_scorecard_adds_ticker_side_time_and_point_in_time_regime_cohorts(self):
        tracker = PaperPerformanceTracker()
        first = trade("cohort-1", 12, ticker="SBER", side="BUY")
        first.update({
            "entry_timestamp": "2026-11-18T10:30:00+03:00",
            "market_regime": "TREND",
            "volatility_regime": "LOW",
            "liquidity_regime": "GOOD",
            "directional_regime": "TREND_UP",
        })
        second = trade("cohort-2", -4, ticker="SBER", side="SELL")
        second.update({
            "entry_timestamp": "2026-11-18T19:15:00+03:00",
            "market_regime": "RANGE_OR_MIXED",
            "volatility_regime": "HIGH",
            "liquidity_regime": "WIDE_SPREAD",
            "directional_regime": "TREND_DOWN",
        })
        legacy = trade("cohort-3", 1, ticker="GAZP", side="BUY")
        legacy["entry_timestamp"] = "2026-11-18T10:30:00"
        tracker.restore([first, second, legacy])

        scorecard = tracker.scorecard()
        self.assertEqual(scorecard["by_ticker_side"]["SBER|BUY"]["trades"], 1)
        self.assertEqual(scorecard["by_ticker_side"]["SBER|SELL"]["net_pnl_rub"], -4.0)
        self.assertEqual(scorecard["by_moscow_entry_time"]["OPEN_10_11"]["trades"], 1)
        self.assertEqual(scorecard["by_moscow_entry_time"]["EVENING_18_23"]["trades"], 1)
        self.assertEqual(scorecard["by_moscow_entry_time"]["UNKNOWN"]["trades"], 1)
        self.assertEqual(scorecard["by_market_regime"]["TREND"]["trades"], 1)
        self.assertEqual(scorecard["by_volatility_regime"]["HIGH"]["trades"], 1)
        self.assertEqual(scorecard["by_liquidity_regime"]["GOOD"]["trades"], 1)
        self.assertEqual(scorecard["by_directional_regime"]["TREND_DOWN"]["trades"], 1)
        self.assertEqual(scorecard["by_market_regime"]["TREND"]["sample_quality"], "INSUFFICIENT_LT_30")
        interval = scorecard["by_ticker_side"]["SBER|BUY"]["win_rate_95_wilson_pct"]
        self.assertIsInstance(interval, list)
        self.assertEqual(len(interval), 2)
        self.assertLessEqual(interval[0], 100.0)
        self.assertGreaterEqual(interval[1], 0.0)

    def test_legacy_cohorts_report_unknown_regimes_without_inference(self):
        tracker = PaperPerformanceTracker()
        tracker.restore([trade("legacy-regime", 2)])
        scorecard = tracker.scorecard()
        self.assertEqual(scorecard["by_market_regime"]["UNKNOWN"]["trades"], 1)
        self.assertEqual(scorecard["by_volatility_regime"]["UNKNOWN"]["trades"], 1)
        self.assertEqual(scorecard["by_liquidity_regime"]["UNKNOWN"]["trades"], 1)
        self.assertEqual(scorecard["by_directional_regime"]["UNKNOWN"]["trades"], 1)
        self.assertEqual(scorecard["overall"]["sample_quality"], "INSUFFICIENT_LT_30")
        self.assertEqual(len(scorecard["overall"]["win_rate_95_wilson_pct"]), 2)

    def test_scorecard_calculates_net_metrics_groups_and_drawdown(self):
        tracker = PaperPerformanceTracker()
        tracker.restore([
            trade("t3", 25, side="BUY", candidate=None, confidence=95,
                  exit_time="2026-11-18T12:00:00"),
            trade("t2", -50, side="SELL", candidate="c2", confidence=65,
                  exit_time="2026-11-18T11:00:00", session="session-2"),
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
        self.assertEqual(scorecard["by_session"]["session-2"]["trades"], 1)
        self.assertEqual(scorecard["by_exit_reason"]["STOP_LOSS"]["trades"], 1)
        self.assertEqual(scorecard["by_confidence"]["80-89"]["trades"], 1)

    def test_scorecard_aggregates_exit_bbo_gaps_overall_and_by_ticker(self):
        tracker = PaperPerformanceTracker()
        tracker.restore([
            {
                **trade("gap-2", -2, ticker="GAZP", exit_time="2026-11-18T11:00:00"),
                "exit_bbo_gap_count": 1,
                "total_exit_bbo_gap_sec": 4.0,
                "max_exit_bbo_gap_sec": 4.0,
                "current_exit_bbo_gap_sec": 0.0,
            },
            {
                **trade("gap-1", 3, ticker="SBER", exit_time="2026-11-18T10:00:00"),
                "exit_bbo_gap_count": 2,
                "total_exit_bbo_gap_sec": 3.5,
                "max_exit_bbo_gap_sec": 2.5,
                "last_exit_bbo_gap_sec": 1.5,
                "current_exit_bbo_gap_sec": 0.0,
            },
        ])

        scorecard = tracker.scorecard()
        overall = scorecard["overall"]
        self.assertEqual(overall["trades_with_exit_bbo_gap"], 2)
        self.assertEqual(overall["exit_bbo_gap_trade_rate_pct"], 100.0)
        self.assertEqual(overall["exit_bbo_gap_episodes"], 3)
        self.assertEqual(overall["total_exit_bbo_gap_sec"], 7.5)
        self.assertEqual(overall["average_exit_bbo_gap_sec"], 2.5)
        self.assertEqual(overall["max_exit_bbo_gap_sec"], 4.0)
        self.assertEqual(
            scorecard["by_ticker"]["SBER"]["total_exit_bbo_gap_sec"], 3.5
        )
        self.assertEqual(
            scorecard["by_ticker"]["GAZP"]["exit_bbo_gap_episodes"], 1
        )

    def test_scorecard_reports_zero_exit_bbo_gaps_when_no_gap_metadata_exists(self):
        tracker = PaperPerformanceTracker()
        tracker.record(trade("no-gap", 1))
        overall = tracker.scorecard()["overall"]
        self.assertEqual(overall["trades_with_exit_bbo_gap"], 0)
        self.assertEqual(overall["exit_bbo_gap_episodes"], 0)
        self.assertEqual(overall["total_exit_bbo_gap_sec"], 0.0)
        self.assertEqual(overall["max_exit_bbo_gap_sec"], 0.0)
        self.assertIsNone(overall["average_exit_bbo_gap_sec"])

    def test_shadow_join_reports_distinct_unmatched_attribution_reasons(self):
        tracker = PaperPerformanceTracker()
        tracker.restore([
            trade("matched", 5, candidate="candidate-ok"),
            trade("no-id", 1, candidate=None),
            trade("pending", -2, candidate="candidate-pending"),
            trade("no-valid-label", -3, candidate="candidate-no-fresh"),
        ])
        labels = [
            {"candidate_id": "candidate-ok", "result": "OK",
             "horizon_sec": 60, "net_executable_return_pct": 0.12},
            {"candidate_id": "candidate-no-fresh", "result": "NO_FRESH_BBO",
             "horizon_sec": 60},
        ]

        comparison = tracker.scorecard(labels)["shadow_comparison"]
        self.assertEqual(comparison["matched_trades"], 1)
        self.assertEqual(comparison["unmatched_trades"], 3)
        self.assertEqual(comparison["matched_trade_rate_pct"], 25.0)
        self.assertEqual(comparison["unmatched_by_reason"], {
            "NO_SHADOW_CANDIDATE_ID": 1,
            "CANDIDATE_NO_LABEL_RECORD": 1,
            "CANDIDATE_LABEL_NO_FRESH_BBO": 1,
        })

    def test_shadow_join_uses_candidate_id_and_deduplicates_candidate_horizon(self):
        tracker = PaperPerformanceTracker()
        tracker.restore([
            trade("paper-3", 2, ticker="GAZP", candidate=None,
                  exit_time="2026-11-18T12:00:00"),
            trade("paper-2", -5, ticker="SBER", candidate="candidate-2",
                  exit_time="2026-11-18T11:00:00"),
            trade("paper-1-duplicate", 4, ticker="SBER", candidate="candidate-1",
                  exit_time="2026-11-18T10:01:00"),
            trade("paper-1", 10, ticker="SBER", candidate="candidate-1",
                  exit_time="2026-11-18T10:00:00"),
        ])
        labels = [
            {"candidate_id": "candidate-1", "ticker": "OTHER", "result": "OK",
             "horizon_sec": 60, "net_executable_return_pct": 0.2,
             "mfe_executable_pct": 0.4, "mae_executable_pct": -0.2,
             "time_to_mfe_sec": 25, "time_to_mae_sec": 35,
             "valid_bbo_observations": 10, "bbo_observation_span_sec": 50},
            {"candidate_id": "candidate-1", "ticker": "OTHER", "result": "OK",
             "horizon_sec": 60, "net_executable_return_pct": 99.0},
            {"candidate_id": "candidate-1", "result": "OK",
             "horizon_sec": 180, "net_executable_return_pct": -0.1},
            {"candidate_id": "candidate-2", "ticker": "SBER", "result": "OK",
             "horizon_sec": 60, "net_executable_return_pct": -0.3,
             "mfe_executable_pct": 0.2, "mae_executable_pct": -0.4,
             "time_to_mfe_sec": 30, "time_to_mae_sec": 40,
             "valid_bbo_observations": 20, "bbo_observation_span_sec": 60},
            {"candidate_id": "candidate-3", "ticker": "GAZP", "result": "OK",
             "horizon_sec": 60, "net_executable_return_pct": 5.0},
            {"candidate_id": "candidate-1", "result": "LABEL_LATE",
             "horizon_sec": 300, "net_executable_return_pct": 7.0},
        ]

        comparison = tracker.scorecard(labels)["shadow_comparison"]
        self.assertEqual(comparison["join_key"], "shadow_candidate_id")
        self.assertEqual(comparison["matched_trades"], 3)
        self.assertEqual(comparison["unmatched_trades"], 1)
        self.assertEqual(comparison["matched_realized_net_pnl_rub"], 9.0)
        self.assertEqual(len(comparison["matched_trade_details"]), 3)
        self.assertEqual(
            comparison["matched_trade_details"][0]["shadow_net_return_pct_by_horizon"]["60"],
            0.2,
        )
        self.assertEqual(comparison["horizons"]["60"]["labels"], 2)
        self.assertAlmostEqual(comparison["horizons"]["60"]["mean_net_return_pct"], -0.05)
        self.assertAlmostEqual(comparison["horizons"]["60"]["average_mfe_net_pct"], 0.3)
        self.assertAlmostEqual(comparison["horizons"]["60"]["average_mae_net_pct"], -0.3)
        self.assertEqual(comparison["horizons"]["60"]["average_time_to_mfe_sec"], 27.5)
        self.assertEqual(comparison["horizons"]["60"]["average_time_to_mae_sec"], 37.5)
        self.assertEqual(comparison["horizons"]["60"]["average_valid_bbo_observations"], 15.0)
        self.assertEqual(comparison["horizons"]["60"]["average_bbo_observation_span_sec"], 55.0)
        self.assertEqual(comparison["horizons"]["180"]["labels"], 1)
        self.assertNotIn("300", comparison["horizons"])


    def test_shadow_comparison_exposes_horizon_side_decision_confidence_segments(self):
        tracker = PaperPerformanceTracker()
        tracker.restore([
            trade("paper-buy", 12, side="BUY", candidate="candidate-buy",
                  confidence=87.0, exit_time="2026-11-18T10:00:00"),
            trade("paper-sell", -4, side="SELL", candidate="candidate-sell",
                  confidence=92.0, exit_time="2026-11-18T10:01:00"),
        ])
        labels = [
            {
                "candidate_id": "candidate-buy", "result": "OK", "horizon_sec": 60,
                "side": "BUY", "net_executable_return_pct": 0.20,
                "mfe_executable_pct": 0.35, "mae_executable_pct": -0.08,
                "llm_model_decision": "APPROVE", "llm_final_decision": "BUY",
                "llm_confidence": 87.0, "llm_execution_status": "EXECUTED",
            },
            {
                "candidate_id": "candidate-sell", "result": "OK", "horizon_sec": 60,
                "side": "SELL", "net_executable_return_pct": -0.10,
                "mfe_executable_pct": 0.05, "mae_executable_pct": -0.22,
                "llm_model_decision": "HOLD", "llm_final_decision": "HOLD",
                "llm_confidence": 92.0, "llm_execution_status": "NOT_EXECUTED",
            },
            {
                "candidate_id": "candidate-buy", "result": "OK", "horizon_sec": 180,
                "side": "BUY", "net_executable_return_pct": 0.30,
                "llm_model_decision": "APPROVE", "llm_final_decision": "BUY",
                "llm_confidence": 87.0, "llm_execution_status": "EXECUTED",
            },
        ]

        comparison = tracker.scorecard(labels)["shadow_comparison"]
        segments = comparison["horizon_segments"]
        self.assertEqual(segments["by_side"]["60s|BUY"]["labels"], 1)
        self.assertEqual(segments["by_side"]["60s|SELL"]["labels"], 1)
        self.assertEqual(
            segments["by_model_decision"]["60s|APPROVE"]["mean_net_return_pct"], 0.2
        )
        self.assertEqual(
            segments["by_model_decision"]["60s|HOLD"]["mean_net_return_pct"], -0.1
        )
        self.assertEqual(
            segments["by_confidence"]["60s|80-89"]["positive_rate_pct"], 100.0
        )
        self.assertEqual(
            segments["by_confidence"]["60s|90-99"]["positive_rate_pct"], 0.0
        )
        self.assertEqual(
            segments["by_model_decision_confidence"]["60s|APPROVE|80-89"]["labels"], 1
        )
        self.assertEqual(
            segments["by_execution_status"]["60s|EXECUTED"]["labels"], 1
        )
        self.assertEqual(
            segments["by_execution_status"]["60s|NOT_EXECUTED"]["labels"], 1
        )
        self.assertIn("hypothetical Shadow outcomes", comparison["horizon_segment_note"])
        self.assertNotIn("180s|SELL", segments["by_side"])



    def test_loss_diagnostics_reports_overlapping_evidence_and_missing_data(self):
        tracker = PaperPerformanceTracker()
        tracker.restore([
            {
                **trade("gross-loss", -12),
                "gross_pnl": -8.0,
                "total_commission_rub": 4.0,
                "max_favorable_excursion_pct": 0.0,
                "exit_bbo_gap_count": 1,
                "total_exit_bbo_gap_sec": 2.5,
                "trigger_to_executable_bbo_delay_sec": 1.2,
                "exit_spread_pct": 0.08,
            },
            {
                **trade("cost-loss", -3),
                "gross_pnl": 2.0,
                "total_commission_rub": 5.0,
                "max_favorable_excursion_pct": 0.15,
                "exit_bbo_gap_count": 0,
                "total_exit_bbo_gap_sec": 0.0,
                "trigger_to_executable_bbo_delay_sec": 0.0,
                "exit_spread_pct": 0.04,
            },
            {
                **trade("unknown-loss", -1),
                "gross_pnl": None,
                "total_commission_rub": None,
                "max_favorable_excursion_pct": None,
            },
        ])
        diagnostics = tracker.scorecard()["loss_diagnostics"]
        self.assertEqual(diagnostics["losing_trades"], 3)
        flags = diagnostics["observable_flags"]
        self.assertEqual(flags["GROSS_LOSS_BEFORE_COSTS"], 1)
        self.assertEqual(flags["COSTS_TURNED_GROSS_WIN_INTO_NET_LOSS"], 1)
        self.assertEqual(flags["EXIT_BBO_GAP_PRESENT"], 1)
        self.assertEqual(flags["TRIGGER_TO_FILL_DELAY_PRESENT"], 1)
        self.assertEqual(flags["FAVORABLE_EXCURSION_WITH_NET_LOSS"], 1)
        self.assertEqual(flags["EXIT_SPREAD_RECORDED"], 2)
        self.assertEqual(flags["INSUFFICIENT_ATTRIBUTION_DATA"], 1)
        self.assertEqual(diagnostics["data_coverage"]["gross_pnl_records"], 2)
        self.assertIn("not mutually exclusive", diagnostics["interpretation"])


    def test_scorecard_segments_realized_results_by_holding_time(self):
        tracker = PaperPerformanceTracker()
        tracker.restore([
            {**trade("under-minute", 3), "time_in_trade_sec": 42.0},
            {**trade("three-minutes", -2), "time_in_trade_sec": 180.0},
            {**trade("ten-minutes", 5), "time_in_trade_sec": 600.0},
            {**trade("unknown-duration", -1), "time_in_trade_sec": None},
        ])
        groups = tracker.scorecard()["by_holding_time"]
        self.assertEqual(groups["LT_60S"]["trades"], 1)
        self.assertEqual(groups["180_299S"]["trades"], 1)
        self.assertEqual(groups["600S_PLUS"]["trades"], 1)
        self.assertEqual(groups["UNKNOWN"]["trades"], 1)



class PaperEntryPerformanceCohortTests(unittest.TestCase):
    def test_scorecard_groups_realized_trades_by_entry_quality(self):
        tracker = PaperPerformanceTracker()
        tracker.restore([
            {
                **trade("entry-good", 5),
                "entry_spread_pct": 0.03,
                "entry_bbo_age_sec": 0.4,
                "entry_execution_shortfall_bps": 2.0,
                "entry_spread_to_atr_ratio": 0.20,
                "entry_signal_adverse_drift_pct": 0.08,
            },
            {
                **trade("entry-bad", -4, side="SELL"),
                "entry_spread_pct": 0.12,
                "entry_bbo_age_sec": 1.2,
                "entry_execution_shortfall_bps": 6.0,
                "entry_spread_to_atr_ratio": 0.60,
                "entry_signal_adverse_drift_pct": 0.15,
            },
            {
                **trade("entry-legacy", 1),
                "entry_spread_pct": None,
                "entry_bbo_age_sec": None,
                "entry_execution_shortfall_bps": None,
                "entry_spread_to_atr_ratio": None,
                "entry_signal_adverse_drift_pct": None,
            },
        ])

        scorecard = tracker.scorecard()
        self.assertEqual(
            scorecard["by_entry_spread"]["SPREAD_PCT_0_02_0_05"]["trades"], 1
        )
        self.assertEqual(
            scorecard["by_entry_spread"]["SPREAD_PCT_GE_0_10"]["trades"], 1
        )
        self.assertEqual(
            scorecard["by_entry_bbo_age"]["BBO_AGE_SEC_0_25_0_50"]["trades"], 1
        )
        self.assertEqual(
            scorecard["by_entry_execution_shortfall"]["SHORTFALL_BPS_1_3"]["trades"], 1
        )
        self.assertEqual(
            scorecard["by_entry_spread_to_atr"]["SPREAD_ATR_0_10_0_25"]["trades"], 1
        )
        self.assertEqual(
            scorecard["by_entry_signal_drift"]["ADVERSE_0_05_TO_0_10"]["trades"], 1
        )
        diagnostics = scorecard["entry_diagnostics"]
        self.assertEqual(diagnostics["trades"], 3)
        self.assertEqual(diagnostics["coverage"]["spread_pct"], 2)
        self.assertEqual(diagnostics["coverage"]["execution_shortfall_bps"], 2)
        self.assertIn("descriptive cohorts", diagnostics["interpretation"])

    def test_entry_quality_groups_keep_unknown_data_separate(self):
        tracker = PaperPerformanceTracker()
        tracker.record({
            **trade("entry-unknown", -2),
            "entry_spread_pct": None,
            "entry_bbo_age_sec": None,
            "entry_execution_shortfall_bps": None,
            "entry_spread_to_atr_ratio": None,
            "entry_signal_adverse_drift_pct": None,
        })
        scorecard = tracker.scorecard()
        self.assertEqual(
            scorecard["by_entry_spread"]["SPREAD_PCT_UNKNOWN"]["trades"], 1
        )
        self.assertEqual(
            scorecard["by_entry_execution_shortfall"]["SHORTFALL_BPS_UNKNOWN"]["trades"], 1
        )


    def test_shadow_dedup_prefers_valid_on_time_label_and_reports_all_horizon_statuses(self):
        tracker = PaperPerformanceTracker()
        tracker.record(trade("paper-1", 5, candidate="candidate-1"))
        labels = [
            {
                "candidate_id": "candidate-1", "horizon_sec": 60,
                "result": "OK", "label_quality": "DELAYED",
                "label_lag_sec": 18.0, "net_executable_return_pct": 99.0,
            },
            {
                "candidate_id": "candidate-1", "horizon_sec": 60,
                "result": "OK", "label_quality": "ON_TIME",
                "label_lag_sec": 2.0, "net_executable_return_pct": 0.2,
            },
            {
                "candidate_id": "candidate-1", "horizon_sec": 180,
                "result": "NO_FRESH_BBO",
            },
            {
                "candidate_id": "candidate-1", "horizon_sec": 300,
                "result": "LABEL_LATE",
            },
        ]

        comparison = tracker.scorecard(labels)["shadow_comparison"]
        self.assertEqual(comparison["horizons"]["60"]["mean_net_return_pct"], 0.2)
        self.assertEqual(comparison["label_coverage_by_horizon"]["60"]["valid_ok_labels"], 1)
        self.assertEqual(
            comparison["label_coverage_by_horizon"]["180"]["status_counts"],
            {"NO_FRESH_BBO": 1},
        )
        self.assertEqual(
            comparison["label_coverage_by_horizon"]["300"]["status_counts"],
            {"LABEL_LATE": 1},
        )

    def test_shadow_confidence_interactions_include_uncertainty_and_do_not_assume_probability(self):
        tracker = PaperPerformanceTracker()
        tracker.restore([
            trade("paper-buy", 5, side="BUY", candidate="candidate-buy"),
            trade("paper-sell", -2, side="SELL", candidate="candidate-sell"),
        ])
        labels = [
            {
                "candidate_id": "candidate-buy", "result": "OK",
                "horizon_sec": 60, "side": "BUY",
                "net_executable_return_pct": 0.2,
                "llm_model_decision": "APPROVE", "llm_confidence": 87.0,
            },
            {
                "candidate_id": "candidate-sell", "result": "OK",
                "horizon_sec": 60, "side": "SELL",
                "net_executable_return_pct": -0.1,
                "llm_model_decision": "HOLD", "llm_confidence": 92.0,
            },
        ]
        comparison = tracker.scorecard(labels)["shadow_comparison"]
        buy = comparison["horizon_segments"]["by_horizon_side_confidence"]["60s|BUY|80-89"]
        self.assertEqual(buy["labels"], 1)
        self.assertEqual(buy["mean_model_confidence_pct"], 87.0)
        self.assertEqual(len(buy["positive_rate_wilson_95_pct"]), 2)
        self.assertLess(
            buy["positive_rate_wilson_95_pct"][0],
            buy["positive_rate_wilson_95_pct"][1],
        )
        self.assertIn(
            "by_horizon_decision_confidence",
            comparison["horizon_segments"],
        )

    def test_realized_drawdown_orders_mixed_timezone_timestamps_by_utc_instant(self):
        tracker = PaperPerformanceTracker()
        tracker.restore([
            trade("loss-early", -100, exit_time="2026-11-18T10:00:00+02:00"),
            trade("win-middle", 200, exit_time="2026-11-18T08:30:00Z"),
            trade("loss-late", -150, exit_time="2026-11-18T11:00:00+02:00"),
        ])
        self.assertEqual(
            tracker.scorecard()["overall"]["max_drawdown_rub"],
            150.0,
        )


    def test_reentry_diagnostics_pair_only_closed_same_ticker_session_trades(self):
        tracker = PaperPerformanceTracker()
        tracker.restore([
            {
                **trade("first-stop", -10, exit_time="2026-11-18T10:01:00+03:00"),
                "entry_timestamp": "2026-11-18T10:00:00+03:00",
            },
            {
                **trade("second-trade", 6, exit_time="2026-11-18T10:03:00+03:00"),
                "entry_timestamp": "2026-11-18T10:01:30+03:00",
                "exit_reason": "TAKE_PROFIT",
            },
            {
                **trade("third-trade", -4, exit_time="2026-11-18T10:08:00+03:00"),
                "entry_timestamp": "2026-11-18T10:06:30+03:00",
            },
            {
                **trade("other-session", 100, exit_time="2026-11-18T10:02:00+03:00", session="session-2"),
                "entry_timestamp": "2026-11-18T10:01:15+03:00",
            },
            {
                **trade("naive-time", 100, exit_time="2026-11-18T10:04:00"),
                "entry_timestamp": "2026-11-18T10:03:00",
            },
        ])

        diagnostics = tracker.scorecard()["reentry_diagnostics"]
        self.assertEqual(diagnostics["paired_reentries"], 2)
        self.assertEqual(diagnostics["eligible_trades"], 4)
        self.assertEqual(diagnostics["trades_without_valid_aware_entry_exit_timestamps"], 1)
        self.assertEqual(diagnostics["by_gap"]["LT_60S"]["trades"], 1)
        self.assertEqual(diagnostics["by_previous_exit_reason"]["STOP_LOSS"]["trades"], 1)
        self.assertEqual(diagnostics["by_previous_exit_reason"]["TAKE_PROFIT"]["trades"], 1)
        self.assertEqual(diagnostics["by_previous_trade_outcome"]["LOSS"]["trades"], 1)
        self.assertEqual(diagnostics["by_previous_trade_outcome"]["WIN"]["trades"], 1)
        self.assertEqual(diagnostics["pairs"][0]["previous_trade_id"], "first-stop")
        self.assertEqual(diagnostics["pairs"][0]["gap_sec"], 30.0)


if __name__ == "__main__":
    unittest.main()
