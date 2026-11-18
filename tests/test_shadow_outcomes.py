from __future__ import annotations

import unittest
from unittest.mock import patch

from shadow_outcomes import ShadowForwardTracker


def quote_metrics(bid: float, ask: float) -> dict[str, float]:
    return {
        "bid": bid,
        "ask": ask,
        "bid_ask_available": 1.0,
        "spread_available": 1.0,
        "order_book_valid": 1.0,
        "bid_quote_age_sec": 0.1,
        "ask_quote_age_sec": 0.1,
        "bbo_max_age_sec": 2.0,
        "spread_pct": (ask - bid) / ((ask + bid) / 2.0) * 100.0,
    }


class ShadowForwardTrackerTests(unittest.TestCase):
    def test_default_paper_horizons_cover_one_three_five_and_ten_minutes(self) -> None:
        tracker = ShadowForwardTracker()
        self.assertEqual(tracker.horizons_seconds, (60, 180, 300, 600))

    def test_buy_labels_use_ask_entry_and_bid_exit_after_one_minute(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,), max_label_lag_sec=30)
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[100.0, 160.0]):
            candidate_id = tracker.record_candidate(
                "SBER", "BUY", quote_metrics(100.0, 100.1),
                {"score": 90.0, "eligible": True, "regime": "TREND_ALIGNED"},
            )
            self.assertIsNotNone(candidate_id)
            outcomes = tracker.observe_market_tick("SBER", quote_metrics(101.0, 101.1))

        self.assertEqual(len(outcomes), 1)
        row = outcomes[0]
        self.assertEqual(row["result"], "OK")
        self.assertEqual(row["entry_executable_price"], 100.1)
        self.assertEqual(row["exit_executable_price"], 101.0)
        self.assertGreater(row["net_executable_return_pct"], 0.0)
        self.assertEqual(tracker.pending_count(), 0)

    def test_mfe_mae_capture_executable_extrema_times_and_quote_coverage(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[100.0, 120.0, 140.0, 160.0]):
            tracker.record_candidate(
                "SBER", "BUY", quote_metrics(100.0, 100.1),
                {"score": 90.0, "eligible": True, "regime": "TREND_ALIGNED"},
            )
            tracker.observe_market_tick("SBER", quote_metrics(100.5, 100.6))
            tracker.observe_market_tick("SBER", quote_metrics(99.5, 99.6))
            outcomes = tracker.observe_market_tick("SBER", quote_metrics(100.3, 100.4))

        self.assertEqual(len(outcomes), 1)
        label = outcomes[0]
        self.assertGreater(label["mfe_executable_pct"], 0.0)
        self.assertLess(label["mae_executable_pct"], 0.0)
        self.assertGreater(label["mfe_gross_executable_pct"], label["mfe_executable_pct"])
        self.assertLess(label["mae_executable_pct"], label["mae_gross_executable_pct"])
        self.assertEqual(label["time_to_mfe_sec"], 20.0)
        self.assertEqual(label["time_to_mae_sec"], 40.0)
        self.assertEqual(label["valid_bbo_observations"], 3)
        self.assertEqual(label["first_valid_bbo_elapsed_sec"], 20.0)
        self.assertEqual(label["last_valid_bbo_elapsed_sec"], 60.0)
        self.assertEqual(label["bbo_observation_span_sec"], 40.0)
        self.assertTrue(label["mfe_at_utc"])
        self.assertTrue(label["mae_at_utc"])

    def test_sell_labels_use_bid_entry_and_ask_exit(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[10.0, 70.0]):
            tracker.record_candidate(
                "SBER", "SELL", quote_metrics(100.0, 100.1),
                {"score": 88.0, "eligible": True, "regime": "TREND_ALIGNED"},
            )
            outcomes = tracker.observe_market_tick("SBER", quote_metrics(99.8, 99.9))
        self.assertEqual(outcomes[0]["entry_executable_price"], 100.0)
        self.assertEqual(outcomes[0]["exit_executable_price"], 99.9)
        self.assertGreater(outcomes[0]["net_executable_return_pct"], 0.0)

    def test_horizon_without_fresh_bbo_is_labelled_not_filled_at_stale_price(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,), max_label_lag_sec=30)
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[100.0, 191.0]):
            tracker.record_candidate(
                "SBER", "BUY", quote_metrics(100.0, 100.1),
                {"score": 80.0, "eligible": False, "regime": "TRANSITION_OR_RANGE"},
            )
            stale = quote_metrics(101.0, 101.1)
            stale["bid_quote_age_sec"] = 5.0
            outcomes = tracker.observe_market_tick("SBER", stale)
        self.assertEqual(outcomes[0]["result"], "NO_FRESH_BBO")
        self.assertNotIn("net_executable_return_pct", outcomes[0])
        self.assertEqual(outcomes[0]["valid_bbo_observations"], 0)
        self.assertEqual(outcomes[0]["no_fresh_bbo_observations"], 1)

    def test_fresh_quote_after_allowed_label_lag_is_classified_as_label_late(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,), max_label_lag_sec=30)
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[100.0, 191.0]):
            tracker.record_candidate(
                "SBER", "BUY", quote_metrics(100.0, 100.1),
                {"score": 90.0, "eligible": True, "regime": "TREND_ALIGNED"},
            )
            outcomes = tracker.observe_market_tick("SBER", quote_metrics(101.0, 101.1))
        self.assertEqual(outcomes[0]["result"], "LABEL_LATE")
        self.assertEqual(outcomes[0]["label_quality"], "LABEL_LATE")
        self.assertNotIn("net_executable_return_pct", outcomes[0])
        self.assertEqual(tracker.statistics_snapshot()["eligible"]["late"], 1)

    def test_configured_costs_reduce_net_executable_return(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_COMMISSION_PCT": "0.10",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "1.0",
        }), patch("strategy.shadow_outcomes.time.monotonic", side_effect=[100.0, 160.0]):
            tracker.record_candidate(
                "SBER", "BUY", quote_metrics(100.0, 100.1),
                {"score": 90.0, "eligible": True, "regime": "TREND_ALIGNED"},
            )
            outcomes = tracker.observe_market_tick("SBER", quote_metrics(101.0, 101.1))
            snapshot = tracker.statistics_snapshot()
        self.assertLess(
            outcomes[0]["net_executable_return_pct"],
            outcomes[0]["gross_executable_return_pct"],
        )
        self.assertEqual(outcomes[0]["commission_pct_per_side"], 0.10)
        self.assertEqual(outcomes[0]["slippage_bps"], 1.0)
        self.assertEqual(snapshot["cost_model"]["commission_pct_per_side"], 0.10)
        self.assertEqual(snapshot["cost_model"]["slippage_bps"], 1.0)
        self.assertEqual(snapshot["cost_model"]["status"], "CONFIGURED")

    def test_zero_cost_assumptions_are_marked_incomplete(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_COMMISSION_PCT": "0",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "0",
        }):
            snapshot = tracker.statistics_snapshot()
        self.assertEqual(snapshot["cost_model"]["status"], "INCOMPLETE")
        self.assertIn("not fully calibrated", snapshot["cost_model"]["note"])

    def test_fresh_bbo_can_label_candidate_even_without_full_book_confirmation(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        metrics = quote_metrics(100.0, 100.1)
        metrics["order_book_valid"] = 0.0
        candidate_id = tracker.record_candidate(
            "SBER", "BUY", metrics,
            {"score": 0.0, "eligible": False, "regime": "UNKNOWN"},
        )
        self.assertIsNotNone(candidate_id)
        self.assertEqual(tracker.pending_count(), 1)

    def test_stale_bbo_is_not_recorded_as_a_candidate_entry(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        metrics = quote_metrics(100.0, 100.1)
        metrics["bid_quote_age_sec"] = 3.0
        candidate_id = tracker.record_candidate(
            "SBER", "BUY", metrics,
            {"score": 90.0, "eligible": True, "regime": "TREND_ALIGNED"},
        )
        self.assertIsNone(candidate_id)
        self.assertEqual(tracker.pending_count(), 0)

    def test_each_horizon_is_emitted_once_without_dropping_over_capacity_candidates(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60, 180), max_pending_per_ticker=1)
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[0.0, 0.0, 60.0, 61.0, 180.0, 181.0]):
            first_id = tracker.record_candidate("SBER", "BUY", quote_metrics(100.0, 100.1), {"score": 90.0})
            second_id = tracker.record_candidate("SBER", "BUY", quote_metrics(100.0, 100.1), {"score": 91.0})
            self.assertNotEqual(first_id, second_id)
            self.assertEqual(tracker.pending_count("SBER"), 2)
            first = tracker.observe_market_tick("SBER", quote_metrics(100.2, 100.3))
            second = tracker.observe_market_tick("SBER", quote_metrics(100.4, 100.5))
            third = tracker.observe_market_tick("SBER", quote_metrics(100.6, 100.7))
            fourth = tracker.observe_market_tick("SBER", quote_metrics(100.8, 100.9))
        self.assertEqual(len(first), 2)
        self.assertEqual({row["candidate_id"] for row in first}, {first_id, second_id})
        self.assertEqual({row["horizon_sec"] for row in first}, {60})
        self.assertEqual(len(second), 0)
        self.assertEqual({row["horizon_sec"] for row in third}, {180})
        self.assertEqual({row["candidate_id"] for row in third}, {first_id, second_id})
        self.assertEqual(len(fourth), 0)
        self.assertEqual(tracker.pending_count(), 0)
        by_horizon = tracker.statistics_snapshot()["by_horizon"]
        self.assertEqual(by_horizon["60s"]["ok"], 2)
        self.assertEqual(by_horizon["180s"]["ok"], 2)


    def test_completed_labels_restore_cumulative_statistics_after_restart(self) -> None:
        original = ShadowForwardTracker(horizons_seconds=(60,))
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[100.0, 160.0]):
            original.record_candidate(
                "SBER", "BUY", quote_metrics(100.0, 100.1),
                {
                    "score": 92.0,
                    "eligible": True,
                    "regime": "TREND_ALIGNED",
                    "spread_atr_ratio": 0.12,
                    "hard_blocks": [],
                },
            )
            labels = original.observe_market_tick("SBER", quote_metrics(100.5, 100.6))
        self.assertEqual(len(labels), 1)

        restored = ShadowForwardTracker(horizons_seconds=(60,))
        count = restored.restore_completed_labels(labels)
        self.assertEqual(count, 1)
        self.assertEqual(
            restored.statistics_snapshot()["eligible"],
            original.statistics_snapshot()["eligible"],
        )
        self.assertEqual(
            restored.statistics_snapshot()["by_horizon"],
            original.statistics_snapshot()["by_horizon"],
        )
        self.assertEqual(
            restored.statistics_snapshot()["segments"],
            original.statistics_snapshot()["segments"],
        )

    def test_restore_ignores_malformed_or_unrecognized_outcome_rows(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        restored = tracker.restore_completed_labels([
            None,
            {},
            {"candidate_id": "x", "ticker": "SBER", "result": "UNKNOWN"},
            {
                "candidate_id": "x", "ticker": "SBER", "result": "OK",
                "side": "BUY", "shadow_eligible": True,
            },
        ])
        self.assertEqual(restored, 1)
        self.assertEqual(tracker.statistics_snapshot()["eligible"]["labels"], 1)

    def test_outcomes_segment_by_spread_atr_trade_count_and_directional_flow(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        metrics = quote_metrics(100.0, 100.1)
        metrics.update({
            "trade_count_60s": 32,
            "trade_flow_imbalance_30s": 0.45,
            "trade_history_coverage_sec": 60.0,
            "trade_realized_volatility_60s_pct": 0.025,
            "trade_volume_5s": 120.0,
            "trade_volume_baseline_5s": 20.0,
        })
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[100.0, 160.0]):
            tracker.record_candidate(
                "SBER", "BUY", metrics,
                {
                    "score": 90.0,
                    "eligible": True,
                    "regime": "TREND_ALIGNED",
                    "spread_atr_ratio": 0.15,
                },
            )
            outcomes = tracker.observe_market_tick("SBER", quote_metrics(100.2, 100.3))

        self.assertEqual(len(outcomes), 1)
        label = outcomes[0]
        self.assertEqual(label["trade_count_60s"], 32)
        self.assertEqual(label["trade_flow_imbalance_30s"], 0.45)
        self.assertEqual(label["trade_history_coverage_sec"], 60.0)
        self.assertEqual(label["trade_realized_volatility_60s_pct"], 0.025)
        self.assertEqual(label["trade_volume_5s"], 120.0)
        self.assertEqual(label["trade_volume_baseline_5s"], 20.0)

        segments = tracker.statistics_snapshot()["segments"]
        self.assertEqual(segments["spread_atr_bucket=0.10-0.20"]["labels"], 1)
        self.assertEqual(segments["trade_count_bucket=20-49"]["labels"], 1)
        self.assertEqual(segments["flow_alignment=strong_aligned"]["labels"], 1)
        self.assertEqual(segments["side_flow_alignment=BUY|strong_aligned"]["labels"], 1)


    def test_llm_verdict_and_final_execution_are_linked_to_forward_labels(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        with patch("strategy.shadow_outcomes.time.monotonic", return_value=100.0):
            candidate_id = tracker.record_candidate(
                "SBER", "BUY", quote_metrics(100.0, 100.1),
                {"score": 91.0, "eligible": True, "regime": "TREND_ALIGNED"},
            )
        self.assertIsNotNone(candidate_id)
        self.assertTrue(tracker.attach_llm_verdict(
            candidate_id,
            verdict_id="verdict-123",
            model_decision="APPROVE",
            final_decision="BUY",
            signal_status="APPROVED",
            confidence=88.5,
            reason_code="APPROVED",
            local_guard="PRE_LLM_VALIDATED",
            execution_status="PENDING",
        ))
        self.assertTrue(tracker.update_execution_result(
            candidate_id, execution_status="OPENED",
        ))

        with patch("strategy.shadow_outcomes.time.monotonic", return_value=160.0):
            labels = tracker.observe_market_tick("SBER", quote_metrics(100.3, 100.4))

        self.assertEqual(len(labels), 1)
        label = labels[0]
        self.assertTrue(label["llm_linked"])
        self.assertEqual(label["llm_verdict_id"], "verdict-123")
        self.assertEqual(label["llm_model_decision"], "APPROVE")
        self.assertEqual(label["llm_final_decision"], "BUY")
        self.assertEqual(label["llm_signal_status"], "APPROVED")
        self.assertEqual(label["llm_confidence"], 88.5)
        self.assertEqual(label["llm_execution_status"], "OPENED")
        segments = tracker.statistics_snapshot()["segments"]
        self.assertEqual(segments["llm_model_decision=APPROVE"]["labels"], 1)
        self.assertEqual(segments["llm_final_decision=BUY"]["labels"], 1)
        self.assertEqual(segments["llm_execution_status=OPENED"]["labels"], 1)

    def test_llm_scorecard_splits_decisions_and_confidence_by_horizon(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60, 180))
        with patch("strategy.shadow_outcomes.time.monotonic", return_value=100.0):
            candidate_id = tracker.record_candidate(
                "SBER", "BUY", quote_metrics(100.0, 100.1),
                {"score": 91.0, "eligible": True, "regime": "TREND_ALIGNED"},
            )
        self.assertTrue(tracker.attach_llm_verdict(
            candidate_id,
            verdict_id="verdict-horizon",
            model_decision="APPROVE",
            final_decision="BUY",
            signal_status="APPROVED",
            confidence=87.0,
            reason_code="TREND_CONFIRMED",
            local_guard="PRE_LLM_VALIDATED",
            execution_status="OPENED",
        ))
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=(160.0, 280.0)):
            first = tracker.observe_market_tick("SBER", quote_metrics(100.2, 100.3))
            second = tracker.observe_market_tick("SBER", quote_metrics(100.4, 100.5))

        self.assertEqual([row["horizon_sec"] for row in first], [60])
        self.assertEqual([row["horizon_sec"] for row in second], [180])
        segments = tracker.statistics_snapshot()["segments"]
        self.assertEqual(segments["horizon_llm_model_decision=60s|APPROVE"]["ok"], 1)
        self.assertEqual(segments["horizon_llm_model_decision=180s|APPROVE"]["ok"], 1)
        self.assertEqual(segments["horizon_llm_decision_confidence=60s|APPROVE|80-89"]["ok"], 1)
        self.assertEqual(segments["horizon_llm_execution=180s|OPENED"]["ok"], 1)
        self.assertEqual(segments["horizon_llm_final_decision=60s|BUY"]["ok"], 1)
        self.assertEqual(segments["horizon_llm_confidence=180s|80-89"]["ok"], 1)
        self.assertEqual(segments["horizon_llm_model_decision=60s|APPROVE"]["sample_quality"], "INSUFFICIENT_LT_30")

    def test_pre_llm_rejection_is_distinct_from_missing_verdict_link(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=(100.0, 160.0)):
            candidate_id = tracker.record_candidate(
                "SBER", "BUY", quote_metrics(100.0, 100.1),
                {"score": 60.0, "eligible": False, "regime": "TRANSITION_OR_RANGE"},
            )
            self.assertTrue(tracker.mark_llm_not_called(
                candidate_id, reason_code="ATR_NOT_READY",
            ))
            # A later callback must not overwrite an already classified pre-LLM reject.
            self.assertFalse(tracker.attach_llm_verdict(
                candidate_id,
                verdict_id="late-verdict",
                model_decision="APPROVE",
                final_decision="BUY",
                signal_status="APPROVED",
                confidence=90.0,
                reason_code="APPROVED",
                local_guard="PRE_LLM_VALIDATED",
                execution_status="PENDING",
            ))
            labels = tracker.observe_market_tick("SBER", quote_metrics(100.2, 100.3))

        self.assertEqual(len(labels), 1)
        label = labels[0]
        self.assertFalse(label["llm_linked"])
        self.assertEqual(label["llm_attribution_state"], "NOT_CALLED")
        self.assertEqual(label["llm_not_called_reason"], "ATR_NOT_READY")
        segments = tracker.statistics_snapshot()["segments"]
        self.assertEqual(segments["llm_attribution_state=NOT_CALLED"]["ok"], 1)
        self.assertEqual(segments["llm_not_called_reason=ATR_NOT_READY"]["ok"], 1)

    def test_market_regime_and_entry_economics_are_carried_into_forward_labels(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        metrics = quote_metrics(100.0, 100.2)
        metrics.update({
            "bar_atr_pct": 0.60,
            "price_change_pct": 0.20,
            "price_vs_vwap_pct": 0.15,
            "bar_trend_up": 1.0,
            "bar_trend_down": 0.0,
            "trade_flow_imbalance_30s": 0.30,
            "trade_count_60s": 40,
            "trade_history_coverage_sec": 60.0,
            "trade_data_ready": 1.0,
            "trade_metrics_age_sec": 0.2,
        })
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_COMMISSION_PCT": "0.05",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "2",
        }), patch("strategy.shadow_outcomes.time.monotonic", side_effect=[100.0, 160.0]):
            tracker.record_candidate(
                "SBER", "BUY", metrics,
                {"score": 90.0, "eligible": True, "regime": "TREND_ALIGNED"},
            )
            labels = tracker.observe_market_tick("SBER", quote_metrics(100.3, 100.4))

        self.assertEqual(len(labels), 1)
        label = labels[0]
        self.assertEqual(label["market_regime"], "TREND")
        self.assertEqual(label["directional_regime"], "TREND_UP")
        self.assertEqual(label["volatility_regime"], "VOLATILITY_UNCLASSIFIED")
        self.assertGreater(label["minimum_favorable_move_pct"], 0.0)
        self.assertGreater(label["cost_to_atr_ratio"], 0.0)
        self.assertEqual(label["entry_economics"]["cost_model_status"], "CONFIGURED")
        segments = tracker.statistics_snapshot()["segments"]
        self.assertEqual(segments["market_regime=TREND"]["ok"], 1)
        self.assertEqual(segments["directional_regime=TREND_UP"]["ok"], 1)
        self.assertEqual(segments["cost_to_atr_bucket=0.25-0.50"]["ok"], 1)
        self.assertEqual(
            segments[
                "market_regime_signal_confluence=TREND|BUY|Q=unavailable|FLOW=strong_aligned|OFI=unavailable"
            ]["ok"],
            1,
        )

    def test_ofi_snapshot_is_carried_into_forward_label_and_segment(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        metrics = quote_metrics(100.0, 100.1)
        metrics.update({
            "ofi_5s": -0.20,
            "ofi_5s_available": 1.0,
            "ofi_5s_samples": 5,
            "ofi_5s_coverage_sec": 1.5,
        })
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[100.0, 160.0]):
            tracker.record_candidate(
                "SBER", "BUY", metrics,
                {"score": 90.0, "eligible": True, "regime": "TREND_ALIGNED"},
            )
            labels = tracker.observe_market_tick("SBER", quote_metrics(100.2, 100.3))

        self.assertEqual(len(labels), 1)
        self.assertEqual(labels[0]["ofi_5s"], -0.20)
        self.assertTrue(labels[0]["ofi_5s_available"])
        self.assertEqual(labels[0]["ofi_5s_samples"], 5)
        segments = tracker.statistics_snapshot()["segments"]
        self.assertEqual(segments["ofi_alignment_5s=opposed"]["ok"], 1)
        self.assertEqual(segments["side_ofi_alignment_5s=BUY|opposed"]["ok"], 1)

    def test_llm_approved_group_reports_profit_factor_from_net_returns(self) -> None:
        tracker = ShadowForwardTracker(horizons_seconds=(60,))
        with patch("strategy.shadow_outcomes.time.monotonic", side_effect=[100.0, 100.0]):
            winners_id = tracker.record_candidate(
                "SBER", "BUY", quote_metrics(100.0, 100.1),
                {"score": 90.0, "eligible": True},
            )
            losers_id = tracker.record_candidate(
                "SBER", "BUY", quote_metrics(100.5, 100.6),
                {"score": 90.0, "eligible": True},
            )
        for candidate_id, verdict_id in ((winners_id, "v-win"), (losers_id, "v-loss")):
            self.assertTrue(tracker.attach_llm_verdict(
                candidate_id,
                verdict_id=verdict_id,
                model_decision="APPROVE",
                final_decision="BUY",
                signal_status="APPROVED",
                confidence=90.0,
                reason_code="APPROVED",
                local_guard="PRE_LLM_VALIDATED",
                execution_status="NOT_REQUESTED",
            ))
        with patch("strategy.shadow_outcomes.time.monotonic", return_value=160.0):
            labels = tracker.observe_market_tick("SBER", quote_metrics(100.3, 100.4))

        self.assertEqual(len(labels), 2)
        approved = tracker.statistics_snapshot()["segments"]["llm_model_decision=APPROVE"]
        self.assertEqual(approved["ok"], 2)
        self.assertGreater(approved["profit_factor"], 0.0)
        self.assertEqual(approved["positive"], 1)
        self.assertEqual(approved["negative"], 1)
        self.assertGreater(approved["average_win_pct"], 0.0)
        self.assertGreater(approved["average_loss_pct"], 0.0)
        self.assertEqual(approved["sample_quality"], "INSUFFICIENT_LT_30")


if __name__ == "__main__":
    unittest.main()
