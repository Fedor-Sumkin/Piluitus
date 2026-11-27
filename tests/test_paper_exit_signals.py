from __future__ import annotations

import unittest

from strategy.paper_exit_signals import evaluate_paper_exit_signal


def _metrics(**updates):
    values = {
        "trade_metrics_age_sec": 0.5,
        "trade_flow_ready": 1.0,
        "trade_flow_known_trade_count_30s": 8,
        "trade_flow_known_volume_share_30s": 0.9,
        "price_change_pct": -0.05,
        "trade_price_change_30s_pct": -0.08,
        "trade_flow_imbalance_30s": -0.35,
        "queue_imbalance_5s_available": 1.0,
        "queue_imbalance_5s_age_sec": 0.4,
        "queue_imbalance_5s_coverage_sec": 4.0,
        "queue_imbalance_5s": -0.2,
    }
    values.update(updates)
    return values


class PaperExitSignalTests(unittest.TestCase):
    def test_long_reversal_requires_two_distinct_fresh_samples(self):
        state = {}
        first = evaluate_paper_exit_signal(
            position_side="BUY", metrics=_metrics(), sample_id="101",
            favorable_r=0.4, state=state,
        )
        duplicate = evaluate_paper_exit_signal(
            position_side="BUY", metrics=_metrics(), sample_id="101",
            favorable_r=0.4, state=state,
        )
        second = evaluate_paper_exit_signal(
            position_side="BUY", metrics=_metrics(), sample_id="102",
            favorable_r=0.4, state=state,
        )
        self.assertEqual(first["action"], "NONE")
        self.assertEqual(duplicate["hard_reversal_confirmations"], 1)
        self.assertFalse(duplicate["new_sample"])
        self.assertEqual(second["action"], "SIGNAL_REVERSAL")

    def test_short_reversal_uses_mirrored_price_flow_and_queue_signs(self):
        state = {}
        metrics = _metrics(
            price_change_pct=0.05,
            trade_price_change_30s_pct=0.08,
            trade_flow_imbalance_30s=0.35,
            queue_imbalance_5s=0.2,
        )
        evaluate_paper_exit_signal(
            position_side="SELL", metrics=metrics, sample_id="201",
            favorable_r=0.2, state=state,
        )
        result = evaluate_paper_exit_signal(
            position_side="SELL", metrics=metrics, sample_id="202",
            favorable_r=0.2, state=state,
        )
        self.assertEqual(result["action"], "SIGNAL_REVERSAL")

    def test_stale_or_unknown_aggressor_flow_never_triggers_exit(self):
        state = {}
        for sample in ("301", "302", "303"):
            result = evaluate_paper_exit_signal(
                position_side="BUY",
                metrics=_metrics(trade_metrics_age_sec=9.0, trade_flow_ready=0.0),
                sample_id=sample, favorable_r=2.0, state=state,
            )
        self.assertEqual(result["action"], "NONE")
        self.assertEqual(result["reason"], "STALE_OR_INSUFFICIENT_AGGRESSOR_FLOW")

    def test_aligned_fresh_queue_suppresses_false_reversal(self):
        state = {}
        metrics = _metrics(queue_imbalance_5s=0.4)
        evaluate_paper_exit_signal(
            position_side="BUY", metrics=metrics, sample_id="401",
            favorable_r=0.2, state=state,
        )
        result = evaluate_paper_exit_signal(
            position_side="BUY", metrics=metrics, sample_id="402",
            favorable_r=0.2, state=state,
        )
        self.assertEqual(result["action"], "NONE")

    def test_partial_profit_requires_profit_r_and_persistent_weakness(self):
        state = {}
        weakening = _metrics(
            price_change_pct=0.01,
            trade_price_change_30s_pct=0.015,
            trade_flow_imbalance_30s=-0.3,
            queue_imbalance_5s=-0.2,
        )
        first = evaluate_paper_exit_signal(
            position_side="BUY", metrics=weakening, sample_id="501",
            favorable_r=1.2, state=state,
        )
        second = evaluate_paper_exit_signal(
            position_side="BUY", metrics=weakening, sample_id="502",
            favorable_r=1.2, state=state,
        )
        self.assertEqual(first["action"], "NONE")
        self.assertEqual(second["action"], "PARTIAL_PROFIT")

    def test_partial_profit_is_not_repeated_after_scaleout(self):
        state = {"paper_profit_partial_taken": True}
        weakening = _metrics(
            price_change_pct=0.01,
            trade_price_change_30s_pct=0.015,
            trade_flow_imbalance_30s=-0.3,
        )
        evaluate_paper_exit_signal(
            position_side="BUY", metrics=weakening, sample_id="601",
            favorable_r=1.5, state=state,
        )
        result = evaluate_paper_exit_signal(
            position_side="BUY", metrics=weakening, sample_id="602",
            favorable_r=1.5, state=state,
        )
        self.assertEqual(result["action"], "NONE")


if __name__ == "__main__":
    unittest.main()
