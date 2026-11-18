"""Exact trade-ID and replay-session execution reconciliation tests."""
from __future__ import annotations

import unittest

from strategy.paper_execution_reconciliation import build_actual_vs_replay_reconciliation


def actual_trade(**overrides):
    row = {
        "id": "PAPER-1",
        "mode": "PAPER",
        "status": "CLOSED",
        "bbo_replay_session_id": "REPLAY-SESSION-1",
        "ticker": "SBER",
        "side": "BUY",
        "volume": 10,
        "pnl": 100.0,
        "exit_price": 101.0,
        "total_commission_rub": 12.0,
        "entry_execution_shortfall_rub": 1.0,
        "exit_execution_shortfall_rub": 2.0,
        "exit_depth_impact_rub": 1.5,
        "exit_partial_fill_count": 2,
        "exit_fill_model": "DEPTH_PARTIALS_PLUS_FINAL_DEPTH_FILL",
        "exit_reason": "TAKE_PROFIT",
    }
    row.update(overrides)
    return row


def replay_trade(**overrides):
    row = {
        "position_id": "PAPER-1",
        "session_id": "REPLAY-SESSION-1",
        "ticker": "SBER",
        "side": "BUY",
        "scenarios": [
            {
                "name": "BASELINE_SL_TP",
                "exit": {
                    "reason": "TAKE_PROFIT",
                    "fill_price_after_slippage": 100.9,
                    "net_pnl_rub": 90.0,
                    "total_commission_rub": 10.0,
                    "event_sequence": 77,
                    "bbo_age_sec": 0.25,
                },
            }
        ],
    }
    row.update(overrides)
    return row


class PaperExecutionReconciliationTests(unittest.TestCase):
    def test_matches_exact_trade_and_replay_session_and_reports_fill_deltas(self) -> None:
        result = build_actual_vs_replay_reconciliation(
            [actual_trade()],
            {"trades": [replay_trade()]},
        )

        self.assertEqual(result["matched_exact_identity"], 1)
        self.assertEqual(result["counts_by_status"]["MATCHED_EXACT_ID_AND_SESSION"], 1)
        self.assertEqual(result["pnl_delta"]["total_rub"], 10.0)
        self.assertEqual(result["pnl_delta"]["actual_better"], 1)
        self.assertEqual(result["actual_exit_advantage"]["total_rub"], 1.0)
        row = result["trades"][0]
        self.assertEqual(row["actual_exit_partial_fill_count"], 2)
        self.assertEqual(row["actual_exit_depth_impact_rub"], 1.5)
        self.assertEqual(row["commission_delta_actual_minus_replay_rub"], 2.0)

    def test_same_ticker_does_not_match_different_trade_id_or_session(self) -> None:
        result = build_actual_vs_replay_reconciliation(
            [actual_trade()],
            {"trades": [replay_trade(position_id="PAPER-OTHER")]},
        )

        self.assertEqual(result["matched_exact_identity"], 0)
        self.assertEqual(result["counts_by_status"]["NO_REPLAY_POSITION"], 1)
        self.assertEqual(result["pnl_delta"]["observations"], 0)

        wrong_session = build_actual_vs_replay_reconciliation(
            [actual_trade()],
            {"trades": [replay_trade(session_id="REPLAY-SESSION-OTHER")]},
        )
        self.assertEqual(wrong_session["matched_exact_identity"], 0)
        self.assertEqual(wrong_session["counts_by_status"]["NO_REPLAY_POSITION"], 1)

    def test_missing_identity_and_duplicate_identity_are_never_joined(self) -> None:
        actual = actual_trade()
        missing_actual = actual_trade(id="", bbo_replay_session_id="")
        duplicate = actual_trade()
        result = build_actual_vs_replay_reconciliation(
            [actual, duplicate, missing_actual],
            {"trades": [replay_trade()]},
        )

        self.assertEqual(result["duplicate_actual_identities"], 1)
        self.assertEqual(result["counts_by_status"]["DUPLICATE_ACTUAL_IDENTITY"], 2)
        self.assertEqual(result["counts_by_status"]["MISSING_ACTUAL_REPLAY_IDENTITY"], 1)
        self.assertEqual(result["matched_exact_identity"], 0)

    def test_missing_replay_exit_is_not_imputed_as_zero_pnl(self) -> None:
        replay = replay_trade(scenarios=[{"name": "BASELINE_SL_TP", "exit": None}])
        result = build_actual_vs_replay_reconciliation(
            [actual_trade()],
            {"trades": [replay]},
        )

        self.assertEqual(result["counts_by_status"]["NO_REPLAY_EXIT"], 1)
        self.assertEqual(result["pnl_delta"]["observations"], 0)
        self.assertIsNone(result["pnl_delta"]["total_rub"])

    def test_legacy_mode_less_paper_trade_is_included_by_explicit_paper_id(self) -> None:
        legacy = actual_trade(id="PAPER_LEGACY_1", mode="")
        replay = replay_trade(position_id="PAPER_LEGACY_1")
        result = build_actual_vs_replay_reconciliation(
            [legacy],
            {"trades": [replay]},
        )
        self.assertEqual(result["actual_closed_paper_trades"], 1)
        self.assertEqual(result["matched_exact_identity"], 1)

    def test_non_paper_and_open_trades_are_excluded(self) -> None:
        result = build_actual_vs_replay_reconciliation(
            [
                actual_trade(mode="LIVE"),
                actual_trade(status="OPEN"),
            ],
            {"trades": [replay_trade()]},
        )

        self.assertEqual(result["actual_closed_paper_trades"], 0)
        self.assertEqual(result["matched_exact_identity"], 0)


if __name__ == "__main__":
    unittest.main()
