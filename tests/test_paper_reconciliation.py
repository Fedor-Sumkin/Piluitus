import unittest

from strategy.paper_reconciliation import build_shadow_reconciliation


class PaperShadowReconciliationTests(unittest.TestCase):
    def test_matches_only_exact_candidate_ids_not_ticker_or_timestamp(self):
        trades = [
            {
                "id": "trade-exact", "ticker": "SBER", "side": "BUY",
                "pnl": 12.0, "shadow_candidate_id": "candidate-1",
                "exit_timestamp": "2026-11-18T12:00:00",
            },
            {
                "id": "trade-same-ticker", "ticker": "SBER", "side": "BUY",
                "pnl": -4.0, "shadow_candidate_id": "candidate-missing",
                "exit_timestamp": "2026-11-18T12:01:00",
            },
            {
                "id": "trade-no-id", "ticker": "GAZP", "side": "SELL",
                "pnl": -2.0, "exit_timestamp": "2026-11-18T12:02:00",
            },
        ]
        labels = [
            {"candidate_id": "candidate-1", "horizon_sec": 60, "result": "OK"},
            {"candidate_id": "candidate-1", "horizon_sec": 180, "result": "OK"},
            {"candidate_id": "different-candidate", "ticker": "SBER",
             "horizon_sec": 300, "result": "OK"},
        ]
        result = build_shadow_reconciliation(trades, labels)
        self.assertEqual(result["realized_closed_trades"], 3)
        self.assertEqual(result["matched_trades"], 1)
        self.assertEqual(result["unmatched_trades"], 2)
        self.assertEqual(
            result["counts_by_status"]["MISSING_TRADE_CANDIDATE_ID"], 1
        )
        self.assertEqual(
            result["counts_by_status"]["NO_SHADOW_LABEL_FOR_CANDIDATE"], 1
        )
        rows = {row["trade_id"]: row for row in result["trades"]}
        self.assertEqual(rows["trade-exact"]["status"], "MATCHED_EXACT_ID")
        self.assertEqual(rows["trade-exact"]["shadow_horizons_sec"], [60, 180])
        self.assertEqual(
            rows["trade-same-ticker"]["status"],
            "NO_SHADOW_LABEL_FOR_CANDIDATE",
        )
        self.assertEqual(rows["trade-no-id"]["status"], "MISSING_TRADE_CANDIDATE_ID")
        self.assertEqual(result["shadow_candidates_without_realized_trade"], 1)

    def test_invalid_horizons_are_ignored_and_limit_only_trims_details(self):
        trades = [
            {"id": "new", "shadow_candidate_id": "c", "exit_timestamp": "2026-11-18T12:00:00"},
            {"id": "old", "shadow_candidate_id": "c", "exit_timestamp": "2026-11-18T11:00:00"},
        ]
        labels = [
            {"candidate_id": "c", "horizon_sec": "60"},
            {"candidate_id": "c", "horizon_sec": "nan"},
            {"candidate_id": "c", "horizon_sec": -1},
            {"candidate_id": "c", "horizon_sec": "bad"},
        ]
        result = build_shadow_reconciliation(trades, labels, limit=1)
        self.assertEqual(result["matched_trades"], 2)
        self.assertEqual(len(result["trades"]), 1)
        self.assertEqual(result["trades"][0]["trade_id"], "new")
        self.assertEqual(result["trades"][0]["shadow_horizons_sec"], [60])


if __name__ == "__main__":
    unittest.main()
