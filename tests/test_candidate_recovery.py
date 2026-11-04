from __future__ import annotations

import threading
import unittest

from candidate_recovery import refresh_candidate_bbo_metrics


class FakeScreener:
    def __init__(self, now: float) -> None:
        self._lock = threading.RLock()
        self.bbo_max_age_sec = 2.0
        self.size_signal_ttl_sec = 2.0
        self.quotes = {
            "SBER": {
                "bid": 100.0,
                "ask": 100.1,
                "bid_size": 20.0,
                "ask_size": 15.0,
                "order_book_valid": True,
                "last_bid_confirmed_seen": now - 0.20,
                "last_ask_confirmed_seen": now - 0.30,
                "last_bid_seen": now - 3.0,
                "last_ask_seen": now - 4.0,
                "last_bid_size_seen": now - 0.2,
                "last_ask_size_seen": now - 0.2,
                "bid_size_drop_pct": 60.0,
                "ask_size_drop_pct": 55.0,
                "bid_size_drop_at": now - 0.4,
                "ask_size_drop_at": now - 0.5,
                "top_bid_history_samples": 4,
                "top_ask_history_samples": 5,
                "last_order_book_seen": now - 0.2,
            }
        }

    @staticmethod
    def _current_top_size(quote, side, now):
        seen = quote.get(f"last_{side}_size_seen", 0.0)
        size = quote.get(f"{side}_size", 0.0)
        return size if seen > 0 and now - seen <= 2.0 else 0.0

    @staticmethod
    def _current_size_drop(quote, side, now):
        seen = quote.get(f"{side}_size_drop_at", 0.0)
        value = quote.get(f"{side}_size_drop_pct", 0.0)
        return value if seen > 0 and now - seen <= 2.0 else 0.0


class CandidateRecoveryTests(unittest.TestCase):
    def test_refreshes_quote_fields_but_preserves_original_trade_evidence(self):
        now = 100.0
        screener = FakeScreener(now)
        metrics = {
            "current_price": 99.9,
            "price_change_pct": 0.2,
            "price_vs_vwap_pct": 0.1,
            "volume_spike": 2.0,
            "bid": 99.0,
            "ask": 99.2,
            "bid_ask_available": 0.0,
        }

        ok, reason = refresh_candidate_bbo_metrics(screener, "SBER@MISX", metrics, now=now)

        self.assertTrue(ok, reason)
        self.assertEqual(metrics["bid"], 100.0)
        self.assertEqual(metrics["ask"], 100.1)
        self.assertAlmostEqual(metrics["spread_pct"], (0.1 / 100.05) * 100.0)
        self.assertEqual(metrics["bid_ask_available"], 1.0)
        self.assertEqual(metrics["ask_size_drop_pct"], 55.0)
        self.assertEqual(metrics["top_ask_history_samples"], 5)
        self.assertEqual(metrics["current_price"], 99.9)
        self.assertEqual(metrics["price_change_pct"], 0.2)
        self.assertEqual(metrics["volume_spike"], 2.0)

    def test_rejects_expired_bbo_confirmation(self):
        now = 100.0
        screener = FakeScreener(now)
        screener.quotes["SBER"]["last_bid_confirmed_seen"] = now - 3.0
        metrics = {}

        ok, reason = refresh_candidate_bbo_metrics(screener, "SBER", metrics, now=now)

        self.assertFalse(ok)
        self.assertIn("свежего подтверждения", reason)
        self.assertNotIn("spread_pct", metrics)

    def test_rejects_invalid_or_crossed_book(self):
        now = 100.0
        screener = FakeScreener(now)
        screener.quotes["SBER"]["ask"] = 99.5
        metrics = {}

        ok, reason = refresh_candidate_bbo_metrics(screener, "SBER", metrics, now=now)

        self.assertFalse(ok)
        self.assertIn("непротиворечив", reason)

    def test_expired_size_drop_is_not_reused(self):
        now = 100.0
        screener = FakeScreener(now)
        screener.quotes["SBER"]["ask_size_drop_at"] = now - 3.0
        metrics = {}

        ok, reason = refresh_candidate_bbo_metrics(screener, "SBER", metrics, now=now)

        self.assertTrue(ok, reason)
        self.assertEqual(metrics["ask_size_drop_pct"], 0.0)
        self.assertEqual(metrics["ask_size_drop_age_sec"], -1.0)


if __name__ == "__main__":
    unittest.main()
