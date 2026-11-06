from __future__ import annotations

import unittest

from candidate_recovery import refresh_candidate_bbo_metrics


class QueueImbalanceRecoveryTests(unittest.TestCase):
    def test_refresh_recalculates_normalized_queue_imbalance_from_fresh_book(self) -> None:
        class Screener:
            bbo_max_age_sec = 2.0
            size_signal_ttl_sec = 2.0
            quotes = {
                "SBER": {
                    "bid": 100.0,
                    "ask": 100.1,
                    "bid_size": 30.0,
                    "ask_size": 10.0,
                    "order_book_valid": True,
                    "last_bid_confirmed_seen": 99.9,
                    "last_ask_confirmed_seen": 99.9,
                    "last_bid_size_seen": 99.9,
                    "last_ask_size_seen": 99.9,
                    "last_order_book_seen": 99.9,
                    "bid_size_drop_pct": 0.0,
                    "ask_size_drop_pct": 0.0,
                }
            }

        metrics = {}
        ready, reason = refresh_candidate_bbo_metrics(Screener(), "SBER", metrics, now=100.0)
        self.assertTrue(ready, reason)
        self.assertEqual(metrics["queue_imbalance_available"], 1.0)
        self.assertAlmostEqual(metrics["queue_imbalance"], 0.5)


if __name__ == "__main__":
    unittest.main()
