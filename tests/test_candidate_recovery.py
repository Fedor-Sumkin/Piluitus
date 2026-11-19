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

    def test_rejects_stale_order_book_even_when_quote_sides_are_fresh(self):
        now = 100.0
        screener = FakeScreener(now)
        screener.quotes["SBER"]["last_order_book_seen"] = now - 2.5
        metrics = {}

        ok, reason = refresh_candidate_bbo_metrics(screener, "SBER", metrics, now=now)

        self.assertFalse(ok)
        self.assertIn("ORDER_BOOK устарело", reason)
        self.assertNotIn("spread_pct", metrics)

    def test_rejects_invalid_or_crossed_book(self):
        now = 100.0
        screener = FakeScreener(now)
        screener.quotes["SBER"]["ask"] = 99.5
        metrics = {}

        ok, reason = refresh_candidate_bbo_metrics(screener, "SBER", metrics, now=now)

        self.assertFalse(ok)
        self.assertIn("непротиворечив", reason)

    def test_refresh_post_llm_evidence_refreshes_bbo_and_trade_flow(self):
        from candidate_recovery import refresh_post_llm_evidence

        now = 100.0
        screener = FakePostLLMScreener(now)
        original = {
            "trade_data_mode": 1.0,
            "trade_data_ready": 1.0,
            "trade_metrics_age_sec": 1.0,
            "current_price": 99.5,
            "price_change_pct": -0.1,
            "price_vs_vwap_pct": -0.05,
            "volume_spike": 1.0,
            "bid": 99.0,
            "ask": 99.2,
            "bid_ask_available": 1.0,
            "spread_available": 1.0,
            "order_book_valid": 1.0,
        }

        evidence, diagnostics = refresh_post_llm_evidence(
            screener, "SBER@MISX", original, now=now
        )

        self.assertEqual(diagnostics["bbo_refresh_status"], "PASS")
        self.assertEqual(diagnostics["trade_refresh_status"], "PASS")
        self.assertTrue(diagnostics["price_refreshed"])
        self.assertEqual(evidence["bid"], 100.0)
        self.assertEqual(evidence["ask"], 100.1)
        self.assertEqual(evidence["current_price"], 100.05)
        self.assertEqual(evidence["volume_spike"], 4.0)
        self.assertEqual(evidence["price_change_pct"], 0.15)
        self.assertEqual(evidence["post_llm_bbo_refresh_valid"], 1.0)
        self.assertEqual(evidence["post_llm_trade_refresh_valid"], 1.0)
        # The caller's pre-LLM snapshot is not mutated.
        self.assertEqual(original["bid"], 99.0)
        self.assertEqual(original["volume_spike"], 1.0)

    def test_failed_post_llm_bbo_refresh_invalidates_old_quote_flags(self):
        from candidate_recovery import refresh_post_llm_evidence

        now = 100.0
        screener = FakePostLLMScreener(now, trade_metrics={
            "trade_data_ready": 1.0,
            "trade_metrics_age_sec": 0.2,
            "current_price": 100.05,
        })
        screener.quotes["SBER"]["last_order_book_seen"] = now - 3.0
        evidence, diagnostics = refresh_post_llm_evidence(
            screener,
            "SBER",
            {
                "trade_data_mode": 0.0,
                "bid": 99.0,
                "ask": 99.1,
                "bid_ask_available": 1.0,
                "spread_available": 1.0,
                "order_book_valid": 1.0,
            },
            now=now,
        )

        self.assertEqual(diagnostics["bbo_refresh_status"], "BLOCKED")
        self.assertEqual(evidence["bid_ask_available"], 0.0)
        self.assertEqual(evidence["spread_available"], 0.0)
        self.assertEqual(evidence["order_book_valid"], 0.0)
        self.assertEqual(evidence["post_llm_bbo_refresh_valid"], 0.0)

    def test_failed_post_llm_trade_refresh_fails_closed(self):
        from candidate_recovery import refresh_post_llm_evidence

        now = 100.0
        screener = FakePostLLMScreener(now, trade_error=RuntimeError("stream unavailable"))
        evidence, diagnostics = refresh_post_llm_evidence(
            screener,
            "SBER",
            {
                "trade_data_mode": 1.0,
                "trade_data_ready": 1.0,
                "trade_metrics_age_sec": 0.2,
                "volume_spike": 20.0,
            },
            now=now,
        )

        self.assertEqual(diagnostics["trade_refresh_status"], "ERROR")
        self.assertEqual(evidence["trade_data_ready"], 0.0)
        self.assertEqual(evidence["trade_metrics_age_sec"], -1.0)
        self.assertEqual(evidence["volume_spike"], 0.0)
        self.assertEqual(evidence["post_llm_trade_refresh_valid"], 0.0)


    def test_expired_size_drop_is_not_reused(self):
        now = 100.0
        screener = FakeScreener(now)
        screener.quotes["SBER"]["ask_size_drop_at"] = now - 3.0
        metrics = {}

        ok, reason = refresh_candidate_bbo_metrics(screener, "SBER", metrics, now=now)

        self.assertTrue(ok, reason)
        self.assertEqual(metrics["ask_size_drop_pct"], 0.0)
        self.assertEqual(metrics["ask_size_drop_age_sec"], -1.0)


class FakePostLLMScreener(FakeScreener):
    def __init__(self, now: float, trade_metrics=None, *, trade_error=None) -> None:
        super().__init__(now)
        self.trade_metrics = trade_metrics or {
            "trade_data_ready": 1.0,
            "trade_metrics_age_sec": 0.25,
            "trade_vwap_60s": 100.0,
            "trade_volume_5s": 20.0,
            "trade_volume_baseline_5s": 5.0,
            "volume_spike": 4.0,
            "volume_spike_raw": 4.0,
            "trade_count_5s": 6,
            "price_change_pct": 0.15,
            "trade_price_change_30s_pct": 0.22,
            "trade_realized_volatility_60s_pct": 0.04,
            "trade_flow_imbalance_30s": 0.30,
            "trade_buy_volume_30s": 30.0,
            "trade_sell_volume_30s": 10.0,
            "trade_count_60s": 20,
            "trade_history_coverage_sec": 60.0,
            "current_price": 100.05,
            "vwap": 100.0,
            "price_vs_vwap_pct": 0.05,
        }
        self.trade_error = trade_error

    def refresh_trade_metrics(self, ticker: str, now: float | None = None):
        if self.trade_error:
            raise self.trade_error
        return dict(self.trade_metrics)


if __name__ == "__main__":
    unittest.main()
