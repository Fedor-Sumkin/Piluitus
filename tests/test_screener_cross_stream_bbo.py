from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from candidate_recovery import refresh_candidate_bbo_metrics
from screener_funnel import HFTMemoryScreener


class ScreenerCrossStreamBboTests(unittest.TestCase):
    def test_small_order_book_timestamp_regression_does_not_age_or_reject_position_bbo(self):
        screener = HFTMemoryScreener(top_n=1, min_samples=3)
        screener.set_universe(["TESTBBO"])

        with patch("screener_funnel.time.monotonic", return_value=10.02), patch(
            "screener_funnel._event_timestamp_to_monotonic",
            side_effect=lambda value, received: float(value) if value is not None else received,
        ):
            first = asyncio.run(screener.push_order_book(
                "TESTBBO", 100.0, 100.1, 10.0, 12.0,
                bid_present=True, ask_present=True,
                bid_size_present=True, ask_size_present=True,
                bid_timestamp=10.0, ask_timestamp=10.0,
                order_book_timestamp=10.0, received_monotonic=10.0,
            ))
            second = asyncio.run(screener.push_order_book(
                "TESTBBO", 100.05, 100.15, 11.0, 13.0,
                bid_present=True, ask_present=True,
                bid_size_present=True, ask_size_present=True,
                bid_timestamp=9.99, ask_timestamp=9.99,
                order_book_timestamp=9.99, received_monotonic=10.02,
            ))
            rejected = asyncio.run(screener.push_order_book(
                "TESTBBO", 100.06, 100.16, 12.0, 14.0,
                bid_present=True, ask_present=True,
                bid_size_present=True, ask_size_present=True,
                bid_timestamp=9.97, ask_timestamp=9.97,
                order_book_timestamp=9.97, received_monotonic=10.03,
            ))

        self.assertTrue(first.get("updated"))
        self.assertTrue(second.get("updated"))
        self.assertTrue(second.get("valid"))
        quote = screener.quotes["TESTBBO"]
        self.assertEqual(quote["bid"], 100.05)
        self.assertEqual(quote["ask"], 100.15)
        self.assertEqual(quote["last_order_book_event_seen"], 10.0)
        self.assertEqual(quote["last_order_book_seen"], 10.0)
        self.assertEqual(quote["last_bid_confirmed_seen"], 10.0)
        self.assertEqual(quote["last_ask_confirmed_seen"], 10.0)
        self.assertTrue(quote["ofi_history"])
        self.assertEqual(quote["ofi_history"][-1][0], 10.0)
        self.assertFalse(rejected.get("updated"))
        self.assertTrue(rejected.get("out_of_order"))
        self.assertEqual(quote["bid"], 100.05)
        self.assertEqual(quote["ask"], 100.15)

    def test_later_received_order_book_confirms_after_slight_cross_stream_timestamp_skew(self):
        screener = HFTMemoryScreener(top_n=1, min_samples=3)
        screener.set_universe(["TESTBBO"])

        # Event timestamps are deliberately in the same monotonic domain for
        # this test: QUOTES is 150ms newer, but the coherent ORDER_BOOK packet
        # is received later. Freshness still uses the provider event timestamp.
        with patch("screener_funnel.time.monotonic", return_value=10.0), patch(
            "screener_funnel._event_timestamp_to_monotonic",
            side_effect=lambda value, received: float(value) if value is not None else received,
        ):
            quote_result = asyncio.run(screener.push_tick(
                "TESTBBO", 100.0, 100.0, bid=100.0, ask=100.1,
                bid_size=10.0, ask_size=12.0,
                event_timestamp=9.95, received_monotonic=9.95,
            ))
            result = asyncio.run(screener.push_order_book(
                "TESTBBO", 100.0, 100.1, 10.0, 12.0,
                bid_present=True, ask_present=True,
                bid_size_present=True, ask_size_present=True,
                bid_timestamp=9.80, ask_timestamp=9.80,
                order_book_timestamp=9.80, received_monotonic=10.0,
            ))
            stale_quote_result = asyncio.run(screener.push_tick(
                "TESTBBO", 100.0, 100.0, bid=99.9, ask=100.2,
                bid_size=10.0, ask_size=12.0,
                event_timestamp=9.90, received_monotonic=10.1,
            ))

        self.assertEqual(
            quote_result.get("quote_side_updates"), {"bid": True, "ask": True}
        )
        self.assertEqual(
            stale_quote_result.get("quote_side_updates"), {"bid": False, "ask": False}
        )
        self.assertTrue(result.get("updated"))
        self.assertTrue(result.get("valid"))
        self.assertEqual(result.get("confirmed_sides"), 2)
        quote = screener.quotes["TESTBBO"]
        self.assertEqual(quote["last_bid_book_event_seen"], 9.80)
        self.assertEqual(quote["last_bid_quotes_event_seen"], 9.95)
        self.assertEqual(quote["last_bid_source_received_seen"], 10.0)
        self.assertEqual(quote["last_bid_confirmed_seen"], 9.80)

        metrics = {}
        ok, reason = refresh_candidate_bbo_metrics(screener, "TESTBBO", metrics, now=10.0)
        self.assertTrue(ok, reason)
        self.assertEqual(metrics["bid"], 100.0)
        self.assertEqual(metrics["ask"], 100.1)


if __name__ == "__main__":
    unittest.main()
