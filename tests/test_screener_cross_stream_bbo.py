from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from candidate_recovery import refresh_candidate_bbo_metrics
from screener_funnel import HFTMemoryScreener


class ScreenerCrossStreamBboTests(unittest.TestCase):
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
            asyncio.run(screener.push_tick(
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
