from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from finam_client import FinamBrokerClient


class FinamOrderBookParserDiagnosticsTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FinamBrokerClient(token="test-token")
        self.now = datetime.now(timezone.utc)

    def envelope(self, timestamp: datetime, book: dict) -> dict:
        return {
            "type": "DATA",
            "subscription_type": "ORDER_BOOK",
            "timestamp": timestamp.isoformat(),
            "payload": {"order_book": [book]},
        }

    def test_out_of_order_delta_is_classified_by_ticker_and_not_applied(self) -> None:
        snapshot = {
            "symbol": "SBER@MISX",
            "is_snapshot": True,
            "bids": [{"price": 100.0, "buySize": 10.0}],
            "asks": [{"price": 101.0, "sellSize": 12.0}],
        }
        first = self.client._extract_order_book_updates(
            self.envelope(self.now, snapshot)
        )
        self.assertEqual(len(first), 1)
        self.assertTrue(first[0]["book_valid"])

        old_delta = {
            "symbol": "SBER@MISX",
            "rows": [{"price": 100.0, "buySize": 2.0}],
        }
        second = self.client._extract_order_book_updates(
            self.envelope(self.now - timedelta(seconds=3), old_delta)
        )

        self.assertEqual(second, [])
        self.assertEqual(self.client._last_order_book_out_of_order_tickers, {"SBER"})
        self.assertNotIn("SBER", self.client._last_order_book_unrecognized_tickers)
        self.assertGreater(
            self.client._last_order_book_out_of_order_details["SBER"]["age_delta_ms"],
            0.0,
        )
        state = self.client._order_book_state["SBER"]
        self.assertEqual(state["bids"][100.0], 10.0)

    def test_unrecognized_symbol_is_not_misclassified_as_out_of_order(self) -> None:
        malformed = {"symbol": "GAZP@MISX", "message": "unsupported payload"}
        updates = self.client._extract_order_book_updates(
            self.envelope(self.now, malformed)
        )

        self.assertEqual(updates, [])
        self.assertEqual(self.client._last_order_book_unrecognized_tickers, {"GAZP"})
        self.assertEqual(self.client._last_order_book_out_of_order_tickers, set())


if __name__ == "__main__":
    unittest.main()
