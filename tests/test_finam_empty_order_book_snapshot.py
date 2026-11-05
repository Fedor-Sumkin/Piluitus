"""Regression tests for Finam's explicit empty ORDER_BOOK snapshots."""
from __future__ import annotations

import json
import time
import unittest

from finam_client import FinamBrokerClient


class EmptyOrderBookSnapshotTests(unittest.TestCase):
    def setUp(self) -> None:
        self.client = FinamBrokerClient(token="unit-test-token")

    def _envelope(self, book: dict, event_time: float) -> dict:
        return {
            "payload": json.dumps({"orderBook": [book]}),
            "timestamp": event_time,
            "_received_monotonic": time.monotonic(),
        }

    def test_empty_snapshot_clears_previous_bbo_and_is_fail_closed(self) -> None:
        initial_time = time.time() - 1.0
        initial = self.client._extract_order_book_updates(
            self._envelope(
                {
                    "symbol": "WUSH@MISX",
                    "isDataSnapshot": True,
                    "rows": [
                        {
                            "price": {"value": "37.00"},
                            "buySize": {"value": "10"},
                            "action": "ACTION_UPDATE",
                        },
                        {
                            "price": {"value": "37.10"},
                            "sellSize": {"value": "12"},
                            "action": "ACTION_UPDATE",
                        },
                    ],
                },
                initial_time,
            )
        )
        self.assertEqual(len(initial), 1)
        self.assertTrue(initial[0]["book_valid"])
        self.assertEqual(initial[0]["bid"], 37.0)
        self.assertEqual(initial[0]["ask"], 37.1)

        empty = self.client._extract_order_book_updates(
            self._envelope(
                {"symbol": "WUSH@MISX", "isDataSnapshot": True},
                time.time(),
            )
        )

        self.assertTrue(self.client._last_order_book_payload_valid)
        self.assertEqual(len(empty), 1)
        update = empty[0]
        self.assertFalse(update["book_valid"])
        self.assertEqual(update["book_reason"], "EMPTY_SNAPSHOT")
        self.assertEqual(update["bid"], 0.0)
        self.assertEqual(update["ask"], 0.0)
        self.assertTrue(update["bid_present"])
        self.assertTrue(update["ask_present"])
        self.assertEqual(update["bid_levels"], [])
        self.assertEqual(update["ask_levels"], [])
        self.assertEqual(self.client._order_book_state["WUSH"]["bids"], {})
        self.assertEqual(self.client._order_book_state["WUSH"]["asks"], {})

    def test_empty_delta_without_snapshot_flag_is_not_accepted(self) -> None:
        updates = self.client._extract_order_book_updates(
            self._envelope({"symbol": "WUSH@MISX"}, time.time())
        )

        self.assertEqual(updates, [])
        self.assertFalse(self.client._last_order_book_payload_valid)
        self.assertNotIn("WUSH", self.client._order_book_state)


if __name__ == "__main__":
    unittest.main()
