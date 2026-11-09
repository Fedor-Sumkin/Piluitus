from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock

from finam_client import FinamBrokerClient


class FinamOrderBookResyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_rest_snapshot_is_diagnostic_only_and_does_not_unblock_ws_snapshot_gate(self):
        client = FinamBrokerClient(token="test-token")
        client._order_book_snapshot_required.add("SBER")
        client._order_book_state["SBER"] = {
            "bids": {99.0: 10.0}, "asks": {102.0: 12.0},
            "last_event_monotonic": 123.0,
        }
        original_state = {
            "bids": dict(client._order_book_state["SBER"]["bids"]),
            "asks": dict(client._order_book_state["SBER"]["asks"]),
            "last_event_monotonic": 123.0,
        }
        client.get_ticker_order_book = AsyncMock(return_value={
            "bids": [{"price": 100.0, "volume": 10}],
            "asks": [{"price": 101.0, "volume": 12}],
        })
        queue = asyncio.Queue()

        refreshed = await client.refresh_order_book_snapshot("SBER", queue)

        self.assertTrue(refreshed)
        self.assertTrue(queue.empty())
        self.assertEqual(client._order_book_state["SBER"], original_state)
        self.assertIn("SBER", client._order_book_snapshot_required)
        client.get_ticker_order_book.assert_awaited_once_with("SBER")

    async def test_crossed_or_incomplete_rest_snapshot_is_rejected(self):
        client = FinamBrokerClient(token="test-token")
        client.get_ticker_order_book = AsyncMock(return_value={
            "bids": [{"price": 102.0, "volume": 10}],
            "asks": [{"price": 101.0, "volume": 12}],
        })
        queue = asyncio.Queue()

        refreshed = await client.refresh_order_book_snapshot("SBER", queue)

        self.assertFalse(refreshed)
        self.assertTrue(queue.empty())

    async def test_empty_rest_snapshot_never_erases_existing_book(self):
        client = FinamBrokerClient(token="test-token")
        client.get_ticker_order_book = AsyncMock(return_value={"bids": [], "asks": []})
        queue = asyncio.Queue()

        refreshed = await client.refresh_order_book_snapshot("SBER", queue)

        self.assertFalse(refreshed)
        self.assertTrue(queue.empty())


if __name__ == "__main__":
    unittest.main()
