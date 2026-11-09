from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock

from finam_client import FinamBrokerClient


class FinamOrderBookResyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_rest_snapshot_rebuilds_book_and_unblocks_snapshot_gate(self):
        client = FinamBrokerClient(token="test-token")
        client._order_book_snapshot_required.add("SBER")
        client.get_ticker_order_book = AsyncMock(return_value={
            "bids": [{"price": 100.0, "volume": 10}],
            "asks": [{"price": 101.0, "volume": 12}],
        })
        queue = asyncio.Queue()

        refreshed = await client.refresh_order_book_snapshot("SBER", queue)

        self.assertTrue(refreshed)
        update = queue.get_nowait()
        self.assertTrue(update["book_valid"])
        self.assertEqual(update["timestamp_source"], "REST_SNAPSHOT")
        self.assertEqual(update["snapshot_source"], "REST_CONTROL_REFRESH")
        self.assertNotIn("SBER", client._order_book_snapshot_required)
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
