import asyncio
import unittest

from exchange.finam_client import _coalesce_queued_market_event


class FinamQueuePolicyTests(unittest.IsolatedAsyncioTestCase):
    async def test_order_book_snapshot_replaces_only_same_symbol_snapshot(self):
        queue = asyncio.Queue(maxsize=2)
        old = {"event_type": "ORDER_BOOK", "ticker": "HYDR", "bid": 10.0}
        other = {"event_type": "ORDER_BOOK", "ticker": "ROSN", "bid": 20.0}
        latest = {"event_type": "ORDER_BOOK", "ticker": "HYDR", "bid": 11.0}
        queue.put_nowait(old)
        queue.put_nowait(other)

        self.assertTrue(_coalesce_queued_market_event(queue, latest))
        self.assertEqual(queue.qsize(), 2)
        first = await queue.get()
        self.assertIs(first, latest)
        queue.task_done()
        second = await queue.get()
        self.assertIs(second, other)
        queue.task_done()
        await queue.join()

    async def test_quote_snapshot_can_replace_same_ticker_quote(self):
        queue = asyncio.Queue(maxsize=1)
        queue.put_nowait({"ticker": "SBER", "price": 100.0})
        latest = {"ticker": "SBER", "price": 101.0}

        self.assertTrue(_coalesce_queued_market_event(queue, latest))
        self.assertIs(await queue.get(), latest)
        queue.task_done()
        await queue.join()

    async def test_trade_prints_are_never_coalesced(self):
        queue = asyncio.Queue(maxsize=1)
        queue.put_nowait({"event_type": "INSTRUMENT_TRADE", "ticker": "SBER", "trade_id": "1"})

        self.assertFalse(_coalesce_queued_market_event(
            queue,
            {"event_type": "INSTRUMENT_TRADE", "ticker": "SBER", "trade_id": "2"},
        ))
        self.assertEqual(queue.qsize(), 1)

    async def test_different_tickers_are_not_overwritten(self):
        queue = asyncio.Queue(maxsize=1)
        queue.put_nowait({"event_type": "ORDER_BOOK", "ticker": "HYDR", "bid": 10.0})

        self.assertFalse(_coalesce_queued_market_event(
            queue,
            {"event_type": "ORDER_BOOK", "ticker": "ROSN", "bid": 20.0},
        ))
        self.assertEqual((await queue.get())["ticker"], "HYDR")
        queue.task_done()
        await queue.join()


if __name__ == "__main__":
    unittest.main()
