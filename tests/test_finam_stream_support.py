"""Tests for extracted Finam WebSocket support objects."""
from __future__ import annotations
import asyncio
import unittest
from unittest.mock import patch
from exchange.finam_stream_support import FinamOrderBookSymbolDiagnostics, FinamPositionSnapshotRefresher, FinamQueueOverflowTracker

class FinamStreamSupportTests(unittest.TestCase):
    def test_symbol_diagnostics_preserve_mutable_counters(self):
        d = FinamOrderBookSymbolDiagnostics()
        first = d.for_ticker("SBER")
        first["window_wire"] = 3
        self.assertIs(d.for_ticker("SBER"), first)
        self.assertEqual(d.for_ticker("GAZP")["window_wire"], 0)
        self.assertEqual(len(list(d.values())), 2)

    def test_queue_overflow_tracker_rate_limits_and_resets_window(self):
        tracker = FinamQueueOverflowTracker()
        with patch("exchange.finam_stream_support.time.monotonic", side_effect=[6.0, 7.0]), patch(
            "exchange.finam_stream_support.logger.warning"
        ) as warning:
            tracker.record("QUOTES", "SBER")
            tracker.record("ORDER_BOOK", "GAZP")
        warning.assert_called_once()
        self.assertEqual(tracker.counts["ORDER_BOOK"], 1)
        self.assertEqual(tracker.counts["QUOTES"], 0)
        self.assertEqual(tracker.last_ticker, "GAZP")

    def test_position_snapshot_refresher_releases_inflight_after_success(self):
        class Broker:
            def __init__(self): self.calls = []
            async def refresh_order_book_snapshot(self, ticker, queue): self.calls.append((ticker, queue))
        broker, queue, inflight = Broker(), object(), {"SBER"}
        asyncio.run(FinamPositionSnapshotRefresher(broker, queue, inflight).refresh("sber@MISX"))
        self.assertEqual(broker.calls, [("SBER", queue)])
        self.assertEqual(inflight, set())

    def test_position_snapshot_refresher_releases_inflight_after_failure(self):
        class Broker:
            async def refresh_order_book_snapshot(self, ticker, queue): raise RuntimeError("offline")
        inflight = {"GAZP"}
        asyncio.run(FinamPositionSnapshotRefresher(Broker(), object(), inflight).refresh("GAZP@MISX"))
        self.assertEqual(inflight, set())

if __name__ == "__main__":
    unittest.main()
