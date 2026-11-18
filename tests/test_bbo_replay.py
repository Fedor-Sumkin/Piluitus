import unittest
from unittest.mock import patch

from strategy.bbo_replay import BBOReplayRecorder, _timestamp_epoch


class BBOReplayRecorderTests(unittest.IsolatedAsyncioTestCase):
    def test_records_state_changes_and_rate_limited_heartbeats(self):
        recorder = BBOReplayRecorder(
            queue_limit=100, heartbeat_interval_sec=1.0
        )
        base = {
            "updated": True,
            "valid": True,
            "bid": 100.0,
            "ask": 100.1,
            "bid_size": 10.0,
            "ask_size": 12.0,
        }
        with patch("strategy.bbo_replay.time.monotonic", side_effect=[10.0, 10.2, 11.1, 11.2]):
            self.assertTrue(recorder.record("TEST", base, {"order_book_timestamp": 1_800_000_000}))
            self.assertFalse(recorder.record("TEST", base, {"order_book_timestamp": 1_800_000_000.2}))
            changed = dict(base, bid=100.05)
            self.assertTrue(recorder.record("TEST", changed, {"order_book_timestamp": 1_800_000_001}))
            invalid = dict(base, valid=False, reason="CROSSED_BBO", bid=100.2, ask=100.1)
            self.assertTrue(recorder.record("TEST", invalid, {"order_book_timestamp": 1_800_000_002}))

        rows = [recorder._queue.get_nowait() for _ in range(3)]
        self.assertEqual([row["event_kind"] for row in rows], [
            "STATE_CHANGE", "STATE_CHANGE", "INVALIDATION"
        ])
        self.assertEqual([row["sequence"] for row in rows], [1, 2, 3])
        self.assertEqual(rows[0]["event_time_source"], "EXCHANGE")
        self.assertEqual(rows[0]["ticker"], "TEST")
        self.assertEqual(recorder.skipped_unchanged, 1)

    def test_records_source_switch_and_per_side_freshness(self):
        recorder = BBOReplayRecorder(queue_limit=100, heartbeat_interval_sec=5.0)
        snapshot = {
            "updated": True, "valid": True, "order_book_valid": True,
            "bid": 100.0, "ask": 100.1, "bid_size": 10.0, "ask_size": 12.0,
        }
        with patch("strategy.bbo_replay.time.monotonic", side_effect=[10.0, 10.2]):
            self.assertTrue(recorder.record(
                "TEST", snapshot,
                {"source": "FINAM_ORDER_BOOK", "timestamp": 1_800_000_000,
                 "received_monotonic": 10.0,
                 "bid_confirmed_monotonic": 9.9, "ask_confirmed_monotonic": 9.8},
            ))
            self.assertTrue(recorder.record(
                "TEST", snapshot,
                {"source": "FINAM_QUOTES", "timestamp": 1_800_000_000.2,
                 "received_monotonic": 10.2,
                 "bid_confirmed_monotonic": 10.2, "ask_confirmed_monotonic": 10.1},
            ))
        first = recorder._queue.get_nowait()
        second = recorder._queue.get_nowait()
        self.assertEqual(first["source"], "FINAM_ORDER_BOOK")
        self.assertEqual(second["source"], "FINAM_QUOTES")
        self.assertEqual(second["bid_age_sec_at_capture"], 0.0)
        self.assertAlmostEqual(second["ask_age_sec_at_capture"], 0.1)

    def test_size_only_updates_are_coalesced_until_heartbeat(self):
        recorder = BBOReplayRecorder(queue_limit=100, heartbeat_interval_sec=1.0)
        base = {
            "updated": True, "valid": True, "bid": 100.0, "ask": 100.1,
            "bid_size": 10.0, "ask_size": 12.0,
        }
        with patch("strategy.bbo_replay.time.monotonic", side_effect=[20.0, 20.2, 21.1]):
            self.assertTrue(recorder.record("SIZE", base, {"source": "FINAM_ORDER_BOOK"}))
            self.assertFalse(recorder.record(
                "SIZE", dict(base, bid_size=15.0), {"source": "FINAM_ORDER_BOOK"}
            ))
            self.assertTrue(recorder.record(
                "SIZE", dict(base, bid_size=16.0), {"source": "FINAM_ORDER_BOOK"}
            ))
        first = recorder._queue.get_nowait()
        heartbeat = recorder._queue.get_nowait()
        self.assertEqual(first["event_kind"], "STATE_CHANGE")
        self.assertEqual(heartbeat["event_kind"], "HEARTBEAT")
        self.assertEqual(heartbeat["bid_size"], 16.0)

    def test_rejects_invalid_executable_bbo(self):
        recorder = BBOReplayRecorder(queue_limit=100)
        result = recorder.record(
            "BAD",
            {"updated": True, "valid": True, "bid": 101, "ask": 100, "bid_size": 1, "ask_size": 1},
        )
        self.assertFalse(result)
        self.assertEqual(recorder.accepted, 0)
        self.assertEqual(recorder.status()["queued"], 0)

    def test_queue_overflow_does_not_advance_symbol_deduplication_state(self):
        recorder = BBOReplayRecorder(queue_limit=100)
        for index in range(100):
            self.assertTrue(recorder.record(
                f"FILL{index}",
                {"updated": True, "valid": True, "bid": 10 + index,
                 "ask": 10.1 + index, "bid_size": 1, "ask_size": 1},
            ))
        dropped = recorder.record(
            "RETRY",
            {"updated": True, "valid": True, "bid": 20, "ask": 20.1,
             "bid_size": 1, "ask_size": 1},
        )
        self.assertFalse(dropped)
        self.assertEqual(recorder.dropped_queue_full, 1)
        recorder._queue.get_nowait()
        retried = recorder.record(
            "RETRY",
            {"updated": True, "valid": True, "bid": 20, "ask": 20.1,
             "bid_size": 1, "ask_size": 1},
        )
        self.assertTrue(retried)
        self.assertEqual(recorder.accepted, 101)

    def test_timestamp_parser_supports_seconds_milliseconds_and_protobuf(self):
        self.assertEqual(_timestamp_epoch(1_800_000_000), 1_800_000_000)
        self.assertEqual(_timestamp_epoch(1_800_000_000_000), 1_800_000_000)
        self.assertEqual(
            _timestamp_epoch({"seconds": "1800000000", "nanos": 500_000_000}),
            1_800_000_000.5,
        )
        self.assertIsNone(_timestamp_epoch("not-a-timestamp"))

    async def test_writer_retries_transient_storage_errors(self):
        class FlakyDB:
            def __init__(self):
                self.attempts = 0
                self.saved = []

            async def push_many_to_list(self, key, items, max_len):
                self.attempts += 1
                if self.attempts < 3:
                    raise RuntimeError("temporary Memurai outage")
                self.saved.extend(items)

        db = FlakyDB()
        recorder = BBOReplayRecorder(
            queue_limit=100, batch_size=10, flush_interval_sec=0.01,
            storage_limit=50,
        )
        self.assertTrue(recorder.record(
            "RETRY",
            {"updated": True, "valid": True, "bid": 10, "ask": 10.1,
             "bid_size": 5, "ask_size": 6},
        ))
        recorder.start(db)
        await recorder.close()

        self.assertEqual(db.attempts, 3)
        self.assertEqual(len(db.saved), 1)
        self.assertEqual(recorder.persisted, 1)
        self.assertEqual(recorder.persistence_errors, 0)

    async def test_writer_batches_and_drains_on_close(self):
        class FakeDB:
            def __init__(self):
                self.calls = []

            async def push_many_to_list(self, key, items, max_len):
                self.calls.append((key, list(items), max_len))

        db = FakeDB()
        recorder = BBOReplayRecorder(
            queue_limit=100, batch_size=2, flush_interval_sec=0.01,
            heartbeat_interval_sec=0.1, storage_limit=50,
        )
        for index in range(3):
            self.assertTrue(recorder.record(
                f"T{index}",
                {"updated": True, "valid": True, "bid": 10 + index,
                 "ask": 10.1 + index, "bid_size": 5, "ask_size": 6},
            ))
        recorder.start(db)
        await recorder.close()

        self.assertEqual(sum(len(call[1]) for call in db.calls), 3)
        self.assertTrue(all(call[0] == "piluitus:paper:bbo_replay" for call in db.calls))
        self.assertTrue(all(call[2] == 50 for call in db.calls))
        self.assertEqual(recorder.persisted, 3)
        self.assertEqual(recorder.status()["queued"], 0)


if __name__ == "__main__":
    unittest.main()
