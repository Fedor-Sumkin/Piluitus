"""Durability and atomicity tests for the local Redis-compatible AOF fallback."""
from __future__ import annotations

import asyncio
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from storage.db_manager import DBManager, _InMemory


class InMemoryAOFTests(unittest.IsolatedAsyncioTestCase):
    async def test_mutations_survive_reload_and_compaction(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "fallback.aof")
            store = _InMemory(path)

            await asyncio.gather(
                *(store.set(f"key:{index}", f"value:{index}") for index in range(40))
            )
            await asyncio.gather(
                *(store.lpush_trim("events", str(index), 7) for index in range(40))
            )
            await store.compact_aof()

            restored = _InMemory(path)
            for index in range(40):
                self.assertEqual(await restored.get(f"key:{index}"), f"value:{index}")
            events = await restored.lrange("events", 0, -1)
            self.assertEqual(len(events), 7)
            self.assertEqual(len(set(events)), 7)

    async def test_concurrent_compaction_does_not_lose_concurrent_writes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = str(Path(directory) / "fallback.aof")
            store = _InMemory(path)

            async def write_batch() -> None:
                for index in range(30):
                    await store.set(f"concurrent:{index}", str(index))

            await asyncio.gather(
                write_batch(),
                *(store.compact_aof() for _ in range(12)),
            )
            restored = _InMemory(path)
            for index in range(30):
                self.assertEqual(await restored.get(f"concurrent:{index}"), str(index))

    async def test_failed_disk_write_does_not_publish_mutation_in_memory(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = _InMemory(str(Path(directory) / "fallback.aof"))
            with patch.object(store, "_sync_write_aof", side_effect=OSError("disk full")):
                with self.assertRaises(OSError):
                    await store.set("must-not-appear", "value")
            self.assertIsNone(await store.get("must-not-appear"))

    async def test_corrupt_aof_fails_closed_instead_of_restoring_partial_state(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fallback.aof"
            path.write_text('{"cmd":"set","key":"good","val":"before"}\n{broken json}\n', encoding="utf-8")
            store = _InMemory(str(path))

            with self.assertRaisesRegex(RuntimeError, "fail-closed"):
                await store.get("good")
            with self.assertRaisesRegex(RuntimeError, "fail-closed"):
                await store.set("new", "value")
            with self.assertRaisesRegex(RuntimeError, "fail-closed"):
                await store.ping()

    async def test_active_position_read_fails_closed_on_corrupt_trade_record(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = _InMemory(str(Path(directory) / "fallback.aof"))
            await store.set("piluitus:trade:PAPER_CORRUPT", "{not-json")
            database = DBManager()
            database._redis = store

            with self.assertRaisesRegex(RuntimeError, "Некорректная запись активной позиции"):
                await database.get_active_positions()

    async def test_paper_session_snapshot_fails_closed_on_corrupt_trade_record(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = _InMemory(str(Path(directory) / "fallback.aof"))
            await store.set("piluitus:trade:PAPER_CORRUPT", "{not-json")
            database = DBManager()
            database._redis = store

            with self.assertRaisesRegex(RuntimeError, "Некорректная запись журнала сделок"):
                await database.get_paper_session_snapshot()

    async def test_atomic_trade_write_restores_trade_and_active_order_index(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            store = _InMemory(str(Path(directory) / "fallback.aof"))
            database = DBManager()
            database._redis = store
            trade = {
                "id": "PAPER_ATOMIC_1",
                "mode": "PAPER",
                "status": "OPEN",
                "ticker": "SBER",
                "volume": 10,
            }
            await database.log_trade("PAPER_ATOMIC_1", trade)

            restored = _InMemory(store.aof_path)
            trade_payload = await restored.get("piluitus:trade:PAPER_ATOMIC_1")
            self.assertIsNotNone(trade_payload)
            self.assertEqual(__import__("json").loads(trade_payload), trade)
            index_rows = await restored.lrange("piluitus:active_orders", 0, -1)
            self.assertEqual(len(index_rows), 1)
            index_item = __import__("json").loads(index_rows[0])
            self.assertEqual(index_item["id"], "PAPER_ATOMIC_1")
            self.assertEqual(index_item["data"], trade)


if __name__ == "__main__":
    unittest.main()
