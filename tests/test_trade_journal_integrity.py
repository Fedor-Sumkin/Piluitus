from __future__ import annotations

import fnmatch
import json
import unittest
from unittest.mock import AsyncMock, patch

from storage.db_manager import DBManager, RedisClient


class FakeRedis:
    def __init__(self, raw_value: object) -> None:
        self.raw_value = raw_value

    async def scan(self, cursor: int, match: str, count: int):
        return 0, [b"piluitus:trade:PAPER_BROKEN"]

    async def get(self, key: str):
        return self.raw_value


class FakeListRedis:
    def __init__(self) -> None:
        self.lists: dict[str, list[str]] = {}
        self.values: dict[str, str] = {}

    async def scan(self, cursor: int, match: str, count: int):
        keys = [key.encode() for key in self.values if fnmatch.fnmatch(key, match)]
        return 0, keys

    async def get(self, key: str):
        return self.values.get(key)

    async def delete(self, *keys: str) -> int:
        deleted = 0
        for key in keys:
            if key in self.values:
                self.values.pop(key, None)
                deleted += 1
            if key in self.lists:
                self.lists.pop(key, None)
                deleted += 1
        return deleted

    async def lrange(self, key: str, start: int, stop: int):
        rows = self.lists.get(key, [])
        end = len(rows) if stop < 0 else stop + 1
        return rows[start:end]

    async def lpush(self, key: str, value: str) -> int:
        self.lists.setdefault(key, []).insert(0, value)
        return len(self.lists[key])

    async def ltrim(self, key: str, start: int, end: int) -> None:
        self.lists[key] = self.lists.get(key, [])[start:end + 1]


class TradeJournalIntegrityTests(unittest.IsolatedAsyncioTestCase):
    async def test_corrupt_json_aborts_full_journal_read(self) -> None:
        client = FakeRedis(b'{"status": ')
        with patch.object(RedisClient, "get_client", new=AsyncMock(return_value=client)):
            with self.assertRaisesRegex(RuntimeError, "полном журнале"):
                await DBManager().get_all_daily_trades()

    async def test_non_object_json_aborts_full_journal_read(self) -> None:
        client = FakeRedis(b'["not", "a", "trade", "record"]')
        with patch.object(RedisClient, "get_client", new=AsyncMock(return_value=client)):
            with self.assertRaisesRegex(RuntimeError, "Некорректный формат"):
                await DBManager().get_all_daily_trades()

    async def test_batched_list_persistence_keeps_newest_first_order_and_limit(self) -> None:
        client = FakeListRedis()
        with patch.object(RedisClient, "get_client", new=AsyncMock(return_value=client)):
            await DBManager().push_many_to_list(
                "piluitus:strategy:shadow:outcomes",
                [{"candidate_id": "old"}, {"candidate_id": "new"}],
                max_len=1,
            )

        self.assertEqual(len(client.lists["piluitus:strategy:shadow:outcomes"]), 1)
        self.assertEqual(
            client.lists["piluitus:strategy:shadow:outcomes"][0],
            '{"candidate_id": "new"}',
        )

    async def test_paper_session_clear_removes_dedicated_closed_trade_journal(self) -> None:
        client = FakeListRedis()
        closed = {
            "id": "PAPER_CLOSED_1",
            "status": "CLOSED",
            "mode": "PAPER",
            "side": "BUY",
            "pnl": 12.5,
        }
        client.values["piluitus:trade:PAPER_OPEN_1"] = json.dumps({
            "id": "PAPER_OPEN_1", "status": "OPEN", "mode": "PAPER",
        })
        client.lists["piluitus:paper:closed_trades"] = [json.dumps(closed)]
        client.lists["piluitus:trade_history"] = [json.dumps(closed)]

        with patch.object(RedisClient, "get_client", new=AsyncMock(return_value=client)):
            db = DBManager()
            snapshot = await db.get_paper_session_snapshot()
            result = await db.clear_paper_session()

        self.assertEqual(snapshot["paper_closed_journal_count"], 1)
        self.assertEqual(result["deleted_paper_performance_rows"], 1)
        self.assertEqual(result["deleted_paper_performance_keys"], 1)
        self.assertNotIn("piluitus:paper:closed_trades", client.lists)

    async def test_panic_journal_clear_removes_realized_paper_journal(self) -> None:
        client = FakeListRedis()
        client.lists["piluitus:paper:closed_trades"] = [
            json.dumps({"id": "PAPER_CLOSED_2", "status": "CLOSED", "mode": "PAPER"})
        ]
        with patch.object(RedisClient, "get_client", new=AsyncMock(return_value=client)):
            await DBManager().clear_journal()
        self.assertNotIn("piluitus:paper:closed_trades", client.lists)

    async def test_valid_trade_record_is_returned(self) -> None:
        client = FakeRedis(b'{"status":"OPEN","mode":"PAPER","ticker":"SBER"}')
        with patch.object(RedisClient, "get_client", new=AsyncMock(return_value=client)):
            result = await DBManager().get_all_daily_trades()

        self.assertEqual(
            result,
            {"PAPER_BROKEN": {"status": "OPEN", "mode": "PAPER", "ticker": "SBER"}},
        )


if __name__ == "__main__":
    unittest.main()
