from __future__ import annotations

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
