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
