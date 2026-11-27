from __future__ import annotations

import json
import unittest
from unittest.mock import AsyncMock, patch

from storage.db_manager import DBManager, RedisClient


class FakeRedis:
    def __init__(self, values):
        self.values = dict(values)

    async def scan(self, cursor=0, match=None, count=200):
        keys = [key.encode() for key in self.values if key.startswith("piluitus:paper:archive:position:")]
        return 0, keys

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, ex=None):
        self.values[key] = value
        return True

    async def delete(self, key):
        self.values.pop(key, None)
        return 1


def paper_trade(order_id="PAPER_1", status="OPEN", **overrides):
    row = {
        "id": order_id, "mode": "PAPER", "status": status,
        "ticker": "SBER", "side": "BUY", "entry_price": 100.0,
        "stop_loss": 99.0, "initial_stop_loss": 99.0,
        "take_profit": 102.0, "volume": 10, "lots": 1,
        "original_volume": 10, "original_lots": 1,
        "paper_trailing_enabled": True, "paper_trailing_armed": True,
        "paper_trailing_high_water_price": 101.0,
    }
    row.update(overrides)
    return row


class DBPaperPositionRestoreTests(unittest.IsolatedAsyncioTestCase):
    async def test_complete_open_position_is_restored_and_archive_removed(self):
        archive_key = "piluitus:paper:archive:position:PAPER_1"
        client = FakeRedis({archive_key: json.dumps({
            "trade_id": "PAPER_1", "original_status": "OPEN", "trade": paper_trade(),
        })})
        db = DBManager()
        db._redis = client

        async def write_trade(order_id, trade_dict, client=None, **kwargs):
            await client.set(f"piluitus:trade:{order_id}", json.dumps(trade_dict))

        with patch.object(RedisClient, "write_trade", new=AsyncMock(side_effect=write_trade)):
            result = await db.restore_archived_paper_positions()

        restored = json.loads(client.values["piluitus:trade:PAPER_1"])
        self.assertEqual(result["restored_open_positions"], 1)
        self.assertEqual(restored["status"], "OPEN")
        self.assertTrue(restored["restored_after_restart"])
        self.assertEqual(restored["paper_trailing_high_water_price"], 101.0)
        self.assertNotIn(archive_key, client.values)

    async def test_invalid_open_position_is_reinserted_as_stale_review(self):
        archive_key = "piluitus:paper:archive:position:PAPER_2"
        client = FakeRedis({archive_key: json.dumps({
            "trade_id": "PAPER_2", "original_status": "OPEN",
            "trade": paper_trade("PAPER_2", stop_loss=103.0),
        })})
        db = DBManager()
        db._redis = client

        async def write_trade(order_id, trade_dict, client=None, **kwargs):
            await client.set(f"piluitus:trade:{order_id}", json.dumps(trade_dict))

        with patch.object(RedisClient, "write_trade", new=AsyncMock(side_effect=write_trade)):
            result = await db.restore_archived_paper_positions()

        restored = json.loads(client.values["piluitus:trade:PAPER_2"])
        self.assertEqual(result["positions_requiring_review"], 1)
        self.assertEqual(restored["status"], "STALE_REVIEW")
        self.assertEqual(restored["reconciliation_status"], "REVIEW_REQUIRED")

    async def test_non_paper_archive_is_not_restored(self):
        archive_key = "piluitus:paper:archive:position:LIVE_1"
        client = FakeRedis({archive_key: json.dumps({
            "trade_id": "LIVE_1", "original_status": "OPEN",
            "trade": paper_trade("LIVE_1", mode="LIVE"),
        })})
        db = DBManager()
        db._redis = client
        with patch.object(RedisClient, "write_trade", new=AsyncMock()) as writer:
            result = await db.restore_archived_paper_positions()
        self.assertEqual(result["restored_open_positions"], 0)
        writer.assert_not_awaited()
        self.assertIn(archive_key, client.values)


if __name__ == "__main__":
    unittest.main()
