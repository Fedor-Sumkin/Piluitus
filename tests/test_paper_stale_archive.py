import json
import unittest

from db_manager import DBManager


class _FakeRedis:
    def __init__(self):
        self.values = {}
        self.lists = {}

    async def scan(self, cursor=0, match="*", count=200):
        import fnmatch
        return 0, [key for key in self.values if fnmatch.fnmatch(key, match)]

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value):
        self.values[key] = value
        return True

    async def delete(self, *keys):
        removed = 0
        for key in keys:
            if key in self.values:
                del self.values[key]
                removed += 1
            if key in self.lists:
                del self.lists[key]
                removed += 1
        return removed

    async def lrange(self, key, start, stop):
        rows = self.lists.get(key, [])
        return rows[start: None if stop == -1 else stop + 1]

    async def lpush(self, key, value):
        self.lists.setdefault(key, []).insert(0, value)

    async def ltrim(self, key, start, stop):
        rows = self.lists.get(key, [])
        self.lists[key] = rows[start: None if stop == -1 else stop + 1]


class StalePaperArchiveTests(unittest.IsolatedAsyncioTestCase):
    async def test_archiving_preserves_full_trade_record_and_closed_history(self):
        redis = _FakeRedis()
        position_id = "PAPER_test_position"
        original = {
            "id": position_id, "mode": "PAPER", "status": "STALE_REVIEW",
            "reconciliation_status": "REVIEW_REQUIRED", "ticker": "WUSH",
            "side": "SELL", "entry_price": 37.0, "volume": 20, "lots": 2,
            "notional_entry_rub": 740.0,
            "entry_timestamp": "2026-11-04T12:00:00+03:00",
            "stop_loss": 37.5, "take_profit": 36.5,
            "custom_audit_field": "keep-me",
        }
        redis.values[f"piluitus:trade:{position_id}"] = json.dumps(original)
        redis.lists["piluitus:trade_history"] = [
            json.dumps({"id": "PAPER_closed", "mode": "PAPER", "status": "CLOSED", "pnl": 12.5})
        ]
        redis.lists["piluitus:verdicts:history"] = [json.dumps({"ticker": "WUSH"})]
        redis.lists["piluitus:active_orders"] = [
            json.dumps({"id": position_id, "mode": "PAPER"}),
            json.dumps({"id": "LIVE_order", "mode": "LIVE"}),
        ]

        db = DBManager()
        db._redis = redis
        result = await db.clear_unreconciled_paper_positions()

        stored = json.loads(redis.values[f"piluitus:trade:{position_id}"])
        self.assertEqual(result["archived_stale_positions"], 1)
        self.assertEqual(result["deleted_trade_keys"], 0)
        self.assertEqual(stored["status"], "STALE_ARCHIVED")
        self.assertEqual(stored["reconciliation_status"], "ARCHIVED_UNRESOLVED")
        self.assertEqual(stored["custom_audit_field"], "keep-me")
        self.assertEqual(stored["entry_price"], 37.0)
        self.assertNotIn("exit_price", stored)
        self.assertNotIn("pnl", stored)
        self.assertEqual(len(redis.lists["piluitus:trade_history"]), 1)
        self.assertEqual(len(redis.lists["active_orders"]), 1)
        self.assertEqual(json.loads(redis.lists["active_orders"][0])["id"], "LIVE_order")

        snapshot = await db.get_paper_session_snapshot()
        self.assertEqual(snapshot["paper_stale_count"], 0)
        self.assertEqual(snapshot["paper_archived_count"], 1)
        self.assertEqual(snapshot["paper_history_count"], 1)


if __name__ == "__main__":
    unittest.main()
