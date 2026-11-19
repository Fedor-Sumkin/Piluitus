from __future__ import annotations

import json
import os
import tempfile
import unittest

from storage.db_manager import DBManager, _InMemory


class PaperSessionArchiveTests(unittest.IsolatedAsyncioTestCase):
    async def test_reset_archives_unresolved_positions_and_removes_them_from_active_session(self):
        with tempfile.TemporaryDirectory() as directory:
            client = _InMemory(aof_path=os.path.join(directory, "paper-test.aof"))
            db = DBManager()
            db._redis = client

            open_trade = {
                "id": "PAPER_OPEN_1", "mode": "PAPER", "status": "OPEN",
                "ticker": "SBER", "side": "BUY", "volume": 4,
                "entry_price": 296.53, "stop_loss": 295.75, "take_profit": 298.10,
            }
            stale_trade = {
                "id": "PAPER_STALE_1", "mode": "PAPER", "status": "STALE_REVIEW",
                "reconciliation_status": "REVIEW_REQUIRED",
                "ticker": "VTBR", "side": "BUY", "volume": 8,
                "entry_price": 60.32, "stop_loss": 60.16, "take_profit": 60.62,
            }
            closed_trade = {
                "id": "PAPER_CLOSED_1", "mode": "PAPER", "status": "CLOSED",
                "ticker": "GAZP", "side": "BUY", "pnl": 12.5,
            }
            for trade in (open_trade, stale_trade, closed_trade):
                await client.set(
                    f"piluitus:trade:{trade['id']}",
                    json.dumps(trade, ensure_ascii=False),
                )

            await client.lpush(
                "piluitus:active_orders",
                json.dumps({"id": "PAPER_OPEN_1", "mode": "PAPER"}),
            )
            await client.lpush(
                "piluitus:active_orders",
                json.dumps({"id": "PAPER_STALE_1", "mode": "PAPER"}),
            )
            await client.lpush(
                "piluitus:paper:closed_trades",
                json.dumps(closed_trade),
            )

            result = await db.clear_paper_session()

            self.assertEqual(result["archived_open_paper_positions"], 1)
            self.assertEqual(result["archived_stale_paper_positions"], 1)
            self.assertEqual(result["archived_unresolved_paper_positions"], 2)
            self.assertIsNone(await client.get("piluitus:trade:PAPER_OPEN_1"))
            self.assertIsNone(await client.get("piluitus:trade:PAPER_STALE_1"))
            self.assertIsNone(await client.get("piluitus:trade:PAPER_CLOSED_1"))

            for trade_id, original_status in (
                ("PAPER_OPEN_1", "OPEN"),
                ("PAPER_STALE_1", "STALE_REVIEW"),
            ):
                raw_archive = await client.get(
                    f"piluitus:paper:archive:position:{trade_id}"
                )
                self.assertIsNotNone(raw_archive)
                archive = json.loads(raw_archive)
                self.assertEqual(archive["original_status"], original_status)
                self.assertEqual(archive["trade"]["id"], trade_id)
                self.assertFalse(archive["realized_pnl_created"])
                self.assertFalse(archive["exit_price_created"])

            self.assertEqual(
                await client.lrange("piluitus:active_orders", 0, -1), []
            )
            self.assertEqual(
                await client.lrange("piluitus:paper:closed_trades", 0, -1), []
            )
            snapshot = await db.get_paper_session_snapshot()
            self.assertEqual(snapshot["paper_trade_count"], 0)
            self.assertEqual(snapshot["paper_active_order_count"], 0)


if __name__ == "__main__":
    unittest.main()
