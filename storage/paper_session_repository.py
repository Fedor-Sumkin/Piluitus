"""Paper-session snapshots, cleanup and reconciliation storage mixin."""
from __future__ import annotations

from storage.storage_shared import *  # noqa: F401,F403
from storage.in_memory_backend import _InMemory


class PaperSessionRepositoryMixin:
    async def _scan_matching_keys(self, client: Any, pattern: str) -> list[str]:
        cursor = 0
        found: list[str] = []
        while True:
            cursor, raw_keys = await client.scan(cursor=cursor, match=pattern, count=200)
            for raw_key in raw_keys:
                key = raw_key.decode() if isinstance(raw_key, (bytes, bytearray)) else str(raw_key)
                found.append(key)
            if cursor == 0:
                break
        return found

    async def _read_list_raw(self, client: Any, key: str) -> list[str]:
        rows = await client.lrange(key, 0, -1)
        return [row.decode() if isinstance(row, (bytes, bytearray)) else str(row) for row in rows]

    async def _replace_list_raw(self, client: Any, key: str, rows: list[str]) -> None:
        """Replace a Redis-compatible list while preserving its newest-first order."""
        await client.delete(key)
        for row in reversed(rows):
            await client.lpush(key, row)

    async def get_paper_session_snapshot(self) -> dict:
        """Return a read-only inventory of persisted PAPER/LIVE session records."""
        client = await self._get_client()
        trade_keys = await self._scan_matching_keys(client, "piluitus:trade:*")
        paper_trades: list[dict] = []
        live_trade_count = 0
        other_trade_count = 0
        for key in trade_keys:
            raw = await client.get(key)
            if raw is None:
                continue
            trade = self._decode_json_value(raw)
            if not isinstance(trade, dict):
                log.critical(
                    "❌ Некорректная запись в PAPER/LIVE inventory (%s); snapshot aborted",
                    key,
                )
                raise RuntimeError(f"Некорректная запись журнала сделок: {key}")
            fallback_id = key.rsplit(":", 1)[-1]
            if self.is_paper_trade(trade, fallback_id):
                if isinstance(trade, dict):
                    row = dict(trade)
                    row.setdefault("id", fallback_id)
                    paper_trades.append(row)
            elif isinstance(trade, dict) and str(trade.get("mode", "")).upper() == "LIVE":
                live_trade_count += 1
            else:
                other_trade_count += 1

        paper_closed_journal_count = len(
            await self._read_list_raw(client, "piluitus:paper:closed_trades")
        )
        paper_history_count = 0
        live_history_count = 0
        for raw in await self._read_list_raw(client, "piluitus:trade_history"):
            item = self._decode_json_value(raw)
            if self.is_paper_trade(item):
                paper_history_count += 1
            elif isinstance(item, dict) and str(item.get("mode", "")).upper() == "LIVE":
                live_history_count += 1

        paper_active_order_count = 0
        live_active_order_count = 0
        for raw in await self._read_list_raw(client, "piluitus:active_orders"):
            item = self._decode_json_value(raw)
            if isinstance(item, dict):
                nested_raw = item.get("data")
                nested: dict[str, Any] = (
                    nested_raw if isinstance(nested_raw, dict) else item
                )
                fallback_id = str(item.get("id") or "")
                if self.is_paper_trade(nested, fallback_id):
                    paper_active_order_count += 1
                elif str(nested.get("mode", "")).upper() == "LIVE":
                    live_active_order_count += 1

        verdict_keys = await self._scan_matching_keys(client, "piluitus:verdict:execution:*")
        verdict_history = await self._read_list_raw(client, "piluitus:verdicts:history")
        client_kind = "fallback_aof" if isinstance(client, _InMemory) else "redis_compatible"
        result = {
            "storage_backend": client_kind,
            "aof_path": client.aof_path if isinstance(client, _InMemory) else None,
            "paper_trade_count": len(paper_trades),
            "paper_open_count": sum(1 for row in paper_trades if str(row.get("status", "")).upper() == "OPEN"),
            "paper_stale_count": sum(1 for row in paper_trades if str(row.get("status", "")).upper() == "STALE_REVIEW"),
            "paper_closed_count": sum(1 for row in paper_trades if str(row.get("status", "")).upper() == "CLOSED"),
            "paper_other_count": sum(1 for row in paper_trades if str(row.get("status", "")).upper() not in {"OPEN", "STALE_REVIEW", "CLOSED"}),
            "live_trade_count": live_trade_count,
            "other_trade_count": other_trade_count,
            "paper_history_count": paper_history_count,
            "paper_closed_journal_count": paper_closed_journal_count,
            "live_history_count": live_history_count,
            "paper_active_order_count": paper_active_order_count,
            "live_active_order_count": live_active_order_count,
            "verdict_history_count": len(verdict_history),
            "verdict_execution_count": len(verdict_keys),
            "paper_trades": paper_trades,
        }
        return result

    async def clear_unreconciled_paper_positions(self) -> dict:
        """Delete only PAPER records explicitly quarantined as STALE_REVIEW.

        Closed PAPER history, verdicts, risk snapshots, LIVE records and all other
        storage keys are preserved. The corresponding active_orders references
        are removed by ID so stale entries cannot be rediscovered as active.
        """
        async with self._paper_session_lock:
            client = await self._get_client()
            snapshot = await self.get_paper_session_snapshot()
            stale_ids = {
                str(row.get("id") or "").strip()
                for row in snapshot.get("paper_trades", [])
                if isinstance(row, dict)
                and str(row.get("status", "")).upper() == "STALE_REVIEW"
                and str(row.get("reconciliation_status", "")).upper() == "REVIEW_REQUIRED"
                and str(row.get("id") or "").strip()
            }
            if not stale_ids:
                return {
                    "deleted_stale_positions": 0,
                    "deleted_trade_keys": 0,
                    "deleted_active_order_rows": 0,
                    "position_ids": [],
                    "preserved_paper_history_rows": snapshot["paper_history_count"],
                    "preserved_verdict_history_rows": snapshot["verdict_history_count"],
                    "preserved_verdict_execution_keys": snapshot["verdict_execution_count"],
                    "preserved_live_trade_keys": snapshot["live_trade_count"],
                    "preserved_live_history_rows": snapshot["live_history_count"],
                    "preserved_live_active_order_rows": snapshot["live_active_order_count"],
                    "storage_backend": snapshot["storage_backend"],
                    "aof_compacted": False,
                }

            matching_trade_keys: list[str] = []
            for key in await self._scan_matching_keys(client, "piluitus:trade:*"):
                fallback_id = key.rsplit(":", 1)[-1]
                if fallback_id not in stale_ids:
                    continue
                trade = self._decode_json_value(await client.get(key))
                if (
                    self.is_paper_trade(trade, fallback_id)
                    and isinstance(trade, dict)
                    and str(trade.get("status", "")).upper() == "STALE_REVIEW"
                    and str(trade.get("reconciliation_status", "")).upper() == "REVIEW_REQUIRED"
                ):
                    matching_trade_keys.append(key)

            deleted_trade_keys = (
                await client.delete(*matching_trade_keys) if matching_trade_keys else 0
            )

            orders_raw = await self._read_list_raw(client, "piluitus:active_orders")
            kept_orders: list[str] = []
            removed_order_rows = 0
            for raw in orders_raw:
                item = self._decode_json_value(raw)
                if not isinstance(item, dict):
                    kept_orders.append(raw)
                    continue
                nested = item.get("data") if isinstance(item.get("data"), dict) else item
                fallback_id = str(item.get("id") or nested.get("id") or "").strip()
                if fallback_id in stale_ids and self.is_paper_trade(nested, fallback_id):
                    removed_order_rows += 1
                    continue
                kept_orders.append(raw)
            if removed_order_rows:
                await self._replace_list_raw(client, "piluitus:active_orders", kept_orders)

            aof_compacted = False
            if isinstance(client, _InMemory):
                await client.compact_aof()
                aof_compacted = True

            result = {
                "deleted_stale_positions": len(matching_trade_keys),
                "deleted_trade_keys": int(deleted_trade_keys),
                "deleted_active_order_rows": removed_order_rows,
                "position_ids": sorted(stale_ids),
                "preserved_paper_history_rows": snapshot["paper_history_count"],
                "preserved_verdict_history_rows": snapshot["verdict_history_count"],
                "preserved_verdict_execution_keys": snapshot["verdict_execution_count"],
                "preserved_live_trade_keys": snapshot["live_trade_count"],
                "preserved_live_history_rows": snapshot["live_history_count"],
                "preserved_live_active_order_rows": snapshot["live_active_order_count"],
                "storage_backend": snapshot["storage_backend"],
                "aof_compacted": aof_compacted,
            }
            log.warning("🧹 STALE_REVIEW PAPER positions removed; other PAPER history preserved: %s", result)
            return result
