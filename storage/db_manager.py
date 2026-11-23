"""Stable DBManager facade composed from modular storage backends and repositories."""
from __future__ import annotations

from storage.storage_shared import *  # noqa: F401,F403

# Preserve the established source-level storage-path contract for diagnostics/tests.
AOF_FILE = str(PROJECT_ROOT / "piluitus_fallback_aof.jsonl")
from storage.in_memory_backend import _InMemory
from storage.redis_client import RedisClient
from storage.paper_session_repository import PaperSessionRepositoryMixin
from storage.trade_journal_repository import TradeJournalRepositoryMixin
from storage.storage_utility_repository import StorageUtilityRepositoryMixin


class DBManager(
    PaperSessionRepositoryMixin,
    TradeJournalRepositoryMixin,
    StorageUtilityRepositoryMixin,
):
    def __init__(self) -> None:
        self._redis: Optional[Union[redis.Redis, _InMemory]] = None
        self._paper_session_lock = asyncio.Lock()

    async def _get_client(self) -> Union[redis.Redis, _InMemory]:
        """Return this manager's connected client, creating the singleton if needed."""
        if self._redis is None:
            self._redis = await RedisClient.get_client()
        return self._redis

    @staticmethod
    def _decode_json_value(raw: Any) -> Any:
        if isinstance(raw, (bytes, bytearray)):
            raw = raw.decode("utf-8", errors="replace")
        if not isinstance(raw, str):
            return raw
        try:
            return json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return None

    @staticmethod
    def is_paper_trade(trade: Any, fallback_id: str = "") -> bool:
        """Recognise explicit PAPER records and legacy simulated trade IDs."""
        if not isinstance(trade, dict):
            return str(fallback_id).startswith(("PAPER_", "TRADE_"))
        mode = str(trade.get("mode", "")).strip().upper()
        # An explicit LIVE marker always wins over a legacy ID prefix. This is
        # essential for the cleanup guard: never classify a known LIVE record
        # as PAPER merely because an older order-ID convention was reused.
        if mode == "LIVE":
            return False
        if mode == "PAPER":
            return True
        trade_id = str(trade.get("id") or fallback_id or "").strip()
        return trade_id.startswith(("PAPER_", "TRADE_"))

    async def clear_paper_session(self) -> dict:
        """Clear PAPER test-session data without touching LIVE trades, RAG or news."""
        async with self._paper_session_lock:
            client = await self._get_client()
            snapshot = await self.get_paper_session_snapshot()
            if snapshot["live_trade_count"] or snapshot["live_history_count"] or snapshot["live_active_order_count"]:
                raise RuntimeError(
                    "Очистка отменена: в общем хранилище обнаружены LIVE-записи. "
                    "Сначала используйте отдельное хранилище/проверьте LIVE-журнал."
                )

            # Every process start is a new PAPER measurement session. Unresolved
            # positions are archived with their complete original payload before
            # active trade keys are removed. They are NOT synthetically closed and
            # contribute neither realized PnL nor the next session's exposure.
            # Archive keys are deterministic by trade ID, making a retry after a
            # partial reset idempotent (archive write happens before deletion).
            paper_trade_keys: list[str] = []
            archived_open_positions = 0
            archived_stale_positions = 0
            archived_unknown_positions = 0
            archived_at = datetime.now().astimezone().isoformat(timespec="seconds")
            for key in await self._scan_matching_keys(client, "piluitus:trade:*"):
                raw = await client.get(key)
                trade = self._decode_json_value(raw)
                fallback_id = key.rsplit(":", 1)[-1]
                if not self.is_paper_trade(trade, fallback_id):
                    continue
                if not isinstance(trade, dict):
                    raise RuntimeError(
                        f"Невозможно безопасно архивировать PAPER-запись {fallback_id}: некорректный формат."
                    )
                status = str(trade.get("status", "")).strip().upper()
                if status != "CLOSED":
                    archive_record = {
                        "archive_type": "PAPER_SESSION_RESET",
                        "archived_at": archived_at,
                        "trade_id": str(trade.get("id") or fallback_id),
                        "original_status": status or "UNKNOWN",
                        "realized_pnl_created": False,
                        "exit_price_created": False,
                        "reason": (
                            "Previous PAPER session ended by process restart. "
                            "Position was not synthetically closed; full original trade payload retained."
                        ),
                        "trade": trade,
                    }
                    archive_key = f"piluitus:paper:archive:position:{fallback_id}"
                    await client.set(
                        archive_key,
                        json.dumps(archive_record, ensure_ascii=False, separators=(",", ":")),
                    )
                    if status in {"OPEN", "ACTIVE"}:
                        archived_open_positions += 1
                    elif status == "STALE_REVIEW":
                        archived_stale_positions += 1
                    else:
                        archived_unknown_positions += 1
                paper_trade_keys.append(key)
            deleted_trade_keys = await client.delete(*paper_trade_keys) if paper_trade_keys else 0

            history_raw = await self._read_list_raw(client, "piluitus:trade_history")
            kept_history: list[str] = []
            removed_history = 0
            for raw in history_raw:
                if self.is_paper_trade(self._decode_json_value(raw)):
                    removed_history += 1
                else:
                    kept_history.append(raw)
            if removed_history:
                await self._replace_list_raw(client, "piluitus:trade_history", kept_history)

            closed_journal_raw = await self._read_list_raw(
                client, "piluitus:paper:closed_trades"
            )
            deleted_paper_performance_rows = len(closed_journal_raw)
            deleted_paper_performance_keys = await client.delete("piluitus:paper:closed_trades")

            orders_raw = await self._read_list_raw(client, "piluitus:active_orders")
            kept_orders: list[str] = []
            removed_orders = 0
            for raw in orders_raw:
                item = self._decode_json_value(raw)
                if isinstance(item, dict):
                    nested = item.get("data") if isinstance(item.get("data"), dict) else item
                    fallback_id = str(item.get("id") or "")
                    if self.is_paper_trade(nested, fallback_id):
                        # All PAPER orders belong to the previous session. Their
                        # full trade records were archived above; none may remain
                        # visible as an active order in the fresh session.
                        removed_orders += 1
                        continue
                kept_orders.append(raw)
            if removed_orders:
                await self._replace_list_raw(client, "piluitus:active_orders", kept_orders)

            verdict_history_count = len(await self._read_list_raw(client, "piluitus:verdicts:history"))
            verdict_keys = await self._scan_matching_keys(client, "piluitus:verdict:execution:*")
            deleted_verdict_history_keys = await client.delete("piluitus:verdicts:history")
            deleted_verdict_execution_keys = await client.delete(*verdict_keys) if verdict_keys else 0
            risk_keys = [
                "piluitus:risk:persistent_balance",
                "piluitus:risk:balance_state",
                "piluitus:risk:high_water_mark:PAPER",
            ]
            deleted_risk_state_keys = await client.delete(*risk_keys)

            aof_compacted = False
            if isinstance(client, _InMemory):
                await client.compact_aof()
                aof_compacted = True

            result = {
                "deleted_paper_trade_keys": int(deleted_trade_keys),
                "archived_open_paper_positions": archived_open_positions,
                "archived_stale_paper_positions": archived_stale_positions,
                "archived_unknown_paper_positions": archived_unknown_positions,
                "archived_unresolved_paper_positions": (
                    archived_open_positions + archived_stale_positions + archived_unknown_positions
                ),
                "deleted_paper_history_rows": removed_history,
                "deleted_paper_performance_rows": deleted_paper_performance_rows,
                "deleted_paper_performance_keys": int(deleted_paper_performance_keys),
                "deleted_paper_active_order_rows": removed_orders,
                "deleted_verdict_history_rows": verdict_history_count,
                "deleted_verdict_history_keys": int(deleted_verdict_history_keys),
                "deleted_verdict_execution_keys": int(deleted_verdict_execution_keys),
                "deleted_risk_state_keys": int(deleted_risk_state_keys),
                "preserved_non_paper_trade_keys": snapshot["other_trade_count"],
                "preserved_live_trade_keys": snapshot["live_trade_count"],
                "preserved_live_history_rows": snapshot["live_history_count"],
                "preserved_live_active_order_rows": snapshot["live_active_order_count"],
                "storage_backend": snapshot["storage_backend"],
                "aof_path": snapshot["aof_path"],
                "aof_compacted": aof_compacted,
            }
            log.warning("🧹 PAPER test session cleared: %s", result)
            return result

    async def connect(self) -> None:
        """
        Открывает (или создаёт) соединение с Redis.
        Вызывать один раз при старте приложения (см. main.py).
        """
        self._redis = await RedisClient.get_client()
        log.info("✅ DBManager: соединение с Redis готово")

    @property
    def _client(self) -> Union[redis.Redis, _InMemory]:
        if self._redis is None:
            raise RuntimeError(
                "DBManager: Redis‑клиент не инициализирован. Вызовите await db.connect()"
            )
        return self._redis

    # ----------------------- журнал сделок (trade journal) -----------------------
