"""
Unified asynchronous Redis manager for the Piluitus v3.0 project.

* Один singleton‑клиент `RedisClient` (реальный Redis или аварийный in‑memory‑мок с дисковым AOF‑бэкапом).
* Публичный класс `DBManager` сохраняет прежний API, которым пользуются
  остальные модули проекта.
* Добавлен метод get_all_daily_trades для сквозного риск-аудита просадки.
"""

from __future__ import annotations

import json
import logging
import os
import asyncio
import fnmatch
from datetime import datetime
from typing import Any, List, Optional, Union, cast

import redis.asyncio as redis                     # pip install redis‑asyncio
from config import Settings                       # ваш Settings‑класс (см. config.py)

log = logging.getLogger(__name__)   # под именем "Piluitus.DBManager"
DB_MANAGER_BUILD_ID = "paper-stale-archive-2026-11-05-r2"


# ----------------------------------------------------------------------
# 1️⃣  In‑memory fallback‑клиент с персистентным AOF-дисковым бэкапом
# ----------------------------------------------------------------------
class _InMemory:
    """
    Самый лёгкий in‑memory‑мок, имитирующий методы redis‑клиента.
    Защищен от потери данных при рестарте через механизм локального AOF-логирования.
    """
    def __init__(self, aof_path: str = "piluitus_fallback_aof.jsonl") -> None:
        self._store: dict[str, Any] = {}
        self.aof_path = aof_path
        self._lock = asyncio.Lock()  # Защита от race conditions при параллельной HFT-записи
        self._load_from_aof()

    def _load_from_aof(self) -> None:
        """Синхронная загрузка состояния из файла при старте объекта."""
        if not os.path.exists(self.aof_path):
            return
        try:
            log.info("💾 _InMemory: Восстановление состояния из локального бэкапа %s...", self.aof_path)
            with open(self.aof_path, "r", encoding="utf-8") as f:
                for line in f:
                    if not line.strip():
                        continue
                    event = json.loads(line)
                    if not isinstance(event, dict):
                        log.warning("Пропущена некорректная AOF-запись: ожидался JSON-объект")
                        continue
                    cmd = event.get("cmd")
                    key = event.get("key")
                    val = event.get("val")

                    if cmd == "set":
                        self._store[key] = val
                    elif cmd == "del":
                        if key in self._store:
                            del self._store[key]
                    elif cmd == "lpush":
                        self._store.setdefault(key, [])
                        self._store[key].insert(0, val)
                    elif cmd == "ltrim":
                        start, end = event.get("start"), event.get("end")
                        lst = self._store.get(key, [])
                        left, right = self._redis_slice_bounds(len(lst), int(start), int(end))
                        self._store[key] = lst[left:right]
                    elif cmd == "clear_trades":
                        keys_to_del = [k for k in list(self._store.keys()) if k.startswith("piluitus:trade:")]
                        for k in keys_to_del:
                            del self._store[k]
                        if "piluitus:active_orders" in self._store:
                            del self._store["piluitus:active_orders"]
            log.info("✅ _InMemory: Стейт успешно восстановлен. Ключей в ОЗУ: %d", len(self._store))
        except Exception as e:
            log.error("❌ _InMemory: Не удалось прочитать бэкап-файл: %s", e)

    def _sync_write_aof(self, event: dict) -> None:
        """Синхронная дозапись строки события в конец файла."""
        try:
            with open(self.aof_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(event, ensure_ascii=False) + "\n")
        except Exception as e:
            log.error("❌ _InMemory AOF Write Error: %s", e)

    async def _append_aof(self, event: dict) -> None:
        """Асинхронная обёртка для записи на диск с защитой от гонок данных."""
        async with self._lock:  # Блокировка гарантирует атомарный порядок записи транзакций
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._sync_write_aof, event)

    def _sync_compact_aof(self, snapshot: dict[str, Any]) -> None:
        """Atomically rewrite fallback AOF from current state (without deleted history)."""
        temporary_path = f"{self.aof_path}.tmp"
        parent = os.path.dirname(os.path.abspath(self.aof_path))
        os.makedirs(parent, exist_ok=True)
        try:
            with open(temporary_path, "w", encoding="utf-8") as stream:
                for key, value in snapshot.items():
                    if isinstance(value, list):
                        # Replay LPUSH from oldest to newest to preserve current list order.
                        for item in reversed(value):
                            stream.write(json.dumps({"cmd": "lpush", "key": key, "val": item}, ensure_ascii=False) + "\n")
                    else:
                        stream.write(json.dumps({"cmd": "set", "key": key, "val": value}, ensure_ascii=False) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary_path, self.aof_path)
        finally:
            if os.path.exists(temporary_path):
                try:
                    os.remove(temporary_path)
                except OSError:
                    pass

    async def compact_aof(self) -> None:
        """Remove obsolete fallback-AOF history while preserving the current keyspace."""
        async with self._lock:
            snapshot = {key: list(value) if isinstance(value, list) else value for key, value in self._store.items()}
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._sync_compact_aof, snapshot)
    # ---------- базовые операции ----------
    async def set(self, key: str, value: str, ex: Optional[int] = None, **_: Any) -> bool:
        self._store[key] = value
        await self._append_aof({"cmd": "set", "key": key, "val": value})
        return True

    async def get(self, key: str, **_: Any) -> Optional[str]:
        val = self._store.get(key)
        return val if isinstance(val, str) else None

    async def delete(self, *keys: str) -> int:
        removed = 0
        for k in keys:
            if k in self._store:
                del self._store[k]
                removed += 1
                await self._append_aof({"cmd": "del", "key": k})
        return removed

    # ---------- list‑ops ----------
    async def lpush(self, key: str, value: str) -> int:
        self._store.setdefault(key, [])
        lst: List[str] = cast(List[str], self._store[key])
        lst.insert(0, value)
        await self._append_aof({"cmd": "lpush", "key": key, "val": value})
        return len(lst)

    @staticmethod
    def _redis_slice_bounds(length: int, start: int, end: int) -> tuple[int, int]:
        """Convert Redis inclusive/negative list indexes to Python slice bounds."""
        normalized_start = start if start >= 0 else max(0, length + start)
        if end == -1:
            normalized_end = length
        elif end < -1:
            normalized_end = max(0, length + end + 1)
        else:
            normalized_end = min(length, end + 1)
        return min(max(normalized_start, 0), length), min(max(normalized_end, 0), length)

    async def ltrim(self, key: str, start: int, end: int) -> None:
        lst = cast(List[Any], self._store.get(key, []))
        left, right = self._redis_slice_bounds(len(lst), start, end)
        self._store[key] = lst[left:right]
        await self._append_aof({"cmd": "ltrim", "key": key, "start": start, "end": end})

    async def lrange(self, key: str, start: int, end: int) -> List[str]:
        lst = cast(List[Any], self._store.get(key, []))
        left, right = self._redis_slice_bounds(len(lst), start, end)
        return [str(item) for item in lst[left:right]]

    # ---------- scan ----------
    async def scan(
        self,
        cursor: int = 0,
        match: Optional[str] = None,
        count: int = 100,
    ) -> tuple[int, list[bytes]]:
        keys = list(self._store.keys())
        if match:
            keys = [k for k in keys if fnmatch.fnmatch(k, match)]

        byte_keys = [k.encode() for k in keys]
        return 0, byte_keys

    # ---------- ping ----------
    async def ping(self) -> bool:
        return True


# ----------------------------------------------------------------------
# 2️⃣  Redis‑клиент‑обёртка (singleton + fallback)
# ----------------------------------------------------------------------
class RedisClient:
    """
    Singleton async Redis client.
    При первой попытке соединения, если реальный Redis недоступен,
    автоматически переключается на _InMemory‑мок.
    """
    _instance: Union[redis.Redis, _InMemory, None] = None

    @classmethod
    async def get_client(cls) -> Union[redis.Redis, _InMemory]:
        """Возвращает готовый клиент, создавая его единожды."""
        if cls._instance is None:
            try:
                log.info(
                    "🔌 Попытка подключения к Redis %s:%s",
                    Settings.REDIS_HOST,
                    Settings.REDIS_PORT,
                )
                client = redis.Redis(
                    host=Settings.REDIS_HOST,
                    port=Settings.REDIS_PORT,
                    db=Settings.REDIS_DB,
                    password=Settings.REDIS_PASSWORD,
                    decode_responses=True,
                )
                await client.ping()
                cls._instance = client
                log.info("✅ Redis‑соединение успешно установлено")
            except Exception as exc:
                log.error("❌ Не удалось подключиться к Redis: %s", exc)
                cls._instance = _InMemory()
                log.warning(
                    "🚧 Запущен fallback‑Redis (in‑memory с персистентным AOF-файлом). "
                    "Критическая история торгов защищена от рестартов приложения."
                )
        return cast(Union[redis.Redis, _InMemory], cls._instance)

    # ---------------- 3️⃣ Универсальные CRUD‑утилиты ----------------
    @classmethod
    async def set_json(cls, key: str, value: Any, ttl: Optional[int] = None) -> None:
        client = await cls.get_client()
        payload = json.dumps(value, ensure_ascii=False)
        await client.set(key, payload, ex=ttl)
        log.debug("🔑 set_json %s (ttl=%s)", key, ttl)

    @classmethod
    async def get_json(cls, key: str) -> Optional[Any]:
        client = await cls.get_client()
        raw = await client.get(key)
        if raw is None:
            log.debug("🔎 get_json %s → miss", key)
            return None
        try:
            data = json.loads(raw)
            log.debug("🔎 get_json %s → hit", key)
            return data
        except json.JSONDecodeError:
            log.error("❌ JSON‑decode error for key %s", key)
            return None

    @classmethod
    async def push_to_list(cls, key: str, item: Any, max_len: int = 500) -> None:
        client = await cls.get_client()
        await client.lpush(key, json.dumps(item, ensure_ascii=False))
        await client.ltrim(key, 0, max_len - 1)
        log.debug("📥 push_to_list %s (max_len=%d)", key, max_len)

    @classmethod
    async def get_list(cls, key: str, start: int = 0, stop: int = -1) -> List[Any]:
        client = await cls.get_client()
        raw_items = await client.lrange(key, start, stop)
        result: List[Any] = []
        for raw in raw_items:
            try:
                result.append(json.loads(raw))
            except json.JSONDecodeError:
                log.error("❌ Corrupt JSON in list %s → %s", key, raw)
        log.debug("📤 get_list %s → %d items", key, len(result))
        return result
# ----------------------------------------------------------------------
# 4️⃣  Публичный API, которым пользуется остальной код (DBManager)
# ----------------------------------------------------------------------
class DBManager:
    """
    Класс‑обёртка, сохраняющий прежний публичный интерфейс проекта.
    Внутри опирается на RedisClient (реальный клиент или fallback‑мок).
    """

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
            trade = self._decode_json_value(raw)
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
            "paper_archived_count": sum(1 for row in paper_trades if str(row.get("status", "")).upper() == "STALE_ARCHIVED"),
            "paper_closed_count": sum(1 for row in paper_trades if str(row.get("status", "")).upper() == "CLOSED"),
            "paper_other_count": sum(1 for row in paper_trades if str(row.get("status", "")).upper() not in {"OPEN", "STALE_REVIEW", "STALE_ARCHIVED", "CLOSED"}),
            "live_trade_count": live_trade_count,
            "other_trade_count": other_trade_count,
            "paper_history_count": paper_history_count,
            "live_history_count": live_history_count,
            "paper_active_order_count": paper_active_order_count,
            "live_active_order_count": live_active_order_count,
            "verdict_history_count": len(verdict_history),
            "verdict_execution_count": len(verdict_keys),
            "paper_trades": paper_trades,
        }
        return result

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

            paper_trade_keys: list[str] = []
            for key in await self._scan_matching_keys(client, "piluitus:trade:*"):
                raw = await client.get(key)
                trade = self._decode_json_value(raw)
                if self.is_paper_trade(trade, key.rsplit(":", 1)[-1]):
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

            orders_raw = await self._read_list_raw(client, "piluitus:active_orders")
            kept_orders: list[str] = []
            removed_orders = 0
            for raw in orders_raw:
                item = self._decode_json_value(raw)
                if isinstance(item, dict):
                    nested = item.get("data") if isinstance(item.get("data"), dict) else item
                    fallback_id = str(item.get("id") or "")
                    if self.is_paper_trade(nested, fallback_id):
                        removed_orders += 1
                        continue
                kept_orders.append(raw)
            if removed_orders:
                await self._replace_list_raw(client, "piluitus:active_orders", kept_orders)

            verdict_history_count = len(await self._read_list_raw(client, "piluitus:verdicts:history"))
            verdict_keys = await self._scan_matching_keys(client, "piluitus:verdict:execution:*")
            deleted_verdict_history_keys = await client.delete("piluitus:verdicts:history")
            deleted_verdict_execution_keys = await client.delete(*verdict_keys) if verdict_keys else 0
            risk_keys = ["piluitus:risk:persistent_balance", "piluitus:risk:balance_state"]
            deleted_risk_state_keys = await client.delete(*risk_keys)

            aof_compacted = False
            if isinstance(client, _InMemory):
                await client.compact_aof()
                aof_compacted = True

            result = {
                "deleted_paper_trade_keys": int(deleted_trade_keys),
                "deleted_paper_history_rows": removed_history,
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

    async def clear_unreconciled_paper_positions(self) -> dict:
        """Archive PAPER records explicitly quarantined as STALE_REVIEW.

        Full trade payloads remain in piluitus:trade:<id> for audit. Only the
        corresponding active_orders references are removed so archived positions
        cannot be rediscovered as active.
        """
        async with self._paper_session_lock:
            client = await self._get_client()
            snapshot = await self.get_paper_session_snapshot()
            stale_rows = [
                row for row in snapshot.get("paper_trades", [])
                if isinstance(row, dict)
                and str(row.get("status", "")).upper() == "STALE_REVIEW"
                and str(row.get("reconciliation_status", "")).upper() == "REVIEW_REQUIRED"
                and str(row.get("id") or "").strip()
            ]
            stale_ids = {str(row.get("id")).strip() for row in stale_rows}
            if not stale_ids:
                return {
                    "archived_stale_positions": 0,
                    "archived_trade_keys": 0,
                    "deleted_stale_positions": 0,
                    "deleted_trade_keys": 0,
                    "deleted_active_order_rows": 0,
                    "position_ids": [],
                    "preserved_paper_trade_keys": snapshot["paper_trade_count"],
                    "preserved_paper_history_rows": snapshot["paper_history_count"],
                    "preserved_verdict_history_rows": snapshot["verdict_history_count"],
                    "preserved_verdict_execution_keys": snapshot["verdict_execution_count"],
                    "preserved_live_trade_keys": snapshot["live_trade_count"],
                    "preserved_live_history_rows": snapshot["live_history_count"],
                    "preserved_live_active_order_rows": snapshot["live_active_order_count"],
                    "storage_backend": snapshot["storage_backend"],
                    "aof_compacted": False,
                }

            now = datetime.now().astimezone().isoformat(timespec="milliseconds")
            archived_trade_keys: list[str] = []
            archived_ids: set[str] = set()
            for key in await self._scan_matching_keys(client, "piluitus:trade:*"):
                fallback_id = key.rsplit(":", 1)[-1]
                if fallback_id not in stale_ids:
                    continue
                trade = self._decode_json_value(await client.get(key))
                if (
                    not self.is_paper_trade(trade, fallback_id)
                    or not isinstance(trade, dict)
                    or str(trade.get("status", "")).upper() != "STALE_REVIEW"
                    or str(trade.get("reconciliation_status", "")).upper() != "REVIEW_REQUIRED"
                ):
                    continue

                archived_trade = dict(trade)
                archived_trade.update({
                    "status": "STALE_ARCHIVED",
                    "archived_from_status": "STALE_REVIEW",
                    "reconciliation_status": "ARCHIVED_UNRESOLVED",
                    "archived_at": now,
                    "archive_reason": (
                        "Оператор выбрал архивирование без фиктивного закрытия. "
                        "Позиция не получила искусственную цену выхода или PnL."
                    ),
                })
                await client.set(key, json.dumps(archived_trade, ensure_ascii=False))
                archived_trade_keys.append(key)
                archived_ids.add(fallback_id)

            # Read back every archived payload before touching active-order references.
            # A storage backend that silently drops a write must never be treated as
            # a successful reconciliation/resume.
            for trade_id in archived_ids:
                archived_key = f"piluitus:trade:{trade_id}"
                persisted = self._decode_json_value(await client.get(archived_key))
                if (
                    not isinstance(persisted, dict)
                    or str(persisted.get("status", "")).upper() != "STALE_ARCHIVED"
                    or str(persisted.get("reconciliation_status", "")).upper() != "ARCHIVED_UNRESOLVED"
                ):
                    raise RuntimeError(
                        f"Не удалось подтвердить архивирование PAPER-позиции {trade_id}; "
                        "исходная запись не удаляется, PAPER должен остаться на паузе."
                    )

            # Remove only stale PAPER references from active_orders. Full records
            # remain durable and available for audit.
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
                if fallback_id in archived_ids and self.is_paper_trade(nested, fallback_id):
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
                "archived_stale_positions": len(archived_ids),
                "archived_trade_keys": len(archived_trade_keys),
                "deleted_stale_positions": 0,
                "deleted_trade_keys": 0,
                "deleted_active_order_rows": removed_order_rows,
                "position_ids": sorted(archived_ids),
                "preserved_paper_trade_keys": snapshot["paper_trade_count"],
                "preserved_paper_history_rows": snapshot["paper_history_count"],
                "preserved_verdict_history_rows": snapshot["verdict_history_count"],
                "preserved_verdict_execution_keys": snapshot["verdict_execution_count"],
                "preserved_live_trade_keys": snapshot["live_trade_count"],
                "preserved_live_history_rows": snapshot["live_history_count"],
                "preserved_live_active_order_rows": snapshot["live_active_order_count"],
                "storage_backend": snapshot["storage_backend"],
                "aof_compacted": aof_compacted,
            }
            log.warning("🗃️ STALE_REVIEW PAPER positions archived; full trade records and other history preserved: %s", result)
            return result

    async def connect(self) -> None:
        """
        Открывает (или создаёт) соединение с Redis.
        Вызывать один раз при старте приложения (см. main.py).
        """
        self._redis = await RedisClient.get_client()
        log.info(
            "✅ DBManager: соединение готово; build=%s module=%s file=%s backend=%s",
            DB_MANAGER_BUILD_ID,
            __name__,
            __file__,
            "fallback_aof" if isinstance(self._redis, _InMemory) else "redis_compatible",
        )

    @property
    def _client(self) -> Union[redis.Redis, _InMemory]:
        if self._redis is None:
            raise RuntimeError(
                "DBManager: Redis‑клиент не инициализирован. Вызовите await db.connect()"
            )
        return self._redis

    # ----------------------- журнал сделок (trade journal) -----------------------
    async def log_trade(self, order_id: str, trade_dict: dict) -> None:
        """Сохраняет/обновляет запись о сделке в Redis."""
        key = f"piluitus:trade:{order_id}"
        await RedisClient.set_json(key, trade_dict)
        await RedisClient.push_to_list("piluitus:active_orders", {"id": order_id, "data": trade_dict})
        log.debug("📝 logged trade %s", order_id)

    async def get_active_positions(self) -> dict:
        """Возвращает словарь {order_id: trade_dict, ...} только для OPEN позиций."""
        client = await RedisClient.get_client()
        if client is None:
            raise RuntimeError("DBManager: Redis-клиент недоступен при чтении активных позиций")
        pattern = "piluitus:trade:*"
        cursor: int = 0
        result: dict = {}

        while True:
            cursor, raw_keys = await client.scan(cursor=cursor, match=pattern, count=200)
            for raw_key in raw_keys:
                key_str = raw_key.decode() if isinstance(raw_key, (bytes, bytearray)) else str(raw_key)
                raw = await client.get(key_str)
                if raw is None:
                    continue
                try:
                    trade = json.loads(raw)
                    if isinstance(trade, dict) and trade.get("status") == "OPEN":
                        order_id = key_str.split(":")[-1]
                        result[order_id] = trade
                except json.JSONDecodeError:
                    log.error("❌ Corrupt trade JSON for key %s", key_str)

            if cursor == 0:
                break

        log.debug("🔎 get_active_positions → %d open trades", len(result))
        return result

    # ----------------------- сквозной суточный риск-аудит -----------------------
    async def get_all_daily_trades(self) -> dict:
        """Возвращает ВСЕ сделки за день (OPEN и CLOSED) по маске ``piluitus:trade:*``."""
        client = await RedisClient.get_client()
        pattern = "piluitus:trade:*"
        cursor: int = 0
        result: dict = {}

        while True:
            cursor, raw_keys = await client.scan(cursor=cursor, match=pattern, count=200)
            for raw_key in raw_keys:
                key_str = raw_key.decode() if isinstance(raw_key, (bytes, bytearray)) else str(raw_key)
                raw = await client.get(key_str)
                if raw is None:
                    continue
                try:
                    trade = json.loads(raw)
                    if not isinstance(trade, dict):
                        log.warning("Пропущена некорректная запись сделки для ключа %s", key_str)
                        continue
                    order_id = key_str.split(":")[-1]
                    result[order_id] = trade
                except json.JSONDecodeError:
                    log.error("❌ Corrupt trade JSON for key %s", key_str)

            if cursor == 0:
                break

        log.debug("🔎 get_all_daily_trades → %d total daily trades", len(result))
        return result

    # ----------------------- RAG‑правила -----------------------
    async def load_rag_rules(self) -> List[dict]:
        data = await RedisClient.get_json("piluitus:rag_rules")
        if not data:
            return []
        if isinstance(data, list):
            return data
        return [data]

    async def save_rag_rules(self, rules: List[dict]) -> None:
        await RedisClient.set_json("piluitus:rag_rules", rules)
        log.info("💾 RAG‑rules saved (%d items)", len(rules))

    # ----------------------- прочие геттеры/сеттеры -----------------------
    async def get_json(self, key: str) -> Optional[Any]:
        return await RedisClient.get_json(key)

    async def set_json(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        await RedisClient.set_json(key, value, ttl=ttl)

    async def push_to_list(self, key: str, item: Any, max_len: int = 500) -> None:
        await RedisClient.push_to_list(key, item, max_len)

    async def get_list(self, key: str, start: int = 0, stop: int = -1) -> List[Any]:
        return await RedisClient.get_list(key, start, stop)

    # ----------------------- паника / очистка -----------------------
    async def clear_journal(self) -> None:
        """Полностью очищает журнал позиций – используется кнопкой «Паника»."""
        client = await RedisClient.get_client()
        pattern = "piluitus:trade:*"
        cursor: int = 0
        while True:
            cursor, raw_keys = await client.scan(cursor=cursor, match=pattern, count=200)
            if raw_keys:
                str_keys = [
                    k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
                    for k in raw_keys
                ]
                await client.delete(*str_keys)
            if cursor == 0:
                break
        await client.delete("piluitus:active_orders")
        
        if isinstance(client, _InMemory):
            await client._append_aof({"cmd": "clear_trades"})
            
        log.warning("🧹 Журнал сделок полностью очищен (panic mode)")

    async def get_last_n_trades(self, n: int = 20) -> List[dict]:
        history = await RedisClient.get_list("piluitus:trade_history", 0, n - 1)
        return history
