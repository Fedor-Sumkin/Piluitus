"""AOF-backed in-memory Redis-compatible fallback client."""
from __future__ import annotations

from storage.storage_shared import *  # noqa: F401,F403

class _InMemory:
    """
    Самый лёгкий in‑memory‑мок, имитирующий методы redis‑клиента.
    Защищен от потери данных при рестарте через механизм локального AOF-логирования.
    """
    def __init__(self, aof_path: str = AOF_FILE) -> None:
        self._store: dict[str, Any] = {}
        self.aof_path = aof_path
        self._lock = asyncio.Lock()
        self._load_error: str | None = None
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
                        raise ValueError("AOF event is not a JSON object")
                    cmd = event.get("cmd")
                    key = event.get("key")
                    val = event.get("val")

                    if cmd != "clear_trades" and (not isinstance(key, str) or not key):
                        raise ValueError("AOF event has no valid key")
                    if cmd == "set":
                        self._store[key] = val
                    elif cmd == "del":
                        self._store.pop(key, None)
                    elif cmd == "del_many":
                        keys = event.get("keys")
                        if not isinstance(keys, list) or not all(isinstance(item, str) for item in keys):
                            raise ValueError("AOF del_many event has invalid keys")
                        for item in keys:
                            self._store.pop(item, None)
                    elif cmd == "lpush":
                        self._store.setdefault(key, [])
                        self._store[key].insert(0, val)
                    elif cmd == "ltrim":
                        start, end = event.get("start"), event.get("end")
                        lst = self._store.get(key, [])
                        left, right = self._redis_slice_bounds(len(lst), int(start), int(end))
                        self._store[key] = lst[left:right]
                    elif cmd == "lpush_trim":
                        list_val = event.get("list_val")
                        limit = int(event.get("max_len", 0))
                        if not isinstance(list_val, str) or limit < 1:
                            raise ValueError("AOF lpush_trim event is invalid")
                        self._store.setdefault(key, [])
                        self._store[key].insert(0, list_val)
                        self._store[key] = self._store[key][:limit]
                    elif cmd == "trade_write":
                        list_key = event.get("list_key")
                        list_val = event.get("list_val")
                        limit = int(event.get("max_len", 0))
                        if not isinstance(list_key, str) or not list_key or not isinstance(list_val, str) or limit < 1:
                            raise ValueError("AOF trade_write event is invalid")
                        self._store[key] = val
                        self._store.setdefault(list_key, [])
                        self._store[list_key].insert(0, list_val)
                        self._store[list_key] = self._store[list_key][:limit]
                    elif cmd == "clear_trades":
                        keys_to_del = [k for k in list(self._store.keys()) if k.startswith("piluitus:trade:")]
                        for k in keys_to_del:
                            del self._store[k]
                        self._store.pop("piluitus:active_orders", None)
                    else:
                        raise ValueError(f"Unknown AOF command: {cmd!r}")
            log.info("✅ _InMemory: Стейт успешно восстановлен. Ключей в ОЗУ: %d", len(self._store))
        except Exception as e:
            self._load_error = f"{type(e).__name__}: {e}"
            self._store.clear()
            log.critical("❌ _InMemory: AOF повреждён; хранилище закрыто для записи/чтения: %s", self._load_error)

    def _ensure_healthy(self) -> None:
        if self._load_error:
            raise RuntimeError(
                "Fallback AOF is unreadable; storage is fail-closed: "
                f"{self._load_error}"
            )

    def _sync_write_aof(self, event: dict) -> None:
        """Append and fsync one journal event; persistence errors must propagate."""
        parent = os.path.dirname(os.path.abspath(self.aof_path))
        os.makedirs(parent, exist_ok=True)
        with open(self.aof_path, "a", encoding="utf-8") as stream:
            stream.write(json.dumps(event, ensure_ascii=False, allow_nan=False) + "\n")
            stream.flush()
            os.fsync(stream.fileno())

    async def _append_aof_locked(self, event: dict) -> None:
        self._ensure_healthy()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, self._sync_write_aof, event)

    async def _append_aof(self, event: dict) -> None:
        """Append under the same lock used by state mutations and compaction."""
        async with self._lock:
            await self._append_aof_locked(event)

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
            self._ensure_healthy()
            snapshot = {key: list(value) if isinstance(value, list) else value for key, value in self._store.items()}
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(None, self._sync_compact_aof, snapshot)
    # ---------- базовые операции ----------
    async def set(self, key: str, value: str, ex: Optional[int] = None, **_: Any) -> bool:
        async with self._lock:
            await self._append_aof_locked({"cmd": "set", "key": key, "val": value})
            self._store[key] = value
        return True

    async def get(self, key: str, **_: Any) -> Optional[str]:
        async with self._lock:
            self._ensure_healthy()
            val = self._store.get(key)
            return val if isinstance(val, str) else None

    async def delete(self, *keys: str) -> int:
        normalized = list(dict.fromkeys(str(key) for key in keys))
        if not normalized:
            return 0
        async with self._lock:
            self._ensure_healthy()
            existing = [key for key in normalized if key in self._store]
            if not existing:
                return 0
            await self._append_aof_locked({"cmd": "del_many", "keys": existing, "key": "__delete__"})
            for key in existing:
                self._store.pop(key, None)
            return len(existing)

    # ---------- list‑ops ----------
    async def lpush(self, key: str, value: str) -> int:
        async with self._lock:
            await self._append_aof_locked({"cmd": "lpush", "key": key, "val": value})
            self._store.setdefault(key, [])
            lst: List[str] = cast(List[str], self._store[key])
            lst.insert(0, value)
            return len(lst)

    async def lpush_trim(self, key: str, value: str, max_len: int) -> int:
        limit = max(1, int(max_len))
        async with self._lock:
            await self._append_aof_locked({
                "cmd": "lpush_trim", "key": key, "list_val": value, "max_len": limit,
            })
            self._store.setdefault(key, [])
            lst: List[str] = cast(List[str], self._store[key])
            lst.insert(0, value)
            del lst[limit:]
            return len(lst)

    async def set_and_lpush_trim(
        self, key: str, value: str, list_key: str, list_value: str, max_len: int
    ) -> None:
        limit = max(1, int(max_len))
        async with self._lock:
            await self._append_aof_locked({
                "cmd": "trade_write", "key": key, "val": value,
                "list_key": list_key, "list_val": list_value, "max_len": limit,
            })
            self._store[key] = value
            self._store.setdefault(list_key, [])
            rows: List[str] = cast(List[str], self._store[list_key])
            rows.insert(0, list_value)
            del rows[limit:]

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
        async with self._lock:
            self._ensure_healthy()
            lst = cast(List[Any], self._store.get(key, []))
            left, right = self._redis_slice_bounds(len(lst), start, end)
            trimmed = lst[left:right]
            await self._append_aof_locked({"cmd": "ltrim", "key": key, "start": start, "end": end})
            self._store[key] = trimmed

    async def lrange(self, key: str, start: int, end: int) -> List[str]:
        async with self._lock:
            self._ensure_healthy()
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
        async with self._lock:
            self._ensure_healthy()
            keys = list(self._store.keys())
            if match:
                keys = [k for k in keys if fnmatch.fnmatch(k, match)]
            byte_keys = [k.encode() for k in keys]
            return 0, byte_keys

    # ---------- ping ----------
    async def ping(self) -> bool:
        self._ensure_healthy()
        return True


# ----------------------------------------------------------------------
# 2️⃣  Redis‑клиент‑обёртка (singleton + fallback)
# ----------------------------------------------------------------------
