"""Singleton Redis client and JSON/list operations for PILUITUS storage."""
from __future__ import annotations

from storage.storage_shared import *  # noqa: F401,F403
from storage.in_memory_backend import _InMemory

class RedisClient:
    """
    Singleton async Redis client.
    При первой попытке соединения, если реальный Redis недоступен,
    автоматически переключается на _InMemory‑мок.
    """
    _instance: Union[redis.Redis, _InMemory, None] = None
    _connect_lock: asyncio.Lock | None = None

    @classmethod
    async def get_client(cls) -> Union[redis.Redis, _InMemory]:
        """Return one process-wide Redis-compatible client, even during concurrent startup."""
        if cls._instance is not None:
            return cast(Union[redis.Redis, _InMemory], cls._instance)
        if cls._connect_lock is None:
            cls._connect_lock = asyncio.Lock()
        async with cls._connect_lock:
            if cls._instance is None:
                client = None
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
                    if client is not None:
                        try:
                            await client.aclose()
                        except Exception:
                            pass
                    log.error("❌ Не удалось подключиться к Redis: %s", exc)
                    cls._instance = _InMemory()
                    log.warning(
                        "🚧 Запущен fallback‑Redis (in‑memory с персистентным AOF-файлом). "
                        "Торговые операции будут доступны только при исправном AOF."
                    )
        return cast(Union[redis.Redis, _InMemory], cls._instance)

    # ---------------- 3️⃣ Универсальные CRUD‑утилиты ----------------
    @classmethod
    async def set_json(cls, key: str, value: Any, ttl: Optional[int] = None) -> None:
        client = await cls.get_client()
        payload = json.dumps(value, ensure_ascii=False, allow_nan=False)
        await client.set(key, payload, ex=ttl)
        log.debug("🔑 set_json %s (ttl=%s)", key, ttl)

    @classmethod
    async def write_trade(
        cls,
        order_id: str,
        trade_dict: dict,
        max_len: int = 500,
        *,
        client: Union[redis.Redis, _InMemory, None] = None,
    ) -> None:
        """Persist the authoritative trade key and its bounded index atomically."""
        if client is None:
            client = await cls.get_client()
        trade_key = f"piluitus:trade:{order_id}"
        trade_payload = json.dumps(trade_dict, ensure_ascii=False, allow_nan=False)
        index_payload = json.dumps(
            {"id": order_id, "data": trade_dict}, ensure_ascii=False, allow_nan=False
        )
        limit = max(1, int(max_len))
        if isinstance(client, _InMemory):
            await client.set_and_lpush_trim(
                trade_key, trade_payload, "piluitus:active_orders", index_payload, limit
            )
            return
        pipeline = client.pipeline(transaction=True)
        pipeline.set(trade_key, trade_payload)
        pipeline.lpush("piluitus:active_orders", index_payload)
        pipeline.ltrim("piluitus:active_orders", 0, limit - 1)
        await pipeline.execute()

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
        payload = json.dumps(item, ensure_ascii=False, allow_nan=False)
        limit = max(1, int(max_len))
        if isinstance(client, _InMemory):
            await client.lpush_trim(key, payload, limit)
        else:
            pipeline = client.pipeline(transaction=True)
            pipeline.lpush(key, payload)
            pipeline.ltrim(key, 0, limit - 1)
            await pipeline.execute()
        log.debug("📥 push_to_list %s (max_len=%d)", key, limit)

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
