"""Generic JSON/list storage helpers and emergency journal cleanup mixin."""
from __future__ import annotations

from storage.storage_shared import *  # noqa: F401,F403
from storage.redis_client import RedisClient
from storage.in_memory_backend import _InMemory


class StorageUtilityRepositoryMixin:
    async def get_json(self, key: str) -> Optional[Any]:
        return await RedisClient.get_json(key)

    async def set_json(self, key: str, value: Any, ttl: Optional[int] = None) -> None:
        await RedisClient.set_json(key, value, ttl=ttl)

    async def push_to_list(self, key: str, item: Any, max_len: int = 500) -> None:
        await RedisClient.push_to_list(key, item, max_len)

    async def push_many_to_list(
        self, key: str, items: list[Any], max_len: int = 500
    ) -> None:
        """Append a batch to a bounded Redis list with one network round trip."""
        if not items:
            return
        client = await self._get_client()
        payloads = [json.dumps(item, ensure_ascii=False) for item in items]
        pipeline_factory = getattr(client, "pipeline", None)
        if callable(pipeline_factory):
            pipeline = pipeline_factory(transaction=False)
            pipeline.lpush(key, *payloads)
            pipeline.ltrim(key, 0, max_len - 1)
            await pipeline.execute()
            return
        # The local AOF fallback has no Redis pipeline; preserve the same
        # newest-first list order while keeping the operation bounded.
        for payload in payloads:
            await client.lpush(key, payload)
        await client.ltrim(key, 0, max_len - 1)

    async def get_list(self, key: str, start: int = 0, stop: int = -1) -> List[Any]:
        return await RedisClient.get_list(key, start, stop)

    # ----------------------- паника / очистка -----------------------

    async def clear_journal(self) -> None:
        """Полностью очищает журнал позиций – используется кнопкой «Паника»."""
        client = await self._get_client()
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
        await client.delete("piluitus:paper:closed_trades")

        if isinstance(client, _InMemory):
            await client._append_aof({"cmd": "clear_trades"})
            
        log.warning("🧹 Журнал сделок полностью очищен (panic mode)")

    async def get_last_n_trades(self, n: int = 20) -> List[dict]:
        history = await RedisClient.get_list("piluitus:trade_history", 0, n - 1)
        return history
