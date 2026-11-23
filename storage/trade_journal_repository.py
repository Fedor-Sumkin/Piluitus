"""Trade journal and RAG-rule storage mixin."""
from __future__ import annotations

from storage.storage_shared import *  # noqa: F401,F403
from storage.redis_client import RedisClient


class TradeJournalRepositoryMixin:
    async def log_trade(self, order_id: str, trade_dict: dict) -> None:
        """Сохраняет/обновляет запись о сделке в Redis."""
        client = await self._get_client()
        await RedisClient.write_trade(order_id, trade_dict, client=client)
        log.debug("📝 atomically logged trade %s", order_id)

    async def get_active_positions(self) -> dict:
        """Возвращает словарь {order_id: trade_dict, ...} только для OPEN позиций."""
        client = await self._get_client()
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
                except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
                    log.critical(
                        "❌ Corrupt trade JSON for key %s; active-position read aborted",
                        key_str,
                    )
                    raise RuntimeError(
                        f"Некорректная запись активной позиции: {key_str}"
                    ) from exc
                if not isinstance(trade, dict):
                    raise RuntimeError(
                        f"Некорректный формат активной позиции: {key_str}"
                    )
                if str(trade.get("status", "")).strip().upper() == "OPEN":
                    order_id = key_str.split(":")[-1]
                    result[order_id] = trade

            if cursor == 0:
                break

        log.debug("🔎 get_active_positions → %d open trades", len(result))
        return result

    # ----------------------- сквозной суточный риск-аудит -----------------------

    async def get_all_daily_trades(self) -> dict:
        """Возвращает ВСЕ сделки за день (OPEN и CLOSED) по маске ``piluitus:trade:*``."""
        client = await self._get_client()
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
                except (json.JSONDecodeError, TypeError, UnicodeDecodeError) as exc:
                    # This method is the authoritative full journal used by
                    # restart reconciliation and risk hydration. Silently
                    # skipping a corrupt key could hide an OPEN/STALE PAPER
                    # position and incorrectly release the entry lock.
                    log.critical("❌ Corrupt trade JSON for key %s; full journal read aborted", key_str)
                    raise RuntimeError(
                        f"Некорректная запись сделки в полном журнале: {key_str}"
                    ) from exc
                if not isinstance(trade, dict):
                    log.critical("❌ Invalid trade record shape for key %s; full journal read aborted", key_str)
                    raise RuntimeError(
                        f"Некорректный формат записи сделки в полном журнале: {key_str}"
                    )
                order_id = key_str.split(":")[-1]
                result[order_id] = trade

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
