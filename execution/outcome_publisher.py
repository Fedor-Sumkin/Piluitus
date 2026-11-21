"""Persist and broadcast finalized execution outcomes."""
from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, MutableSequence
from typing import Any


async def publish_execution_outcome(
    update: dict[str, Any],
    *,
    db: Any,
    verdict_cache: MutableSequence[dict[str, Any]],
    get_open_positions: Callable[[], Awaitable[list[dict[str, Any]]]],
    refresh_balance_snapshot: Callable[[list[dict[str, Any]]], Awaitable[dict[str, Any]]],
    manager: Any,
    execution_engine: Any,
    logger: Any,
    side: str,
    ticker: str,
    status: str,
    actual_lots: int | None,
    actual_notional: float | None,
    order_id: str,
) -> dict[str, Any]:
    """Update local history, persist the outcome, and publish dashboard events.

    Persistence and websocket failures are isolated, matching the former
    MarketWorker behavior: neither failure changes the executor's outcome.
    """
    verdict_id = str(update.get("verdict_id") or "")
    for cached in verdict_cache:
        if isinstance(cached, dict) and cached.get("verdict_id") == verdict_id:
            cached.update(update)
            break

    try:
        redis_inst = getattr(db, "redis", getattr(db, "_redis", None))
        if redis_inst is not None:
            await redis_inst.set(
                f"piluitus:verdict:execution:{verdict_id}",
                json.dumps(update, ensure_ascii=False),
                ex=7 * 24 * 60 * 60,
            )
    except Exception as persist_exc:
        logger.warning(
            "Не удалось сохранить статус исполнения вердикта %s: %s",
            verdict_id,
            persist_exc,
        )

    try:
        positions = await get_open_positions()
        balance = await refresh_balance_snapshot(positions)
        update["active_positions"] = positions
        update["balance"] = balance
        await manager.broadcast(
            json.dumps({"type": "EXECUTION_UPDATE", **update}, ensure_ascii=False)
        )
        await manager.broadcast(
            json.dumps(
                {
                    "type": "TABLES_UPDATE",
                    "active_positions": positions,
                    "closed_trades": getattr(execution_engine, "closed_trades", [])[-30:],
                    "balance": balance,
                },
                ensure_ascii=False,
            )
        )
    except Exception as broadcast_exc:
        logger.error(
            "Не удалось отправить статус исполнения в веб-интерфейс: %s",
            broadcast_exc,
            exc_info=True,
        )

    if status in {"OPENED", "ACCEPTED"}:
        logger.info(
            "📡 [DASHBOARD EXECUTION] %s %s %s: %s лот(а), %s ₽, order=%s",
            side,
            ticker,
            status,
            actual_lots,
            actual_notional,
            order_id or "N/A",
        )
    else:
        logger.warning(
            "📡 [DASHBOARD EXECUTION] %s %s: %s — %s",
            side,
            ticker,
            status,
            update.get("execution_message", ""),
        )
    return update
