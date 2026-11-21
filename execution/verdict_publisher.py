"""Persist and broadcast finalized market/LLM verdicts."""
from __future__ import annotations

import json
from collections.abc import MutableSequence
from typing import Any


async def publish_market_verdict(
    verdict: dict[str, Any],
    *,
    db: Any,
    verdict_cache: MutableSequence[dict[str, Any]],
    max_cache_size: int,
    manager: Any,
    balance_snapshot: dict[str, Any],
    logger: Any,
    candidate_id: str = "NONE",
) -> dict[str, Any]:
    """Append, persist, and broadcast a verdict without changing its decision.

    Storage and websocket failures are isolated so dashboard publication never
    changes the already-completed market-analysis result.
    """
    verdict_cache.append(verdict)
    if len(verdict_cache) > max_cache_size:
        verdict_cache.pop(0)

    redis_inst = getattr(db, "redis", None) or getattr(db, "_redis", None)
    if redis_inst is None:
        logger.error(
            "[LLM VERDICT PERSIST ERROR] verdict_id=%s candidate_id=%s ticker=%s: "
            "Redis/Memurai client unavailable; verdict remains only in memory.",
            verdict.get("verdict_id"),
            candidate_id,
            verdict.get("ticker"),
        )
    else:
        try:
            await redis_inst.lpush(
                "piluitus:verdicts:history",
                json.dumps(verdict, ensure_ascii=False),
            )
            await redis_inst.ltrim(
                "piluitus:verdicts:history", 0, max_cache_size - 1
            )
        except Exception as redis_err:
            logger.error(
                "[LLM VERDICT PERSIST ERROR] verdict_id=%s candidate_id=%s ticker=%s error=%s",
                verdict.get("verdict_id"),
                candidate_id,
                verdict.get("ticker"),
                redis_err,
                exc_info=True,
            )

    payload = {
        "type": "MARKET_UPDATE",
        **verdict,
        "balance": dict(balance_snapshot),
    }
    try:
        await manager.broadcast(json.dumps(payload, ensure_ascii=False))
    except Exception as broadcast_err:
        logger.error("Ошибка WebSocket бродкаста: %s", broadcast_err)
    return verdict
