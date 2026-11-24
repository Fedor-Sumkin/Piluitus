"""Reusable control-plane primitives for Finam dynamic WebSocket subscriptions."""
from __future__ import annotations

import asyncio
import json
from typing import Any, Iterable


def retain_subscription_overlap(
    current: Iterable[str],
    wanted: Iterable[str],
    subscribed_since: dict[str, float],
    now: float,
    min_hold_sec: float,
    max_overbook: int,
) -> tuple[set[str], set[str]]:
    """Keep the newest recently-added obsolete subscriptions for a bounded grace period."""
    current_set = set(current)
    wanted_set = set(wanted)
    obsolete = current_set - wanted_set
    protected = {
        symbol for symbol in obsolete
        if now - subscribed_since.get(symbol, 0.0) < min_hold_sec
    }
    limit = max(0, int(max_overbook))
    if len(protected) > limit:
        protected = set(sorted(
            protected,
            key=lambda item: subscribed_since.get(item, 0.0),
            reverse=True,
        )[:limit])
    return wanted_set | protected, protected


async def send_finam_subscription(
    websocket: Any,
    jwt_token: str,
    subscription_type: str,
    action: str,
    symbol: str,
) -> None:
    """Send one symbol-level SUBSCRIBE/UNSUBSCRIBE command."""
    request = {
        "action": str(action).upper(),
        "type": str(subscription_type).upper(),
        "data": {"symbol": str(symbol)},
        "token": jwt_token,
    }
    await websocket.send(json.dumps(request, ensure_ascii=False))


async def resubscribe_finam_symbol(
    websocket: Any,
    jwt_token: str,
    subscription_type: str,
    symbol: str,
    *,
    delay_sec: float = 0.05,
) -> None:
    """Restart one symbol subscription, preserving unsubscribe-before-subscribe order."""
    await send_finam_subscription(websocket, jwt_token, subscription_type, "UNSUBSCRIBE", symbol)
    if delay_sec > 0:
        await asyncio.sleep(delay_sec)
    await send_finam_subscription(websocket, jwt_token, subscription_type, "SUBSCRIBE", symbol)
