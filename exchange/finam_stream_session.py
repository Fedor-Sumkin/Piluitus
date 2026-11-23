"""Finam WebSocket handshake and initial QUOTES subscription session helpers."""
from __future__ import annotations

import asyncio
import json
from typing import Any, Iterable


async def wait_for_finam_handshake(
    websocket: Any,
    logger: Any,
    *,
    timeout_sec: float = 10.0,
) -> None:
    """Wait for provider HANDSHAKE_SUCCESS, preserving pre-handshake event handling."""
    deadline = asyncio.get_running_loop().time() + float(timeout_sec)
    while True:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            raise TimeoutError(
                f"Finam WebSocket не прислал HANDSHAKE_SUCCESS в течение {timeout_sec:g} секунд."
            )
        raw_message = await asyncio.wait_for(websocket.recv(), timeout=remaining)
        try:
            message = json.loads(raw_message)
        except (TypeError, json.JSONDecodeError):
            logger.warning("Finam WS прислал не-JSON сообщение до handshake.")
            continue
        if not isinstance(message, dict):
            continue
        message_type = str(message.get("type", "")).upper()
        if message_type == "ERROR":
            error_info = message.get("error_info", message)
            raise RuntimeError(f"Finam отклонил WebSocket handshake: {error_info}")
        if message_type != "EVENT":
            continue
        event_info = message.get("event_info") or {}
        event_name = str(event_info.get("event", "")).upper() if isinstance(event_info, dict) else ""
        if event_name == "HANDSHAKE_SUCCESS":
            logger.info("✅ Finam WS: авторизация успешна (HANDSHAKE_SUCCESS)")
            return
        logger.info("Событие Finam WS до подписки: %s", event_info)
        if event_name == "CONNECTION_CLOSED":
            raise RuntimeError(f"Finam закрыл соединение во время handshake: {event_info}")


async def send_quote_subscription_batches(
    websocket: Any,
    jwt_token: str,
    batches: Iterable[Iterable[str]],
    logger: Any,
    *,
    inter_batch_delay_sec: float = 0.05,
) -> None:
    """Send ordered QUOTES batches on an already authenticated socket."""
    batch_list = list(batches)
    for batch_number, symbol_batch in enumerate(batch_list, start=1):
        symbols = list(symbol_batch)
        request = {
            "action": "SUBSCRIBE",
            "type": "QUOTES",
            "data": {"symbols": symbols},
            "token": jwt_token,
        }
        await websocket.send(json.dumps(request, ensure_ascii=False))
        logger.info(
            "Подписка QUOTES отправлена: пакет %d/%d, символов %d (%s%s).",
            batch_number, len(batch_list), len(symbols),
            ", ".join(symbols[:3]), "…" if len(symbols) > 3 else "",
        )
        if batch_number < len(batch_list) and inter_batch_delay_sec > 0:
            await asyncio.sleep(inter_batch_delay_sec)
