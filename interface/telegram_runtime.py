"""Resilient lifecycle helpers for optional Telegram control-plane polling."""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Awaitable, Callable
from typing import Any


async def run_polling_with_retry(
    start_polling: Callable[[], Awaitable[Any]],
    *,
    logger: logging.Logger,
    initial_delay_sec: float = 5.0,
    max_delay_sec: float = 300.0,
) -> None:
    """Keep Telegram polling alive across transient API/network failures.

    Telegram is an optional control-plane integration: an outage must not stop
    market-data processing or the trading worker. Cancellation is always
    propagated so the application can shut down cleanly.
    """

    def _positive_delay(value: object, fallback: float) -> float:
        try:
            result = float(value)
        except (TypeError, ValueError, OverflowError):
            result = fallback
        if not math.isfinite(result):
            result = fallback
        return max(0.1, result)

    initial_delay = _positive_delay(initial_delay_sec, 5.0)
    max_delay = max(initial_delay, _positive_delay(max_delay_sec, 300.0))
    failed_attempts = 0

    while True:
        try:
            await start_polling()
            # start_polling normally returns only after an explicit stop. If
            # that happens without application shutdown, restore polling.
            failed_attempts = 0
            logger.warning(
                "Telegram polling завершился без остановки приложения; "
                "повторный запуск через %.1f с.",
                initial_delay,
            )
            await asyncio.sleep(initial_delay)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            delay = min(max_delay, initial_delay * (2 ** min(failed_attempts, 6)))
            failed_attempts += 1
            logger.warning(
                "Telegram API/сеть недоступны (ошибка %s, попытка %d); "
                "повторное подключение через %.1f с.",
                type(exc).__name__,
                failed_attempts,
                delay,
                exc_info=True,
            )
            await asyncio.sleep(delay)
