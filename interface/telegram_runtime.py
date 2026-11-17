"""Resilient lifecycle helpers for optional Telegram control-plane polling."""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Awaitable, Callable
from typing import Any


def _is_expected_network_error(exc: BaseException) -> bool:
    """Return True for network/transport failures, including wrapped causes."""

    pending: list[BaseException] = [exc]
    seen: set[int] = set()

    while pending:
        current = pending.pop()
        if id(current) in seen:
            continue
        seen.add(id(current))

        if isinstance(current, (OSError, TimeoutError, asyncio.TimeoutError)):
            return True

        module = type(current).__module__
        name = type(current).__name__
        if module.startswith("aiohttp.") and (
            "Error" in name or "Timeout" in name or "Disconnected" in name
        ):
            return True
        if module.startswith("aiogram.") and name in {
            "TelegramNetworkError",
            "TelegramServerError",
        }:
            return True

        cause = current.__cause__
        context = current.__context__
        if cause is not None:
            pending.append(cause)
        if context is not None:
            pending.append(context)

    return False


async def run_polling_with_retry(
    start_polling: Callable[[], Awaitable[Any]],
    *,
    logger: logging.Logger,
    initial_delay_sec: float = 5.0,
    max_delay_sec: float = 300.0,
) -> None:
    """Keep Telegram polling alive across transient API/network failures.

    Telegram is an optional control-plane integration: an outage must not stop
    market-data processing or the trading worker. Expected network failures are
    reported as concise status messages without tracebacks. Unexpected errors
    retain their traceback for diagnosis. Cancellation is always propagated.
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
    network_outage_reported = False

    while True:
        try:
            await start_polling()
            # start_polling normally returns only after an explicit stop. If
            # that happens without application shutdown, restore polling.
            failed_attempts = 0
            network_outage_reported = False
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

            if _is_expected_network_error(exc):
                if not network_outage_reported:
                    logger.warning(
                        "[TELEGRAM] Подключение недоступно; бот работает без "
                        "Telegram. Повторная попытка через %.1f с.",
                        delay,
                    )
                    network_outage_reported = True
                else:
                    logger.debug(
                        "[TELEGRAM] Сервер по-прежнему недоступен; "
                        "следующая попытка через %.1f с.",
                        delay,
                    )
            else:
                logger.error(
                    "[TELEGRAM] Неожиданная ошибка polling "
                    "(%s, попытка %d); повтор через %.1f с.",
                    type(exc).__name__,
                    failed_attempts,
                    delay,
                    exc_info=True,
                )

            await asyncio.sleep(delay)
