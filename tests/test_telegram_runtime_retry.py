from __future__ import annotations

import asyncio
import logging
import unittest
from unittest.mock import AsyncMock, call, patch

from interface.telegram_runtime import run_polling_with_retry


class TelegramPollingRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_network_errors_use_exponential_backoff_and_retry(self) -> None:
        attempts = 0

        async def start_polling() -> None:
            nonlocal attempts
            attempts += 1
            if attempts <= 2:
                raise ConnectionError("temporary Telegram API outage")
            raise asyncio.CancelledError

        with patch("interface.telegram_runtime.asyncio.sleep", new_callable=AsyncMock) as sleep:
            with self.assertLogs("Piluitus.Telegram", level="WARNING"):
                with self.assertRaises(asyncio.CancelledError):
                    await run_polling_with_retry(
                        start_polling,
                        logger=logging.getLogger("Piluitus.Telegram"),
                        initial_delay_sec=3.0,
                        max_delay_sec=10.0,
                    )

        self.assertEqual(attempts, 3)
        self.assertEqual(sleep.await_args_list, [call(3.0), call(6.0)])

    async def test_unexpected_clean_return_restarts_polling(self) -> None:
        attempts = 0

        async def start_polling() -> None:
            nonlocal attempts
            attempts += 1
            if attempts == 2:
                raise asyncio.CancelledError

        with patch("interface.telegram_runtime.asyncio.sleep", new_callable=AsyncMock) as sleep:
            with self.assertLogs("Piluitus.Telegram", level="WARNING"):
                with self.assertRaises(asyncio.CancelledError):
                    await run_polling_with_retry(
                        start_polling,
                        logger=logging.getLogger("Piluitus.Telegram"),
                    )

        self.assertEqual(attempts, 2)
        self.assertEqual(sleep.await_args_list, [call(5.0)])

    async def test_cancellation_is_not_retried(self) -> None:
        async def start_polling() -> None:
            raise asyncio.CancelledError

        with patch("interface.telegram_runtime.asyncio.sleep", new_callable=AsyncMock) as sleep:
            with self.assertRaises(asyncio.CancelledError):
                await run_polling_with_retry(
                    start_polling,
                    logger=logging.getLogger("Piluitus.Telegram"),
                )

        sleep.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
