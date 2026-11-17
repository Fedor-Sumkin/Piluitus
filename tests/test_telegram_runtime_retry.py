from __future__ import annotations

import asyncio
import logging
import unittest
from unittest.mock import AsyncMock, call, patch

from interface.telegram_runtime import run_polling_with_retry


class TelegramPollingRetryTests(unittest.IsolatedAsyncioTestCase):
    async def test_network_errors_use_backoff_without_traceback_or_repeated_warnings(self) -> None:
        attempts = 0

        async def start_polling() -> None:
            nonlocal attempts
            attempts += 1
            if attempts <= 2:
                raise ConnectionError("temporary Telegram API outage")
            raise asyncio.CancelledError

        logger = logging.getLogger("Piluitus.Telegram")
        with patch("interface.telegram_runtime.asyncio.sleep", new_callable=AsyncMock) as sleep:
            with self.assertLogs(logger, level="DEBUG") as captured:
                with self.assertRaises(asyncio.CancelledError):
                    await run_polling_with_retry(
                        start_polling,
                        logger=logger,
                        initial_delay_sec=3.0,
                        max_delay_sec=10.0,
                    )

        self.assertEqual(attempts, 3)
        self.assertEqual(sleep.await_args_list, [call(3.0), call(6.0)])
        warning_lines = [line for line in captured.output if "WARNING" in line]
        self.assertEqual(len(warning_lines), 1)
        self.assertIn("бот работает без Telegram", warning_lines[0])
        self.assertFalse(any("Traceback" in line for line in captured.output))

    async def test_wrapped_aiohttp_network_error_is_reported_as_status(self) -> None:
        class TelegramNetworkError(Exception):
            pass

        async def start_polling() -> None:
            try:
                raise OSError(64, "network unavailable")
            except OSError as cause:
                raise TelegramNetworkError("Telegram transport failed") from cause

        logger = logging.getLogger("Piluitus.Telegram")
        sleep_started = asyncio.Event()

        async def blocked_sleep(_delay: float) -> None:
            sleep_started.set()
            await asyncio.Future()

        with patch("interface.telegram_runtime.asyncio.sleep", side_effect=blocked_sleep):
            with self.assertLogs(logger, level="WARNING") as captured:
                task = asyncio.create_task(
                    run_polling_with_retry(
                        start_polling,
                        logger=logger,
                        initial_delay_sec=2.0,
                        max_delay_sec=2.0,
                    )
                )
                await sleep_started.wait()
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

        self.assertEqual(len(captured.output), 1)
        self.assertIn("[TELEGRAM] Подключение недоступно", captured.output[0])
        self.assertFalse(any("Traceback" in line for line in captured.output))

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

    async def test_unexpected_programming_error_keeps_traceback(self) -> None:
        async def start_polling() -> None:
            raise ValueError("programming defect")

        logger = logging.getLogger("Piluitus.Telegram")
        sleep_started = asyncio.Event()

        async def blocked_sleep(_delay: float) -> None:
            sleep_started.set()
            await asyncio.Future()

        with patch("interface.telegram_runtime.asyncio.sleep", side_effect=blocked_sleep):
            with self.assertLogs(logger, level="ERROR") as captured:
                task = asyncio.create_task(
                    run_polling_with_retry(
                        start_polling,
                        logger=logger,
                        initial_delay_sec=1.0,
                        max_delay_sec=1.0,
                    )
                )
                await sleep_started.wait()
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task

        self.assertTrue(any("Traceback" in line for line in captured.output))
        self.assertTrue(any("programming defect" in line for line in captured.output))

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
