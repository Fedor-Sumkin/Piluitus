"""Lifecycle tracking for critical fire-and-forget asyncio operations."""
from __future__ import annotations

import asyncio
import logging
from collections.abc import Coroutine
from typing import Any


class BackgroundOperationTracker:
    """Keep strong task references and drain outstanding work during shutdown.

    The tracker does not create its own event loop and must be used from the
    running loop that owns the tracked tasks.
    """

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger
        self._tasks: set[asyncio.Task[Any]] = set()

    @property
    def pending_count(self) -> int:
        """Number of tracked tasks that have not completed yet."""
        return len(self._tasks)

    def spawn(
        self,
        coro: Coroutine[Any, Any, Any],
        *,
        name: str,
    ) -> asyncio.Task[Any]:
        """Schedule an operation and report uncaught failures through the logger."""
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)

        def _on_done(completed: asyncio.Task[Any]) -> None:
            self._tasks.discard(completed)
            if completed.cancelled():
                return
            try:
                error = completed.exception()
            except asyncio.CancelledError:
                return
            if error is not None:
                self._logger.error(
                    "[BACKGROUND OPERATION FAILED] task=%s error=%r",
                    completed.get_name(),
                    error,
                    exc_info=(type(error), error, error.__traceback__),
                )

        task.add_done_callback(_on_done)
        return task

    async def drain(self) -> None:
        """Wait until every tracked operation has completed."""
        while self._tasks:
            pending = tuple(self._tasks)
            await asyncio.gather(*pending, return_exceptions=True)
            for task in pending:
                self._tasks.discard(task)
        self._logger.info("[BACKGROUND OPERATIONS] shutdown drain complete.")
