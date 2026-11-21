"""Regression tests for the extracted background operation tracker."""
from __future__ import annotations

import asyncio
import logging

from monitoring.background_tasks import BackgroundOperationTracker


def test_spawn_tracks_and_releases_completed_task() -> None:
    async def scenario() -> None:
        tracker = BackgroundOperationTracker(logging.getLogger("test.background"))
        started = asyncio.Event()

        async def operation() -> str:
            started.set()
            return "complete"

        task = tracker.spawn(operation(), name="test-operation")
        assert tracker.pending_count == 1
        await started.wait()
        assert await task == "complete"
        await asyncio.sleep(0)
        assert tracker.pending_count == 0

    asyncio.run(scenario())


def test_drain_waits_for_all_tracked_operations() -> None:
    async def scenario() -> None:
        tracker = BackgroundOperationTracker(logging.getLogger("test.background"))
        release = asyncio.Event()
        completed: list[str] = []

        async def operation() -> None:
            await release.wait()
            completed.append("done")

        tracker.spawn(operation(), name="slow-operation")
        drain_task = asyncio.create_task(tracker.drain())
        await asyncio.sleep(0)
        assert not drain_task.done()

        release.set()
        await drain_task
        assert completed == ["done"]
        assert tracker.pending_count == 0

    asyncio.run(scenario())


def test_spawn_logs_uncaught_operation_failure(caplog) -> None:
    async def scenario() -> None:
        test_logger = logging.getLogger("test.background.failure")
        tracker = BackgroundOperationTracker(test_logger)

        async def operation() -> None:
            raise RuntimeError("expected failure")

        with caplog.at_level(logging.ERROR, logger=test_logger.name):
            task = tracker.spawn(operation(), name="failing-operation")
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.sleep(0)

        assert "[BACKGROUND OPERATION FAILED]" in caplog.text
        assert "failing-operation" in caplog.text
        assert "expected failure" in caplog.text
        assert tracker.pending_count == 0

    asyncio.run(scenario())


def test_drain_handles_cancelled_operations() -> None:
    async def scenario() -> None:
        tracker = BackgroundOperationTracker(logging.getLogger("test.background.cancel"))

        async def operation() -> None:
            await asyncio.Event().wait()

        task = tracker.spawn(operation(), name="cancelled-operation")
        task.cancel()
        await tracker.drain()
        assert tracker.pending_count == 0

    asyncio.run(scenario())


def test_supervisor_cancels_and_awaits_running_workers() -> None:
    from monitoring.background_tasks import BackgroundTaskSupervisor

    async def scenario() -> None:
        tracker = BackgroundTaskSupervisor(logging.getLogger("test.supervisor"))
        started = asyncio.Event()
        cancelled = asyncio.Event()

        async def worker() -> None:
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        task = tracker.spawn(worker(), name="long-running-worker")
        await started.wait()
        assert tracker.pending_count == 1
        await tracker.cancel_and_drain()
        assert task.cancelled()
        assert cancelled.is_set()
        assert tracker.pending_count == 0

    asyncio.run(scenario())


def test_supervisor_observes_failures_of_early_completed_workers(caplog) -> None:
    from monitoring.background_tasks import BackgroundTaskSupervisor

    async def scenario() -> None:
        test_logger = logging.getLogger("test.supervisor.failure")
        supervisor = BackgroundTaskSupervisor(test_logger)

        async def worker() -> None:
            raise RuntimeError("worker crashed")

        with caplog.at_level(logging.ERROR, logger=test_logger.name):
            task = supervisor.spawn(worker(), name="failed-worker")
            await asyncio.gather(task, return_exceptions=True)
            await asyncio.sleep(0)
            await supervisor.cancel_and_drain()

        assert supervisor.pending_count == 0
        assert "[BACKGROUND TASK FAILED]" in caplog.text
        assert "failed-worker" in caplog.text
        assert "worker crashed" in caplog.text

    asyncio.run(scenario())


def test_supervisor_drains_multiple_workers_and_is_idempotent() -> None:
    from monitoring.background_tasks import BackgroundTaskSupervisor

    async def scenario() -> None:
        supervisor = BackgroundTaskSupervisor(logging.getLogger("test.supervisor.multiple"))
        cancelled: list[str] = []

        async def worker(name: str) -> None:
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.append(name)

        first = supervisor.spawn(worker("first"), name="first")
        second = supervisor.spawn(worker("second"), name="second")
        await asyncio.sleep(0)
        await supervisor.cancel_and_drain()
        await supervisor.cancel_and_drain()

        assert first.cancelled()
        assert second.cancelled()
        assert sorted(cancelled) == ["first", "second"]
        assert supervisor.pending_count == 0

    asyncio.run(scenario())
