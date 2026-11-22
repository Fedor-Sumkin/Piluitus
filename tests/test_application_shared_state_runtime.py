"""Behavioral contracts for application-wide shared mutable state."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from monitoring.background_tasks import BackgroundOperationTracker
from runtime.application_shared_state_runtime import ApplicationSharedStateRuntime
from runtime.priority_queue import PriorityQueueItemFactory
from strategy.bbo_replay import BBOReplayRecorder
from strategy.shadow_outcomes import ShadowForwardTracker
from interface.websocket_manager import ConnectionManager


class _Logger:
    def info(self, *args):
        pass

    def error(self, *args, **kwargs):
        pass


def test_shared_state_runtime_builds_and_retains_singletons() -> None:
    engine = SimpleNamespace()
    runtime = ApplicationSharedStateRuntime(
        logger=_Logger(),
        execution_engine=engine,
    )

    assert runtime.create() is runtime
    assert isinstance(runtime.anomaly_queue, asyncio.PriorityQueue)
    assert runtime.anomaly_queue.maxsize == 1000
    assert isinstance(runtime.priority_queue_item_factory, PriorityQueueItemFactory)
    assert isinstance(runtime.paper_session_lock, asyncio.Lock)
    assert runtime.paper_session_generation == 0
    assert runtime.bg_market_task is None
    assert isinstance(runtime.shadow_tracker, ShadowForwardTracker)
    assert isinstance(runtime.bbo_replay_recorder, BBOReplayRecorder)
    assert runtime.paper_shadow_labels == []
    assert isinstance(runtime.background_operations, BackgroundOperationTracker)
    assert isinstance(runtime.connection_manager, ConnectionManager)
    assert engine.bbo_replay_session_id == runtime.bbo_replay_recorder.session_id
    assert engine.paper_exit_event_recorder.__self__ is runtime.bbo_replay_recorder

    queue = runtime.anomaly_queue
    lock = runtime.paper_session_lock
    recorder = runtime.bbo_replay_recorder
    assert runtime.anomaly_queue is queue
    assert runtime.paper_session_lock is lock
    assert runtime.bbo_replay_recorder is recorder


def test_shared_state_runtime_updates_session_and_replaced_runtime_handles() -> None:
    runtime = ApplicationSharedStateRuntime(
        logger=_Logger(),
        execution_engine=SimpleNamespace(),
    ).create()

    assert runtime.advance_paper_session_generation() == 1
    assert runtime.advance_paper_session_generation() == 2

    replacement_recorder = SimpleNamespace(session_id="rotated")
    runtime.set_bbo_replay_recorder(replacement_recorder)
    assert runtime.bbo_replay_recorder is replacement_recorder

    market_task = object()
    runtime.set_market_task(market_task)
    assert runtime.bg_market_task is market_task


def test_shared_state_runtime_rejects_duplicate_creation() -> None:
    runtime = ApplicationSharedStateRuntime(
        logger=_Logger(),
        execution_engine=SimpleNamespace(),
    ).create()

    with pytest.raises(RuntimeError, match="only run once"):
        runtime.create()


def test_shared_state_mutators_require_creation() -> None:
    runtime = ApplicationSharedStateRuntime(
        logger=_Logger(),
        execution_engine=SimpleNamespace(),
    )

    with pytest.raises(RuntimeError, match="create\\(\\) must run first"):
        runtime.advance_paper_session_generation()
    with pytest.raises(RuntimeError, match="create\\(\\) must run first"):
        runtime.set_bbo_replay_recorder(object())
    with pytest.raises(RuntimeError, match="create\\(\\) must run first"):
        runtime.set_market_task(object())
