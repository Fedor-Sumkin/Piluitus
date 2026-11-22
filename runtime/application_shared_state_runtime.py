"""Composition of application-wide mutable state and shared infrastructure objects.

The runtime keeps the single instances passed to market, PAPER and lifecycle
components. It owns no trading policy and does not alter queue or risk settings.
"""
from __future__ import annotations

import asyncio
from typing import Any, Callable

from interface.websocket_manager import ConnectionManager
from monitoring.background_tasks import BackgroundOperationTracker
from runtime.priority_queue import PriorityItem, PriorityQueueItemFactory
from strategy.bbo_replay import BBOReplayRecorder
from strategy.shadow_outcomes import ShadowForwardTracker


class ApplicationSharedStateRuntime:
    """Create and retain the shared application state exactly once."""

    def __init__(
        self,
        *,
        logger: Any,
        execution_engine: Any,
        shadow_tracker_factory: Callable[[], Any] = ShadowForwardTracker,
        bbo_replay_recorder_factory: Callable[[], Any] = BBOReplayRecorder,
        background_operations_factory: Callable[[Any], Any] = BackgroundOperationTracker,
        connection_manager_factory: Callable[[], Any] = ConnectionManager,
    ) -> None:
        self.logger = logger
        self.execution_engine = execution_engine
        self.shadow_tracker_factory = shadow_tracker_factory
        self.bbo_replay_recorder_factory = bbo_replay_recorder_factory
        self.background_operations_factory = background_operations_factory
        self.connection_manager_factory = connection_manager_factory

        self.anomaly_queue: asyncio.PriorityQueue[PriorityItem] | None = None
        self.priority_queue_item_factory: PriorityQueueItemFactory | None = None
        self.paper_session_lock: asyncio.Lock | None = None
        self.paper_session_generation = 0
        self.bg_market_task: Any | None = None
        self.shadow_tracker: Any | None = None
        self.bbo_replay_recorder: Any | None = None
        self.paper_shadow_labels: list[dict[str, Any]] | None = None
        self.background_operations: Any | None = None
        self.connection_manager: Any | None = None
        self._created = False

    def create(self) -> "ApplicationSharedStateRuntime":
        """Instantiate shared objects once, preserving their original defaults."""
        if self._created:
            raise RuntimeError(
                "ApplicationSharedStateRuntime.create() may only run once."
            )

        # Keep the established object creation order and exact queue capacity.
        anomaly_queue: asyncio.PriorityQueue[PriorityItem] = asyncio.PriorityQueue(
            maxsize=1000
        )
        priority_queue_item_factory = PriorityQueueItemFactory()
        paper_session_lock = asyncio.Lock()
        shadow_tracker = self.shadow_tracker_factory()
        bbo_replay_recorder = self.bbo_replay_recorder_factory()
        paper_shadow_labels: list[dict[str, Any]] = []
        background_operations = self.background_operations_factory(self.logger)
        connection_manager = self.connection_manager_factory()

        # Preserve the execution engine's replay hooks and initial session id.
        self.execution_engine.bbo_replay_session_id = bbo_replay_recorder.session_id
        self.execution_engine.paper_exit_event_recorder = (
            bbo_replay_recorder.record_exit_trigger
        )

        self.anomaly_queue = anomaly_queue
        self.priority_queue_item_factory = priority_queue_item_factory
        self.paper_session_lock = paper_session_lock
        self.paper_session_generation = 0
        self.bg_market_task = None
        self.shadow_tracker = shadow_tracker
        self.bbo_replay_recorder = bbo_replay_recorder
        self.paper_shadow_labels = paper_shadow_labels
        self.background_operations = background_operations
        self.connection_manager = connection_manager
        self._created = True
        return self

    def _require_created(self) -> None:
        if not self._created:
            raise RuntimeError(
                "ApplicationSharedStateRuntime.create() must run first."
            )

    def advance_paper_session_generation(self) -> int:
        """Invalidate queued/in-flight work from the previous PAPER session."""
        self._require_created()
        self.paper_session_generation += 1
        return self.paper_session_generation

    def set_bbo_replay_recorder(self, recorder: Any) -> None:
        """Publish a successfully rotated recorder as the shared active recorder."""
        self._require_created()
        self.bbo_replay_recorder = recorder

    def set_market_task(self, task: Any) -> None:
        """Retain the broker/stream task handle used by lifecycle diagnostics."""
        self._require_created()
        self.bg_market_task = task
