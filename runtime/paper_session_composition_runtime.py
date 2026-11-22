"""Composition root for PAPER session persistence and replay lifecycle.

Storage adapters own persistence semantics. This module assembles them and
exposes stable callbacks to market ingestion and application lifecycle runtimes.
"""
from __future__ import annotations

from typing import Any, Callable

from storage.paper_session_runtime import PaperSessionRuntime
from storage.shadow_outcomes import (
    SHADOW_OUTCOMES_STORAGE_LIMIT,
    ShadowOutcomePersistence,
)


class PaperSessionCompositionRuntime:
    """Compose shadow-label persistence and PAPER session lifecycle services."""

    def __init__(
        self,
        *,
        db_manager: Any,
        execution_engine: Any,
        shadow_tracker: Any,
        bbo_replay_recorder: Any,
        bbo_replay_recorder_factory: Callable[[], Any],
        paper_performance_tracker: Any,
        paper_shadow_labels: list[dict[str, Any]],
        logger: Any,
        closed_trades_storage_key: str,
        closed_trades_storage_limit: int,
        shadow_outcomes_storage_limit: int = SHADOW_OUTCOMES_STORAGE_LIMIT,
        set_bbo_replay_recorder: Callable[[Any], None],
    ) -> None:
        self.db_manager = db_manager
        self.execution_engine = execution_engine
        self.shadow_tracker = shadow_tracker
        self.bbo_replay_recorder = bbo_replay_recorder
        self.bbo_replay_recorder_factory = bbo_replay_recorder_factory
        self.paper_performance_tracker = paper_performance_tracker
        self.paper_shadow_labels = paper_shadow_labels
        self.logger = logger
        self.closed_trades_storage_key = closed_trades_storage_key
        self.closed_trades_storage_limit = int(closed_trades_storage_limit)
        self.shadow_outcomes_storage_limit = max(1, int(shadow_outcomes_storage_limit))
        self.set_bbo_replay_recorder = set_bbo_replay_recorder
        self.shadow_outcome_persistence: Any | None = None
        self.paper_session_runtime: Any | None = None

    def create(self) -> "PaperSessionCompositionRuntime":
        """Construct the storage adapters once and retain their shared state."""
        self.shadow_outcome_persistence = ShadowOutcomePersistence(
            self.db_manager,
            self.shadow_tracker,
            self.paper_shadow_labels,
            self.logger,
            storage_limit=self.shadow_outcomes_storage_limit,
        )
        self.paper_session_runtime = PaperSessionRuntime(
            db_manager=self.db_manager,
            execution_engine=self.execution_engine,
            shadow_outcome_persistence=self.shadow_outcome_persistence,
            bbo_replay_recorder=self.bbo_replay_recorder,
            bbo_replay_recorder_factory=self.bbo_replay_recorder_factory,
            paper_performance_tracker=self.paper_performance_tracker,
            paper_shadow_labels=self.paper_shadow_labels,
            logger=self.logger,
            closed_trades_storage_key=self.closed_trades_storage_key,
            closed_trades_storage_limit=self.closed_trades_storage_limit,
        )
        return self

    def _require_created(self) -> None:
        if self.paper_session_runtime is None:
            raise RuntimeError("PaperSessionCompositionRuntime.create() must run first.")

    async def persist_shadow_outcomes(
        self, labels: list[dict[str, Any]], *, source: str
    ) -> None:
        self._require_created()
        await self.paper_session_runtime.persist_shadow_outcomes(
            labels, source=source
        )

    async def restore_shadow_outcomes(self) -> None:
        self._require_created()
        await self.paper_session_runtime.restore_shadow_outcomes()

    async def rotate_bbo_replay_session(self) -> Any:
        """Rotate the recorder, publishing the replacement only after success."""
        self._require_created()
        recorder = await self.paper_session_runtime.rotate_bbo_replay_session()
        self.set_bbo_replay_recorder(recorder)
        return recorder

    async def restore_paper_performance(self) -> None:
        self._require_created()
        await self.paper_session_runtime.restore_paper_performance()
