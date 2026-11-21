"""Persistence adapter for completed strategy-shadow outcome labels.

The adapter keeps research history separate from order execution and makes
session scoping, bounded retention, and restoration independently testable.
"""
from __future__ import annotations

import logging
from typing import Any


SHADOW_OUTCOMES_STORAGE_KEY = "piluitus:strategy:shadow:outcomes"
SHADOW_OUTCOMES_STORAGE_LIMIT = 20000


class ShadowOutcomePersistence:
    """Persist and restore completed shadow labels without affecting execution."""

    def __init__(
        self,
        storage: Any,
        tracker: Any,
        labels: list[dict[str, Any]],
        logger: logging.Logger,
        *,
        storage_key: str = SHADOW_OUTCOMES_STORAGE_KEY,
        storage_limit: int = SHADOW_OUTCOMES_STORAGE_LIMIT,
    ) -> None:
        self.storage = storage
        self.tracker = tracker
        self.labels = labels
        self.logger = logger
        self.storage_key = storage_key
        self.storage_limit = max(1, int(storage_limit))

    async def persist_completed_labels(
        self,
        labels: list[dict[str, Any]],
        *,
        session_id: str,
        source: str,
    ) -> None:
        """Persist labels with a session marker and update bounded in-memory history."""
        if not labels:
            return
        session_labels = [
            {**dict(label), "paper_session_id": session_id}
            for label in labels if isinstance(label, dict)
        ]
        if not session_labels:
            return
        try:
            await self.storage.push_many_to_list(
                self.storage_key,
                session_labels,
                max_len=self.storage_limit,
            )
            self.labels.extend(session_labels)
            if len(self.labels) > self.storage_limit:
                del self.labels[:-self.storage_limit]
            self.logger.info(
                "[STRATEGY SHADOW PERSIST] source=%s saved=%d retained_limit=%d session_id=%s",
                source, len(session_labels), self.storage_limit, session_id,
            )
        except Exception as exc:
            self.logger.error(
                "[STRATEGY SHADOW PERSIST ERROR] source=%s batch_size=%d error=%s",
                source, len(session_labels), exc, exc_info=True,
            )

    async def restore_completed_labels(
        self,
        *,
        paper_mode: bool,
        session_id: str = "unknown",
    ) -> None:
        """Restore research history outside PAPER; PAPER always starts with empty aggregates."""
        self.labels.clear()
        if paper_mode:
            discarded_pending = self.tracker.clear()
            self.logger.info(
                "[STRATEGY SHADOW SESSION RESET] active_session=%s labels=0 pending_discarded=%d "
                "historical_journal_retained=true historical_labels_loaded=false",
                session_id,
                discarded_pending,
            )
            return
        try:
            stored = await self.storage.get_list(
                self.storage_key,
                start=0,
                stop=self.storage_limit - 1,
            )
            restored = self.tracker.restore_completed_labels(stored)
            self.labels.extend(
                dict(label) for label in stored if isinstance(label, dict)
            )
            self.logger.info(
                "[STRATEGY SHADOW RESTORE] loaded=%d restored=%d pending=0 storage_limit=%d",
                len(stored), restored, self.storage_limit,
            )
        except Exception as exc:
            self.logger.error("[STRATEGY SHADOW RESTORE ERROR] %s", exc, exc_info=True)
