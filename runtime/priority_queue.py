"""Stable priority-queue item construction for runtime market events.

The sequence number is a deterministic tie-breaker: payload dictionaries are
never compared when two events have the same priority.
"""
from __future__ import annotations

import asyncio
from itertools import count
from typing import Any, TypeAlias

PriorityItem: TypeAlias = tuple[int, int, float, str, dict[str, Any]]


class PriorityQueueItemFactory:
    """Create comparable queue tuples with a unique sequence component."""

    def __init__(self) -> None:
        self._sequence = count()

    def create(
        self, priority: int, event_type: str, payload: dict[str, Any]
    ) -> PriorityItem:
        """Build an item using the active event loop's monotonic clock."""
        loop = asyncio.get_running_loop()
        return (
            int(priority),
            next(self._sequence),
            loop.time(),
            event_type,
            payload,
        )
