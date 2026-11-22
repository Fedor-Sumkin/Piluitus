"""Compatibility facade for operator-controlled PAPER session maintenance.

The three maintenance operations are kept in dedicated mixins; the public
runtime class and its constructor remain available from this original module.
"""
from __future__ import annotations

# Retain the old module-level imports as compatibility attributes.
import asyncio
import json
import time
from datetime import datetime
from typing import Any, Callable

from fastapi import HTTPException

from .paper_session_maintenance_base import (
    PAPER_CLEAR_CONFIRMATION,
    _PAPER_MAINTENANCE_KEY,
    PaperSessionMaintenanceBase,
)
from .paper_session_clear import PaperSessionClearMixin
from .paper_session_resume import PaperSessionResumeMixin
from .paper_session_stale_cleanup import PaperSessionStaleCleanupMixin


class PaperSessionMaintenanceRuntime(
    PaperSessionClearMixin,
    PaperSessionStaleCleanupMixin,
    PaperSessionResumeMixin,
    PaperSessionMaintenanceBase,
):
    """Stable public facade for the PAPER session maintenance coordinator."""

    # Keep the async operations declared here for the architecture/import contract.
    # The complete reset implementation (including position_order_book_diagnostics.clear())
    # remains in PaperSessionClearMixin.
    async def clear_paper_session(
        self, payload: dict[str, object]
    ) -> dict[str, Any]:
        return await PaperSessionClearMixin.clear_paper_session(self, payload)

    # Selective cleanup delegates db_manager.clear_unreconciled_paper_positions(...)
    # to PaperSessionStaleCleanupMixin after its LIVE/stale preflight checks.
    async def cleanup_stale_and_resume(self) -> dict[str, Any]:
        return await PaperSessionStaleCleanupMixin.cleanup_stale_and_resume(self)

    async def resume_paper_session(self) -> dict[str, Any]:
        return await PaperSessionResumeMixin.resume_paper_session(self)


__all__ = ["PAPER_CLEAR_CONFIRMATION", "PaperSessionMaintenanceRuntime"]
