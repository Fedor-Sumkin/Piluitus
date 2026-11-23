"""Stable PAPER position-exit facade composed from focused runtime mixins."""
from __future__ import annotations

from execution.paper_exit_shared import (
    asyncio, logging, math, os, time, Mapping, datetime, timedelta, timezone, Any, cast,
    LOT_SIZES, DBManager, PAPER_CLOSED_TRADES_STORAGE_KEY, PAPER_CLOSED_TRADES_STORAGE_LIMIT,
    paper_performance_tracker, build_exit_execution_diagnostics,
    BBOExitQuote, TradeRecord, _as_string_mapping, _finite_or_default, logger,
)

from execution.paper_exit_monitor import PaperExitMonitorMixin
from execution.paper_exit_finalization import PaperExitFinalizationMixin
from execution.paper_exit_recovery import PaperExitRecoveryMixin


class PaperPositionExitRuntimeMixin(
    PaperExitMonitorMixin,
    PaperExitFinalizationMixin,
    PaperExitRecoveryMixin,
):
    """Compatibility facade retaining the historical exit-runtime API."""

    pass
