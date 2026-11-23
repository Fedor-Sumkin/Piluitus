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

    def _capture_paper_exit_trigger(self, *args: Any, **kwargs: Any):
        return super()._capture_paper_exit_trigger(*args, **kwargs)

    def schedule_paper_position_monitor(self, *args: Any, **kwargs: Any):
        return super().schedule_paper_position_monitor(*args, **kwargs)

    async def _drain_monitor_prices(self, *args: Any, **kwargs: Any):
        return await super()._drain_monitor_prices(*args, **kwargs)

    async def quarantine_unreconciled_paper_positions(self, *args: Any, **kwargs: Any):
        return await super().quarantine_unreconciled_paper_positions(*args, **kwargs)

    def _paper_exit_depth_fill(self, *args: Any, **kwargs: Any):
        return super()._paper_exit_depth_fill(*args, **kwargs)

    async def _finalize_paper_partial_close(self, *args: Any, **kwargs: Any):
        return await super()._finalize_paper_partial_close(*args, **kwargs)

    async def _finalize_paper_close(self, *args: Any, **kwargs: Any):
        return await super()._finalize_paper_close(*args, **kwargs)

    @staticmethod
    def _track_exit_bbo_gap(*args: Any, **kwargs: Any):
        return PaperExitMonitorMixin._track_exit_bbo_gap(*args, **kwargs)

    def _should_log_paper_exit_status(self, *args: Any, **kwargs: Any):
        return super()._should_log_paper_exit_status(*args, **kwargs)

    async def monitor_paper_positions(self, *args: Any, **kwargs: Any):
        return await super().monitor_paper_positions(*args, **kwargs)

    async def close_all_positions(self, *args: Any, **kwargs: Any):
        return await super().close_all_positions(*args, **kwargs)
