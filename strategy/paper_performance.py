"""Realized PAPER trade journal analytics.

Only final CLOSED trade records are accepted. Shadow labels are joined by the
stable candidate_id carried on the trade; ticker/time proximity is never used.

The implementation is divided into primitives, pure metrics, record lifecycle,
scorecard assembly and Shadow comparison. This module remains the compatibility
facade for existing imports and the application singleton.
"""
from __future__ import annotations

from .paper_performance_metrics import (
    _entry_diagnostics,
    _exit_diagnostics,
    _loss_diagnostics,
    _metrics,
    _reentry_diagnostics,
    _shadow_segment_metrics,
    _session_stability,
    _wilson_interval_pct,
)
from .paper_performance_primitives import (
    PAPER_CLOSED_TRADES_STORAGE_KEY,
    PAPER_CLOSED_TRADES_STORAGE_LIMIT,
    _aware_timestamp_epoch,
    _confidence_bucket,
    _entry_bbo_age_bucket,
    _entry_shortfall_bucket,
    _entry_signal_drift_bucket,
    _entry_spread_atr_bucket,
    _entry_spread_bucket,
    _exit_depth_impact_bucket,
    _exit_shortfall_bucket,
    _exit_timestamp_sort_key,
    _holding_time_bucket,
    _moscow_exit_date,
    _number,
    _numeric_bucket,
    _reentry_gap_bucket,
    _text,
)
from .paper_performance_records import PaperPerformanceRecordsMixin
from .paper_performance_scorecard import PaperPerformanceScorecardMixin
from .paper_performance_shadow import PaperPerformanceShadowMixin


class PaperPerformanceTracker(
    PaperPerformanceScorecardMixin,
    PaperPerformanceShadowMixin,
    PaperPerformanceRecordsMixin,
):
    """Bounded, idempotent in-memory view of the durable closed-trade journal."""


paper_performance_tracker = PaperPerformanceTracker()
