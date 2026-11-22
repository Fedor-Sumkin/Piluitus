"""Forward outcome labelling for research-only PILUITUS strategy candidates.

All observations are hypothetical. Entries and exits use the executable side
of a fresh BBO, with the PAPER commission/slippage settings applied for
sensitivity analysis. Nothing in this module submits, changes or closes orders.

The implementation is split into candidate lifecycle, market observation,
statistics/state and shared primitives. This module preserves the public class
and the historical monotonic-clock monkeypatch seam.
"""
from __future__ import annotations

# Keep these imports in the facade for compatibility with historical patches
# and callers that used strategy.shadow_outcomes as their integration point.
import logging
import math
import os
import time
from collections import deque
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from statistics import median
from typing import Any
from uuid import uuid4

from strategy.market_context import classify_market_regime, estimate_entry_economics

from .shadow_outcomes_candidates import ShadowCandidateMixin
from .shadow_outcomes_observation import ShadowMarketObservationMixin
from .shadow_outcomes_primitives import (
    _MOSCOW_TZ,
    _apply_adverse_slippage,
    _cost_settings,
    _number,
    logger,
)
from .shadow_outcomes_state import ShadowOutcomeStateMixin
from .shadow_outcomes_statistics import ShadowOutcomeStatisticsMixin


class ShadowForwardTracker(
    ShadowCandidateMixin,
    ShadowMarketObservationMixin,
    ShadowOutcomeStatisticsMixin,
    ShadowOutcomeStateMixin,
):
    """Label candidate forward performance at fixed horizons using fresh executable BBO."""

    def __init__(
        self,
        horizons_seconds: tuple[int, ...] = (60, 180, 300, 600),
        *,
        max_label_lag_sec: float = 30.0,
        on_time_label_lag_sec: float = 10.0,
        max_pending_per_ticker: int = 100,
    ) -> None:
        normalized = tuple(sorted({int(x) for x in horizons_seconds if int(x) > 0}))
        if not normalized:
            raise ValueError("At least one positive shadow horizon is required.")
        self.horizons_seconds = normalized
        self.max_label_lag_sec = max(1.0, float(max_label_lag_sec))
        self.on_time_label_lag_sec = max(0.0, float(on_time_label_lag_sec))
        # This is a warning threshold, not a hard cap. Dropping the oldest
        # candidate here silently destroys forward-label coverage precisely
        # during high-activity periods. Keep every candidate until its horizons
        # are labelled and expose sustained backlog through a rate-limited log.
        self.max_pending_per_ticker = max(1, int(max_pending_per_ticker))
        self._pending: dict[str, list[dict[str, Any]]] = {}
        self._pending_backlog_warned: set[str] = set()
        self.max_segment_groups = 1800
        self._label_stats: dict[str, dict[str, float | int]] = {
            "eligible": self._new_stats_bucket(),
            "ineligible": self._new_stats_bucket(),
        }
        # Bounded cross-sections let PAPER research compare direction, ticker,
        # market regime, score band, horizon and Moscow trading hour.
        self._segment_stats: dict[str, dict[str, float | int]] = {}
        # Per-ticker ATR reference uses only earlier candidate snapshots.
        self._atr_history: dict[str, deque[float]] = {}

    def clear(self) -> int:
        """Drop pending labels and all in-memory aggregates for a clean session."""
        discarded_pending = sum(len(rows) for rows in self._pending.values())
        self._pending.clear()
        self._pending_backlog_warned.clear()
        self._label_stats = {
            "eligible": self._new_stats_bucket(),
            "ineligible": self._new_stats_bucket(),
        }
        self._segment_stats.clear()
        self._atr_history.clear()
        return discarded_pending
