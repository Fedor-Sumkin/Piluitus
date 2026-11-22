"""Compatibility facade for Shadow validation and cost-sensitivity reports.

Implementation lives in focused modules; keep this import path stable for
runtime callers, tests and persisted diagnostics integrations.
"""
from __future__ import annotations

from .shadow_validation_cost_sensitivity import build_shadow_cost_sensitivity
from .shadow_validation_metrics import (
    _metrics_by_horizon_side,
    _metrics_by_horizon_side_dimensions,
    _metrics_by_regime,
    _summarize,
)
from .shadow_validation_primitives import (
    _confidence_band,
    _finite,
    _quality_rank,
    _timestamp,
    _wilson_interval_pct,
)
from .shadow_walk_forward_validation import build_shadow_walk_forward_validation

__all__ = [
    "build_shadow_walk_forward_validation",
    "build_shadow_cost_sensitivity",
]
