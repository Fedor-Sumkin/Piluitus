"""Deterministic, research-only exit replay over persisted PAPER market events.

The baseline consumes actual latched EXIT_TRIGGER events when the journal has
them. Alternative stop-management scenarios use executable BBO thresholds and
are explicitly marked as research hypotheses, not production behavior.

The implementation is split into focused primitives, exit, analysis, portfolio
and batch modules. This module remains the backwards-compatible import facade.
"""
from __future__ import annotations

from .bbo_replay_simulator_analysis import (
    _paired_scenario_comparison,
    _sample_quality,
    _summarize_replay_trades,
)
from .bbo_replay_simulator_batch import replay_exit_batch
from .bbo_replay_simulator_exit import replay_exit_scenarios
from .bbo_replay_simulator_portfolio import _portfolio_capacity_simulation
from .bbo_replay_simulator_primitives import (
    DEFAULT_SCENARIOS,
    _adverse_exit_fill,
    _break_even_trigger,
    _epoch,
    _fresh_side,
    _net_pnl,
    _number,
    _observation_epoch,
    _prepare_events,
)
