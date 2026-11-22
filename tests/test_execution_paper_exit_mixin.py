"""Execution engine delegates PAPER exit operations to its dedicated mixin."""
from __future__ import annotations

import ast
from pathlib import Path

from execution.execution_layer import BaseExecutionEngine
from execution.execution_primitives import (
    BBOExitQuote,
    TradeRecord,
    _as_string_mapping,
    _finite_or_default,
)
from execution.paper_position_exit_runtime import PaperPositionExitRuntimeMixin


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_base_execution_engine_inherits_paper_exit_runtime_contract():
    assert issubclass(BaseExecutionEngine, PaperPositionExitRuntimeMixin)
    for name in (
        "_capture_paper_exit_trigger",
        "schedule_paper_position_monitor",
        "_drain_monitor_prices",
        "quarantine_unreconciled_paper_positions",
        "_paper_exit_depth_fill",
        "_finalize_paper_partial_close",
        "_finalize_paper_close",
        "_track_exit_bbo_gap",
        "_should_log_paper_exit_status",
        "monitor_paper_positions",
        "close_all_positions",
    ):
        method = getattr(BaseExecutionEngine, name)
        assert method.__qualname__.startswith("PaperPositionExitRuntimeMixin.")


def test_execution_layer_reexports_stable_shared_types_and_primitives():
    from execution import execution_layer

    assert execution_layer.BBOExitQuote is BBOExitQuote
    assert execution_layer.TradeRecord is TradeRecord
    assert execution_layer._as_string_mapping is _as_string_mapping
    assert execution_layer._finite_or_default is _finite_or_default


def test_paper_exit_mixin_does_not_import_the_execution_engine_module():
    module_path = PROJECT_ROOT / "execution" / "paper_position_exit_runtime.py"
    module = ast.parse(module_path.read_text(encoding="utf-8"))
    imported_modules = {
        node.module
        for node in ast.walk(module)
        if isinstance(node, ast.ImportFrom)
    }
    assert "execution.execution_layer" not in imported_modules
    assert "execution_layer" not in imported_modules
