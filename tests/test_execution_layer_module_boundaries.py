"""Composition contract for the modular execution engine implementation."""
from __future__ import annotations

import unittest

from execution.execution_layer import BaseExecutionEngine
from execution.execution_position_risk import ExecutionPositionRiskMixin
from execution.execution_market_session import ExecutionMarketSessionMixin
from execution.execution_paper_mechanics import ExecutionPaperMechanicsMixin
from execution.execution_order_runtime import ExecutionOrderRuntimeMixin
from execution.paper_position_exit_runtime import PaperPositionExitRuntimeMixin


class ExecutionLayerModuleBoundaryTests(unittest.TestCase):
    def test_public_engine_composes_all_execution_responsibilities(self):
        for mixin in (
            ExecutionPositionRiskMixin,
            ExecutionMarketSessionMixin,
            ExecutionPaperMechanicsMixin,
            ExecutionOrderRuntimeMixin,
            PaperPositionExitRuntimeMixin,
        ):
            self.assertTrue(issubclass(BaseExecutionEngine, mixin))

    def test_public_execution_contract_remains_available(self):
        for name in (
            "get_open_position_tickers", "_reserve_order_size", "calculate_atr_stop",
            "_latest_fresh_exit_quote", "_latest_fresh_entry_quote",
            "_market_session_status", "_calculate_paper_pnl",
            "_simulate_paper_depth_fill", "execute_order", "monitor_paper_positions",
        ):
            self.assertTrue(callable(getattr(BaseExecutionEngine, name)))


if __name__ == "__main__":
    unittest.main()
