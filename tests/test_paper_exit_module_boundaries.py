"""Composition contract for the modular PAPER exit runtime."""
from __future__ import annotations

import unittest

from execution.paper_position_exit_runtime import PaperPositionExitRuntimeMixin
from execution.paper_exit_monitor import PaperExitMonitorMixin
from execution.paper_exit_finalization import PaperExitFinalizationMixin
from execution.paper_exit_recovery import PaperExitRecoveryMixin


class PaperExitModuleBoundaryTests(unittest.TestCase):
    def test_public_mixin_composes_monitor_finalization_and_recovery(self):
        self.assertTrue(issubclass(PaperPositionExitRuntimeMixin, PaperExitMonitorMixin))
        self.assertTrue(issubclass(PaperPositionExitRuntimeMixin, PaperExitFinalizationMixin))
        self.assertTrue(issubclass(PaperPositionExitRuntimeMixin, PaperExitRecoveryMixin))

    def test_historical_exit_api_remains_available(self):
        for name in (
            "schedule_paper_position_monitor", "monitor_paper_positions",
            "_finalize_paper_partial_close", "_finalize_paper_close",
            "quarantine_unreconciled_paper_positions", "close_all_positions",
        ):
            self.assertTrue(callable(getattr(PaperPositionExitRuntimeMixin, name)))


if __name__ == "__main__":
    unittest.main()
