"""Regression coverage for stable execution dependency monkeypatch seams."""
from __future__ import annotations

import unittest
from unittest.mock import patch

from execution.execution_layer import BaseExecutionEngine


class ExecutionFacadePatchSeamTests(unittest.TestCase):
    def test_market_session_methods_are_defined_on_public_facade(self):
        for name in (
            "_entry_cutoff_minutes", "_entry_hard_cutoff_msk", "_market_session_status",
            "_reserve_order_size",
        ):
            self.assertEqual(
                getattr(BaseExecutionEngine, name).__qualname__.split(".", 1)[0],
                "BaseExecutionEngine",
            )

    def test_execution_facade_retains_patchable_legacy_dependencies(self):
        import execution.execution_layer as module
        with patch("execution.execution_layer.FINAM_TRADE_LOT_SIZES", {"TEST": 1}):
            self.assertEqual(module.FINAM_TRADE_LOT_SIZES, {"TEST": 1})


if __name__ == "__main__":
    unittest.main()
