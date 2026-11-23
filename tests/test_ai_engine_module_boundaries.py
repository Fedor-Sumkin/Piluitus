"""Import and composition contract for the modular AIEngine implementation."""
from __future__ import annotations

import unittest

from ai.ai_engine import AIEngine
from ai.ai_engine_prompt import AIEnginePromptMixin
from ai.ai_engine_response import AIEngineResponseMixin
from ai.ai_engine_transport import AIEngineTransportMixin


class AIEngineModuleBoundaryTests(unittest.TestCase):
    def test_public_engine_composes_all_responsibility_mixins(self):
        self.assertTrue(issubclass(AIEngine, AIEngineTransportMixin))
        self.assertTrue(issubclass(AIEngine, AIEngineResponseMixin))
        self.assertTrue(issubclass(AIEngine, AIEnginePromptMixin))

    def test_public_methods_remain_available_from_compatibility_path(self):
        for name in (
            "analyze_market_anomaly", "analyze_market_state", "_build_live_prompt",
            "_extract_json_object", "_post_with_retry", "close",
        ):
            self.assertTrue(callable(getattr(AIEngine, name)))


if __name__ == "__main__":
    unittest.main()
