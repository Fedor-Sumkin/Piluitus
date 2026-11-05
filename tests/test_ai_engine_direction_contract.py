"""Tests that the LLM prompt cannot invert the locally selected candidate side."""
from __future__ import annotations

import unittest

from ai_engine import AIEngine


class AIEngineDirectionContractTests(unittest.TestCase):
    def _prompt(self, direction: str) -> str:
        return AIEngine._build_live_prompt(
            ticker="WUSH",
            price=38.0,
            volume=100,
            candidate_direction=direction,
            metrics={},
            rag_context="",
            max_chars=980,
            include_rule_text=False,
        )

    def test_buy_candidate_allows_only_buy_or_hold(self) -> None:
        prompt = self._prompt("BUY")
        self.assertIn("Candidate side is binding", prompt)
        self.assertIn("BUY candidate allows BUY or HOLD only", prompt)

    def test_sell_candidate_allows_only_sell_or_hold(self) -> None:
        prompt = self._prompt("SELL")
        self.assertIn("Candidate side is binding", prompt)
        self.assertIn("SELL candidate allows SELL or HOLD only", prompt)

    def test_missing_candidate_requires_hold(self) -> None:
        prompt = self._prompt("NONE")
        self.assertIn("NONE requires HOLD", prompt)


if __name__ == "__main__":
    unittest.main()
