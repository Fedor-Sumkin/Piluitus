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

    def test_buy_candidate_requests_approval_not_direction_choice(self) -> None:
        prompt = self._prompt("BUY")
        self.assertIn("final_decision (APPROVE/HOLD)", prompt)
        self.assertIn("candidate side is immutable", prompt)
        self.assertIn("You must NOT choose, change, invert, or output BUY/SELL", prompt)
        self.assertIn("local system has already selected BUY", prompt)

    def test_sell_candidate_requests_approval_not_direction_choice(self) -> None:
        prompt = self._prompt("SELL")
        self.assertIn("final_decision (APPROVE/HOLD)", prompt)
        self.assertIn("candidate side is immutable", prompt)
        self.assertIn("local system has already selected SELL", prompt)

    def test_missing_candidate_requires_hold(self) -> None:
        prompt = self._prompt("NONE")
        self.assertIn("NONE requires HOLD", prompt)


if __name__ == "__main__":
    unittest.main()
