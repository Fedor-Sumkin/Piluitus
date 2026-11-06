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
        self.assertIn("Never select, change, invert, or output BUY/SELL", prompt)
        self.assertIn("APPROVE permits only the exact candidate BUY/SELL; HOLD rejects.", prompt)
        self.assertIn("candidate=BUY", prompt)
        self.assertIn("BUY: approve only if positive change", prompt)
        self.assertLessEqual(len(prompt), 980)

    def test_sell_candidate_requests_approval_not_direction_choice(self) -> None:
        prompt = self._prompt("SELL")
        self.assertIn("final_decision (APPROVE/HOLD)", prompt)
        self.assertIn("Never select, change, invert, or output BUY/SELL", prompt)
        self.assertIn("APPROVE permits only the exact candidate BUY/SELL; HOLD rejects.", prompt)
        self.assertIn("candidate=SELL", prompt)
        self.assertIn("SELL: approve only if negative change", prompt)
        self.assertLessEqual(len(prompt), 980)

    def test_missing_candidate_requires_hold(self) -> None:
        prompt = self._prompt("NONE")
        self.assertIn("candidate=NONE => final_decision MUST be HOLD.", prompt)
        self.assertIn("No local direction: MUST HOLD.", prompt)
        self.assertLessEqual(len(prompt), 980)

    def test_contract_survives_metric_overflow_at_compact_limit(self) -> None:
        metrics = {
            key: "x" * 200
            for key in (
                "price_change_pct",
                "price_vs_vwap_pct",
                "volume_spike",
                "spread_pct",
                "bid_ask_available",
                "spread_available",
                "bar_pattern_confirmed",
                "bar_pattern_direction_code",
                "bar_breakout_up",
                "bar_pullback_buy",
                "bar_trend_up",
                "ask_size_drop_pct",
                "top_ask_history_samples",
                "ask_size_drop_age_sec",
                "bar_pattern_name",
                "bar_signal_gap_atr",
            )
        }
        prompt = AIEngine._build_live_prompt(
            ticker="WUSH",
            price=38.0,
            volume=100,
            candidate_direction="NONE",
            metrics=metrics,
            rag_context="",
            max_chars=980,
            include_rule_text=False,
        )
        self.assertIn("candidate=NONE => final_decision MUST be HOLD.", prompt)
        self.assertIn("No local direction: MUST HOLD.", prompt)
        self.assertLessEqual(len(prompt), 980)


if __name__ == "__main__":
    unittest.main()
