from __future__ import annotations

import unittest

from rag_prompt_utils import normalise_rag_context, select_prompt_rules


class RagPromptTests(unittest.TestCase):
    def test_rag_list_is_normalized_without_decorative_prefix(self):
        context = normalise_rag_context(
            ["  - Паттерн: [RISK / RISK_PER_TRADE]: Risk rule", "  - Паттерн: [BUY / BUY_IMPULSE]: Buy rule"]
        )
        self.assertEqual(
            context,
            "[RISK / RISK_PER_TRADE]: Risk rule\n[BUY / BUY_IMPULSE]: Buy rule",
        )

    def test_prompt_rule_selection_keeps_risk_and_candidate_direction(self):
        context = normalise_rag_context(
            [
                "[RISK / RISK_PER_TRADE]: Risk <=0.5% per trade.",
                "[SELL / SELL_IMPULSE]: Sell rule.",
                "[BUY / BUY_IMPULSE]: Price up, above VWAP, ask size drop.",
            ]
        )
        prompt_rules = select_prompt_rules("BUY", context)
        self.assertIn("[RISK / RISK_PER_TRADE]", prompt_rules)
        self.assertIn("[BUY / BUY_IMPULSE]", prompt_rules)
        self.assertNotIn("[SELL / SELL_IMPULSE]", prompt_rules)

    def test_rule_text_is_bounded_without_removing_its_prefix(self):
        context = "[RISK / RISK_PER_TRADE]: " + ("very long risk condition " * 100)
        selected = select_prompt_rules("NONE", context, max_chars=80)
        self.assertLessEqual(len(selected), 80)
        self.assertTrue(selected.startswith("[RISK / RISK_PER_TRADE]"))


if __name__ == "__main__":
    unittest.main()
