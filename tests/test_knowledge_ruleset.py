from __future__ import annotations

import json
import unittest
from pathlib import Path

from knowledge_parser import TechnicalKnowledgeParser


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RULES_PATH = PROJECT_ROOT / "data" / "knowledge" / "ai_market_knowledge.json"


class KnowledgeRulesetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.rules = json.loads(RULES_PATH.read_text(encoding="utf-8"))
        cls.by_id = {item["rule_id"]: item for item in cls.rules}

    def test_active_knowledge_contains_only_enabled_rules(self):
        self.assertTrue(self.rules)
        self.assertTrue(all(item.get("enabled") is True for item in self.rules))
        self.assertEqual(len(self.rules), 11)

    def test_new_trade_flow_rules_are_measurable_and_directional(self):
        for rule_id, side in (
            ("BUY_TRADE_FLOW_MOMENTUM", "BUY"),
            ("SELL_TRADE_FLOW_MOMENTUM", "SELL"),
            ("BUY_M1_TREND_CONTINUATION", "BUY"),
            ("SELL_M1_TREND_CONTINUATION", "SELL"),
        ):
            rule = self.by_id[rule_id]
            self.assertEqual(rule["direction"], side)
            self.assertTrue(rule["conditions"])
            self.assertTrue(rule["rules"])

    def test_buy_trade_flow_rule_matches_only_complete_evidence(self):
        rule = self.by_id["BUY_TRADE_FLOW_MOMENTUM"]
        evidence = {
            "trade_data_ready": 1,
            "trade_flow_imbalance_30s": 0.2,
            "price_change_pct": 0.1,
            "price_vs_vwap_pct": 0.05,
            "volume_spike": 2.0,
            "spread_available": 1,
            "spread_pct": 0.1,
        }
        self.assertTrue(TechnicalKnowledgeParser._conditions_match(rule["conditions"], evidence))
        evidence["trade_data_ready"] = 0
        self.assertFalse(TechnicalKnowledgeParser._conditions_match(rule["conditions"], evidence))


if __name__ == "__main__":
    unittest.main()
