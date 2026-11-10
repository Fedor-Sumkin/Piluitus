from __future__ import annotations

import unittest
from tools.knowledge.merge_rules import merge_approved_rules


class MergeRulesSafetyTests(unittest.TestCase):
    def test_unapproved_or_disabled_rules_never_enter_runtime_knowledge(self):
        existing = [{"rule_id": "RISK", "enabled": True}]
        purified = [
            {"rule_id": "UNAPPROVED", "enabled": True, "conditions": [{"metric": "x", "op": "gt", "value": 1}], "rules": "x"},
            {"rule_id": "DISABLED", "enabled": False, "approved_for_runtime": True, "conditions": [{"metric": "x", "op": "gt", "value": 1}], "rules": "x"},
            {"rule_id": "UNMEASURABLE", "enabled": True, "approved_for_runtime": True, "conditions": [], "rules": "x"},
        ]
        merged, added = merge_approved_rules(existing, purified)
        self.assertEqual(added, 0)
        self.assertEqual([item["rule_id"] for item in merged], ["RISK"])

    def test_only_explicitly_approved_measurable_unique_rule_is_added(self):
        existing = [{"rule_id": "RISK", "enabled": True}]
        candidate = {"rule_id": "NEW", "enabled": True, "approved_for_runtime": True,
            "conditions": [{"metric": "volume_spike", "op": "gte", "value": 1.5}], "rules": "Measured rule"}
        merged, added = merge_approved_rules(existing, [candidate, candidate])
        self.assertEqual(added, 1)
        self.assertEqual([item["rule_id"] for item in merged], ["RISK", "NEW"])


if __name__ == "__main__":
    unittest.main()
