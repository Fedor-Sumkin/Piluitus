from __future__ import annotations

import unittest

import candidate_recovery as legacy_candidate_recovery
from strategy import candidate_recovery
from strategy import candidate_bbo_recovery
from strategy import candidate_pending_bbo
from strategy import candidate_post_llm_refresh
from strategy.candidate_recovery_primitives import _finite


class CandidateRecoveryModuleTests(unittest.TestCase):
    def test_legacy_import_path_and_public_helpers_are_preserved(self):
        self.assertIs(legacy_candidate_recovery, candidate_recovery)
        self.assertIs(
            candidate_recovery.refresh_candidate_bbo_metrics,
            candidate_bbo_recovery.refresh_candidate_bbo_metrics,
        )
        self.assertIs(
            candidate_recovery.refresh_post_llm_evidence,
            candidate_post_llm_refresh.refresh_post_llm_evidence,
        )
        self.assertIs(
            candidate_recovery.coalesce_pending_bbo_entry,
            candidate_pending_bbo.coalesce_pending_bbo_entry,
        )

    def test_numeric_helper_and_private_compatibility_names_remain_available(self):
        self.assertEqual(_finite("12.5"), 12.5)
        self.assertEqual(_finite(float("inf"), -1.0), -1.0)
        self.assertIs(candidate_recovery._finite, _finite)
        self.assertIs(
            candidate_recovery._refresh_locked,
            candidate_bbo_recovery._refresh_locked,
        )
        self.assertIs(
            candidate_recovery._POST_LLM_TRADE_METRIC_FIELDS,
            candidate_post_llm_refresh._POST_LLM_TRADE_METRIC_FIELDS,
        )

    def test_monotonic_patch_seam_uses_one_shared_time_module(self):
        self.assertIs(candidate_recovery.time, candidate_bbo_recovery.time)
        self.assertIs(candidate_recovery.time, candidate_post_llm_refresh.time)

    def test_pending_candidate_coalescing_preserves_original_ttl(self):
        first_payload = {
            "candidate_direction": "BUY",
            "metrics": {"bid": 100.0},
        }
        first, status = candidate_recovery.coalesce_pending_bbo_entry(
            None, first_payload, now=100.0, ttl_sec=1.5
        )
        self.assertEqual(status, "created")
        self.assertEqual(first["created_at"], 100.0)
        self.assertIsNot(first["payload"], first_payload)
        self.assertIsNot(first["payload"]["metrics"], first_payload["metrics"])

        repeated, status = candidate_recovery.coalesce_pending_bbo_entry(
            first,
            {"candidate_direction": "BUY", "metrics": {"bid": 100.1}},
            now=101.0,
            ttl_sec=1.5,
        )
        self.assertEqual(status, "coalesced")
        self.assertEqual(repeated["created_at"], 100.0)

        changed, status = candidate_recovery.coalesce_pending_bbo_entry(
            repeated,
            {"candidate_direction": "SELL", "metrics": {"bid": 99.9}},
            now=101.1,
            ttl_sec=1.5,
        )
        self.assertEqual(status, "superseded")
        self.assertEqual(changed["created_at"], 101.1)


if __name__ == "__main__":
    unittest.main()
