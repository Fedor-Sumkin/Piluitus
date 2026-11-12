"""Regression tests for same-ticker pending BBO candidate coalescing."""
from __future__ import annotations

import unittest

from strategy.candidate_recovery import coalesce_pending_bbo_entry


class PendingBBOCoalescingTests(unittest.TestCase):
    def test_same_direction_refreshes_payload_without_restarting_ttl(self):
        previous = {
            "created_at": 10.0,
            "payload": {
                "candidate_direction": "BUY",
                "metrics": {"bid": 100.0},
            },
        }
        entry, status = coalesce_pending_bbo_entry(
            previous,
            {"candidate_direction": "BUY", "metrics": {"bid": 101.0}},
            now=10.8,
            ttl_sec=1.5,
        )

        self.assertEqual(status, "coalesced")
        self.assertEqual(entry["created_at"], 10.0)
        self.assertEqual(entry["payload"]["metrics"]["bid"], 101.0)
        self.assertEqual(previous["payload"]["metrics"]["bid"], 100.0)

    def test_direction_change_supersedes_existing_candidate(self):
        previous = {
            "created_at": 10.0,
            "payload": {"candidate_direction": "BUY"},
        }
        entry, status = coalesce_pending_bbo_entry(
            previous,
            {"candidate_direction": "SELL"},
            now=10.5,
            ttl_sec=1.5,
        )

        self.assertEqual(status, "superseded")
        self.assertEqual(entry["created_at"], 10.5)
        self.assertEqual(entry["payload"]["candidate_direction"], "SELL")

    def test_expired_candidate_is_replaced_with_new_ttl(self):
        previous = {
            "created_at": 10.0,
            "payload": {"candidate_direction": "BUY"},
        }
        entry, status = coalesce_pending_bbo_entry(
            previous,
            {"candidate_direction": "BUY"},
            now=11.6,
            ttl_sec=1.5,
        )

        self.assertEqual(status, "expired")
        self.assertEqual(entry["created_at"], 11.6)

    def test_missing_previous_candidate_creates_new_waiter(self):
        entry, status = coalesce_pending_bbo_entry(
            None,
            {"candidate_direction": "SELL"},
            now=20.0,
            ttl_sec=1.5,
        )

        self.assertEqual(status, "created")
        self.assertEqual(entry["created_at"], 20.0)
        self.assertEqual(entry["payload"]["candidate_direction"], "SELL")


if __name__ == "__main__":
    unittest.main()
