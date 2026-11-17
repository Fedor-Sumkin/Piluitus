import unittest

from strategy.candidate_dedup import CandidateDeduplicator, candidate_is_expired


class CandidateDeduplicatorTests(unittest.TestCase):
    def test_same_side_and_pattern_are_suppressed_during_cooldown(self):
        dedup = CandidateDeduplicator(cooldown_sec=8.0)
        self.assertTrue(dedup.reserve("SBER", "BUY", "FLOW", now=100.0))
        dedup.release("SBER")
        self.assertFalse(dedup.reserve("SBER", "BUY", "FLOW", now=105.0))
        self.assertTrue(dedup.reserve("SBER", "BUY", "FLOW", now=109.0))

    def test_opposite_side_has_independent_fingerprint_after_release(self):
        dedup = CandidateDeduplicator(cooldown_sec=8.0)
        self.assertTrue(dedup.reserve("SBER", "BUY", "FLOW", now=100.0))
        dedup.release("SBER")
        self.assertTrue(dedup.reserve("SBER", "SELL", "FLOW", now=101.0))

    def test_pending_ticker_cannot_have_two_concurrent_candidates(self):
        dedup = CandidateDeduplicator(cooldown_sec=8.0)
        self.assertTrue(dedup.reserve("SBER", "BUY", "FLOW", now=100.0))
        self.assertFalse(dedup.reserve("SBER", "SELL", "FLOW", now=101.0))
        dedup.release("SBER")
        self.assertTrue(dedup.reserve("SBER", "SELL", "FLOW", now=101.0))

    def test_pattern_change_is_a_distinct_signal_fingerprint(self):
        dedup = CandidateDeduplicator(cooldown_sec=8.0)
        self.assertTrue(dedup.reserve("SBER", "BUY", "FLOW", now=100.0))
        dedup.release("SBER")
        self.assertTrue(dedup.reserve("SBER", "BUY", "M1_RANGE_BREAKOUT_BUY", now=101.0))

    def test_candidate_ttl_accepts_fresh_market_snapshot(self):
        self.assertFalse(candidate_is_expired(100.0, now=104.9, ttl_sec=5.0))

    def test_candidate_ttl_rejects_stale_market_snapshot(self):
        self.assertTrue(candidate_is_expired(100.0, now=105.1, ttl_sec=5.0))

    def test_candidate_ttl_rejects_signal_after_slow_llm_inference(self):
        # A slow model response must not authorize entry using expired market evidence.
        self.assertTrue(candidate_is_expired(100.0, now=113.322, ttl_sec=5.0))

    def test_candidate_ttl_rejects_invalid_or_future_timestamp(self):
        self.assertTrue(candidate_is_expired(None, now=101.0, ttl_sec=5.0))
        self.assertTrue(candidate_is_expired(102.0, now=101.0, ttl_sec=5.0))

    def test_clear_resets_pending_and_cooldown(self):
        dedup = CandidateDeduplicator(cooldown_sec=8.0)
        self.assertTrue(dedup.reserve("SBER", "BUY", "FLOW", now=100.0))
        dedup.clear()
        self.assertTrue(dedup.reserve("SBER", "BUY", "FLOW", now=101.0))


if __name__ == "__main__":
    unittest.main()
