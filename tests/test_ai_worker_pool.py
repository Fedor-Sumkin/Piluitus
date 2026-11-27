import unittest

from ai.worker_pool import effective_ai_worker_count


class AIWorkerPoolSizingTests(unittest.TestCase):
    def test_default_pool_is_three_workers(self):
        self.assertEqual(effective_ai_worker_count(), 3)

    def test_pool_never_exceeds_vllm_transport_concurrency(self):
        self.assertEqual(effective_ai_worker_count(4, 2), 2)
        self.assertEqual(effective_ai_worker_count(2, 4), 2)

    def test_configured_count_is_clamped_to_safe_bounds(self):
        self.assertEqual(effective_ai_worker_count(99, 16), 4)
        self.assertEqual(effective_ai_worker_count(0, 4), 1)

    def test_invalid_values_use_safe_defaults(self):
        self.assertEqual(effective_ai_worker_count("invalid", 4), 3)
        self.assertEqual(effective_ai_worker_count(4, "invalid"), 4)
        self.assertEqual(effective_ai_worker_count(True, 4), 3)


if __name__ == "__main__":
    unittest.main()
