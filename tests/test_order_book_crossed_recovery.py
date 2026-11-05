import unittest

from finam_client import _crossed_bbo_recovery_due


class CrossedBookRecoveryTests(unittest.TestCase):
    def test_waits_for_persistence_threshold(self):
        self.assertFalse(_crossed_bbo_recovery_due(104.9, 100.0, 0.0, 5.0, 60.0))
        self.assertTrue(_crossed_bbo_recovery_due(105.0, 100.0, 0.0, 5.0, 60.0))

    def test_respects_per_symbol_cooldown(self):
        self.assertFalse(_crossed_bbo_recovery_due(159.9, 150.0, 100.0, 5.0, 60.0))
        self.assertTrue(_crossed_bbo_recovery_due(160.0, 150.0, 100.0, 5.0, 60.0))

    def test_rejects_non_monotonic_inputs(self):
        self.assertFalse(_crossed_bbo_recovery_due(99.0, 100.0, 0.0, 5.0, 60.0))
        self.assertFalse(_crossed_bbo_recovery_due(105.0, 100.0, 106.0, 5.0, 60.0))


if __name__ == "__main__":
    unittest.main()
