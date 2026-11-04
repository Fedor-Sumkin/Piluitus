import unittest
from risk_limits import validate_daily_loss_limit

class DailyLossLimitTests(unittest.TestCase):
    def test_valid_values(self):
        self.assertEqual(validate_daily_loss_limit(0.015), 0.015)
        self.assertEqual(validate_daily_loss_limit("0.025"), 0.025)
        self.assertEqual(validate_daily_loss_limit(0.10), 0.10)

    def test_rejects_out_of_range(self):
        for value in (0, -0.01, 0.100001, 1):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_daily_loss_limit(value)

    def test_rejects_non_finite(self):
        for value in (float("nan"), float("inf"), float("-inf"), "nan", "inf"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                validate_daily_loss_limit(value)

    def test_rejects_wrong_types(self):
        for value in (True, False, None, [], {}, object(), "abc"):
            with self.subTest(value=repr(value)), self.assertRaises(ValueError):
                validate_daily_loss_limit(value)

if __name__ == "__main__":
    unittest.main()
