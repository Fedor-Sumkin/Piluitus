import unittest

from execution_layer import BaseExecutionEngine


class PaperDepthFillTests(unittest.TestCase):
    def test_shares_unit_limits_fill_to_participation_and_lot_multiple(self):
        result = BaseExecutionEngine._simulate_paper_depth_fill(
            [[100.0, 20.0], [101.0, 20.0]],
            requested_lots=3,
            lot_size=10,
            size_unit="shares",
            max_participation_pct=50.0,
        )

        self.assertEqual(result["filled_lots"], 2)
        self.assertEqual(result["filled_shares"], 20)
        self.assertAlmostEqual(result["average_price"], 100.5)
        self.assertAlmostEqual(result["fill_ratio"], 2.0 / 3.0)
        self.assertEqual(result["levels_used"], 2)

    def test_lots_unit_is_converted_to_shares(self):
        result = BaseExecutionEngine._simulate_paper_depth_fill(
            [[100.0, 2.0], [101.0, 2.0]],
            requested_lots=3,
            lot_size=10,
            size_unit="lots",
            max_participation_pct=50.0,
        )

        self.assertEqual(result["filled_lots"], 2)
        self.assertEqual(result["filled_shares"], 20)
        self.assertAlmostEqual(result["average_price"], 100.5)

    def test_unknown_unit_keeps_depth_model_disabled(self):
        result = BaseExecutionEngine._simulate_paper_depth_fill(
            [[100.0, 1000.0]],
            requested_lots=1,
            lot_size=10,
            size_unit="unknown",
            max_participation_pct=10.0,
        )

        self.assertIsNone(result)

    def test_insufficient_depth_returns_zero_fill(self):
        result = BaseExecutionEngine._simulate_paper_depth_fill(
            [[100.0, 5.0]],
            requested_lots=1,
            lot_size=10,
            size_unit="shares",
            max_participation_pct=10.0,
        )

        self.assertEqual(result["filled_lots"], 0)
        self.assertEqual(result["filled_shares"], 0)


if __name__ == "__main__":
    unittest.main()
