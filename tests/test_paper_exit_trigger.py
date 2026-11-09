import unittest

from execution_layer import BaseExecutionEngine


class PaperExitTriggerTests(unittest.TestCase):
    def setUp(self):
        self.engine = BaseExecutionEngine.__new__(BaseExecutionEngine)

    def _trade(self, side="BUY"):
        if side == "BUY":
            return {
                "id": "PAPER_TEST",
                "status": "OPEN",
                "side": "BUY",
                "entry_price": 100.0,
                "stop_loss": 99.0,
                "take_profit": 102.0,
            }
        return {
            "id": "PAPER_TEST",
            "status": "OPEN",
            "side": "SELL",
            "entry_price": 100.0,
            "stop_loss": 101.0,
            "take_profit": 98.0,
        }

    def test_long_stop_trigger_is_latched_from_last_trade(self):
        trade = self._trade("BUY")
        reason = self.engine._capture_paper_exit_trigger(
            "TEST", trade, 98.75, source="LAST_TRADE",
            market_timestamp="2026-11-09T12:00:00+03:00",
        )

        self.assertEqual(reason, "STOP_LOSS")
        self.assertEqual(trade["pending_exit_reason"], "STOP_LOSS")
        self.assertEqual(trade["pending_exit_trigger_price"], 99.0)
        self.assertEqual(trade["exit_trigger_source"], "LAST_TRADE")
        self.assertEqual(trade["exit_trigger_observed_price"], 98.75)
        self.assertEqual(
            trade["exit_trigger_market_timestamp"],
            "2026-11-09T12:00:00+03:00",
        )
        self.assertGreater(trade["exit_trigger_monotonic"], 0.0)

    def test_short_take_profit_trigger_is_latched(self):
        trade = self._trade("SELL")
        reason = self.engine._capture_paper_exit_trigger(
            "TEST", trade, 97.5, source="LAST_TRADE"
        )

        self.assertEqual(reason, "TAKE_PROFIT")
        self.assertEqual(trade["pending_exit_trigger_price"], 98.0)

    def test_first_trigger_is_not_overwritten_by_later_price(self):
        trade = self._trade("BUY")
        first = self.engine._capture_paper_exit_trigger(
            "TEST", trade, 98.5, source="LAST_TRADE"
        )
        first_timestamp = trade["exit_trigger_timestamp"]
        second = self.engine._capture_paper_exit_trigger(
            "TEST", trade, 103.0, source="EXECUTABLE_BBO"
        )

        self.assertEqual(first, "STOP_LOSS")
        self.assertEqual(second, "STOP_LOSS")
        self.assertEqual(trade["exit_trigger_timestamp"], first_timestamp)
        self.assertEqual(trade["exit_trigger_source"], "LAST_TRADE")

    def test_price_inside_levels_does_not_create_trigger(self):
        trade = self._trade("BUY")
        reason = self.engine._capture_paper_exit_trigger(
            "TEST", trade, 100.5, source="LAST_TRADE"
        )

        self.assertEqual(reason, "")
        self.assertNotIn("pending_exit_reason", trade)


if __name__ == "__main__":
    unittest.main()
