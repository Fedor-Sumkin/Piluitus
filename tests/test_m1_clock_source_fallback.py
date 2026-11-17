import unittest
from unittest.mock import patch

from strategy.minute_bar_patterns import MinuteBarPatternEngine


class M1ClockSourceFallbackTests(unittest.TestCase):
    @staticmethod
    def _history(first_bucket, last_bucket):
        return [
            {
                "timestamp": bucket * 60,
                "open": 100.0 + bucket * 0.01,
                "high": 100.3 + bucket * 0.01,
                "low": 99.9 + bucket * 0.01,
                "close": 100.2 + bucket * 0.01,
            }
            for bucket in range(first_bucket, last_bucket + 1)
        ]

    def test_missing_quote_timestamp_keeps_exchange_clock_and_warmed_atr(self):
        engine = MinuteBarPatternEngine()
        current_bucket = int(1_800_000_000 // 60)
        epoch_now = current_bucket * 60 + 20
        engine.load_closed_bars(
            "FEES", self._history(current_bucket - 10, current_bucket - 1),
            now=500.0, now_epoch=epoch_now,
        )
        before = engine.get_atr_snapshot("FEES")
        self.assertEqual(before["periods"], 9.0)

        with patch("strategy.minute_bar_patterns.time.time", return_value=epoch_now + 5):
            engine.update("FEES", 100.5, now=501.0, event_timestamp=None)
        after_missing_timestamp = engine.get_atr_snapshot("FEES")
        self.assertEqual(after_missing_timestamp["periods"], 9.0)
        self.assertEqual(engine._states["FEES"]["clock_source"], "exchange_fallback")

        engine.update(
            "FEES", 100.6, now=502.0,
            event_timestamp=epoch_now + 10,
        )
        after_exchange_timestamp = engine.get_atr_snapshot("FEES")
        self.assertEqual(after_exchange_timestamp["periods"], 9.0)
        self.assertGreater(after_exchange_timestamp["atr"], 0.0)

    def test_switch_from_monotonic_to_exchange_discards_incomparable_buckets(self):
        engine = MinuteBarPatternEngine()
        engine.update("SBER", 100.0, now=100.0, event_timestamp=None)
        engine.update("SBER", 100.1, now=161.0, event_timestamp=None)
        self.assertGreater(len(engine._states["SBER"]["bars"]), 0)

        engine.update("SBER", 100.2, now=162.0, event_timestamp=1_800_000_000)
        self.assertEqual(len(engine._states["SBER"]["bars"]), 0)
        self.assertEqual(engine._states["SBER"]["clock_source"], "exchange")


if __name__ == "__main__":
    unittest.main()
