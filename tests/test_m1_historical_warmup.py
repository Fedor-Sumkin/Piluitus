import unittest

from strategy.minute_bar_patterns import MinuteBarPatternEngine
from strategy.signal_guard import validate_atr_readiness


class HistoricalM1WarmupTests(unittest.TestCase):
    def _bars(self, first_bucket, last_bucket):
        return [
            {
                "timestamp": bucket * 60,
                "open": 100.0 + bucket * 0.01,
                "high": 100.3 + bucket * 0.01,
                "low": 99.9 + bucket * 0.01,
                "close": 100.2 + bucket * 0.01,
                "volume": 1000,
            }
            for bucket in range(first_bucket, last_bucket + 1)
        ]

    def test_imports_only_fully_closed_bars_and_warms_atr(self):
        engine = MinuteBarPatternEngine()
        base_bucket = int(1_800_000_000 // 60)
        now_epoch = base_bucket * 60 + 20
        imported = engine.load_closed_bars(
            "SBER", self._bars(base_bucket - 10, base_bucket), now=500.0, now_epoch=now_epoch
        )
        snapshot = engine.get_atr_snapshot("SBER")
        self.assertEqual(imported, 10)
        self.assertEqual(snapshot["bars"], 10.0)
        self.assertEqual(snapshot["periods"], 9.0)
        self.assertGreater(snapshot["atr"], 0.0)
        self.assertEqual(engine._states["SBER"]["bars"][-1].bucket, base_bucket - 1)

        metrics = engine._summarize(engine._states["SBER"], 106.0, 1000.0)
        ready, reason = validate_atr_readiness(metrics)
        self.assertTrue(ready, reason)

    def test_gaps_are_not_synthesized_and_break_atr_contiguous_suffix(self):
        engine = MinuteBarPatternEngine()
        base_bucket = int(1_800_000_000 // 60)
        now_epoch = base_bucket * 60 + 20
        bars = self._bars(base_bucket - 20, base_bucket - 15) + self._bars(base_bucket - 10, base_bucket - 1)
        imported = engine.load_closed_bars(
            "SBER", bars, now=500.0, now_epoch=now_epoch
        )
        snapshot = engine.get_atr_snapshot("SBER")
        self.assertEqual(imported, 16)
        self.assertEqual(snapshot["periods"], 9.0)
        self.assertEqual(snapshot["bars"], 10.0)

    def test_current_minute_and_invalid_ohlc_are_excluded(self):
        engine = MinuteBarPatternEngine()
        base_bucket = int(1_800_000_000 // 60)
        bars = self._bars(base_bucket - 10, base_bucket - 1)
        bars.append({
            "timestamp": base_bucket * 60, "open": 100, "high": 99,
            "low": 101, "close": 100, "volume": 100,
        })
        imported = engine.load_closed_bars(
            "SBER", bars, now=500.0, now_epoch=base_bucket * 60 + 10
        )
        self.assertEqual(imported, 9)
        self.assertEqual(engine._states["SBER"]["bars"][-1].bucket, base_bucket - 1)

    def test_old_history_remains_unready_due_to_freshness_guard(self):
        engine = MinuteBarPatternEngine()
        base_bucket = int(1_800_000_000 // 60)
        imported = engine.load_closed_bars(
            "SBER", self._bars(base_bucket - 10, base_bucket - 1), now=1000.0,
            now_epoch=base_bucket * 60 + 600,
        )
        self.assertEqual(imported, 10)
        metrics = engine._summarize(engine._states["SBER"], 106.0, 500.0)
        ready, reason = validate_atr_readiness(metrics)
        self.assertFalse(ready)
        self.assertIn("История ATR устарела", reason)


if __name__ == "__main__":
    unittest.main()
