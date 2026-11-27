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
        self.assertEqual(engine._states["SBER"]["bars"][-1].ticks, 0)

        metrics = engine._summarize(engine._states["SBER"], 106.0, 500.0)
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
        # A malformed but otherwise closed bar must be rejected independently
        # of the current-minute exclusion below.
        bars.append({
            "timestamp": (base_bucket - 11) * 60, "open": 100, "high": 99,
            "low": 101, "close": 100, "volume": 100,
        })
        bars.append({
            "timestamp": base_bucket * 60, "open": 100, "high": 99,
            "low": 101, "close": 100, "volume": 100,
        })
        imported = engine.load_closed_bars(
            "SBER", bars, now=500.0, now_epoch=base_bucket * 60 + 10
        )
        # Ten valid prior-minute bars are fully closed. The malformed row is
        # in the current minute and is excluded by the close-time guard.
        self.assertEqual(imported, 10)
        self.assertEqual(engine._states["SBER"]["bars"][-1].bucket, base_bucket - 1)

    def test_history_diagnostics_separate_new_bars_from_merged_history(self):
        engine = MinuteBarPatternEngine()
        base_bucket = int(1_800_000_000 // 60)
        now_epoch = base_bucket * 60 + 20
        first_count = engine.load_closed_bars(
            "SBER", self._bars(base_bucket - 10, base_bucket - 1),
            now=500.0, now_epoch=now_epoch,
        )
        self.assertEqual(first_count, 10)

        # Only three already-known bars are returned by the next provider call.
        # The retained history remains ten bars, but no bar is newly imported.
        merged_count = engine.load_closed_bars(
            "SBER", self._bars(base_bucket - 3, base_bucket - 1),
            now=501.0, now_epoch=now_epoch,
        )
        self.assertEqual(merged_count, 10)
        diagnostics = engine._states["SBER"]["last_history_load"]
        self.assertEqual(diagnostics["received_rows"], 3)
        self.assertEqual(diagnostics["valid_closed_unique"], 3)
        self.assertEqual(diagnostics["new_history_bars"], 0)
        self.assertEqual(diagnostics["merged_closed_total"], 10)
        self.assertEqual(diagnostics["atr_contiguous_ranges"], 9)
        self.assertEqual(diagnostics["atr_contiguous_bars"], 10)

    def test_old_history_remains_unready_due_to_freshness_guard(self):
        engine = MinuteBarPatternEngine()
        base_bucket = int(1_800_000_000 // 60)
        imported = engine.load_closed_bars(
            "SBER", self._bars(base_bucket - 10, base_bucket - 1), now=1000.0,
            now_epoch=base_bucket * 60 + 600,
        )
        self.assertEqual(imported, 10)
        metrics = engine._summarize(engine._states["SBER"], 106.0, 1000.0)
        ready, reason = validate_atr_readiness(metrics)
        self.assertFalse(ready)
        self.assertIn("История ATR устарела", reason)


if __name__ == "__main__":
    unittest.main()
