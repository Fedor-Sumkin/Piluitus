from __future__ import annotations

import unittest
from unittest.mock import patch

from minute_bar_patterns import MinuteBar, MinuteBarPatternEngine


class MinuteBarBreakoutConfirmationTests(unittest.TestCase):
    def _engine(self, extension: str = "0.10") -> MinuteBarPatternEngine:
        with patch.dict("os.environ", {"PILUITUS_M1_BREAKOUT_EXTENSION_ATR": extension}):
            return MinuteBarPatternEngine()

    def _bars(self) -> tuple[dict, MinuteBar]:
        # Five preceding closed bars define a 100.0 high and approximately 2.0 ATR.
        bars = [
            MinuteBar(bucket=i, open=99.0, high=100.0, low=98.0, close=99.5, ticks=4)
            for i in range(5)
        ]
        # A strong bullish close just 0.075 ATR beyond the range: it used to pass
        # the 0.05 ATR threshold but should fail the new 0.10 ATR confirmation.
        weak = MinuteBar(bucket=5, open=99.5, high=100.30, low=99.5, close=100.15, ticks=4)
        return {"bars": __import__("collections").deque(bars + [weak]), "last_closed_bar_monotonic": 100.0}, weak

    def test_breakout_needs_ten_percent_atr_extension_by_default(self) -> None:
        engine = self._engine()
        state, closed = self._bars()
        result = engine._pattern_for_closed_bar(state, closed, 100.15, 100.0)
        self.assertEqual(result["bar_breakout_up"], 0.0)
        self.assertEqual(result["bar_pattern_confirmed"], 0.0)
        self.assertAlmostEqual(result["bar_breakout_extension_atr"], 0.10)

    def test_stronger_breakout_is_still_detected(self) -> None:
        engine = self._engine()
        bars = [
            MinuteBar(bucket=i, open=99.0, high=100.0, low=98.0, close=99.5, ticks=4)
            for i in range(5)
        ]
        strong = MinuteBar(bucket=5, open=99.5, high=100.5, low=99.5, close=100.4, ticks=4)
        state = {"bars": __import__("collections").deque(bars + [strong]), "last_closed_bar_monotonic": 100.0}
        result = engine._pattern_for_closed_bar(state, strong, 100.4, 100.0)
        self.assertEqual(result["bar_breakout_up"], 1.0)
        self.assertEqual(result["bar_pattern_name"], "M1_RANGE_BREAKOUT_BUY")


if __name__ == "__main__":
    unittest.main()
