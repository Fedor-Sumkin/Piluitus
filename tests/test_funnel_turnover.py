"""Regression tests for bounded score-only Top-N turnover."""
from __future__ import annotations

import unittest

from screener_funnel import HFTMemoryScreener


class FunnelTurnoverTests(unittest.TestCase):
    @staticmethod
    def _feature(volatility: float) -> dict[str, float]:
        return {
            "volatility_pct": volatility,
            "momentum_pct": 1.0,
            "volume_spike": 1.0,
            "liquidity_measure": 100.0,
            "tick_activity_30s": 10.0,
            "spread_pct": 0.1,
        }

    def test_score_only_rotation_is_capped_after_initial_population(self) -> None:
        screener = HFTMemoryScreener(
            top_n=3,
            min_samples=3,
            warmup_timeout=5.0,
            minimum_coverage=0.1,
        )
        screener.universe = ["A", "B", "C", "D", "E"]
        screener.selection_ready = True
        features = {
            "A": self._feature(10.0),
            "B": self._feature(9.0),
            "C": self._feature(8.0),
            "D": self._feature(1.0),
            "E": self._feature(0.0),
        }
        screener._features_for = lambda ticker, now: features.get(ticker)
        screener._refresh_selection(10.0)
        self.assertEqual(set(screener.selected_tickers), {"A", "B", "C"})

        features["D"] = self._feature(20.0)
        features["E"] = self._feature(19.0)
        screener._refresh_selection(15.0)

        self.assertEqual(len(screener.selected_tickers), 3)
        self.assertEqual(len(set(screener.selected_tickers) - {"A", "B", "C"}), 1)


if __name__ == "__main__":
    unittest.main()
