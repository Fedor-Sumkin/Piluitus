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
        screener.min_selected_hold_sec = 0.0
        screener.replacement_confirmations = 1
        screener.universe = ["A", "B", "C", "D", "E"]
        screener.selection_ready = True
        screener.retention_score_gap = 15.0
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

    def test_score_only_replacement_requires_confirmation_and_minimum_hold(self) -> None:
        screener = HFTMemoryScreener(top_n=2, min_samples=3, minimum_coverage=0.1)
        screener.universe = ["A", "B", "C"]
        screener.selection_ready = True
        screener.min_selected_hold_sec = 60.0
        screener.replacement_confirmations = 2
        screener.max_replacements_per_refresh = 1
        features = {
            "A": self._feature(10.0),
            "B": self._feature(9.0),
            "C": self._feature(1.0),
        }
        screener._features_for = lambda ticker, now: features.get(ticker)
        screener._refresh_selection(100.0)
        self.assertEqual(set(screener.selected_tickers), {"A", "B"})

        features["C"] = self._feature(20.0)
        screener._refresh_selection(130.0)
        self.assertEqual(set(screener.selected_tickers), {"A", "B"})
        screener._refresh_selection(160.0)
        self.assertEqual(set(screener.selected_tickers), {"A", "C"})

    def test_temporary_quote_coverage_dip_retains_only_still_eligible_members(self) -> None:
        screener = HFTMemoryScreener(top_n=3, min_samples=3, minimum_coverage=1.0)
        screener.universe = ["A", "B", "C", "D"]
        screener.selection_ready = True
        features = {
            "A": self._feature(10.0),
            "B": self._feature(9.0),
            "C": self._feature(8.0),
            "D": self._feature(7.0),
        }
        screener._features_for = lambda ticker, now: features.get(ticker)
        screener._refresh_selection(10.0)
        self.assertEqual(set(screener.selected_tickers), {"A", "B", "C"})

        features.pop("C")
        features.pop("D")
        screener._refresh_selection(20.0)
        self.assertFalse(screener.selection_ready)
        self.assertEqual(set(screener.selected_tickers), {"A", "B"})

        # A partial recovery below the 100% coverage threshold must not flush
        # the still-eligible cohort or trigger another subscription reset.
        features["C"] = self._feature(8.0)
        screener._refresh_selection(30.0)
        self.assertFalse(screener.selection_ready)
        self.assertEqual(set(screener.selected_tickers), {"A", "B"})

        features["D"] = self._feature(7.0)
        screener._refresh_selection(40.0)
        self.assertTrue(screener.selection_ready)
        self.assertEqual(set(screener.selected_tickers), {"A", "B", "C"})


if __name__ == "__main__":
    unittest.main()
