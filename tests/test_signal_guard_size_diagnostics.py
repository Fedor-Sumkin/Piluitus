from __future__ import annotations

import unittest

from signal_guard import validate_signal_evidence


class SignalGuardSizeDiagnosticsTests(unittest.TestCase):
    @staticmethod
    def metrics(drop: float) -> dict:
        return {
            "price_change_pct": -0.20,
            "price_vs_vwap_pct": -0.10,
            "volume_spike": 2.0,
            "spread_pct": 0.10,
            "spread_available": 1.0,
            "bid_ask_available": 1.0,
            "bid_size_available": 1.0,
            "bid_size_drop_pct": drop,
            "bid_size_baseline": 100.0,
            "bid_size_current": 100.0 * (1.0 - drop / 100.0),
            "top_bid_history_samples": 4,
            "bid_size_drop_age_sec": 0.25,
        }

    def test_missing_size_depletion_is_diagnostic_not_a_hard_block(self) -> None:
        ok, reason = validate_signal_evidence("SELL", self.metrics(0.0))

        self.assertTrue(ok, reason)
        self.assertIn("разгрузка лучшего Bid не подтверждена", reason)
        self.assertIn("история=4", reason)
        self.assertIn("возраст=0.25s", reason)

    def test_diagnostic_reports_observed_drop_separately_from_confirmed_event(self) -> None:
        metrics = self.metrics(0.0)
        metrics["bid_size_baseline"] = 1762.0
        metrics["bid_size_current"] = 1162.0

        ok, reason = validate_signal_evidence("SELL", metrics)

        self.assertTrue(ok, reason)
        self.assertIn("наблюдаемое изменение размера 34.1%", reason)
        self.assertIn("подтверждено=0.0%", reason)

    def test_recent_depletion_at_threshold_passes_size_guard(self) -> None:
        ok, reason = validate_signal_evidence("SELL", self.metrics(50.0))

        self.assertTrue(ok, reason)

    def test_below_threshold_is_supporting_only_not_a_hard_block(self) -> None:
        ok, reason = validate_signal_evidence("SELL", self.metrics(49.9))

        self.assertTrue(ok, reason)
        self.assertIn("не подтверждена", reason)

    def test_missing_size_measurement_does_not_block_valid_directional_candidate(self) -> None:
        metrics = self.metrics(0.0)
        for key in (
            "bid_size_available", "bid_size_drop_pct", "bid_size_baseline",
            "bid_size_current", "top_bid_history_samples", "bid_size_drop_age_sec",
        ):
            metrics.pop(key, None)
        ok, reason = validate_signal_evidence("SELL", metrics)
        self.assertTrue(ok, reason)
        self.assertIn("наблюдение размера отсутствует", reason)

    def test_conflicting_fresh_m1_pattern_blocks_candidate(self) -> None:
        import time
        metrics = self.metrics(0.0)
        metrics.update({
            "bar_pattern_confirmed": 1.0,
            "bar_pattern_name": "M1_RANGE_BREAKOUT_BUY",
            "bar_breakout_up": 1.0,
            "bar_pattern_direction_code": 1.0,
            "_bar_pattern_signal_at_monotonic": time.monotonic() - 1.0,
        })
        ok, reason = validate_signal_evidence("SELL", metrics)
        self.assertFalse(ok)
        self.assertIn("не совпадает", reason)


if __name__ == "__main__":
    unittest.main()
