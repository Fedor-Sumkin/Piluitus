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

    def test_size_guard_reports_baseline_current_history_and_age(self) -> None:
        ok, reason = validate_signal_evidence("SELL", self.metrics(0.0))

        self.assertFalse(ok)
        self.assertIn("0.0%", reason)
        self.assertIn("текущий размер=100.00", reason)
        self.assertIn("медиана предыдущих=100.00", reason)
        self.assertIn("история=4", reason)
        self.assertIn("возраст сигнала=0.25s", reason)

    def test_recent_depletion_at_threshold_passes_size_guard(self) -> None:
        ok, reason = validate_signal_evidence("SELL", self.metrics(50.0))

        self.assertTrue(ok, reason)

    def test_below_threshold_is_not_allowed_even_with_fresh_history(self) -> None:
        ok, reason = validate_signal_evidence("SELL", self.metrics(49.9))

        self.assertFalse(ok)
        self.assertIn("49.9%", reason)


if __name__ == "__main__":
    unittest.main()
