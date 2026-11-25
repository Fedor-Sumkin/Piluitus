from __future__ import annotations

import unittest
from unittest.mock import patch

from strategy.signal_guard import validate_entry_evidence


def buy_metrics() -> dict[str, float]:
    return {
        "price_change_pct": 0.20,
        "price_vs_vwap_pct": 0.12,
        "volume_spike": 2.0,
        "spread_pct": 0.08,
        "spread_available": 1.0,
        "bid_ask_available": 1.0,
        "bid_quote_age_sec": 0.2,
        "ask_quote_age_sec": 0.2,
        "bid": 99.9,
        "ask": 100.0,
        "order_book_valid": 1.0,
        "order_book_age_sec": 0.2,
        "bbo_max_age_sec": 2.0,
        "bar_atr_pct": 0.40,
        "trade_data_ready": 1.0,
        "trade_metrics_age_sec": 0.5,
        "trade_flow_imbalance_30s": 0.25,
    }


class PaperBuyQualityGateTests(unittest.TestCase):
    def _validate(self, metrics: dict[str, float], *, enabled: str = "true"):
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_BUY_STRICT_GATE": enabled,
            "PILUITUS_PAPER_BUY_MIN_FLOW_IMBALANCE": "0.10",
            "PILUITUS_PAPER_COMMISSION_PCT": "0.0354",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "0",
            "PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO": "0.50",
        }, clear=False):
            return validate_entry_evidence("BUY", metrics, paper_mode=True)

    def test_buy_requires_price_and_vwap_to_be_directionally_aligned(self) -> None:
        metrics = buy_metrics()
        metrics["price_vs_vwap_pct"] = 0.01

        valid, reason = self._validate(metrics)

        self.assertFalse(valid)
        self.assertIn("PAPER BUY требует полного согласования", reason)

    def test_buy_requires_fresh_positive_aggressor_flow(self) -> None:
        metrics = buy_metrics()
        metrics["trade_flow_imbalance_30s"] = 0.05

        valid, reason = self._validate(metrics)

        self.assertFalse(valid)
        self.assertIn("агрессивного потока покупателей", reason)
        self.assertIn("требуется >= 0.10", reason)

    def test_buy_blocks_stale_aggressor_flow(self) -> None:
        metrics = buy_metrics()
        metrics["trade_metrics_age_sec"] = 5.1

        valid, reason = self._validate(metrics)

        self.assertFalse(valid)
        self.assertIn("поток сделок устарел", reason)

    def test_buy_passes_with_aligned_direction_and_fresh_buyer_flow(self) -> None:
        valid, reason = self._validate(buy_metrics())

        self.assertTrue(valid, reason)
        self.assertIn("PAPER BUY прошёл строгую проверку", reason)

    def test_buy_strict_gate_can_be_disabled_for_controlled_ab_test(self) -> None:
        metrics = buy_metrics()
        metrics["price_vs_vwap_pct"] = 0.01
        metrics["trade_flow_imbalance_30s"] = 0.0

        valid, reason = self._validate(metrics, enabled="false")

        self.assertTrue(valid, reason)

    def test_live_buy_path_is_not_changed_by_paper_directional_gate(self) -> None:
        metrics = buy_metrics()
        metrics["price_vs_vwap_pct"] = 0.01
        metrics["trade_flow_imbalance_30s"] = -0.25

        with patch.dict("os.environ", {
            "PILUITUS_PAPER_BUY_STRICT_GATE": "true",
        }, clear=False):
            valid, reason = validate_entry_evidence("BUY", metrics, paper_mode=False)

        self.assertTrue(valid, reason)


if __name__ == "__main__":
    unittest.main()
