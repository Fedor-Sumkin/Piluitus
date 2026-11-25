from __future__ import annotations

import unittest
from unittest.mock import patch

from strategy.signal_guard import (
    validate_entry_evidence, validate_paper_sell_quality, validate_signal_evidence,
)


def sell_metrics() -> dict[str, float]:
    return {
        "price_change_pct": -0.20,
        "price_vs_vwap_pct": -0.12,
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
        "trade_flow_imbalance_30s": -0.25,
    }


def buy_metrics() -> dict[str, float]:
    metrics = sell_metrics()
    metrics["price_change_pct"] = 0.20
    metrics["price_vs_vwap_pct"] = 0.12
    metrics["trade_flow_imbalance_30s"] = 0.25
    return metrics


class PaperSellQualityGateTests(unittest.TestCase):
    def test_sell_passes_only_with_full_direction_and_fresh_seller_flow(self) -> None:
        with patch.dict("os.environ", {
            "PAPER_TRADING": "true",
            "PILUITUS_PAPER_SELL_STRICT_GATE": "true",
            "PILUITUS_PAPER_SELL_MIN_FLOW_IMBALANCE": "0.10",
        }, clear=False):
            valid, reason = validate_paper_sell_quality(sell_metrics())
        self.assertTrue(valid, reason)
        self.assertIn("строгую проверку", reason)

    def test_neutral_vwap_is_not_enough_for_paper_sell(self) -> None:
        metrics = sell_metrics()
        metrics["price_vs_vwap_pct"] = -0.01
        with patch.dict("os.environ", {
            "PAPER_TRADING": "true",
            "PILUITUS_PAPER_SELL_STRICT_GATE": "true",
        }, clear=False):
            valid, reason = validate_paper_sell_quality(metrics)
        self.assertFalse(valid)
        self.assertIn("полного согласования", reason)

    def test_weak_or_opposing_aggressor_flow_blocks_paper_sell(self) -> None:
        metrics = sell_metrics()
        metrics["trade_flow_imbalance_30s"] = 0.05
        with patch.dict("os.environ", {
            "PAPER_TRADING": "true",
            "PILUITUS_PAPER_SELL_STRICT_GATE": "true",
        }, clear=False):
            valid, reason = validate_paper_sell_quality(metrics)
        self.assertFalse(valid)
        self.assertIn("агрессивного потока продавцов", reason)

    def test_stale_trade_flow_blocks_paper_sell(self) -> None:
        metrics = sell_metrics()
        metrics["trade_metrics_age_sec"] = 5.1
        with patch.dict("os.environ", {
            "PAPER_TRADING": "true",
            "PILUITUS_PAPER_SELL_STRICT_GATE": "true",
        }, clear=False):
            valid, reason = validate_paper_sell_quality(metrics)
        self.assertFalse(valid)
        self.assertIn("устарел", reason)

    def test_gate_does_not_change_live_or_explicitly_disabled_mode(self) -> None:
        with patch.dict("os.environ", {
            "PAPER_TRADING": "false",
            "PILUITUS_PAPER_SELL_STRICT_GATE": "true",
        }, clear=False):
            self.assertTrue(validate_paper_sell_quality({})[0])
        with patch.dict("os.environ", {
            "PAPER_TRADING": "true",
            "PILUITUS_PAPER_SELL_STRICT_GATE": "false",
        }, clear=False):
            self.assertTrue(validate_paper_sell_quality({})[0])

    def test_common_signal_validator_does_not_require_trade_flow_metrics(self) -> None:
        metrics = sell_metrics()
        metrics.pop("trade_data_ready")
        metrics.pop("trade_metrics_age_sec")
        metrics.pop("trade_flow_imbalance_30s")
        valid, reason = validate_signal_evidence("SELL", metrics)
        self.assertTrue(valid, reason)

    def test_strict_sell_gate_is_applied_by_paper_entry_validator(self) -> None:
        metrics = sell_metrics()
        metrics["trade_flow_imbalance_30s"] = 0.0
        with patch.dict("os.environ", {
            "PAPER_TRADING": "true",
            "PILUITUS_PAPER_SELL_STRICT_GATE": "true",
        }, clear=False):
            valid, reason = validate_entry_evidence("SELL", metrics, paper_mode=True)
        self.assertFalse(valid)
        self.assertIn("агрессивного потока продавцов", reason)

    def test_paper_buy_requires_aligned_positive_price_and_vwap(self) -> None:
        metrics = buy_metrics()
        metrics["price_vs_vwap_pct"] = 0.01
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_BUY_STRICT_GATE": "true",
            "PILUITUS_PAPER_BUY_MIN_FLOW_IMBALANCE": "0.10",
            "PILUITUS_PAPER_COMMISSION_PCT": "0.0354",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "0",
            "PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO": "0.50",
        }, clear=False):
            valid, reason = validate_entry_evidence("BUY", metrics, paper_mode=True)
        self.assertFalse(valid)
        self.assertIn("PAPER BUY требует полного согласования", reason)

    def test_paper_buy_requires_fresh_positive_aggressor_flow(self) -> None:
        metrics = buy_metrics()
        metrics["trade_flow_imbalance_30s"] = 0.05
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_BUY_STRICT_GATE": "true",
            "PILUITUS_PAPER_BUY_MIN_FLOW_IMBALANCE": "0.10",
            "PILUITUS_PAPER_COMMISSION_PCT": "0.0354",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "0",
            "PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO": "0.50",
        }, clear=False):
            valid, reason = validate_entry_evidence("BUY", metrics, paper_mode=True)
        self.assertFalse(valid)
        self.assertIn("агрессивного потока покупателей", reason)
        self.assertIn("требуется >= 0.10", reason)

    def test_paper_buy_blocks_stale_aggressor_flow(self) -> None:
        metrics = buy_metrics()
        metrics["trade_metrics_age_sec"] = 5.1
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_BUY_STRICT_GATE": "true",
            "PILUITUS_PAPER_COMMISSION_PCT": "0.0354",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "0",
            "PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO": "0.50",
        }, clear=False):
            valid, reason = validate_entry_evidence("BUY", metrics, paper_mode=True)
        self.assertFalse(valid)
        self.assertIn("поток сделок устарел", reason)

    def test_paper_buy_passes_with_aligned_price_vwap_and_fresh_buyer_flow(self) -> None:
        metrics = buy_metrics()
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_BUY_STRICT_GATE": "true",
            "PILUITUS_PAPER_BUY_MIN_FLOW_IMBALANCE": "0.10",
            "PILUITUS_PAPER_COMMISSION_PCT": "0.0354",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "0",
            "PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO": "0.50",
        }, clear=False):
            valid, reason = validate_entry_evidence("BUY", metrics, paper_mode=True)
        self.assertTrue(valid, reason)
        self.assertIn("PAPER BUY прошёл строгую проверку", reason)

    def test_paper_buy_strict_gate_can_be_disabled_for_controlled_ab_test(self) -> None:
        metrics = buy_metrics()
        metrics["price_vs_vwap_pct"] = 0.01
        metrics["trade_flow_imbalance_30s"] = 0.0
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_BUY_STRICT_GATE": "false",
            "PILUITUS_PAPER_COMMISSION_PCT": "0.0354",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "0",
            "PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO": "0.50",
        }, clear=False):
            valid, reason = validate_entry_evidence("BUY", metrics, paper_mode=True)
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

    def test_paper_entry_requires_a_confirmed_order_book(self) -> None:
        metrics = buy_metrics()
        metrics["order_book_valid"] = 0.0
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_COMMISSION_PCT": "0.0354",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "0",
            "PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO": "0.50",
        }, clear=False):
            valid, reason = validate_entry_evidence("BUY", metrics, paper_mode=True)
        self.assertFalse(valid)
        self.assertIn("ORDER_BOOK не подтверждён", reason)

    def test_paper_entry_rejects_stale_order_book(self) -> None:
        metrics = buy_metrics()
        metrics["order_book_age_sec"] = 2.5
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_COMMISSION_PCT": "0.0354",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "0",
            "PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO": "0.50",
        }, clear=False):
            valid, reason = validate_entry_evidence("BUY", metrics, paper_mode=True)
        self.assertFalse(valid)
        self.assertIn("стакан устарел", reason)

    def test_paper_entry_blocks_round_trip_costs_that_consume_too_much_atr(self) -> None:
        metrics = buy_metrics()
        metrics["bar_atr_pct"] = 0.15
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_COMMISSION_PCT": "0.0354",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "0",
            "PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO": "0.50",
        }, clear=False):
            valid, reason = validate_entry_evidence("BUY", metrics, paper_mode=True)
        self.assertFalse(valid)
        self.assertIn("round-trip издержки слишком велики", reason)

    def test_paper_entry_accepts_confirmed_book_when_known_costs_fit_atr(self) -> None:
        metrics = buy_metrics()
        with patch.dict("os.environ", {
            "PILUITUS_PAPER_COMMISSION_PCT": "0.0354",
            "PILUITUS_PAPER_SLIPPAGE_BPS": "0",
            "PILUITUS_MAX_ENTRY_COST_TO_ATR_RATIO": "0.50",
        }, clear=False):
            valid, reason = validate_entry_evidence("BUY", metrics, paper_mode=True)
        self.assertTrue(valid, reason)
        self.assertIn("cost/ATR=", reason)

    def test_paper_entry_validator_does_not_apply_sell_gate_outside_paper_mode(self) -> None:
        metrics = sell_metrics()
        metrics["trade_flow_imbalance_30s"] = 0.0
        with patch.dict("os.environ", {
            "PAPER_TRADING": "true",
            "PILUITUS_PAPER_SELL_STRICT_GATE": "true",
        }, clear=False):
            valid, reason = validate_entry_evidence("SELL", metrics, paper_mode=False)
        self.assertTrue(valid, reason)


if __name__ == "__main__":
    unittest.main()
