from __future__ import annotations

import time
import unittest
from collections import deque

from screener_funnel import HFTMemoryScreener


class TradeFlowMetricsTests(unittest.TestCase):
    def test_rolling_vwap_volume_spike_and_imbalance_use_unique_trade_events(self):
        state = {"trade_events": deque([
            (940.0, 99.0, 10.0, "BUY"), (950.0, 100.0, 10.0, "BUY"), (960.0, 101.0, 10.0, "BUY"),
            (970.0, 102.0, 10.0, "SELL"), (980.0, 103.0, 10.0, "BUY"),
            (990.0, 104.0, 10.0, "BUY"), (999.0, 105.0, 50.0, "BUY"),
        ])}
        metrics = HFTMemoryScreener._trade_metrics_locked(state, 1000.0)
        self.assertEqual(metrics["trade_data_ready"], 1.0)
        self.assertGreaterEqual(metrics["trade_history_coverage_sec"], 60.0)
        self.assertGreater(metrics["volume_spike"], 1.5)
        self.assertGreater(metrics["price_change_pct"], 0.0)
        self.assertGreater(metrics["trade_flow_imbalance_30s"], 0.0)
        self.assertGreater(metrics["trade_vwap_60s"], 100.0)
        self.assertLess(metrics["trade_vwap_60s"], 105.0)

    def test_trade_flow_reset_does_not_erase_quote_driven_m1_history(self):
        screener = HFTMemoryScreener()
        engine = screener.minute_bar_patterns
        for minute in range(7):
            engine.update("SBER", 100.0 + minute, now=1.0 + minute * 60.0)
        self.assertEqual(len(engine._states["SBER"]["bars"]), 6)

        now = time.monotonic()
        screener.push_trade("SBER", 107.0, 10.0, "trade-1", "SIDE_BUY", received_monotonic=now)
        self.assertEqual(len(screener.history["SBER"]["trade_events"]), 1)
        screener.reset_trade_history("SBER")
        self.assertEqual(len(screener.history["SBER"]["trade_events"]), 0)
        self.assertEqual(screener.quotes["SBER"]["trade_metrics"], {})
        self.assertEqual(len(engine._states["SBER"]["bars"]), 6)

    def test_subscription_rotation_preserves_closed_and_partial_quote_m1_bars(self):
        screener = HFTMemoryScreener()
        screener.register_ticker("SBER")
        engine = screener.minute_bar_patterns
        for minute in range(7):
            engine.update("SBER", 100.0 + minute, now=1.0 + minute * 60.0)
        self.assertEqual(engine.get_atr_snapshot("SBER")["periods"], 5.0)
        self.assertEqual(len(engine._states["SBER"]["bars"]), 6)

        screener.history["SBER"]["trade_events"].append((1000.0, 106.0, 5.0, "BUY"))
        screener.reset_trade_subscription("SBER")

        state = engine._states["SBER"]
        self.assertEqual(len(state["bars"]), 6)
        self.assertIsNotNone(state["current"])
        self.assertEqual(len(screener.history["SBER"]["trade_events"]), 0)

        # A returning ticker cannot reuse an old ATR as if its M1 feed were fresh.
        metrics = engine.update("SBER", 110.0, now=601.0)
        self.assertGreater(metrics["bar_atr_age_sec"], 120.0)
        from strategy.signal_guard import validate_atr_readiness
        ready, reason = validate_atr_readiness(metrics)
        self.assertFalse(ready)
        self.assertIn("История ATR устарела", reason)

    def test_quote_ticks_build_m1_for_all_symbols_in_trade_data_mode(self):
        screener = HFTMemoryScreener()
        screener.enable_trade_data_mode()
        for minute in range(7):
            screener.push_tick(
                "SBER", 100.0 + minute, 999999.0,
                event_timestamp=1_800_000_000 + minute * 60,
            )
        snapshot = screener.minute_bar_patterns.get_atr_snapshot("SBER")
        self.assertEqual(snapshot["periods"], 5.0)
        self.assertEqual(len(screener.history["SBER"]["trade_events"]), 0)
        self.assertEqual(float(screener.history["SBER"]["volumes"].sum()), 0.0)

    def test_atr_age_advances_with_monotonic_time_even_without_new_closed_bar(self):
        from strategy.signal_guard import validate_atr_readiness

        engine = HFTMemoryScreener().minute_bar_patterns
        for minute in range(7):
            metrics = engine.update("SBER", 100.0 + minute, now=1.0 + minute * 60.0)
        metrics = engine.update("SBER", 107.0, now=600.0)
        self.assertGreater(metrics["bar_atr_age_sec"], 120.0)
        ready, reason = validate_atr_readiness(metrics)
        self.assertFalse(ready)
        self.assertIn("История ATR устарела", reason)

    def test_push_trade_deduplicates_exchange_trade_id(self):
        screener = HFTMemoryScreener()
        now = time.monotonic()
        first = screener.push_trade("SBER", 100.0, 10.0, "trade-1", "SIDE_BUY", received_monotonic=now)
        second = screener.push_trade("SBER", 100.0, 10.0, "trade-1", "SIDE_BUY", received_monotonic=now)
        self.assertTrue(first["accepted"])
        self.assertFalse(second["accepted"])
        self.assertEqual(second["reason"], "duplicate_trade")


if __name__ == "__main__":
    unittest.main()
