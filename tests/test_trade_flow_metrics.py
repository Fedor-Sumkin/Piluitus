from __future__ import annotations

import time
import unittest
from collections import deque

from screener_funnel import HFTMemoryScreener


class TradeFlowMetricsTests(unittest.TestCase):
    def test_rolling_vwap_volume_spike_and_imbalance_use_unique_trade_events(self):
        state = {"trade_events": deque([
            (950.0, 100.0, 10.0, "BUY"), (960.0, 101.0, 10.0, "BUY"),
            (970.0, 102.0, 10.0, "SELL"), (980.0, 103.0, 10.0, "BUY"),
            (990.0, 104.0, 10.0, "BUY"), (999.0, 105.0, 50.0, "BUY"),
        ])}
        metrics = HFTMemoryScreener._trade_metrics_locked(state, 1000.0)
        self.assertEqual(metrics["trade_data_ready"], 1.0)
        self.assertGreater(metrics["volume_spike"], 1.5)
        self.assertGreater(metrics["price_change_pct"], 0.0)
        self.assertGreater(metrics["trade_flow_imbalance_30s"], 0.0)
        self.assertGreater(metrics["trade_vwap_60s"], 100.0)
        self.assertLess(metrics["trade_vwap_60s"], 105.0)

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
