from __future__ import annotations

import unittest
from signal_guard import validate_signal_evidence


class TradeDataGuardTests(unittest.TestCase):
    def test_live_trade_mode_fails_closed_when_unique_trade_stream_is_not_ready(self):
        metrics = {"trade_data_mode": 1.0, "trade_data_ready": 0.0,
            "trade_metrics_age_sec": -1.0, "price_change_pct": 0.2,
            "price_vs_vwap_pct": 0.1, "volume_spike": 2.0,
            "spread_pct": 0.1, "spread_available": 1.0, "bid_ask_available": 1.0}
        ok, reason = validate_signal_evidence("BUY", metrics)
        self.assertFalse(ok)
        self.assertIn("INSTRUMENT_TRADES", reason)

    def test_live_trade_mode_blocks_stale_trade_metrics(self):
        metrics = {"trade_data_mode": 1.0, "trade_data_ready": 1.0,
            "trade_metrics_age_sec": 6.0, "price_change_pct": 0.2,
            "price_vs_vwap_pct": 0.1, "volume_spike": 2.0,
            "spread_pct": 0.1, "spread_available": 1.0, "bid_ask_available": 1.0}
        ok, reason = validate_signal_evidence("BUY", metrics)
        self.assertFalse(ok)
        self.assertIn("устарел", reason)


if __name__ == "__main__":
    unittest.main()
