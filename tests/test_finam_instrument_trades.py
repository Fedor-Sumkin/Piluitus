from __future__ import annotations

import unittest
from finam_client import FinamBrokerClient


class FinamInstrumentTradesTests(unittest.TestCase):
    def test_extracts_public_trade_stream_and_decimal_objects(self):
        envelope = {
            "type": "DATA", "subscription_type": "INSTRUMENT_TRADES",
            "timestamp": "2026-11-10T12:00:00Z",
            "payload": {"symbol": "SBER@MISX", "trades": [{
                "trade_id": "exchange-123", "price": {"value": "100.25"},
                "size": {"value": "17"}, "side": "SIDE_BUY",
                "timestamp": "2026-11-10T12:00:00Z",
            }]},
            "_received_monotonic": 123.0,
        }
        trades = FinamBrokerClient._extract_instrument_trades(envelope)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["event_type"], "INSTRUMENT_TRADE")
        self.assertEqual(trades[0]["ticker"], "SBER")
        self.assertEqual(trades[0]["trade_id"], "exchange-123")
        self.assertEqual(trades[0]["price"], 100.25)
        self.assertEqual(trades[0]["size"], 17.0)
        self.assertEqual(trades[0]["side"], "SIDE_BUY")
        self.assertEqual(trades[0]["received_monotonic"], 123.0)

    def test_supports_finams_camel_case_trade_payload(self):
        envelope = {
            "type": "DATA", "subscription_type": "INSTRUMENT_TRADES",
            "payload": {"symbol": "SBER@MISX", "instrumentTrades": [{
                "tradeId": "camel-1", "tradePrice": "101.5", "tradeSize": "8",
                "tradeSide": "SIDE_SELL",
            }]},
        }
        trades = FinamBrokerClient._extract_instrument_trades(envelope)
        self.assertEqual(len(trades), 1)
        self.assertEqual(trades[0]["trade_id"], "camel-1")
        self.assertEqual(trades[0]["price"], 101.5)
        self.assertEqual(trades[0]["size"], 8.0)
        self.assertEqual(trades[0]["side"], "SIDE_SELL")

    def test_rejects_invalid_trade_rows_without_fabricating_values(self):
        envelope = {"type": "DATA", "subscription_type": "INSTRUMENT_TRADES",
            "payload": {"symbol": "SBER@MISX", "trades": [
                {"trade_id": "bad-price", "price": 0, "size": 10},
                {"trade_id": "bad-size", "price": 100, "size": 0},
            ]}}
        self.assertEqual(FinamBrokerClient._extract_instrument_trades(envelope), [])


if __name__ == "__main__":
    unittest.main()
