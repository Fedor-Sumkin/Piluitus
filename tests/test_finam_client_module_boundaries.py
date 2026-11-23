"""Composition contract for the modular Finam broker gateway."""
from __future__ import annotations

import unittest

from exchange.finam_client import FinamBrokerClient
from exchange.finam_helpers import FinamHelpersMixin
from exchange.finam_accounts import FinamAccountMixin
from exchange.finam_assets import FinamAssetMixin
from exchange.finam_market_data import FinamMarketDataMixin
from exchange.finam_payload_parsers import FinamPayloadParserMixin
from exchange.finam_order_book import FinamOrderBookMixin
from exchange.finam_orders import FinamOrdersMixin


class FinamClientModuleBoundaryTests(unittest.TestCase):
    def test_public_client_composes_all_gateway_responsibilities(self):
        for mixin in (
            FinamHelpersMixin, FinamAccountMixin, FinamAssetMixin,
            FinamPayloadParserMixin, FinamMarketDataMixin, FinamOrderBookMixin, FinamOrdersMixin,
        ):
            self.assertTrue(issubclass(FinamBrokerClient, mixin))

    def test_market_connection_state_starts_disconnected(self):
        client = FinamBrokerClient(token="test-token")
        self.assertFalse(client.is_ws_running)
        self.assertFalse(client._market_stream_connected)

    def test_historical_gateway_api_remains_available(self):
        for name in (
            "connect", "get_account", "discover_accounts", "get_asset",
            "get_asset_schedule", "start_market_data_stream", "stop_market_data_stream",
            "refresh_order_book_snapshot", "get_ticker_order_book", "place_order",
            "get_portfolio", "get_securities", "market_close_all_positions",
        ):
            self.assertTrue(callable(getattr(FinamBrokerClient, name)))


if __name__ == "__main__":
    unittest.main()
