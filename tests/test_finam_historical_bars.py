from __future__ import annotations

import unittest
from unittest.mock import AsyncMock, patch

from exchange.finam_client import FinamBrokerClient


class _FakeResponse:
    status_code = 200

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {
            "symbol": "SBER@MISX",
            "bars": [{
                "timestamp": "2026-11-17T10:00:00Z",
                "open": {"value": "100.0"},
                "high": {"value": "101.0"},
                "low": {"value": "99.0"},
                "close": {"value": "100.5"},
                "volume": {"value": "12345"},
            }],
        }


class _FakeAsyncClient:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs
        self.calls = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def get(self, url, *, headers, params):
        self.calls.append((url, headers, params))
        return _FakeResponse()


class FinamHistoricalBarsTests(unittest.IsolatedAsyncioTestCase):
    async def test_requests_m1_bars_and_returns_raw_ohlc_payload(self):
        client = object.__new__(FinamBrokerClient)
        client.jwt_token = "test-jwt"
        client.base_url = "https://api.finam.ru"
        client._refresh_auth_if_unchanged = AsyncMock(return_value=True)
        fake_client = _FakeAsyncClient()

        with patch("exchange.finam_client.httpx.AsyncClient", return_value=fake_client):
            bars = await client.get_historical_m1_bars("sber", lookback_minutes=120)

        self.assertEqual(len(bars), 1)
        self.assertEqual(bars[0]["close"]["value"], "100.5")
        self.assertEqual(len(fake_client.calls), 1)
        url, headers, params = fake_client.calls[0]
        self.assertEqual(url, "https://api.finam.ru/v1/instruments/SBER@MISX/bars")
        self.assertEqual(params["timeframe"], "TIME_FRAME_M1")
        self.assertTrue(params["interval.start_time"].endswith("Z"))
        self.assertTrue(params["interval.end_time"].endswith("Z"))
        self.assertEqual(headers["Authorization"], "Bearer test-jwt")

    async def test_empty_jwt_skips_historical_request(self):
        client = object.__new__(FinamBrokerClient)
        client.jwt_token = ""
        client.base_url = "https://api.finam.ru"
        with patch("exchange.finam_client.httpx.AsyncClient") as http_client:
            bars = await client.get_historical_m1_bars("SBER")
        self.assertEqual(bars, [])
        http_client.assert_not_called()


if __name__ == "__main__":
    unittest.main()
