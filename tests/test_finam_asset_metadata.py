from __future__ import annotations

import asyncio
import unittest
from unittest.mock import patch

from config import ASSET_METADATA, FINAM_TRADE_LOT_SIZES, LOT_SIZES
from finam_client import FinamBrokerClient
from risk_manager import RiskManager


class _FinamMetadataStub:
    async def get_asset(self, ticker: str, account_id: str | None = None):
        if ticker != "TESTMETA":
            return None
        return {
            "symbol": "TESTMETA@MISX",
            "ticker": "TESTMETA",
            "mic": "MISX",
            "decimals": 2,
            "min_step": "5",
            "lot_size": {"value": "10"},
            "quote_currency": "RUB",
        }

    async def get_asset_params(self, ticker: str, account_id: str | None = None):
        if ticker == "TESTMETA":
            return {"trade_lot_size": "7", "is_tradable": True}
        return None


class FinamAssetMetadataTests(unittest.TestCase):
    def test_paper_uses_public_exchange_lot_and_asset_details(self):
        old_lot = LOT_SIZES.get("TESTMETA")
        old_trade_lot = FINAM_TRADE_LOT_SIZES.get("TESTMETA")
        old_metadata = ASSET_METADATA.get("TESTMETA")
        try:
            LOT_SIZES["TESTMETA"] = 1
            ASSET_METADATA.pop("TESTMETA", None)
            with patch.dict("os.environ", {
                "FINAM_METADATA_REQUEST_INTERVAL_SEC": "0",
                "FINAM_ACCOUNT_ID": "",
            }):
                asyncio.run(RiskManager.update_finam_lot_sizes(_FinamMetadataStub(), paper_mode=True))

            self.assertEqual(LOT_SIZES["TESTMETA"], 1)
            self.assertEqual(FINAM_TRADE_LOT_SIZES["TESTMETA"], 10)
            self.assertEqual(ASSET_METADATA["TESTMETA"]["price_tick"], 0.05)
            self.assertEqual(ASSET_METADATA["TESTMETA"]["lot_size"], 10.0)
            self.assertEqual(ASSET_METADATA["TESTMETA"]["metadata_source"], "FINAM_GET_ASSET")
        finally:
            if old_lot is None:
                LOT_SIZES.pop("TESTMETA", None)
            else:
                LOT_SIZES["TESTMETA"] = old_lot
            if old_trade_lot is None:
                FINAM_TRADE_LOT_SIZES.pop("TESTMETA", None)
            else:
                FINAM_TRADE_LOT_SIZES["TESTMETA"] = old_trade_lot
            if old_metadata is None:
                ASSET_METADATA.pop("TESTMETA", None)
            else:
                ASSET_METADATA["TESTMETA"] = old_metadata

    def test_positive_lot_size_without_explicit_tradability_is_rejected(self):
        class _NotTradable(_FinamMetadataStub):
            async def get_asset_params(self, ticker: str, account_id: str | None = None):
                if ticker == "TESTMETA":
                    return {"trade_lot_size": "7", "is_tradable": False}
                return None

        old_lot = LOT_SIZES.get("TESTMETA")
        old_trade_lot = FINAM_TRADE_LOT_SIZES.get("TESTMETA")
        try:
            LOT_SIZES["TESTMETA"] = 1
            FINAM_TRADE_LOT_SIZES["TESTMETA"] = 7
            with patch.dict("os.environ", {
                "FINAM_METADATA_REQUEST_INTERVAL_SEC": "0",
                "FINAM_ACCOUNT_ID": "TEST-ACCOUNT-123",
            }):
                stub = _NotTradable()
                stub.account_id = "TEST-ACCOUNT-123"
                asyncio.run(RiskManager.update_finam_lot_sizes(stub, paper_mode=False))
            self.assertNotIn("TESTMETA", FINAM_TRADE_LOT_SIZES)
        finally:
            if old_lot is None:
                LOT_SIZES.pop("TESTMETA", None)
            else:
                LOT_SIZES["TESTMETA"] = old_lot
            if old_trade_lot is None:
                FINAM_TRADE_LOT_SIZES.pop("TESTMETA", None)
            else:
                FINAM_TRADE_LOT_SIZES["TESTMETA"] = old_trade_lot

    def test_paper_mode_uses_exchange_lot_without_finam_account(self):
        class _NoAccount(_FinamMetadataStub):
            calls = 0
            discovery_calls = 0
            async def get_asset(self, ticker: str, account_id: str | None = None):
                self.assert_no_account_id = account_id
                return await super().get_asset(ticker, account_id)
            async def discover_accounts(self):
                self.discovery_calls += 1
                raise AssertionError("PAPER must not discover a Finam brokerage account")
            async def get_asset_params(self, ticker: str, account_id: str | None = None):
                self.calls += 1
                raise AssertionError("PAPER must not request account-specific parameters")

        stub = _NoAccount()
        stub.account_id = "STALE-BROKER-ACCOUNT"
        old_lot = LOT_SIZES.get("TESTMETA")
        old_trade_lot = FINAM_TRADE_LOT_SIZES.get("TESTMETA")
        try:
            LOT_SIZES["TESTMETA"] = 1
            FINAM_TRADE_LOT_SIZES["TESTMETA"] = 7
            with patch.dict("os.environ", {
                "FINAM_METADATA_REQUEST_INTERVAL_SEC": "0",
                "FINAM_ACCOUNT_ID": "",
                "FINAM_CLIENT_ID": "",
            }):
                asyncio.run(RiskManager.update_finam_lot_sizes(stub, paper_mode=True))
            self.assertEqual(stub.calls, 0)
            self.assertEqual(stub.discovery_calls, 0)
            self.assertIsNone(stub.assert_no_account_id)
            self.assertEqual(FINAM_TRADE_LOT_SIZES["TESTMETA"], 10)
        finally:
            if old_lot is None:
                LOT_SIZES.pop("TESTMETA", None)
            else:
                LOT_SIZES["TESTMETA"] = old_lot
            if old_trade_lot is None:
                FINAM_TRADE_LOT_SIZES.pop("TESTMETA", None)
            else:
                FINAM_TRADE_LOT_SIZES["TESTMETA"] = old_trade_lot


    def test_public_get_asset_does_not_inherit_client_account_id(self):
        captured = {}

        class _Response:
            def raise_for_status(self):
                return None

            @staticmethod
            def json():
                return {"symbol": "VTBR@MISX", "lot_size": {"value": "10"}}

        class _AsyncClient:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            async def get(self, url, *, headers, params=None):
                captured["url"] = url
                captured["params"] = params
                return _Response()

        client = FinamBrokerClient(token="test-token")
        client.jwt_token = "test-jwt"
        client.account_id = "STALE-PAPER-ACCOUNT"

        with patch("exchange.finam_client.httpx.AsyncClient", return_value=_AsyncClient()):
            payload = asyncio.run(client.get_asset("VTBR"))

        self.assertEqual(payload["lot_size"]["value"], "10")
        self.assertIsNone(captured["params"])


    def test_paper_accepts_nested_camel_case_and_object_wrapped_lot_metadata(self):
        from types import SimpleNamespace

        class _ObjectPayload:
            async def get_asset(self, ticker: str):
                if ticker != "TESTMETA":
                    return None
                return SimpleNamespace(asset=SimpleNamespace(
                    symbol="TESTMETA@MISX",
                    mic="MISX",
                    decimals=2,
                    min_step=5,
                    lotSize=SimpleNamespace(value="15"),
                    quote_currency="RUB",
                ))

        old_lot = LOT_SIZES.get("TESTMETA")
        old_trade_lot = FINAM_TRADE_LOT_SIZES.get("TESTMETA")
        try:
            LOT_SIZES["TESTMETA"] = 1
            with patch.dict("os.environ", {"FINAM_METADATA_REQUEST_INTERVAL_SEC": "0"}):
                asyncio.run(RiskManager.update_finam_lot_sizes(_ObjectPayload(), paper_mode=True))
            self.assertEqual(FINAM_TRADE_LOT_SIZES["TESTMETA"], 15)
            self.assertEqual(ASSET_METADATA["TESTMETA"]["lot_size"], 15.0)
        finally:
            if old_lot is None:
                LOT_SIZES.pop("TESTMETA", None)
            else:
                LOT_SIZES["TESTMETA"] = old_lot
            if old_trade_lot is None:
                FINAM_TRADE_LOT_SIZES.pop("TESTMETA", None)
            else:
                FINAM_TRADE_LOT_SIZES["TESTMETA"] = old_trade_lot
            ASSET_METADATA.pop("TESTMETA", None)

    def test_paper_transient_metadata_failure_preserves_last_confirmed_exchange_lot(self):
        class _Unavailable:
            async def get_asset(self, ticker: str):
                return None

        old_lot = LOT_SIZES.get("TESTMETA")
        old_trade_lot = FINAM_TRADE_LOT_SIZES.get("TESTMETA")
        try:
            LOT_SIZES["TESTMETA"] = 1
            FINAM_TRADE_LOT_SIZES["TESTMETA"] = 10
            with patch.dict("os.environ", {"FINAM_METADATA_REQUEST_INTERVAL_SEC": "0"}):
                asyncio.run(RiskManager.update_finam_lot_sizes(_Unavailable(), paper_mode=True))
            self.assertEqual(FINAM_TRADE_LOT_SIZES["TESTMETA"], 10)
        finally:
            if old_lot is None:
                LOT_SIZES.pop("TESTMETA", None)
            else:
                LOT_SIZES["TESTMETA"] = old_lot
            if old_trade_lot is None:
                FINAM_TRADE_LOT_SIZES.pop("TESTMETA", None)
            else:
                FINAM_TRADE_LOT_SIZES["TESTMETA"] = old_trade_lot

if __name__ == "__main__":
    unittest.main()
