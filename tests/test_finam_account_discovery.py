"""Finam session account discovery must never guess or misuse a client ID."""
from __future__ import annotations

import asyncio
import os
import unittest
from unittest.mock import patch

from exchange.finam_client import FinamBrokerClient


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        return None

    def json(self):
        return self._payload


class _FakeAsyncClient:
    def __init__(self, session_payload, accounts):
        self.session_payload = session_payload
        self.accounts = accounts
        self.posts = []
        self.gets = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    async def post(self, url, **kwargs):
        self.posts.append((url, kwargs))
        return _FakeResponse(self.session_payload)

    async def get(self, url, **kwargs):
        self.gets.append((url, kwargs))
        if "/v1/accounts/" in url:
            account_id = url.rsplit("/", 1)[-1]
            return _FakeResponse(self.accounts.get(account_id, {}))
        return _FakeResponse({
            "symbol": "SBER@MISX",
            "account_id": (kwargs.get("params") or {}).get("account_id", ""),
            "tradeable": True,
            "is_tradable": True,
            "trade_lot_size": "10",
        })


class FinamAccountDiscoveryTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.env = patch.dict(
            os.environ,
            {"FINAM_ACCOUNT_ID": "", "FINAM_CLIENT_ID": ""},
            clear=False,
        )
        self.env.start()
        self.addCleanup(self.env.stop)

    async def test_single_account_is_discovered_without_account_env_variable(self):
        fake = _FakeAsyncClient(
            {"account_ids": ["DEMO-1234"], "readonly": False},
            {"DEMO-1234": {"account_id": "DEMO-1234", "type": "ACCOUNT_TYPE_DEMO", "status": "ACTIVE"}},
        )
        broker = FinamBrokerClient("secret", is_demo=True)
        broker.jwt_token = "jwt"
        with patch("exchange.finam_client.httpx.AsyncClient", return_value=fake):
            selected = await broker.discover_accounts()

        self.assertEqual(selected, "DEMO-1234")
        self.assertEqual(broker.account_id_source, "session_details_single_account")
        self.assertEqual(fake.posts[0][1]["json"], {"token": "jwt"})

    async def test_unique_demo_account_is_selected_when_session_has_multiple_accounts(self):
        fake = _FakeAsyncClient(
            {"account_ids": ["REAL-1111", "DEMO-2222"]},
            {
                "REAL-1111": {"account_id": "REAL-1111", "type": "ACCOUNT_TYPE_REAL"},
                "DEMO-2222": {"account_id": "DEMO-2222", "type": "ACCOUNT_TYPE_DEMO"},
            },
        )
        broker = FinamBrokerClient("secret", is_demo=True)
        broker.jwt_token = "jwt"
        with patch("exchange.finam_client.httpx.AsyncClient", return_value=fake):
            selected = await broker.discover_accounts()

        self.assertEqual(selected, "DEMO-2222")

    async def test_ambiguous_multiple_accounts_are_not_guessed(self):
        fake = _FakeAsyncClient(
            {"account_ids": ["A-1111", "B-2222"]},
            {
                "A-1111": {"account_id": "A-1111", "type": "ACCOUNT_TYPE_BROKERAGE"},
                "B-2222": {"account_id": "B-2222", "type": "ACCOUNT_TYPE_BROKERAGE"},
            },
        )
        broker = FinamBrokerClient("secret", is_demo=True)
        broker.jwt_token = "jwt"
        with patch("exchange.finam_client.httpx.AsyncClient", return_value=fake):
            selected = await broker.discover_accounts()

        self.assertIsNone(selected)
        self.assertEqual(broker.account_id, "")

    async def test_explicit_account_id_must_be_present_in_session(self):
        os.environ["FINAM_ACCOUNT_ID"] = "NOT-IN-TOKEN"
        fake = _FakeAsyncClient(
            {"account_ids": ["DEMO-2222"]},
            {"DEMO-2222": {"account_id": "DEMO-2222", "type": "ACCOUNT_TYPE_DEMO"}},
        )
        broker = FinamBrokerClient("secret", is_demo=True)
        broker.jwt_token = "jwt"
        with patch("exchange.finam_client.httpx.AsyncClient", return_value=fake):
            selected = await broker.discover_accounts()

        self.assertIsNone(selected)
        self.assertEqual(broker.account_id, "")

    async def test_explicit_account_id_is_validated_when_session_account_ids_are_empty(self):
        os.environ["FINAM_ACCOUNT_ID"] = "DEMO-8888"
        fake = _FakeAsyncClient(
            {"account_ids": [], "readonly": False},
            {"DEMO-8888": {"account_id": "DEMO-8888", "type": "ACCOUNT_TYPE_DEMO"}},
        )
        broker = FinamBrokerClient("secret", is_demo=True)
        broker.jwt_token = "jwt"
        with patch("exchange.finam_client.httpx.AsyncClient", return_value=fake):
            selected = await broker.discover_accounts()

        self.assertEqual(selected, "DEMO-8888")
        self.assertEqual(broker.account_id_source, "FINAM_ACCOUNT_ID")
        self.assertTrue(any("/v1/accounts/DEMO-8888" in item[0] for item in fake.gets))

    async def test_explicit_account_id_is_rejected_if_account_endpoint_returns_another_identity(self):
        os.environ["FINAM_ACCOUNT_ID"] = "DEMO-9999"
        fake = _FakeAsyncClient(
            {"account_ids": []},
            {"DEMO-9999": {"account_id": "SOMEONE-ELSE", "type": "ACCOUNT_TYPE_DEMO"}},
        )
        broker = FinamBrokerClient("secret", is_demo=True)
        broker.jwt_token = "jwt"
        with patch("exchange.finam_client.httpx.AsyncClient", return_value=fake):
            selected = await broker.discover_accounts()

        self.assertIsNone(selected)
        self.assertEqual(broker.account_id, "")

    async def test_missing_account_ids_reject_unverifiable_client_id(self):
        os.environ["FINAM_CLIENT_ID"] = "DEMO-1212"
        fake = _FakeAsyncClient({"readonly": False}, {})
        broker = FinamBrokerClient("secret", is_demo=True)
        broker.jwt_token = "jwt"
        with patch("exchange.finam_client.httpx.AsyncClient", return_value=fake):
            selected = await broker.discover_accounts()

        self.assertIsNone(selected)
        self.assertEqual(broker.account_id, "")
        self.assertTrue(any("/v1/accounts/DEMO-1212" in item[0] for item in fake.gets))

    async def test_legacy_client_id_is_verified_directly_when_session_account_ids_are_empty(self):
        os.environ["FINAM_CLIENT_ID"] = "DEMO-6161"
        fake = _FakeAsyncClient(
            {"account_ids": [], "readonly": False},
            {"DEMO-6161": {"account_id": "DEMO-6161", "type": "ACCOUNT_TYPE_DEMO"}},
        )
        broker = FinamBrokerClient("secret", is_demo=True)
        broker.jwt_token = "jwt"
        with patch("exchange.finam_client.httpx.AsyncClient", return_value=fake):
            selected = await broker.discover_accounts()

        self.assertEqual(selected, "DEMO-6161")
        self.assertEqual(broker.account_id_source, "FINAM_CLIENT_ID_verified_by_account_endpoint")
        self.assertTrue(any("/v1/accounts/DEMO-6161" in item[0] for item in fake.gets))

    async def test_legacy_client_id_accepts_account_resource_without_redundant_id_field(self):
        os.environ["FINAM_CLIENT_ID"] = "DEMO-6363"
        fake = _FakeAsyncClient(
            {"account_ids": []},
            {"DEMO-6363": {"type": "ACCOUNT_TYPE_DEMO", "status": "ACTIVE"}},
        )
        broker = FinamBrokerClient("secret", is_demo=True)
        broker.jwt_token = "jwt"
        with patch("exchange.finam_client.httpx.AsyncClient", return_value=fake):
            selected = await broker.discover_accounts()

        self.assertEqual(selected, "DEMO-6363")
        self.assertEqual(broker.account_id_source, "FINAM_CLIENT_ID_verified_by_account_endpoint")

    async def test_legacy_client_id_direct_validation_rejects_conflicting_account_identity(self):
        os.environ["FINAM_CLIENT_ID"] = "DEMO-6262"
        fake = _FakeAsyncClient(
            {"account_ids": []},
            {"DEMO-6262": {"account_id": "OTHER-ACCOUNT", "type": "ACCOUNT_TYPE_DEMO"}},
        )
        broker = FinamBrokerClient("secret", is_demo=True)
        broker.jwt_token = "jwt"
        with patch("exchange.finam_client.httpx.AsyncClient", return_value=fake):
            selected = await broker.discover_accounts()

        self.assertIsNone(selected)
        self.assertEqual(broker.account_id, "")

    async def test_concurrent_discovery_is_serialized_and_reuses_verified_account(self):
        fake = _FakeAsyncClient(
            {"account_ids": ["DEMO-1313"]},
            {"DEMO-1313": {"account_id": "DEMO-1313", "type": "ACCOUNT_TYPE_DEMO"}},
        )
        broker = FinamBrokerClient("secret", is_demo=True)
        broker.jwt_token = "jwt"
        with patch("exchange.finam_client.httpx.AsyncClient", return_value=fake):
            first, second = await asyncio.gather(
                broker.discover_accounts(), broker.discover_accounts()
            )

        self.assertEqual(first, "DEMO-1313")
        self.assertEqual(second, "DEMO-1313")
        self.assertEqual(len(fake.posts), 1)

    async def test_legacy_client_id_is_used_only_when_it_matches_session_account_id(self):
        os.environ["FINAM_CLIENT_ID"] = "DEMO-3333"
        fake = _FakeAsyncClient(
            {"account_ids": ["DEMO-3333", "OTHER-4444"]},
            {
                "DEMO-3333": {"account_id": "DEMO-3333", "type": "ACCOUNT_TYPE_DEMO"},
                "OTHER-4444": {"account_id": "OTHER-4444", "type": "ACCOUNT_TYPE_BROKERAGE"},
            },
        )
        broker = FinamBrokerClient("secret", is_demo=True)
        broker.jwt_token = "jwt"
        with patch("exchange.finam_client.httpx.AsyncClient", return_value=fake):
            selected = await broker.discover_accounts()

        self.assertEqual(selected, "DEMO-3333")
        self.assertEqual(broker.account_id_source, "FINAM_CLIENT_ID_matched_account_ids")

    async def test_nonmatching_client_id_is_never_sent_as_account_id(self):
        os.environ["FINAM_CLIENT_ID"] = "CLIENT-9999"
        fake = _FakeAsyncClient(
            {"account_ids": ["DEMO-5555"]},
            {"DEMO-5555": {"account_id": "DEMO-5555", "type": "ACCOUNT_TYPE_DEMO"}},
        )
        broker = FinamBrokerClient("secret", is_demo=True)
        broker.jwt_token = "jwt"
        with patch("exchange.finam_client.httpx.AsyncClient", return_value=fake):
            selected = await broker.discover_accounts()

        self.assertEqual(selected, "DEMO-5555")
        self.assertEqual(broker.account_id, "DEMO-5555")

    async def test_get_asset_params_uses_discovered_account_id_by_default(self):
        fake = _FakeAsyncClient({"account_ids": []}, {})
        broker = FinamBrokerClient("secret", is_demo=True)
        broker.jwt_token = "jwt"
        broker.account_id = "DEMO-7777"
        with patch("exchange.finam_client.httpx.AsyncClient", return_value=fake):
            result = await broker.get_asset_params("SBER")

        self.assertEqual(result["trade_lot_size"], "10")
        self.assertEqual(fake.gets[-1][1]["params"], {"account_id": "DEMO-7777"})


if __name__ == "__main__":
    unittest.main()
