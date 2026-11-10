from __future__ import annotations

import asyncio
import unittest

from config import ASSET_METADATA, LOT_SIZES
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
            return {"trade_lot_size": "7"}
        return None


class FinamAssetMetadataTests(unittest.TestCase):
    def test_asset_details_and_account_trade_lot_are_loaded_separately(self):
        old_lot = LOT_SIZES.get("TESTMETA")
        old_metadata = ASSET_METADATA.get("TESTMETA")
        try:
            LOT_SIZES["TESTMETA"] = 1
            ASSET_METADATA.pop("TESTMETA", None)
            asyncio.run(RiskManager.update_finam_lot_sizes(_FinamMetadataStub()))

            self.assertEqual(LOT_SIZES["TESTMETA"], 7)
            self.assertEqual(ASSET_METADATA["TESTMETA"]["price_tick"], 0.05)
            self.assertEqual(ASSET_METADATA["TESTMETA"]["lot_size"], 10.0)
            self.assertEqual(ASSET_METADATA["TESTMETA"]["metadata_source"], "FINAM_GET_ASSET")
        finally:
            if old_lot is None:
                LOT_SIZES.pop("TESTMETA", None)
            else:
                LOT_SIZES["TESTMETA"] = old_lot
            if old_metadata is None:
                ASSET_METADATA.pop("TESTMETA", None)
            else:
                ASSET_METADATA["TESTMETA"] = old_metadata


if __name__ == "__main__":
    unittest.main()
