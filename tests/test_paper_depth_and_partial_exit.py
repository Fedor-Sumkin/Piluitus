from __future__ import annotations

import asyncio
import unittest

from config import resolve_paper_book_size_unit
from execution_layer import BaseExecutionEngine


class _MemoryDB:
    def __init__(self) -> None:
        self.values: dict[str, dict] = {}

    async def set_json(self, key: str, value: dict) -> None:
        self.values[key] = dict(value)


def _trade(side: str = "BUY", volume: int = 100) -> dict:
    return {
        "id": "PAPER-PARTIAL-TEST",
        "ticker": "SBER",
        "status": "OPEN",
        "mode": "PAPER",
        "side": side,
        "entry_price": 100.0,
        "volume": volume,
        "lots": volume // 10,
        "lot_size": 10,
        "original_volume": volume,
        "original_lots": volume // 10,
        "notional_entry_rub": 100.0 * volume,
        "entry_commission_rub": 0.354,
        "commission_pct_per_side": 0.0354,
        "realized_gross_pnl_rub": 0.0,
        "realized_entry_commission_rub": 0.0,
        "realized_exit_commission_rub": 0.0,
        "realized_net_pnl_rub": 0.0,
    }


def _bbo(side: str = "BUY", *, age: float = 0.2) -> dict:
    if side == "BUY":
        bid, ask = 100.8, 101.0
        executable = bid
        executable_side = "BID"
    else:
        bid, ask = 99.0, 99.2
        executable = ask
        executable_side = "ASK"
    return {
        "bid": bid,
        "ask": ask,
        "bid_size": 12345.0,
        "ask_size": 12345.0,
        "bid_levels": [[bid, 50.0]],
        "ask_levels": [[ask, 50.0]],
        "bid_age_sec": age,
        "ask_age_sec": age,
        "age_sec": age,
        "executable_price": executable,
        "executable_side": executable_side,
        "spread_pct": 0.4,
        "max_age_sec": 2.0,
        "order_book_valid": True,
        "order_book_age_sec": age,
    }


def _engine(unit: str = "unknown") -> BaseExecutionEngine:
    engine = BaseExecutionEngine.__new__(BaseExecutionEngine)
    engine.paper_mode = True
    engine.paper_book_size_unit = unit
    engine.paper_max_book_participation_pct = 100.0
    engine.paper_commission_pct = 0.0354
    engine.paper_slippage_bps = 0.0
    return engine


class PaperDepthAndPartialExitTests(unittest.TestCase):
    def test_book_unit_stays_unknown_without_external_provenance(self) -> None:
        self.assertEqual(resolve_paper_book_size_unit("shares", ""), "unknown")
        self.assertEqual(resolve_paper_book_size_unit("lots", "INFERRED_FROM_LOT_SIZE"), "unknown")

    def test_book_unit_accepts_explicit_supported_evidence_source(self) -> None:
        self.assertEqual(resolve_paper_book_size_unit("shares", "FINAM_SUPPORT"), "shares")
        self.assertEqual(resolve_paper_book_size_unit("lots", "BROKER_EXPORT"), "lots")

    def test_depth_fill_converts_share_sizes_into_lot_aligned_fills(self) -> None:
        result = BaseExecutionEngine._simulate_paper_depth_fill(
            [[99.8, 20.0], [99.7, 30.0]],
            requested_lots=5,
            lot_size=10,
            size_unit="shares",
            max_participation_pct=100.0,
        )
        self.assertEqual(result["filled_shares"], 50)
        self.assertEqual(result["filled_lots"], 5)
        self.assertEqual(result["levels_used"], 2)
        self.assertAlmostEqual(result["average_price"], (20 * 99.8 + 30 * 99.7) / 50)

    def test_depth_fill_converts_lot_sizes_into_shares(self) -> None:
        result = BaseExecutionEngine._simulate_paper_depth_fill(
            [[99.8, 2.0], [99.7, 3.0]],
            requested_lots=5,
            lot_size=10,
            size_unit="lots",
            max_participation_pct=100.0,
        )
        self.assertEqual(result["filled_shares"], 50)
        self.assertEqual(result["filled_lots"], 5)
        self.assertEqual(result["levels_used"], 2)

    def test_unknown_unit_uses_explicit_bbo_partial_plan_without_reading_depth_size(self) -> None:
        engine = _engine("unknown")
        plan = engine._paper_profit_partial_fill(
            "SBER", _trade("BUY"), _bbo("BUY"), 0.5
        )
        self.assertIsNotNone(plan)
        self.assertEqual(plan["filled_shares"], 50)
        self.assertEqual(plan["filled_lots"], 5)
        self.assertEqual(plan["average_price"], 100.8)
        self.assertEqual(plan["execution_model"], "BBO_TOP_OF_BOOK_PARTIAL_ASSUMED")
        self.assertEqual(plan["size_unit"], "UNKNOWN")
        self.assertNotIn("depth_vwap", plan)

    def test_bbo_partial_plan_is_mirrored_for_short_positions(self) -> None:
        engine = _engine("unknown")
        plan = engine._paper_profit_partial_fill(
            "SBER", _trade("SELL"), _bbo("SELL"), 0.5
        )
        self.assertIsNotNone(plan)
        self.assertEqual(plan["average_price"], 99.2)
        self.assertEqual(plan["execution_model"], "BBO_TOP_OF_BOOK_PARTIAL_ASSUMED")

    def test_bbo_partial_plan_rejects_stale_or_wrong_executable_side(self) -> None:
        engine = _engine("unknown")
        stale = _bbo("BUY", age=2.1)
        wrong_side = _bbo("BUY")
        wrong_side["executable_side"] = "ASK"
        self.assertIsNone(engine._paper_profit_partial_fill("SBER", _trade(), stale, 0.5))
        self.assertIsNone(engine._paper_profit_partial_fill("SBER", _trade(), wrong_side, 0.5))

    def test_partial_plan_never_closes_entire_position_or_creates_sub_lot_fill(self) -> None:
        engine = _engine("unknown")
        self.assertIsNone(engine._paper_profit_partial_fill("SBER", _trade(volume=10), _bbo(), 0.5))
        self.assertIsNone(engine._paper_profit_partial_fill("SBER", _trade(), _bbo(), 1.0))

    def test_confirmed_depth_uses_depth_model_instead_of_bbo_fallback(self) -> None:
        engine = _engine("shares")
        plan = engine._paper_profit_partial_fill(
            "SBER", _trade(), _bbo("BUY"), 0.5
        )
        self.assertIsNotNone(plan)
        self.assertEqual(plan["filled_shares"], 50)
        self.assertNotEqual(plan.get("execution_model"), "BBO_TOP_OF_BOOK_PARTIAL_ASSUMED")

    def test_bbo_partial_close_persists_remaining_position_and_does_not_fabricate_depth(self) -> None:
        engine = _engine("unknown")
        trade = _trade("BUY")
        bbo = _bbo("BUY")
        plan = engine._paper_profit_partial_fill("SBER", trade, bbo, 0.5)
        self.assertIsNotNone(plan)
        db = _MemoryDB()

        asyncio.run(
            engine._finalize_paper_partial_close(
                "SBER",
                trade,
                db,
                bbo,
                plan,
                exit_reason="PROFIT_PROTECTION",
                trigger_price=99.8,
                observed_last_price=101.2,
            )
        )

        saved = db.values["piluitus:trade:PAPER-PARTIAL-TEST"]
        self.assertEqual(saved["status"], "OPEN")
        self.assertEqual(saved["volume"], 50)
        self.assertEqual(saved["lots"], 5)
        self.assertEqual(saved["exit_fill_model"], "BBO_TOP_OF_BOOK_PARTIAL_ASSUMED")
        self.assertEqual(saved["exit_depth_unit"], "UNKNOWN")
        self.assertIsNone(saved["exit_depth_levels_used_last"])
        self.assertIsNone(saved["exit_depth_fill_ratio_last"])
        self.assertEqual(saved["exit_partial_fill_count"], 1)
        self.assertTrue(saved["paper_profit_partial_taken"])
        fill = saved["exit_partial_fills"][-1]
        self.assertEqual(fill["quantity_shares"], 50)
        self.assertEqual(fill["execution_model"], "BBO_TOP_OF_BOOK_PARTIAL_ASSUMED")
        self.assertIsNone(fill["depth_levels_used"])
        self.assertIsNone(fill["depth_fill_ratio"])
        self.assertIsNone(fill["execution_diagnostics"]["exit_depth_vwap"])
        self.assertIsNone(fill["execution_diagnostics"]["exit_depth_impact_rub"])
        self.assertGreater(saved["realized_net_pnl_rub"], 0.0)
        self.assertGreater(saved["entry_commission_rub"], 0.0)
        self.assertEqual(trade["volume"], 50)


if __name__ == "__main__":
    unittest.main()
