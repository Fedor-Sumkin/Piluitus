import unittest
from unittest.mock import AsyncMock

from execution_layer import BaseExecutionEngine


class PaperExitTriggerTests(unittest.TestCase):
    def setUp(self):
        self.engine = BaseExecutionEngine.__new__(BaseExecutionEngine)

    def _trade(self, side="BUY"):
        if side == "BUY":
            return {
                "id": "PAPER_TEST",
                "status": "OPEN",
                "side": "BUY",
                "entry_price": 100.0,
                "stop_loss": 99.0,
                "take_profit": 102.0,
            }
        return {
            "id": "PAPER_TEST",
            "status": "OPEN",
            "side": "SELL",
            "entry_price": 100.0,
            "stop_loss": 101.0,
            "take_profit": 98.0,
        }

    def test_long_stop_trigger_is_latched_from_last_trade(self):
        trade = self._trade("BUY")
        reason = self.engine._capture_paper_exit_trigger(
            "TEST", trade, 98.75, source="LAST_TRADE",
            market_timestamp="2026-11-09T12:00:00+03:00",
        )

        self.assertEqual(reason, "STOP_LOSS")
        self.assertEqual(trade["pending_exit_reason"], "STOP_LOSS")
        self.assertEqual(trade["pending_exit_trigger_price"], 99.0)
        self.assertEqual(trade["exit_trigger_source"], "LAST_TRADE")
        self.assertEqual(trade["exit_trigger_observed_price"], 98.75)
        self.assertEqual(
            trade["exit_trigger_market_timestamp"],
            "2026-11-09T12:00:00+03:00",
        )
        self.assertGreater(trade["exit_trigger_monotonic"], 0.0)

    def test_short_take_profit_trigger_is_latched(self):
        trade = self._trade("SELL")
        reason = self.engine._capture_paper_exit_trigger(
            "TEST", trade, 97.5, source="LAST_TRADE"
        )

        self.assertEqual(reason, "TAKE_PROFIT")
        self.assertEqual(trade["pending_exit_trigger_price"], 98.0)

    def test_first_trigger_is_not_overwritten_by_later_price(self):
        trade = self._trade("BUY")
        first = self.engine._capture_paper_exit_trigger(
            "TEST", trade, 98.5, source="LAST_TRADE"
        )
        first_timestamp = trade["exit_trigger_timestamp"]
        second = self.engine._capture_paper_exit_trigger(
            "TEST", trade, 103.0, source="EXECUTABLE_BBO"
        )

        self.assertEqual(first, "STOP_LOSS")
        self.assertEqual(second, "STOP_LOSS")
        self.assertEqual(trade["exit_trigger_timestamp"], first_timestamp)
        self.assertEqual(trade["exit_trigger_source"], "LAST_TRADE")

    def test_price_inside_levels_does_not_create_trigger(self):
        trade = self._trade("BUY")
        reason = self.engine._capture_paper_exit_trigger(
            "TEST", trade, 100.5, source="LAST_TRADE"
        )

        self.assertEqual(reason, "")
        self.assertNotIn("pending_exit_reason", trade)

    def test_order_book_event_does_not_reinterpret_old_last_price_as_new_trigger(self):
        class ActiveTask:
            @staticmethod
            def done():
                return False

        self.engine.paper_mode = True
        self.engine._maintenance_active = False
        self.engine.paper_positions = {"TEST": self._trade("BUY")}
        self.engine._pending_monitor_prices = {}
        self.engine._monitor_worker_task = ActiveTask()

        self.engine.schedule_paper_position_monitor(
            "TEST", 98.0, object(), event_source="ORDER_BOOK"
        )

        self.assertNotIn("pending_exit_reason", self.engine.paper_positions["TEST"])
        self.assertEqual(
            self.engine._pending_monitor_prices["TEST"],
            (98.0, "ORDER_BOOK", None),
        )

    def test_last_trade_trigger_is_latched_before_monitor_queue_coalescing(self):
        class ActiveTask:
            @staticmethod
            def done():
                return False

        self.engine.paper_mode = True
        self.engine._maintenance_active = False
        self.engine.paper_positions = {"TEST": self._trade("BUY")}
        self.engine._pending_monitor_prices = {}
        self.engine._monitor_worker_task = ActiveTask()

        self.engine.schedule_paper_position_monitor(
            "TEST", 98.0, object(), event_source="LAST_TRADE"
        )

        self.assertEqual(
            self.engine.paper_positions["TEST"]["pending_exit_reason"],
            "STOP_LOSS",
        )


class PaperExitDepthTests(unittest.TestCase):
    def setUp(self):
        self.engine = BaseExecutionEngine.__new__(BaseExecutionEngine)
        self.engine.paper_book_size_unit = "shares"
        self.engine.paper_max_book_participation_pct = 100.0

    def test_long_exit_consumes_bid_depth_and_uses_share_units(self):
        trade = {
            "side": "BUY", "volume": 20, "lots": 2, "lot_size": 10,
        }
        fill = self.engine._paper_exit_depth_fill(
            "SBER", trade,
            {
                "bid_levels": [[99.9, 10], [99.8, 10]],
                "ask_levels": [[100.0, 100]],
            },
        )
        self.assertEqual(fill["filled_lots"], 2)
        self.assertEqual(fill["filled_shares"], 20)
        self.assertAlmostEqual(fill["average_price"], 99.85)
        self.assertEqual(fill["levels_used"], 2)

    def test_short_exit_consumes_ask_depth(self):
        trade = {
            "side": "SELL", "volume": 20, "lots": 2, "lot_size": 10,
        }
        fill = self.engine._paper_exit_depth_fill(
            "SBER", trade,
            {
                "bid_levels": [[99.9, 100]],
                "ask_levels": [[100.1, 10], [100.2, 10]],
            },
        )
        self.assertEqual(fill["filled_lots"], 2)
        self.assertAlmostEqual(fill["average_price"], 100.15)

    def test_lot_unit_converts_book_lots_to_share_quantity(self):
        self.engine.paper_book_size_unit = "lots"
        trade = {
            "side": "BUY", "volume": 20, "lots": 2, "lot_size": 10,
        }
        fill = self.engine._paper_exit_depth_fill(
            "SBER", trade, {"bid_levels": [[99.9, 1], [99.8, 1]]}
        )
        self.assertEqual(fill["filled_lots"], 2)
        self.assertEqual(fill["filled_shares"], 20)
        self.assertAlmostEqual(fill["average_price"], 99.85)

    def test_insufficient_depth_is_reported_as_partial_not_full_fill(self):
        trade = {
            "side": "BUY", "volume": 20, "lots": 2, "lot_size": 10,
        }
        fill = self.engine._paper_exit_depth_fill(
            "SBER", trade, {"bid_levels": [[99.9, 10]]}
        )
        self.assertEqual(fill["filled_lots"], 1)
        self.assertEqual(fill["fill_ratio"], 0.5)

    def test_unknown_finam_book_unit_does_not_infer_size_semantics(self):
        self.engine.paper_book_size_unit = "unknown"
        fill = self.engine._paper_exit_depth_fill(
            "SBER",
            {"side": "BUY", "volume": 20, "lots": 2, "lot_size": 10},
            {"bid_levels": [[99.9, 1000]]},
        )
        self.assertIsNone(fill)


class PaperMonitorBatchTests(unittest.IsolatedAsyncioTestCase):
    async def test_bad_ticker_event_does_not_discard_other_positions(self) -> None:
        engine = BaseExecutionEngine.__new__(BaseExecutionEngine)
        engine._pending_monitor_prices = {
            "BROKEN": ("not-a-price", "LAST_TRADE", None),
            "SBER": (100.0, "LAST_TRADE", "market-time"),
        }
        engine._monitor_worker_task = None
        engine.monitor_paper_positions = AsyncMock()
        db = object()

        await engine._drain_monitor_prices(db)

        engine.monitor_paper_positions.assert_awaited_once_with(
            "SBER", 100.0, db,
            event_source="LAST_TRADE",
            event_timestamp="market-time",
        )
        self.assertIsNone(engine._monitor_worker_task)




class PaperPartialExitTests(unittest.IsolatedAsyncioTestCase):
    def _engine(self):
        engine = BaseExecutionEngine.__new__(BaseExecutionEngine)
        engine.paper_mode = True
        engine.paper_book_size_unit = "shares"
        engine.paper_max_book_participation_pct = 100.0
        engine.paper_commission_pct = 0.1
        engine.paper_slippage_bps = 0.0
        engine.paper_positions = {}
        engine.closed_trades = []
        engine._last_bbo_exit_warning_at = {}
        return engine

    def _trade(self):
        return {
            "id": "PAPER_PARTIAL_TEST",
            "status": "OPEN",
            "side": "BUY",
            "entry_price": 100.0,
            "volume": 20,
            "lots": 2,
            "lot_size": 10,
            "entry_commission_rub": 2.0,
            "commission_pct_per_side": 0.1,
            "notional_entry_rub": 2000.0,
            "stop_loss": 99.0,
            "take_profit": 102.0,
            "pending_exit_reason": "TAKE_PROFIT",
        }

    def _bbo(self, bid_levels):
        return {
            "bid": 101.0,
            "ask": 101.1,
            "executable_price": 101.0,
            "bid_size": 10.0,
            "ask_size": 10.0,
            "bid_levels": bid_levels,
            "ask_levels": [[101.1, 100]],
            "age_sec": 0.1,
            "spread_pct": 0.1,
        }

    async def test_partial_exit_keeps_residual_open_and_persists_cost_allocation(self):
        engine = self._engine()
        trade = self._trade()
        db = AsyncMock()

        result = await engine._finalize_paper_close(
            "SBER", trade, db, self._bbo([[101.0, 10]]),
            exit_reason="TAKE_PROFIT", trigger_price=102.0,
        )

        self.assertIs(result, trade)
        self.assertEqual(trade["status"], "OPEN")
        self.assertEqual(trade["volume"], 10)
        self.assertEqual(trade["lots"], 1)
        self.assertAlmostEqual(trade["entry_commission_rub"], 1.0)
        self.assertEqual(trade["exit_partial_fill_count"], 1)
        self.assertAlmostEqual(trade["realized_gross_pnl_rub"], 10.0)
        self.assertEqual(trade["exit_fill_model"], "DEPTH_PARTIAL_FILL")
        db.set_json.assert_awaited_once()
        saved = db.set_json.await_args.args[1]
        self.assertEqual(saved["status"], "OPEN")
        self.assertEqual(saved["volume"], 10)

    async def test_missing_exit_depth_does_not_fabricate_a_full_fill(self):
        engine = self._engine()
        trade = self._trade()
        db = AsyncMock()

        result = await engine._finalize_paper_close(
            "SBER", trade, db, self._bbo([]),
            exit_reason="TAKE_PROFIT", trigger_price=102.0,
        )

        self.assertIs(result, trade)
        self.assertEqual(trade["status"], "OPEN")
        self.assertEqual(trade["volume"], 20)
        self.assertEqual(trade["exit_depth_last_fill_ratio"], 0.0)
        db.set_json.assert_not_awaited()

    async def test_final_depth_fill_closes_residual_and_aggregates_prior_partial(self):
        engine = self._engine()
        trade = self._trade()
        db = AsyncMock()
        engine.paper_positions["SBER"] = trade

        await engine._finalize_paper_close(
            "SBER", trade, db, self._bbo([[101.0, 10]]),
            exit_reason="TAKE_PROFIT", trigger_price=102.0,
        )
        partial_net = trade["realized_net_pnl_rub"]
        result = await engine._finalize_paper_close(
            "SBER", trade, db, self._bbo([[100.5, 10]]),
            exit_reason="TAKE_PROFIT", trigger_price=102.0,
        )

        self.assertEqual(result["status"], "CLOSED")
        self.assertEqual(result["volume"], 20)
        self.assertEqual(result["lots"], 2)
        self.assertEqual(result["exit_partial_fill_count"], 1)
        self.assertEqual(result["exit_fill_model"], "DEPTH_PARTIALS_PLUS_FINAL_DEPTH_FILL")
        self.assertAlmostEqual(result["exit_price"], 100.75)
        self.assertAlmostEqual(result["entry_commission_rub"], 2.0)
        self.assertAlmostEqual(result["pnl"], partial_net + (
            (100.5 - 100.0) * 10
            - 100.5 * 10 * 0.001
            - 1.0
        ), places=4)
        self.assertNotIn("SBER", engine.paper_positions)
        db.set_json.assert_awaited()
        db.push_to_list.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
