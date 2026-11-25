from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from execution_layer import BaseExecutionEngine


class PaperExitWatchdogTests(unittest.TestCase):
    def _engine(self):
        engine = BaseExecutionEngine.__new__(BaseExecutionEngine)
        engine.paper_mode = True
        engine._maintenance_active = False
        engine.paper_positions = {
            "SBER": {
                "id": "PAPER_LONG",
                "status": "OPEN",
                "side": "BUY",
                "ticker": "SBER",
            },
            "GAZP": {
                "id": "PAPER_SHORT",
                "status": "OPEN",
                "side": "SELL",
                "ticker": "GAZP",
            },
            "LKOH": {
                "id": "PAPER_CLOSED",
                "status": "CLOSED",
                "side": "BUY",
                "ticker": "LKOH",
            },
            "ROSN": {
                "id": "PAPER_BAD_SIDE",
                "status": "OPEN",
                "side": "HOLD",
                "ticker": "ROSN",
            },
        }
        engine._latest_fresh_exit_quote = Mock()
        engine.schedule_paper_position_monitor = Mock()
        return engine

    def test_watchdog_schedules_only_positions_with_fresh_executable_bbo(self):
        engine = self._engine()
        engine._latest_fresh_exit_quote.side_effect = [
            {"executable_price": 99.5},
            None,
        ]
        db = object()

        scheduled = engine.monitor_open_paper_positions_once(db)

        self.assertEqual(scheduled, 1)
        engine._latest_fresh_exit_quote.assert_any_call("SBER", "BUY")
        engine._latest_fresh_exit_quote.assert_any_call("GAZP", "SELL")
        engine.schedule_paper_position_monitor.assert_called_once_with(
            "SBER",
            99.5,
            db,
            event_source="WATCHDOG",
        )

    def test_watchdog_does_not_schedule_using_missing_or_stale_quote(self):
        engine = self._engine()
        engine._latest_fresh_exit_quote.return_value = None

        scheduled = engine.monitor_open_paper_positions_once(object())

        self.assertEqual(scheduled, 0)
        engine.schedule_paper_position_monitor.assert_not_called()

    def test_watchdog_is_disabled_outside_paper_mode_or_during_maintenance(self):
        engine = self._engine()
        engine.paper_mode = False
        self.assertEqual(engine.monitor_open_paper_positions_once(object()), 0)
        engine._latest_fresh_exit_quote.assert_not_called()

        engine.paper_mode = True
        engine._maintenance_active = True
        self.assertEqual(engine.monitor_open_paper_positions_once(object()), 0)
        engine._latest_fresh_exit_quote.assert_not_called()


class PaperWatchdogEventSourceTests(unittest.IsolatedAsyncioTestCase):
    async def test_watchdog_source_does_not_latch_a_last_trade_exit_trigger(self):
        engine = BaseExecutionEngine.__new__(BaseExecutionEngine)
        engine.paper_mode = True
        engine._maintenance_active = False
        trade = {
            "id": "PAPER_WATCHDOG",
            "status": "OPEN",
            "side": "BUY",
            "entry_price": 100.0,
            "stop_loss": 99.0,
            "take_profit": 102.0,
        }
        engine.paper_positions = {"SBER": trade}
        engine._pending_monitor_prices = {}
        engine._monitor_worker_task = Mock()
        engine._monitor_worker_task.done.return_value = False
        engine._capture_paper_exit_trigger = Mock()
        db = object()

        with patch("execution.execution_layer.asyncio.create_task") as create_task:
            engine.schedule_paper_position_monitor(
                "SBER",
                98.0,
                db,
                event_source="WATCHDOG",
            )

        self.assertEqual(
            engine._pending_monitor_prices["SBER"],
            (98.0, "WATCHDOG", None),
        )
        engine._capture_paper_exit_trigger.assert_not_called()
        create_task.assert_not_called()


if __name__ == "__main__":
    unittest.main()
