from __future__ import annotations

import unittest
from unittest.mock import Mock, patch

from execution_layer import BaseExecutionEngine, _timestamp_age_seconds


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
        engine._latest_price = Mock(return_value=98.75)
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
            98.75,
            db,
            event_source="WATCHDOG",
        )

    def test_watchdog_does_not_schedule_using_missing_or_stale_quote(self):
        engine = self._engine()
        engine._latest_fresh_exit_quote.return_value = None

        scheduled = engine.monitor_open_paper_positions_once(object())

        self.assertEqual(scheduled, 0)
        engine.schedule_paper_position_monitor.assert_not_called()

    def test_watchdog_logs_detailed_bbo_gap_once_per_throttle_window(self):
        from types import SimpleNamespace
        engine = self._engine()
        engine.paper_positions = {"SBER": engine.paper_positions["SBER"]}
        engine.paper_exit_status_log_interval_sec = 300.0
        engine._last_bbo_exit_warning_at = {}
        now = 25.0  # Simulate a freshly booted Windows host.
        engine.screener = SimpleNamespace(
            quotes={
                "SBER": {
                    "bid": 99.5,
                    "ask": 99.6,
                    "last_bid_confirmed_seen": now - 10.0,
                    "last_ask_confirmed_seen": now - 0.1,
                    "last_order_book_seen": now - 10.0,
                    "order_book_valid": False,
                    "order_book_last_reason": "WAITING_FOR_SNAPSHOT",
                }
            },
            bbo_max_age_sec=2.0,
        )
        engine._latest_fresh_exit_quote.return_value = None

        with patch("execution.execution_layer.time.monotonic", return_value=now), patch(
            "execution.execution_layer.logger.warning"
        ) as warning:
            self.assertEqual(engine.monitor_open_paper_positions_once(object()), 0)
            self.assertEqual(engine.monitor_open_paper_positions_once(object()), 0)

        self.assertEqual(
            engine.paper_positions["SBER"]["exit_watchdog_retry_count"], 2
        )
        self.assertIsNotNone(
            engine.paper_positions["SBER"]["exit_watchdog_last_check_at"]
        )
        warning.assert_called_once()
        message = warning.call_args.args[0]
        self.assertIn("[PAPER EXIT WATCHDOG WAIT]", message)
        self.assertIn("book_reason=%s", message)
        self.assertIn("фиктивное исполнение", message)

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



def test_persisted_exit_wait_timestamp_survives_monotonic_clock_reset():
    from datetime import datetime, timedelta, timezone

    current = datetime(2026, 11, 28, 12, 0, 10, tzinfo=timezone.utc)
    started = (current - timedelta(seconds=12)).isoformat()
    assert _timestamp_age_seconds(started, now=current) == 12.0
    assert _timestamp_age_seconds("invalid timestamp", now=current) is None
