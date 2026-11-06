"""Regression tests for portfolio-wide Money Management limits."""
from __future__ import annotations

import asyncio
import json
import unittest
from datetime import datetime, timedelta

from execution_layer import BaseExecutionEngine
from risk_manager import RiskManager


class FakeRedis:
    def __init__(self, history: list[dict] | None = None) -> None:
        self.values: dict[str, str] = {}
        self.history = [json.dumps(row) for row in (history or [])]

    async def get(self, key: str):
        return self.values.get(key)

    async def set(self, key: str, value):
        self.values[key] = str(value)
        return True

    async def lrange(self, key: str, start: int, end: int):
        rows = self.history
        stop = None if end == -1 else end + 1
        return rows[start:stop]


class FakeDB:
    def __init__(self, trades: dict[str, dict], history: list[dict] | None = None) -> None:
        self.redis = FakeRedis(history)
        self.trades = trades

    async def get_all_daily_trades(self):
        return self.trades

    async def get_active_positions(self):
        return {}


def closed_trade(order_id: str, pnl: float, when: datetime) -> dict:
    return {
        "id": order_id,
        "status": "CLOSED",
        "mode": "PAPER",
        "ticker": "SBER",
        "side": "BUY",
        "entry_timestamp": (when - timedelta(minutes=1)).isoformat(),
        "exit_timestamp": when.isoformat(),
        "pnl": pnl,
    }


class RiskManagerMoneyManagementTests(unittest.IsolatedAsyncioTestCase):
    async def test_configured_deposit_overrides_stale_redis_balance(self) -> None:
        db = FakeDB({})
        db.redis.values["piluitus:risk:persistent_balance"] = "20000.00"
        manager = RiskManager(initial_deposit=40_000.0, daily_loss_limit=0.015)
        manager.open_pnl_provider = lambda: 0.0

        allowed = await manager.is_trading_allowed(db)

        self.assertTrue(allowed)
        self.assertEqual(manager.initial_deposit, 40_000.0)
        self.assertEqual(db.redis.values["piluitus:risk:persistent_balance"], "40000.00")

    async def test_daily_loss_uses_full_ledger_not_only_last_100_history_rows(self) -> None:
        now = datetime.now()
        rows = {
            f"order-{index}": closed_trade(
                f"order-{index}", 1.0 if index == 103 else -3.0,
                now - timedelta(seconds=104 - index),
            )
            for index in range(104)
        }
        # Simulate trade_history retaining only its newest 100 items.
        history = [rows[f"order-{index}"] for index in range(4, 104)]
        db = FakeDB(rows, history)
        manager = RiskManager(initial_deposit=20_000.0, daily_loss_limit=0.015)
        manager.open_pnl_provider = lambda: 0.0

        allowed = await manager.is_trading_allowed(db)

        self.assertFalse(allowed)
        self.assertAlmostEqual(manager.realized_pnl, -308.0)
        self.assertEqual(manager._blocked_date, now.date())

    async def test_high_water_drawdown_blocks_new_entries(self) -> None:
        yesterday = datetime.now() - timedelta(days=1)
        rows = {"old-loss": closed_trade("old-loss", -1_000.0, yesterday)}
        db = FakeDB(rows)
        manager = RiskManager(
            initial_deposit=20_000.0,
            daily_loss_limit=0.015,
            drawdown_warning_pct=0.03,
            max_drawdown_pct=0.05,
        )
        manager.open_pnl_provider = lambda: 0.0

        allowed = await manager.is_trading_allowed(db)

        self.assertFalse(allowed)
        self.assertAlmostEqual(manager.current_drawdown_pct, 0.05)
        self.assertEqual(db.redis.values["piluitus:risk:high_water_mark:PAPER"], "20000.00")

    async def test_hwm_reconstructs_previous_realized_equity_peak(self) -> None:
        now = datetime.now()
        two_days_ago = now - timedelta(days=2)
        yesterday = now - timedelta(days=1)
        rows = {
            "historical-win": closed_trade("historical-win", 1_200.0, two_days_ago),
            "historical-loss": closed_trade("historical-loss", -1_200.0, yesterday),
        }
        db = FakeDB(rows)
        manager = RiskManager(
            initial_deposit=20_000.0,
            daily_loss_limit=0.015,
            drawdown_warning_pct=0.03,
            max_drawdown_pct=0.05,
        )
        manager.open_pnl_provider = lambda: 0.0

        allowed = await manager.is_trading_allowed(db)

        self.assertFalse(allowed)
        self.assertAlmostEqual(manager.high_water_mark, 21_200.0)
        self.assertAlmostEqual(manager.current_equity, 20_000.0)
        self.assertAlmostEqual(manager.current_drawdown_pct, 1_200.0 / 21_200.0)


    async def test_three_consecutive_losses_start_global_pause(self) -> None:
        now = datetime.now()
        rows = {
            f"loss-{index}": closed_trade(
                f"loss-{index}", -1.0, now - timedelta(seconds=30 - index * 10)
            )
            for index in range(3)
        }
        db = FakeDB(rows)
        manager = RiskManager(initial_deposit=20_000.0, daily_loss_limit=0.015)
        manager.open_pnl_provider = lambda: 0.0

        allowed = await manager.is_trading_allowed(db)

        self.assertFalse(allowed)
        self.assertEqual(manager._loss_streak_count, 3)

    async def test_winning_trade_breaks_global_loss_streak(self) -> None:
        now = datetime.now()
        outcomes = [-1.0, -1.0, 2.0, -1.0, -1.0]
        rows = {
            f"trade-{index}": closed_trade(
                f"trade-{index}", pnl, now - timedelta(seconds=50 - index * 10)
            )
            for index, pnl in enumerate(outcomes)
        }
        db = FakeDB(rows)
        manager = RiskManager(initial_deposit=20_000.0, daily_loss_limit=0.015)
        manager.open_pnl_provider = lambda: 0.0

        allowed = await manager.is_trading_allowed(db)

        self.assertTrue(allowed)
        self.assertEqual(manager._loss_streak_count, 2)


class ExecutionStopRiskTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.engine = object.__new__(BaseExecutionEngine)
        self.engine.paper_mode = True
        self.engine._maintenance_active = False
        self.engine._maintenance_reason = ""
        self.engine._portfolio_lock = asyncio.Lock()
        self.engine._ticker_cooldowns = {}
        self.engine._opening_tickers = set()
        self.engine._unreconciled_paper_positions = {}
        self.engine._reserved_notional = {}
        self.engine._reserved_stop_risk = {}
        self.engine.initial_capital = 20_000.0
        self.engine.max_gross_exposure_pct = 0.10
        self.engine.max_single_position_pct = 0.03
        self.engine.max_open_stop_risk_pct = 0.01
        self.engine.paper_positions = {
            "EXIST": {
                "status": "OPEN",
                "ticker": "EXIST",
                "side": "BUY",
                "volume": 190,
                "entry_price": 2.60,
                "stop_loss": 1.60,
            }
        }

    async def test_single_position_cap_limits_each_order_to_three_percent(self) -> None:
        self.engine.paper_positions = {}

        approved, reason = await self.engine._reserve_order_size(
            "LSRG", 10, 100.0, object(), side="BUY", stop_loss=99.0
        )

        # 3% of 20,000 ₽ is 600 ₽. LSRG has a one-share lot.
        self.assertEqual(approved, 6)
        self.assertIn("single_cap=600.00 ₽", reason)
        self.assertAlmostEqual(self.engine._reserved_notional["LSRG"], 600.0)

    async def test_order_size_is_reduced_by_aggregate_stop_risk_budget(self) -> None:
        approved, reason = await self.engine._reserve_order_size(
            "SBER", 2, 10.0, object(), side="BUY", stop_loss=9.0
        )

        # Existing downside risk is 190 ₽ of a 200 ₽ budget.
        # One SBER lot adds 10 ₽ risk; two would exceed the portfolio cap.
        self.assertEqual(approved, 1)
        self.assertIn("open_stop_risk=190.00/200.00 ₽", reason)
        self.assertAlmostEqual(self.engine._reserved_stop_risk["SBER"], 10.0)

    async def test_unknown_existing_stop_risk_blocks_new_order(self) -> None:
        self.engine.paper_positions["BROKEN"] = {
            "status": "OPEN",
            "ticker": "BROKEN",
            "side": "BUY",
            "volume": 10,
            "entry_price": 10.0,
        }

        approved, reason = await self.engine._reserve_order_size(
            "SBER", 1, 10.0, object(), side="BUY", stop_loss=9.0
        )

        self.assertEqual(approved, 0)
        self.assertIn("суммарный риск открытых стопов", reason)


if __name__ == "__main__":
    unittest.main()
