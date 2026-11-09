import os
import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from risk_manager import RiskManager


MSK = timezone(timedelta(hours=3))


class FakeRedis:
    def __init__(self):
        self.values = {}

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value):
        self.values[key] = value

    async def lrange(self, key, start, end):
        return []


class FakeDB:
    def __init__(self, trades):
        self.redis = FakeRedis()
        self._trades = trades

    async def get_all_daily_trades(self):
        return dict(self._trades)

    async def get_active_positions(self):
        return {}


def closed_loss(order_id, timestamp):
    return {
        "id": order_id,
        "order_id": order_id,
        "status": "CLOSED",
        "mode": "PAPER",
        "ticker": "TEST",
        "side": "BUY",
        "pnl": -1.0,
        "exit_timestamp": timestamp.isoformat(),
    }


class SessionLossStreakTests(unittest.IsolatedAsyncioTestCase):
    async def test_previous_day_losses_do_not_pause_first_trade_of_new_day(self):
        now = datetime.now(MSK)
        previous_day = now - timedelta(days=1, minutes=10)
        trades = {
            f"OLD_{i}": closed_loss(f"OLD_{i}", previous_day - timedelta(minutes=i))
            for i in range(10)
        }
        trades["TODAY_1"] = closed_loss("TODAY_1", now - timedelta(minutes=2))

        manager = RiskManager(
            initial_deposit=20000.0,
            loss_streak_3_pause_sec=600,
            loss_streak_5_pause_sec=1800,
        )
        db = FakeDB(trades)

        with patch.dict(os.environ, {"PAPER_TRADING": "True"}):
            allowed = await manager.is_trading_allowed(db)

        self.assertTrue(allowed)
        self.assertEqual(manager._loss_streak_count, 1)
        self.assertAlmostEqual(manager.realized_pnl, -11.0, places=2)
        self.assertGreater(manager.current_drawdown_pct, 0.0)

    async def test_force_refresh_bypasses_recent_allowed_cache(self):
        now = datetime.now(MSK)
        trades = {
            f"TODAY_{i}": closed_loss(
                f"TODAY_{i}", now - timedelta(minutes=5 - i)
            )
            for i in range(5)
        }
        manager = RiskManager(initial_deposit=20000.0)
        manager._cached_trading_allowed = True
        manager._last_risk_snapshot_monotonic = time.monotonic()
        manager._blocked_date = now.date()
        db = FakeDB(trades)

        with patch.dict(os.environ, {"PAPER_TRADING": "True"}):
            cached = await manager.is_trading_allowed(db)
            refreshed = await manager.is_trading_allowed(db, force_refresh=True)

        self.assertTrue(cached)
        self.assertFalse(refreshed)
        self.assertTrue(manager.block_trading)

    async def test_five_losses_in_current_session_still_trigger_pause(self):
        now = datetime.now(MSK)
        trades = {
            f"TODAY_{i}": closed_loss(
                f"TODAY_{i}", now - timedelta(minutes=5 - i)
            )
            for i in range(5)
        }
        manager = RiskManager(
            initial_deposit=20000.0,
            loss_streak_3_pause_sec=600,
            loss_streak_5_pause_sec=1800,
        )
        db = FakeDB(trades)

        with patch.dict(os.environ, {"PAPER_TRADING": "True"}):
            allowed = await manager.is_trading_allowed(db)

        self.assertFalse(allowed)
        self.assertEqual(manager._loss_streak_count, 5)
        self.assertTrue(manager.block_trading)
        self.assertEqual(manager._blocked_date, now.date())

        # The hard stop must remain active even after the timed pause would
        # otherwise have expired; only a new Moscow trading date resets it.
        allowed_again = await manager.is_trading_allowed(db, force_refresh=True)
        self.assertFalse(allowed_again)
        self.assertTrue(manager.block_trading)


if __name__ == "__main__":
    unittest.main()
