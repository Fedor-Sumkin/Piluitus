import unittest


class _FakeRedis:
    def __init__(self, saved_balance):
        self.saved_balance = saved_balance
        self.values = {}
        self.writes = {}
        self.history = []

    async def get(self, key):
        if key == "piluitus:risk:persistent_balance":
            return self.saved_balance
        return self.values.get(key)

    async def set(self, key, value):
        self.writes[key] = value
        self.values[key] = value

    async def lrange(self, key, start, stop):
        if key == "piluitus:trade_history":
            return self.history[start: None if stop == -1 else stop + 1]
        return []


class _FakeDB:
    def __init__(self, redis):
        self.redis = redis

    async def get_active_positions(self):
        return {}


class RiskManagerDepositPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_legacy_redis_deposit_does_not_override_configured_deposit(self):
        from risk_manager import RiskManager

        redis = _FakeRedis("400000.00")
        manager = RiskManager(initial_deposit=20000.0)

        allowed = await manager.is_trading_allowed(_FakeDB(redis))

        self.assertTrue(allowed)
        self.assertEqual(manager.initial_deposit, 20000.0)
        self.assertEqual(manager.current_equity, 20000.0)
        self.assertEqual(redis.writes["piluitus:risk:persistent_balance"], "20000.00")

    async def test_missing_legacy_key_is_initialized_from_configured_deposit(self):
        from risk_manager import RiskManager

        redis = _FakeRedis(None)
        manager = RiskManager(initial_deposit=20000.0)

        allowed = await manager.is_trading_allowed(_FakeDB(redis))

        self.assertTrue(allowed)
        self.assertEqual(manager.initial_deposit, 20000.0)
        self.assertEqual(redis.writes["piluitus:risk:persistent_balance"], "20000.00")



    async def test_old_history_is_preserved_but_excluded_from_new_accounting_period(self):
        import json
        from datetime import datetime, timedelta

        from risk_manager import RiskManager

        period_start = datetime.now().astimezone() - timedelta(days=3)
        old_timestamp = (period_start - timedelta(days=2)).isoformat()
        new_timestamp = (period_start + timedelta(days=1)).isoformat()
        period = {
            "version": 1,
            "period_id": "test-period",
            "start_timestamp": period_start.isoformat(),
            "initial_deposit": 20000.0,
        }
        redis = _FakeRedis("20000.00")
        redis.values["piluitus:risk:accounting_period"] = json.dumps(period)
        redis.history = [
            json.dumps({"mode": "PAPER", "pnl": 500.0, "exit_timestamp": old_timestamp}),
            json.dumps({"mode": "PAPER", "pnl": -25.0, "exit_timestamp": new_timestamp}),
        ]
        manager = RiskManager(initial_deposit=20000.0)

        allowed = await manager.is_trading_allowed(_FakeDB(redis))

        self.assertTrue(allowed)
        self.assertEqual(manager.realized_pnl, -25.0)
        self.assertEqual(manager.current_equity, 19975.0)
        self.assertEqual(manager.accounting_period_start.date(), period_start.date())
        self.assertEqual(
            json.loads(redis.values["piluitus:risk:accounting_period"])["period_id"],
            "test-period",
        )

    async def test_missing_accounting_period_is_initialized_and_persisted(self):
        import json

        from risk_manager import RiskManager

        redis = _FakeRedis("20000.00")
        manager = RiskManager(initial_deposit=20000.0)

        allowed = await manager.is_trading_allowed(_FakeDB(redis))

        self.assertTrue(allowed)
        period = json.loads(redis.values["piluitus:risk:accounting_period"])
        self.assertEqual(period["initial_deposit"], 20000.0)
        self.assertEqual(period["reason"], "period_initialized")
        self.assertIsNotNone(manager.accounting_period_start)

if __name__ == "__main__":
    unittest.main()
