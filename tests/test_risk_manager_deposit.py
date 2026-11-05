import unittest


class _FakeRedis:
    def __init__(self, saved_balance):
        self.saved_balance = saved_balance
        self.values = {}
        self.writes = {}

    async def get(self, key):
        if key == "piluitus:risk:persistent_balance":
            return self.saved_balance
        return self.values.get(key)

    async def set(self, key, value):
        self.writes[key] = value
        self.values[key] = value

    async def lrange(self, key, start, stop):
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


if __name__ == "__main__":
    unittest.main()
