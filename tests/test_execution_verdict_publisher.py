import asyncio
import json
from types import SimpleNamespace

from execution.verdict_publisher import publish_market_verdict


class FakeRedis:
    def __init__(self):
        self.items = []
        self.trim = None

    async def lpush(self, key, value):
        self.items.insert(0, (key, value))

    async def ltrim(self, key, start, end):
        self.trim = (key, start, end)


class FakeManager:
    def __init__(self):
        self.messages = []

    async def broadcast(self, message):
        self.messages.append(json.loads(message))


class FakeLogger:
    def __init__(self):
        self.errors = []

    def error(self, *args, **kwargs):
        self.errors.append((args, kwargs))


def _publish(verdict, *, db=None, manager=None, cache=None, max_size=2):
    manager = manager or FakeManager()
    logger = FakeLogger()
    cache = cache if cache is not None else []
    result = asyncio.run(
        publish_market_verdict(
            verdict,
            db=db or SimpleNamespace(),
            verdict_cache=cache,
            max_cache_size=max_size,
            manager=manager,
            balance_snapshot={"cash": 1234.5},
            logger=logger,
            candidate_id="candidate-1",
        )
    )
    return result, cache, manager, logger


def test_publisher_appends_cache_persists_and_broadcasts_verdict():
    redis = FakeRedis()
    verdict = {"verdict_id": "v-1", "ticker": "SBER", "decision": "BUY"}
    result, cache, manager, logger = _publish(
        verdict, db=SimpleNamespace(redis=redis)
    )

    assert result is verdict
    assert cache == [verdict]
    assert json.loads(redis.items[0][1]) == verdict
    assert redis.trim == ("piluitus:verdicts:history", 0, 1)
    assert manager.messages == [{
        "type": "MARKET_UPDATE",
        **verdict,
        "balance": {"cash": 1234.5},
    }]
    assert logger.errors == []


def test_publisher_keeps_cache_bounded():
    cache = [{"verdict_id": "old-1"}, {"verdict_id": "old-2"}]
    verdict = {"verdict_id": "new"}
    _, updated, _, _ = _publish(verdict, cache=cache)

    assert updated == [{"verdict_id": "old-2"}, verdict]


def test_publisher_broadcasts_when_storage_fails():
    class BrokenRedis:
        async def lpush(self, *args):
            raise RuntimeError("storage unavailable")

    _, cache, manager, logger = _publish(
        {"verdict_id": "v-2", "ticker": "SBER"},
        db=SimpleNamespace(redis=BrokenRedis()),
    )

    assert len(cache) == 1
    assert len(manager.messages) == 1
    assert len(logger.errors) == 1


def test_publisher_isolates_websocket_failure():
    class BrokenManager:
        async def broadcast(self, message):
            raise RuntimeError("websocket unavailable")

    _, cache, _, logger = _publish(
        {"verdict_id": "v-3", "ticker": "SBER"},
        db=SimpleNamespace(redis=FakeRedis()),
        manager=BrokenManager(),
    )

    assert len(cache) == 1
    assert len(logger.errors) == 1


def test_publisher_handles_missing_storage_client():
    _, cache, manager, logger = _publish(
        {"verdict_id": "v-4", "ticker": "SBER"}
    )

    assert len(cache) == 1
    assert len(manager.messages) == 1
    assert "remains only in memory" in logger.errors[0][0][0]
