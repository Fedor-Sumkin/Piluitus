import asyncio
import json
from types import SimpleNamespace

from execution.outcome_publisher import publish_execution_outcome


class FakeRedis:
    def __init__(self):
        self.values = {}

    async def set(self, key, value, *, ex):
        self.values[key] = (value, ex)


class FakeManager:
    def __init__(self):
        self.messages = []

    async def broadcast(self, message):
        self.messages.append(json.loads(message))


class FakeLogger:
    def __init__(self):
        self.info_calls = []
        self.warning_calls = []
        self.error_calls = []

    def info(self, *args):
        self.info_calls.append(args)

    def warning(self, *args):
        self.warning_calls.append(args)

    def error(self, *args, **kwargs):
        self.error_calls.append((args, kwargs))


def _run(update, *, redis=None, manager=None, positions=None):
    cache = [{"verdict_id": update["verdict_id"], "decision": "BUY"}]
    manager = manager or FakeManager()
    logger = FakeLogger()
    engine = SimpleNamespace(closed_trades=[{"id": i} for i in range(35)])

    async def get_positions():
        return positions if positions is not None else [{"ticker": "SBER"}]

    async def refresh_balance(items):
        return {"cash": 1000.0, "position_count": len(items)}

    result = asyncio.run(
        publish_execution_outcome(
            update,
            db=SimpleNamespace(redis=redis) if redis is not None else SimpleNamespace(),
            verdict_cache=cache,
            get_open_positions=get_positions,
            refresh_balance_snapshot=refresh_balance,
            manager=manager,
            execution_engine=engine,
            logger=logger,
            side="BUY",
            ticker="SBER",
            status="OPENED",
            actual_lots=2,
            actual_notional=620.0,
            order_id="paper-1",
        )
    )
    return result, cache, manager, logger


def test_publisher_updates_cache_persists_and_broadcasts_both_events():
    redis = FakeRedis()
    update = {"verdict_id": "v-1", "execution_status": "OPENED"}
    result, cache, manager, logger = _run(update, redis=redis)

    assert cache[0]["execution_status"] == "OPENED"
    assert redis.values["piluitus:verdict:execution:v-1"][1] == 7 * 24 * 60 * 60
    persisted = json.loads(redis.values["piluitus:verdict:execution:v-1"][0])
    assert persisted["verdict_id"] == "v-1"
    assert [item["type"] for item in manager.messages] == [
        "EXECUTION_UPDATE",
        "TABLES_UPDATE",
    ]
    assert manager.messages[0]["balance"]["cash"] == 1000.0
    assert len(manager.messages[1]["closed_trades"]) == 30
    assert len(logger.info_calls) == 1
    assert result is update


def test_publisher_continues_to_broadcast_when_storage_fails():
    class BrokenRedis:
        async def set(self, *args, **kwargs):
            raise RuntimeError("storage unavailable")

    _, _, manager, logger = _run(
        {"verdict_id": "v-2", "execution_status": "BLOCKED"},
        redis=BrokenRedis(),
    )
    assert len(manager.messages) == 2
    assert len(logger.warning_calls) == 1


def test_publisher_isolates_dashboard_broadcast_failure():
    class BrokenManager:
        async def broadcast(self, message):
            raise RuntimeError("websocket unavailable")

    _, cache, _, logger = _run(
        {"verdict_id": "v-3", "execution_status": "OPENED"},
        manager=BrokenManager(),
    )
    assert cache[0]["execution_status"] == "OPENED"
    assert len(logger.error_calls) == 1
    assert len(logger.info_calls) == 1


def test_publisher_handles_missing_redis_connection():
    update = {"verdict_id": "v-4", "execution_status": "BLOCKED"}
    _, cache, manager, _ = _run(update)
    assert cache[0]["execution_status"] == "BLOCKED"
    assert len(manager.messages) == 2
