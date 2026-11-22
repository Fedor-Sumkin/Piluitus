"""Tests for the extracted market-worker composition boundary."""
from __future__ import annotations

import asyncio

import runtime.market_worker as market_worker_module
from runtime.market_worker import MarketWorkerFactory


def test_market_worker_factory_preserves_dependency_and_delegation_contract(monkeypatch):
    class FakeExecutionRuntime:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.calls = []

        async def execute_and_publish(self, **kwargs):
            self.calls.append(("execute", kwargs))

        async def execute_and_publish_locked(self, **kwargs):
            self.calls.append(("execute_locked", kwargs))

    class FakeProcessingRuntime:
        def __init__(self, **kwargs):
            self.kwargs = kwargs
            self.calls = []

        async def publish_pre_llm_risk_block(self, *args):
            self.calls.append(("risk_block", args))

        async def publish_pre_llm_risk_block_locked(self, *args):
            self.calls.append(("risk_block_locked", args))

        async def start_processing(self):
            self.calls.append(("start", ()))

    monkeypatch.setattr(
        market_worker_module, "MarketWorkerExecutionRuntime", FakeExecutionRuntime
    )
    monkeypatch.setattr(
        market_worker_module, "MarketWorkerProcessingRuntime", FakeProcessingRuntime
    )

    db, ai, broker = object(), object(), object()
    execution_engine, queue = object(), object()
    factory = MarketWorkerFactory(
        client_id="PILUITUS_PAPER",
        execution_runtime_dependencies={"execution_engine": execution_engine},
        processing_runtime_dependencies={"anomaly_queue": queue},
    )
    worker = factory.create(db, ai, broker)

    assert worker.db is db and worker.ai is ai and worker.broker is broker
    assert worker.execution_runtime.kwargs == {
        "execution_engine": execution_engine,
        "db": db,
        "client_id": "PILUITUS_PAPER",
    }
    assert worker.processing_runtime.kwargs["anomaly_queue"] is queue
    assert worker.processing_runtime.kwargs["db"] is db
    assert worker.processing_runtime.kwargs["ai"] is ai
    assert worker.processing_runtime.kwargs["execute_and_publish"].__self__ is worker

    payload = {"ticker": "SBER"}
    asyncio.run(worker._execute_and_publish(
        verdict_id="v1", ticker="SBER", lots=1, side="BUY", price=100.0,
        ai_analysis={}, session_generation=3,
    ))
    asyncio.run(worker._execute_and_publish_locked(
        verdict_id="v2", ticker="GAZP", lots=2, side="SELL", price=200.0,
        ai_analysis={}, session_generation=4,
    ))
    asyncio.run(worker._publish_pre_llm_risk_block(payload, 5))
    asyncio.run(worker._publish_pre_llm_risk_block_locked(payload))
    asyncio.run(worker.start_processing())

    assert [name for name, _ in worker.execution_runtime.calls] == [
        "execute", "execute_locked"
    ]
    assert [name for name, _ in worker.processing_runtime.calls] == [
        "risk_block", "risk_block_locked", "start"
    ]
