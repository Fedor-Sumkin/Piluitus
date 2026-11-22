"""Composition boundary for the market worker and its processing runtimes.

The factory owns dependency wiring; the worker preserves the established
execution and processing delegation contract without owning trading policy.
"""
from __future__ import annotations

from typing import Any, Mapping

from execution.market_worker_execution_runtime import MarketWorkerExecutionRuntime
from runtime.market_worker_processing_runtime import MarketWorkerProcessingRuntime


class MarketWorker:
    """Thin worker facade delegating execution and AI processing to runtimes."""

    def __init__(
        self,
        db: Any,
        ai: Any,
        broker_client: Any,
        *,
        client_id: str,
        execution_runtime_dependencies: Mapping[str, Any],
        processing_runtime_dependencies: Mapping[str, Any],
    ) -> None:
        self.db = db
        self.ai = ai
        self.broker = broker_client
        self.client_id = client_id

        execution_dependencies = dict(execution_runtime_dependencies)
        execution_dependencies.update(db=self.db, client_id=self.client_id)
        self.execution_runtime = MarketWorkerExecutionRuntime(**execution_dependencies)

        processing_dependencies = dict(processing_runtime_dependencies)
        processing_dependencies.update(
            db=self.db,
            ai=self.ai,
            execute_and_publish=self._execute_and_publish,
        )
        self.processing_runtime = MarketWorkerProcessingRuntime(**processing_dependencies)

    async def _execute_and_publish(
        self,
        *,
        verdict_id: str,
        ticker: str,
        lots: int,
        side: str,
        price: float,
        ai_analysis: dict,
        session_generation: int,
        candidate_created_monotonic: object | None = None,
        shadow_candidate_id: str | None = None,
    ) -> None:
        await self.execution_runtime.execute_and_publish(
            verdict_id=verdict_id,
            ticker=ticker,
            lots=lots,
            side=side,
            price=price,
            ai_analysis=ai_analysis,
            session_generation=session_generation,
            candidate_created_monotonic=candidate_created_monotonic,
            shadow_candidate_id=shadow_candidate_id,
        )

    async def _execute_and_publish_locked(
        self,
        *,
        verdict_id: str,
        ticker: str,
        lots: int,
        side: str,
        price: float,
        ai_analysis: dict,
        candidate_created_monotonic: object | None = None,
        shadow_candidate_id: str | None = None,
    ) -> None:
        await self.execution_runtime.execute_and_publish_locked(
            verdict_id=verdict_id,
            ticker=ticker,
            lots=lots,
            side=side,
            price=price,
            ai_analysis=ai_analysis,
            candidate_created_monotonic=candidate_created_monotonic,
            shadow_candidate_id=shadow_candidate_id,
        )

    async def _publish_pre_llm_risk_block(
        self, event_payload: dict, session_generation: int
    ) -> None:
        await self.processing_runtime.publish_pre_llm_risk_block(
            event_payload, session_generation
        )

    async def _publish_pre_llm_risk_block_locked(self, event_payload: dict) -> None:
        await self.processing_runtime.publish_pre_llm_risk_block_locked(event_payload)

    async def start_processing(self) -> None:
        await self.processing_runtime.start_processing()


class MarketWorkerFactory:
    """Create workers with one shared, explicit dependency configuration."""

    def __init__(
        self,
        *,
        client_id: str,
        execution_runtime_dependencies: Mapping[str, Any],
        processing_runtime_dependencies: Mapping[str, Any],
    ) -> None:
        self.client_id = str(client_id)
        self.execution_runtime_dependencies = dict(execution_runtime_dependencies)
        self.processing_runtime_dependencies = dict(processing_runtime_dependencies)

    def create(self, db: Any, ai: Any, broker_client: Any) -> MarketWorker:
        return MarketWorker(
            db,
            ai,
            broker_client,
            client_id=self.client_id,
            execution_runtime_dependencies=self.execution_runtime_dependencies,
            processing_runtime_dependencies=self.processing_runtime_dependencies,
        )
