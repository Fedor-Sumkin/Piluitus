"""Contracts for the split market-worker processing runtime."""
from __future__ import annotations

import inspect

from runtime.market_worker_processing_base import MarketWorkerProcessingBase
from runtime.market_worker_processing_runtime import MarketWorkerProcessingRuntime
from runtime.market_worker_queue_processing_runtime import MarketWorkerQueueProcessingMixin
from runtime.market_worker_risk_verdict_runtime import MarketWorkerRiskVerdictMixin


def test_processing_facade_keeps_constructor_and_method_ownership() -> None:
    assert MarketWorkerProcessingRuntime.__init__ is MarketWorkerProcessingBase.__init__
    assert (
        inspect.signature(MarketWorkerProcessingRuntime.__init__)
        == inspect.signature(MarketWorkerProcessingBase.__init__)
    )
    assert (
        MarketWorkerProcessingRuntime.process_next_item
        is MarketWorkerQueueProcessingMixin.process_next_item
    )
    assert (
        MarketWorkerProcessingRuntime.start_processing
        is MarketWorkerQueueProcessingMixin.start_processing
    )
    assert (
        MarketWorkerProcessingRuntime.publish_pre_llm_risk_block
        is MarketWorkerRiskVerdictMixin.publish_pre_llm_risk_block
    )
    assert (
        MarketWorkerProcessingRuntime.publish_pre_llm_risk_block_locked
        is MarketWorkerRiskVerdictMixin.publish_pre_llm_risk_block_locked
    )


def test_processing_facade_preserves_all_runtime_mixins() -> None:
    assert issubclass(MarketWorkerProcessingRuntime, MarketWorkerQueueProcessingMixin)
    assert issubclass(MarketWorkerProcessingRuntime, MarketWorkerRiskVerdictMixin)
    assert issubclass(MarketWorkerProcessingRuntime, MarketWorkerProcessingBase)
