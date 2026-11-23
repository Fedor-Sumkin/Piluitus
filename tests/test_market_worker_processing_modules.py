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


def test_processing_facade_keeps_pipeline_phase_order_and_mixin_ownership() -> None:
    from runtime.market_worker_candidate_admission_runtime import MarketWorkerCandidateAdmissionMixin
    from runtime.market_worker_llm_candidate_runtime import MarketWorkerLlmCandidateMixin
    from runtime.market_worker_verdict_construction_runtime import MarketWorkerVerdictConstructionMixin
    from runtime.market_worker_verdict_diagnostics_runtime import MarketWorkerVerdictDiagnosticsMixin
    from runtime.market_worker_verdict_event_logging_runtime import MarketWorkerVerdictEventLoggingMixin
    from runtime.market_worker_verdict_record_runtime import MarketWorkerVerdictRecordMixin
    from runtime.market_worker_verdict_shadow_runtime import MarketWorkerVerdictShadowMixin
    from runtime.market_worker_verdict_publication_runtime import MarketWorkerVerdictPublicationMixin

    assert MarketWorkerProcessingRuntime._prepare_market_candidate_state is MarketWorkerCandidateAdmissionMixin._prepare_market_candidate_state
    assert MarketWorkerProcessingRuntime._analyze_candidate_and_prepare_entry is MarketWorkerLlmCandidateMixin._analyze_candidate_and_prepare_entry
    assert MarketWorkerProcessingRuntime._build_candidate_verdict is MarketWorkerVerdictConstructionMixin._build_candidate_verdict
    assert MarketWorkerProcessingRuntime._prepare_candidate_verdict_diagnostics is MarketWorkerVerdictDiagnosticsMixin._prepare_candidate_verdict_diagnostics
    assert MarketWorkerProcessingRuntime._create_candidate_verdict_record is MarketWorkerVerdictRecordMixin._create_candidate_verdict_record
    assert MarketWorkerProcessingRuntime._correlate_candidate_verdict_shadow is MarketWorkerVerdictShadowMixin._correlate_candidate_verdict_shadow
    assert MarketWorkerProcessingRuntime._schedule_candidate_verdict_event_log is MarketWorkerVerdictEventLoggingMixin._schedule_candidate_verdict_event_log
    assert MarketWorkerProcessingRuntime._publish_candidate_verdict is MarketWorkerVerdictPublicationMixin._publish_candidate_verdict

    assert issubclass(MarketWorkerVerdictConstructionMixin, MarketWorkerVerdictDiagnosticsMixin)
    assert issubclass(MarketWorkerVerdictConstructionMixin, MarketWorkerVerdictRecordMixin)
    assert issubclass(MarketWorkerVerdictConstructionMixin, MarketWorkerVerdictShadowMixin)
    assert issubclass(MarketWorkerVerdictConstructionMixin, MarketWorkerVerdictEventLoggingMixin)
