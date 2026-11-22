"""Compatibility facade for the market-worker processing runtime."""
from __future__ import annotations

from runtime.market_worker_candidate_admission_runtime import MarketWorkerCandidateAdmissionMixin
from runtime.market_worker_llm_candidate_runtime import MarketWorkerLlmCandidateMixin
from runtime.market_worker_processing_base import MarketWorkerProcessingBase
from runtime.market_worker_queue_processing_runtime import MarketWorkerQueueProcessingMixin
from runtime.market_worker_risk_verdict_runtime import MarketWorkerRiskVerdictMixin
from runtime.market_worker_verdict_construction_runtime import MarketWorkerVerdictConstructionMixin
from runtime.market_worker_verdict_publication_runtime import MarketWorkerVerdictPublicationMixin


class MarketWorkerProcessingRuntime(
    MarketWorkerQueueProcessingMixin,
    MarketWorkerCandidateAdmissionMixin,
    MarketWorkerLlmCandidateMixin,
    MarketWorkerVerdictConstructionMixin,
    MarketWorkerVerdictPublicationMixin,
    MarketWorkerRiskVerdictMixin,
    MarketWorkerProcessingBase,
):
    """Stable public runtime combining candidate, LLM, verdict and queue phases."""

    __init__ = MarketWorkerProcessingBase.__init__
