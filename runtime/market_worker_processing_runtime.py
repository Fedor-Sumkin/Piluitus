"""Compatibility facade for the market-worker processing runtime."""
from __future__ import annotations

from runtime.market_worker_processing_base import MarketWorkerProcessingBase
from runtime.market_worker_queue_processing_runtime import MarketWorkerQueueProcessingMixin
from runtime.market_worker_risk_verdict_runtime import MarketWorkerRiskVerdictMixin


class MarketWorkerProcessingRuntime(
    MarketWorkerQueueProcessingMixin,
    MarketWorkerRiskVerdictMixin,
    MarketWorkerProcessingBase,
):
    """Stable public runtime combining queue, risk-verdict and state components."""

    __init__ = MarketWorkerProcessingBase.__init__
