"""Orchestrate the diagnostic, record, shadow and event-log verdict phases."""
from __future__ import annotations

from runtime.market_worker_candidate_state import MarketWorkerCandidateState
from runtime.market_worker_verdict_diagnostics_runtime import MarketWorkerVerdictDiagnosticsMixin
from runtime.market_worker_verdict_event_logging_runtime import MarketWorkerVerdictEventLoggingMixin
from runtime.market_worker_verdict_record_runtime import MarketWorkerVerdictRecordMixin
from runtime.market_worker_verdict_shadow_runtime import MarketWorkerVerdictShadowMixin


class MarketWorkerVerdictConstructionMixin(
    MarketWorkerVerdictDiagnosticsMixin,
    MarketWorkerVerdictRecordMixin,
    MarketWorkerVerdictShadowMixin,
    MarketWorkerVerdictEventLoggingMixin,
):
    """Compose the verdict-construction phases without changing their order."""

    def _build_candidate_verdict(self, state: MarketWorkerCandidateState) -> None:
        diagnostics = self._prepare_candidate_verdict_diagnostics(state)
        verdict_data = self._create_candidate_verdict_record(state, diagnostics)
        shadow_context = self._correlate_candidate_verdict_shadow(
            state, diagnostics, verdict_data
        )
        self._schedule_candidate_verdict_event_log(
            state, diagnostics, shadow_context
        )
        state.update({
            "verdict_data": verdict_data,
            "signal_status": shadow_context["signal_status"],
            "shadow_candidate_id": shadow_context["shadow_candidate_id"],
            "tactic_text": diagnostics["tactic_text"],
            "critic_text": diagnostics["critic_text"],
            "local_evidence_diagnostic": diagnostics["local_evidence_diagnostic"],
        })
