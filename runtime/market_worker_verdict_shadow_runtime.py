"""Correlate canonical verdicts with shadow candidates and AI analysis."""
from __future__ import annotations

from typing import Any

from runtime.market_worker_candidate_state import MarketWorkerCandidateState


class MarketWorkerVerdictShadowMixin:
    """Own shadow-candidate linkage and verdict-derived AI metadata."""

    def _correlate_candidate_verdict_shadow(
        self,
        state: MarketWorkerCandidateState,
        diagnostics: dict[str, Any],
        verdict_data: dict[str, Any],
    ) -> dict[str, Any]:
        event_payload = state["event_payload"]
        verdict_id = state["verdict_id"]
        model_decision = state["model_decision"]
        decision = state["decision"]
        candidate_confidence = state["candidate_confidence"]
        model_reason_code = state["model_reason_code"]
        execution_status = state["execution_status"]
        ticker = state["ticker"]
        signal_status = verdict_data["signal_status"]
        shadow_candidate_id = str(event_payload.get("shadow_candidate_id") or "")

        self.link_shadow_candidate_verdict(
            verdict_data,
            shadow_candidate_id=shadow_candidate_id,
            shadow_tracker=self.shadow_tracker,
            logger=self.logger,
            verdict_id=verdict_id,
            model_decision=model_decision,
            final_decision=decision,
            signal_status=signal_status,
            confidence=candidate_confidence,
            reason_code=model_reason_code,
            local_guard=diagnostics["local_evidence_diagnostic"],
            execution_status=execution_status,
            ticker=ticker,
        )
        self.enrich_ai_analysis_with_verdict(
            state["ai_analysis"],
            verdict_data=verdict_data,
            shadow_candidate_id=shadow_candidate_id,
            verdict_id=verdict_id,
            decision=decision,
            confidence=state["confidence"],
            confidence_valid=state["confidence_valid"],
            candidate_confidence=candidate_confidence,
            signal_status=signal_status,
            model_decision=model_decision,
            model_reason_code=model_reason_code,
            local_evidence_diagnostic=diagnostics["local_evidence_diagnostic"],
            candidate_decision=state["candidate_decision"],
            llm_input_features=state["llm_input_features"],
            post_llm_features=state["post_llm_features"],
            post_llm_evidence_validation=state["post_llm_evidence_validation"],
            post_llm_evidence_reason=state["post_llm_evidence_reason"],
            post_llm_refresh=state["post_llm_refresh"],
            metrics=state["metrics"],
            tactic_text=diagnostics["tactic_text"],
            critic_text=diagnostics["critic_text"],
        )
        return {
            "signal_status": signal_status,
            "shadow_candidate_id": shadow_candidate_id,
        }
