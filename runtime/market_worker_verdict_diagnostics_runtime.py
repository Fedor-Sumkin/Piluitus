"""Create locally grounded explanations and structured candidate diagnostics."""
from __future__ import annotations

import json
from typing import Any

from runtime.market_worker_candidate_state import MarketWorkerCandidateState


class MarketWorkerVerdictDiagnosticsMixin:
    """Own diagnostic classification and the structured LLM decision log."""

    def _prepare_candidate_verdict_diagnostics(
        self, state: MarketWorkerCandidateState
    ) -> dict[str, Any]:
        event_payload = state["event_payload"]
        ticker = state["ticker"]
        metrics = state["metrics"]
        llm_input_features = state["llm_input_features"]
        ai_analysis = state["ai_analysis"]
        request_error = state["request_error"]
        request_error_type = state["request_error_type"]
        model_decision = state["model_decision"]
        candidate_direction = state["candidate_direction"]
        model_reason_code = state["model_reason_code"]
        candidate_decision = state["candidate_decision"]
        model_contract_violation = state["model_contract_violation"]
        decision = state["decision"]
        confidence_valid = state["confidence_valid"]
        confidence = state["confidence"]
        candidate_confidence = state["candidate_confidence"]
        evidence = state["evidence"]
        block_reason = state["block_reason"]
        post_llm_features = state["post_llm_features"]
        post_llm_evidence_validation = state["post_llm_evidence_validation"]
        post_llm_evidence_reason = state["post_llm_evidence_reason"]
        post_llm_refresh = state["post_llm_refresh"]
        verdict_id = state["verdict_id"]
        signal_reason = self.build_default_signal_reason(
            decision=decision,
            candidate_decision=candidate_decision,
            confidence_valid=confidence_valid,
            signal_reason=state["signal_reason"],
            model_decision=model_decision,
        )
        tactic_text, critic_text = self.build_verdict_explanation(
            decision, candidate_decision, evidence, signal_reason
        )
        local_evidence_diagnostic = self.classify_local_evidence_diagnostic(
            block_reason=block_reason,
            candidate_direction=candidate_direction,
            request_error=request_error,
            request_error_type=request_error_type,
            model_contract_violation=model_contract_violation,
            model_decision=model_decision,
            model_reason_code=model_reason_code,
            confidence_valid=confidence_valid,
            decision=decision,
            confidence=confidence,
        )

        # Do not expose free-form LLM reasoning as evidence. User-facing text
        # is generated only from locally measured values and guard outcomes.
        self.logger.info(
            "[LLM DECISION] %s candidate=%s candidate_id=%s verdict_id=%s model=%s final=%s "
            "model_confidence=%s final_confidence=%s confidence_valid=%s "
            "contract_violation=%s model_reason_code=%s model_reason_raw=%s "
            "reason_validation=%s m1=%s direction_gate=%s prompt_chars=%s prompt_fingerprint=%s "
            "reason=%s local_evidence=%s pre_llm_features=%s post_llm_features=%s "
            "post_llm_evidence_validation=%s post_llm_evidence_reason=%s post_llm_refresh=%s",
            ticker,
            candidate_direction or "NONE",
            str(event_payload.get("shadow_candidate_id") or "NONE"),
            verdict_id,
            model_decision,
            decision,
            f"{candidate_confidence:.1f}%" if candidate_confidence is not None else "invalid",
            f"{confidence:.1f}%" if confidence_valid else "invalid",
            confidence_valid,
            model_contract_violation,
            model_reason_code,
            ai_analysis.get("model_reason_code_raw", "NOT_PROVIDED"),
            ai_analysis.get("model_reason_validation", "UNKNOWN"),
            ai_analysis.get("m1_evidence_state", "UNAVAILABLE"),
            metrics.get("direction_evidence_state", "UNAVAILABLE"),
            ai_analysis.get("prompt_chars", "N/A"),
            ai_analysis.get("prompt_fingerprint", "N/A"),
            signal_reason or ("APPROVED" if decision in {"BUY", "SELL"} else "MODEL_HOLD"),
            local_evidence_diagnostic,
            json.dumps(llm_input_features, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            json.dumps(post_llm_features, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            post_llm_evidence_validation,
            post_llm_evidence_reason,
            json.dumps(post_llm_refresh, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        )
        return {
            "signal_reason": signal_reason,
            "tactic_text": tactic_text,
            "critic_text": critic_text,
            "local_evidence_diagnostic": local_evidence_diagnostic,
        }
