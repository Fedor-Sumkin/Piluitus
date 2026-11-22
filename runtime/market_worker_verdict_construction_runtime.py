"""Build auditable, locally grounded market verdicts from the LLM response."""
from __future__ import annotations

import json
from datetime import datetime

from runtime.market_worker_candidate_state import MarketWorkerCandidateState


class MarketWorkerVerdictConstructionMixin:
    """Construct, correlate and enrich verdict records before publication."""

    def _build_candidate_verdict(self, state: MarketWorkerCandidateState) -> None:
        event_payload = state["event_payload"]
        ticker = state["ticker"]
        price = state["price"]
        metrics = state["metrics"]
        candidate_direction_for_diag = state["candidate_direction_for_diag"]
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
        post_llm_refresh = state["post_llm_refresh"]
        valid_evidence = state["valid_evidence"]
        block_reason = state["block_reason"]
        post_llm_features = state["post_llm_features"]
        post_llm_evidence_validation = state["post_llm_evidence_validation"]
        post_llm_evidence_reason = state["post_llm_evidence_reason"]
        verdict_id = state["verdict_id"]
        signal_reason = state["signal_reason"]
        execution_status = state["execution_status"]
        execution_message = state["execution_message"]
        requested_lots = state["requested_lots"]
        requested_notional = state["requested_notional"]
        # Do not expose free-form LLM reasoning as evidence. User-facing text
        # is generated only from locally measured values and guard outcomes.
        signal_reason = self.build_default_signal_reason(
            decision=decision,
            candidate_decision=candidate_decision,
            confidence_valid=confidence_valid,
            signal_reason=signal_reason,
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

        verdict_data = self.build_llm_verdict_record(
            verdict_id=verdict_id,
            event_payload=event_payload,
            now_local=datetime.now().astimezone(),
            ticker=ticker,
            price=price,
            decision=decision,
            candidate_decision=candidate_decision,
            model_decision=model_decision,
            candidate_confidence=candidate_confidence,
            confidence=confidence,
            confidence_valid=confidence_valid,
            signal_reason=signal_reason,
            model_reason_code=model_reason_code,
            execution_status=execution_status,
            execution_message=execution_message,
            requested_lots=requested_lots,
            requested_notional=requested_notional,
            tactic_text=tactic_text,
            critic_text=critic_text,
            local_evidence_diagnostic=local_evidence_diagnostic,
            llm_input_features=llm_input_features,
            post_llm_features=post_llm_features,
            post_llm_evidence_validation=post_llm_evidence_validation,
            post_llm_evidence_reason=post_llm_evidence_reason,
            post_llm_refresh=post_llm_refresh,
            ai_analysis=ai_analysis,
        )
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
            local_guard=local_evidence_diagnostic,
            execution_status=execution_status,
            ticker=ticker,
        )
        self.enrich_ai_analysis_with_verdict(
            ai_analysis,
            verdict_data=verdict_data,
            shadow_candidate_id=shadow_candidate_id,
            verdict_id=verdict_id,
            decision=decision,
            confidence=confidence,
            confidence_valid=confidence_valid,
            candidate_confidence=candidate_confidence,
            signal_status=signal_status,
            model_decision=model_decision,
            model_reason_code=model_reason_code,
            local_evidence_diagnostic=local_evidence_diagnostic,
            candidate_decision=candidate_decision,
            llm_input_features=llm_input_features,
            post_llm_features=post_llm_features,
            post_llm_evidence_validation=post_llm_evidence_validation,
            post_llm_evidence_reason=post_llm_evidence_reason,
            post_llm_refresh=post_llm_refresh,
            metrics=metrics,
            tactic_text=tactic_text,
            critic_text=critic_text,
        )

        self.background_operations.spawn(
            self.log_trading_event(
                ticker=ticker,
                price=price,
                decision=f"ВЕРДИКТ ИИ: {decision} (кандидат={candidate_decision}, исполнение={execution_status})",
                confidence=round(candidate_confidence, 1) if candidate_confidence is not None else 0.0,
                tactic=tactic_text,
                critic=critic_text,
                candidate_id=shadow_candidate_id or None,
                verdict_id=verdict_id,
                model_confidence=candidate_confidence,
                final_confidence=round(confidence, 1) if confidence_valid else None,
            ),
            name=f"trading-log-verdict:{ticker}:{verdict_id}",
        )

        state.update({
            "verdict_data": verdict_data, "signal_status": signal_status,
            "shadow_candidate_id": shadow_candidate_id, "tactic_text": tactic_text,
            "critic_text": critic_text, "local_evidence_diagnostic": local_evidence_diagnostic,
        })
