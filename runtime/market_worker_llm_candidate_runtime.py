"""LLM request, response normalization and post-LLM entry preparation."""
from __future__ import annotations

import asyncio
import json
import os
from uuid import uuid4

from runtime.market_worker_candidate_state import MarketWorkerCandidateState


class MarketWorkerLlmCandidateMixin:
    """Run the LLM contract and refresh executable evidence after its latency."""

    async def _analyze_candidate_and_prepare_entry(self, state: MarketWorkerCandidateState) -> bool:
        event_payload = state["event_payload"]
        ticker = state["ticker"]
        price = state["price"]
        metrics = state["metrics"]
        candidate_direction_for_diag = state["candidate_direction_for_diag"]
        llm_input_features = state["llm_input_features"]
        await self.attach_llm_rule_context(
            event_payload,
            db=self.db,
            ticker=ticker,
            candidate_direction=candidate_direction_for_diag,
            logger=self.logger,
        )

        self.logger.info(
            "🧭 [LLM INPUT] ticker=%s candidate=%s features=%s",
            ticker, candidate_direction_for_diag,
            json.dumps(llm_input_features, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
        )
        self.trade_flow_increment("llm_calls")
        try:
            ai_response = await asyncio.wait_for(
                self.ai.analyze_market_state(event_payload),
                timeout=float(Settings.VLLM_TIMEOUT) * 2.0 + 1.0,
            )
        except asyncio.TimeoutError as exc:
            ai_response = self.build_llm_error_response(
                exc, error_type="worker_timeout"
            )
        except Exception as exc:
            ai_response = self.build_llm_error_response(
                exc, error_type="worker_error"
            )

        if (
            getattr(execution_engine, "_maintenance_active", False)
            or event_payload.get("paper_session_generation") != self.session_generation_provider()
        ):
            return False

        ai_analysis, llm_state = self.process_llm_response(
            ai_response,
            candidate_direction=event_payload.get("candidate_direction", ""),
            ticker=ticker,
            increment_counter=self.trade_flow_increment,
            logger=self.logger,
        )
        request_error = llm_state["request_error"]
        request_error_type = llm_state["request_error_type"]
        model_decision = llm_state["model_decision"]
        candidate_direction = llm_state["candidate_direction"]
        model_reason_code = llm_state["model_reason_code"]
        candidate_decision = llm_state["candidate_decision"]
        model_contract_violation = llm_state["model_contract_violation"]
        decision = llm_state["decision"]
        confidence_valid = llm_state["confidence_valid"]
        confidence = llm_state["confidence"]
        candidate_confidence = llm_state["candidate_confidence"]

        # Rebuild executable BBO and trade-flow evidence after LLM latency.
        # Pre-LLM metrics remain available separately for attribution, but
        # stale quote sides must never authorize a new entry.
        post_llm_state = validate_post_llm_entry(
            screener=self.screener,
            ticker=ticker,
            metrics=metrics,
            decision=decision,
            candidate_direction=candidate_direction,
            paper_mode=bool(getattr(self.execution_engine, "paper_mode", False)),
            queue_flow_conflict_reason=self.queue_flow_conflict_reason,
            parse_finite_float=self.parse_finite_float,
        )
        evidence = post_llm_state["evidence"]
        post_llm_refresh = post_llm_state["post_llm_refresh"]
        refreshed_price = post_llm_state["refreshed_price"]
        if refreshed_price is not None:
            price = refreshed_price
        valid_evidence = post_llm_state["valid_evidence"]
        block_reason = post_llm_state["block_reason"]
        post_llm_features = post_llm_state["post_llm_features"]
        post_llm_evidence_validation = post_llm_state["post_llm_evidence_validation"]
        post_llm_evidence_reason = post_llm_state["post_llm_evidence_reason"]

        verdict_id = uuid4().hex
        entry_plan = self.prepare_entry_execution_plan(
            ticker=ticker,
            price=price,
            decision=decision,
            confidence=confidence,
            confidence_valid=confidence_valid,
            valid_evidence=valid_evidence,
            block_reason=block_reason,
            signal_reason=(
                f"Ошибка запроса к vLLM ({request_error_type}); вход запрещён."
                if request_error else
                "LLM нарушила контракт APPROVE/HOLD; вход запрещён."
                if model_contract_violation else ""
            ),
            metrics=metrics,
            ai_analysis=ai_analysis,
            lot_multiplier=self.lot_sizes.get(ticker, 0),
            execution_engine=self.execution_engine,
            risk_manager=self.risk_manager,
            paper_trading=bool(self.settings.PAPER_TRADING),
            increment_counter=self.trade_flow_increment,
            guard_block=self.trade_flow_guard_block,
            finite_float=self.finite_float,
            nonnegative_int=self.nonnegative_int,
            logger=self.logger,
            min_atr_periods_raw=os.getenv("PILUITUS_ATR_MIN_PERIODS", "5"),
            atr_multiplier_raw=os.getenv("ATR_MULTIPLIER", "1.5"),
        )
        decision = entry_plan["decision"]
        confidence = entry_plan["confidence"]
        signal_reason = entry_plan["signal_reason"]
        execution_status = entry_plan["execution_status"]
        execution_message = entry_plan["execution_message"]
        requested_lots = entry_plan["requested_lots"]
        requested_notional = entry_plan["requested_notional"]
        lot_multiplier = entry_plan["lot_multiplier"]

        state.update({
            "price": price, "ai_analysis": ai_analysis,
            "request_error": request_error, "request_error_type": request_error_type,
            "model_decision": model_decision, "candidate_direction": candidate_direction,
            "model_reason_code": model_reason_code, "candidate_decision": candidate_decision,
            "model_contract_violation": model_contract_violation, "decision": decision,
            "confidence_valid": confidence_valid, "confidence": confidence,
            "candidate_confidence": candidate_confidence, "evidence": evidence,
            "post_llm_refresh": post_llm_refresh, "valid_evidence": valid_evidence,
            "block_reason": block_reason, "post_llm_features": post_llm_features,
            "post_llm_evidence_validation": post_llm_evidence_validation,
            "post_llm_evidence_reason": post_llm_evidence_reason,
            "verdict_id": verdict_id, "signal_reason": signal_reason,
            "execution_status": execution_status, "execution_message": execution_message,
            "requested_lots": requested_lots, "requested_notional": requested_notional,
            "lot_multiplier": lot_multiplier,
        })
        return True
