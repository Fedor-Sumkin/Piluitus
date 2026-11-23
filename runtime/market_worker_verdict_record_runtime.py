"""Create the persisted verdict record from locally validated candidate state."""
from __future__ import annotations

from datetime import datetime
from typing import Any

from runtime.market_worker_candidate_state import MarketWorkerCandidateState


class MarketWorkerVerdictRecordMixin:
    """Own construction of the canonical, auditable verdict record."""

    def _create_candidate_verdict_record(
        self,
        state: MarketWorkerCandidateState,
        diagnostics: dict[str, Any],
    ) -> dict[str, Any]:
        return self.build_llm_verdict_record(
            verdict_id=state["verdict_id"],
            event_payload=state["event_payload"],
            now_local=datetime.now().astimezone(),
            ticker=state["ticker"],
            price=state["price"],
            decision=state["decision"],
            candidate_decision=state["candidate_decision"],
            model_decision=state["model_decision"],
            candidate_confidence=state["candidate_confidence"],
            confidence=state["confidence"],
            confidence_valid=state["confidence_valid"],
            signal_reason=diagnostics["signal_reason"],
            model_reason_code=state["model_reason_code"],
            execution_status=state["execution_status"],
            execution_message=state["execution_message"],
            requested_lots=state["requested_lots"],
            requested_notional=state["requested_notional"],
            tactic_text=diagnostics["tactic_text"],
            critic_text=diagnostics["critic_text"],
            local_evidence_diagnostic=diagnostics["local_evidence_diagnostic"],
            llm_input_features=state["llm_input_features"],
            post_llm_features=state["post_llm_features"],
            post_llm_evidence_validation=state["post_llm_evidence_validation"],
            post_llm_evidence_reason=state["post_llm_evidence_reason"],
            post_llm_refresh=state["post_llm_refresh"],
            ai_analysis=state["ai_analysis"],
        )
