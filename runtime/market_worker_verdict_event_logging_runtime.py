"""Schedule tracked verdict events for the trading event log."""
from __future__ import annotations

from runtime.market_worker_candidate_state import MarketWorkerCandidateState


class MarketWorkerVerdictEventLoggingMixin:
    """Own background persistence of the completed AI verdict event."""

    def _schedule_candidate_verdict_event_log(
        self,
        state: MarketWorkerCandidateState,
        diagnostics: dict,
        shadow_context: dict,
    ) -> None:
        ticker = state["ticker"]
        verdict_id = state["verdict_id"]
        candidate_confidence = state["candidate_confidence"]
        confidence = state["confidence"]
        confidence_valid = state["confidence_valid"]
        decision = state["decision"]
        candidate_decision = state["candidate_decision"]
        execution_status = state["execution_status"]
        self.background_operations.spawn(
            self.log_trading_event(
                ticker=ticker,
                price=state["price"],
                decision=f"ВЕРДИКТ ИИ: {decision} (кандидат={candidate_decision}, исполнение={execution_status})",
                confidence=round(candidate_confidence, 1) if candidate_confidence is not None else 0.0,
                tactic=diagnostics["tactic_text"],
                critic=diagnostics["critic_text"],
                candidate_id=shadow_context["shadow_candidate_id"] or None,
                verdict_id=verdict_id,
                model_confidence=candidate_confidence,
                final_confidence=round(confidence, 1) if confidence_valid else None,
            ),
            name=f"trading-log-verdict:{ticker}:{verdict_id}",
        )
