"""Typed state contract shared by the market-worker processing phases."""
from __future__ import annotations

from typing import Any, TypedDict


class MarketWorkerCandidateState(TypedDict, total=False):
    event_payload: dict[str, Any]
    ticker: str
    price: Any
    metrics: dict[str, Any]
    candidate_direction_for_diag: str
    llm_input_features: dict[str, Any]
    ai_analysis: dict[str, Any]
    request_error: bool
    request_error_type: str
    model_decision: str
    candidate_direction: str
    model_reason_code: str
    candidate_decision: str
    model_contract_violation: bool
    decision: str
    confidence_valid: bool
    confidence: float
    candidate_confidence: float | None
    evidence: dict[str, Any]
    post_llm_refresh: dict[str, Any]
    valid_evidence: bool
    block_reason: str
    post_llm_features: dict[str, Any]
    post_llm_evidence_validation: str
    post_llm_evidence_reason: str
    verdict_id: str
    signal_reason: str
    execution_status: str
    execution_message: str
    requested_lots: int
    requested_notional: float
    lot_multiplier: int
    verdict_data: dict[str, Any]
    signal_status: str
    shadow_candidate_id: str
    tactic_text: str
    critic_text: str
    local_evidence_diagnostic: str
