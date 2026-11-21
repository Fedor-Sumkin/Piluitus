"""Deterministic construction and linkage of strategy verdict records."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from datetime import datetime
from typing import Any


def build_default_signal_reason(
    *,
    decision: str,
    candidate_decision: str,
    confidence_valid: bool,
    signal_reason: str,
    model_decision: str,
) -> str:
    """Return the existing safe HOLD explanation when no reason was supplied."""
    if decision != "HOLD" or signal_reason:
        return signal_reason
    if not confidence_valid:
        return "Ответ модели не содержит корректной числовой оценки; вход запрещён."
    if model_decision == "HOLD" and candidate_decision in {"BUY", "SELL"}:
        return "Модель отклонила локальный кандидат: вердикт HOLD."
    if candidate_decision in {"BUY", "SELL"}:
        return "Кандидат ИИ не прошёл локальные проверки входа."
    return "Модель не предложила направленный вход."


def classify_local_evidence_diagnostic(
    *,
    block_reason: str,
    candidate_direction: str,
    request_error: bool,
    request_error_type: str,
    model_contract_violation: bool,
    model_decision: str,
    model_reason_code: str,
    confidence_valid: bool,
    decision: str,
    confidence: float,
) -> str:
    """Preserve the precedence of the existing post-LLM diagnostic branches."""
    if block_reason:
        return block_reason
    if candidate_direction not in {"BUY", "SELL"}:
        return "NO_DIRECTIONAL_CANDIDATE"
    if request_error:
        return f"LLM_REQUEST_ERROR:{request_error_type}"
    if model_contract_violation:
        return "LLM_CONTRACT_VIOLATION"
    if model_decision == "HOLD":
        return f"MODEL_HOLD:{model_reason_code}"
    if not confidence_valid:
        return "CONFIDENCE_INVALID"
    if decision == "HOLD" and confidence < 75.0:
        return f"CONFIDENCE_BELOW_THRESHOLD:{confidence:.1f}"
    if decision == "HOLD":
        return "POST_LLM_GUARD_BLOCK"
    return "PRE_LLM_VALIDATED"


def build_pre_llm_risk_block_verdict(
    event_payload: Mapping[str, Any],
    *,
    now_local: datetime,
    verdict_id: str,
    explanation_builder: Callable[
        [str, str, dict[str, Any], str], tuple[str, str]
    ],
) -> dict[str, Any]:
    """Build the dashboard record for a candidate stopped before LLM inference."""
    ticker = str(event_payload.get("ticker", "UNKNOWN")).strip().upper()
    price = event_payload.get("price", 0.0)
    metrics = event_payload.get("metrics", {})
    if not isinstance(metrics, dict):
        metrics = {}
    candidate = str(event_payload.get("candidate_direction", "HOLD")).upper()
    reason = "RiskManager остановил торговлю до запроса к ИИ; исполнитель не вызывался."
    tactic, critic = explanation_builder("HOLD", candidate, metrics, reason)
    return {
        "verdict_id": verdict_id,
        "timestamp": now_local.strftime("%H:%M:%S"),
        "timestamp_iso": now_local.isoformat(timespec="milliseconds"),
        "ticker": ticker,
        "price": price,
        "decision": "HOLD",
        "candidate_decision": candidate,
        "confidence": None,
        "decision_source": "RISK_MANAGER",
        "signal_status": "BLOCKED" if candidate in {"BUY", "SELL"} else "NO_ENTRY",
        "signal_reason": reason,
        "execution_status": "NOT_REQUESTED",
        "execution_message": "Не запрашивалось: торговля остановлена RiskManager.",
        "tactic_opinion": tactic,
        "critic_opinion": critic,
    }


def build_llm_verdict_record(
    *,
    verdict_id: str,
    event_payload: Mapping[str, Any],
    now_local: datetime,
    ticker: str,
    price: float,
    decision: str,
    candidate_decision: str,
    model_decision: str,
    candidate_confidence: float | None,
    confidence: float,
    confidence_valid: bool,
    signal_reason: str,
    model_reason_code: str,
    execution_status: str,
    execution_message: str,
    requested_lots: int | None,
    requested_notional: float,
    tactic_text: str,
    critic_text: str,
    local_evidence_diagnostic: str,
    llm_input_features: dict[str, Any],
    post_llm_features: dict[str, Any],
    post_llm_evidence_validation: str,
    post_llm_evidence_reason: str,
    post_llm_refresh: dict[str, Any],
    ai_analysis: Mapping[str, Any],
) -> dict[str, Any]:
    """Build the existing LLM verdict schema without mutating its inputs."""
    signal_status = (
        "APPROVED" if decision in {"BUY", "SELL"}
        else "BLOCKED" if candidate_decision in {"BUY", "SELL"}
        else "NO_ENTRY"
    )
    return {
        "verdict_id": verdict_id,
        "shadow_candidate_id": event_payload.get("shadow_candidate_id"),
        "timestamp": now_local.strftime("%H:%M:%S"),
        "timestamp_iso": now_local.isoformat(timespec="milliseconds"),
        "ticker": ticker,
        "price": price,
        "decision": decision,
        "candidate_decision": candidate_decision,
        "model_decision": model_decision,
        # Keep model confidence separate from the post-LLM final gate.
        "confidence": round(candidate_confidence, 1) if candidate_confidence is not None else None,
        "model_confidence": round(candidate_confidence, 1) if candidate_confidence is not None else None,
        "final_confidence": round(confidence, 1) if confidence_valid else None,
        "signal_status": signal_status,
        "signal_reason": signal_reason,
        "execution_status": execution_status,
        "execution_message": execution_message,
        "requested_lots": requested_lots or None,
        "requested_notional_rub": round(requested_notional, 2) if requested_notional else None,
        "executed_lots": None,
        "executed_shares": None,
        "actual_notional_rub": None,
        "order_id": None,
        "tactic_opinion": tactic_text,
        "critic_opinion": critic_text,
        "local_evidence_reason": local_evidence_diagnostic,
        "llm_input_features": llm_input_features,
        "pre_llm_features": llm_input_features,
        "post_llm_features": post_llm_features,
        "post_llm_evidence_validation": post_llm_evidence_validation,
        "post_llm_evidence_reason": post_llm_evidence_reason,
        "post_llm_refresh": post_llm_refresh,
        "llm_candidate_direction": candidate_decision,
        "llm_model_decision": model_decision,
        "llm_reason_code": model_reason_code,
        "llm_reason_code_raw": ai_analysis.get("model_reason_code_raw", "NOT_PROVIDED"),
        "llm_reason_validation": ai_analysis.get("model_reason_validation", "UNKNOWN"),
        "llm_m1_evidence_state": ai_analysis.get("m1_evidence_state", "UNAVAILABLE"),
        "llm_prompt_chars": ai_analysis.get("prompt_chars"),
        "llm_prompt_fingerprint": ai_analysis.get("prompt_fingerprint"),
    }


def link_shadow_candidate_verdict(
    verdict_data: dict[str, Any],
    *,
    shadow_candidate_id: str,
    shadow_tracker: Any,
    logger: Any,
    verdict_id: str,
    model_decision: str,
    final_decision: str,
    signal_status: str,
    confidence: float | None,
    reason_code: str,
    local_guard: str,
    execution_status: str,
    ticker: str,
) -> bool:
    """Attach a completed verdict to its research-only shadow candidate."""
    if not shadow_candidate_id:
        verdict_data["shadow_linked"] = False
        return False

    linked = shadow_tracker.attach_llm_verdict(
        shadow_candidate_id,
        verdict_id=verdict_id,
        model_decision=model_decision,
        final_decision=final_decision,
        signal_status=signal_status,
        confidence=confidence,
        reason_code=reason_code,
        local_guard=local_guard,
        execution_status=execution_status,
    )
    verdict_data["shadow_linked"] = bool(linked)
    if not linked:
        logger.warning(
            "[STRATEGY SHADOW LINK MISS] candidate_id=%s verdict_id=%s ticker=%s",
            shadow_candidate_id,
            verdict_id,
            ticker,
        )
    return bool(linked)


def enrich_ai_analysis_with_verdict(
    ai_analysis: dict[str, Any],
    *,
    verdict_data: Mapping[str, Any],
    shadow_candidate_id: str,
    verdict_id: str,
    decision: str,
    confidence: float,
    confidence_valid: bool,
    candidate_confidence: float | None,
    signal_status: str,
    model_decision: str,
    model_reason_code: str,
    local_evidence_diagnostic: str,
    candidate_decision: str,
    llm_input_features: dict[str, Any],
    post_llm_features: dict[str, Any],
    post_llm_evidence_validation: str,
    post_llm_evidence_reason: str,
    post_llm_refresh: dict[str, Any],
    metrics: Mapping[str, Any],
    tactic_text: str,
    critic_text: str,
) -> None:
    """Attach normalized verdict fields and point-in-time market regimes to AI data."""
    ai_analysis["final_decision"] = decision
    ai_analysis["confidence_score"] = (confidence / 100.0) if confidence_valid else 0.0
    ai_analysis.update({
        "shadow_candidate_id": shadow_candidate_id or None,
        "llm_verdict_id": verdict_id,
        "llm_model_decision": model_decision,
        "llm_final_decision": decision,
        "llm_confidence_pct": candidate_confidence,
        "llm_final_confidence_pct": round(confidence, 1) if confidence_valid else None,
        "llm_signal_status": signal_status,
        "llm_reason_code": model_reason_code,
        "llm_local_guard": local_evidence_diagnostic,
        "candidate_direction": candidate_decision,
        "llm_shadow_linked": bool(verdict_data.get("shadow_linked", False)),
        "pre_llm_features": llm_input_features,
        "post_llm_features": post_llm_features,
        "post_llm_evidence_validation": post_llm_evidence_validation,
        "post_llm_evidence_reason": post_llm_evidence_reason,
        "post_llm_refresh": post_llm_refresh,
        # Persist point-in-time regime labels with the trade. Missing source
        # fields remain UNKNOWN rather than being inferred later.
        "market_regime": metrics.get("market_regime", metrics.get("regime", "UNKNOWN")),
        "volatility_regime": metrics.get("volatility_regime", "UNKNOWN"),
        "liquidity_regime": metrics.get("liquidity_regime", "UNKNOWN"),
        "directional_regime": metrics.get("directional_regime", "UNKNOWN"),
    })
    ai_analysis["tactic_opinion"] = tactic_text
    ai_analysis["critic_opinion"] = critic_text
