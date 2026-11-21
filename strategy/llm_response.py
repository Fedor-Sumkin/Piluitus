"""Normalize and enforce the LLM response contract for market candidates."""
from __future__ import annotations

import math
from typing import Any, Callable

from strategy.decision_diagnostics import classify_hold_reason_bucket


def build_llm_error_response(exc: BaseException, *, error_type: str) -> dict[str, Any]:
    """Build a fail-closed response for timeout and worker-level LLM failures."""
    is_timeout = str(error_type) == "worker_timeout"
    return {
        "final_decision": "HOLD",
        "model_decision": "HOLD",
        "model_raw_decision": "INVALID_RESPONSE",
        "model_contract_violation": False,
        "contract_error_type": None,
        "error_type": str(error_type),
        "request_error": True,
        "request_error_type": str(error_type),
        "confidence_score": 0.0,
        "confidence_valid": False,
        "tactic_opinion": (
            "Превышено общее время ожидания ИИ; торговый вход заблокирован."
            if is_timeout else
            "Ошибка обработки запроса ИИ; торговый вход заблокирован."
        ),
        "critic_opinion": str(exc),
    }


def normalize_llm_response(ai_response: Any) -> dict[str, Any]:
    """Select the first valid response object or a deterministic HOLD default."""
    if isinstance(ai_response, list) and len(ai_response) > 0:
        first_item = next(iter(ai_response), None)
        if isinstance(first_item, dict):
            return first_item
    elif isinstance(ai_response, dict):
        return ai_response
    return {
        "final_decision": "HOLD",
        "confidence_score": 0.0,
        "confidence_valid": False,
        "tactic_opinion": "",
        "critic_opinion": "",
    }


def process_llm_response(
    ai_response: Any,
    *,
    candidate_direction: object,
    ticker: str,
    increment_counter: Callable[[str], Any],
    logger: Any,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Apply the APPROVE/HOLD contract and normalize confidence without entry gates.

    Entry-evidence and confidence-threshold checks intentionally remain a
    subsequent stage. This helper only determines what the model approved and
    whether its response satisfies the declared response contract.
    """
    ai_analysis = normalize_llm_response(ai_response)

    if ai_analysis.get("contract_retry_attempted"):
        increment_counter("llm_contract_retry")
    if ai_analysis.get("contract_retry_recovered"):
        increment_counter("llm_contract_retry_recovered")
    if ai_analysis.get("contract_retry_failed"):
        increment_counter("llm_contract_retry_failed")

    request_error = bool(ai_analysis.get("request_error", False))
    request_error_type = str(ai_analysis.get("request_error_type") or "request_error")
    if request_error:
        increment_counter("llm_request_error")
        if "timeout" in request_error_type:
            increment_counter("llm_timeout")

    # LLM returns only APPROVE/HOLD. Trade direction comes exclusively from
    # the deterministic local candidate and cannot be changed by the model.
    model_decision = str(
        ai_analysis.get("model_decision", ai_analysis.get("final_decision", "HOLD"))
    ).strip().upper()
    normalized_candidate_direction = str(candidate_direction or "").strip().upper()
    model_reason_code = str(
        ai_analysis.get("model_reason_code")
        or (
            "NO_LOCAL_CANDIDATE"
            if normalized_candidate_direction not in {"BUY", "SELL"}
            else "NOT_PROVIDED"
        )
    ).strip().upper()
    if (
        model_decision == "HOLD"
        and normalized_candidate_direction in {"BUY", "SELL"}
        and not request_error
    ):
        hold_bucket = classify_hold_reason_bucket(model_reason_code)
        increment_counter(
            f"llm_hold_{normalized_candidate_direction.lower()}_{hold_bucket}"
        )

    candidate_decision = (
        normalized_candidate_direction
        if normalized_candidate_direction in {"BUY", "SELL"}
        else "HOLD"
    )
    model_contract_violation = bool(
        ai_analysis.get("model_contract_violation", False)
    )
    if not request_error:
        if (
            model_decision == "APPROVE"
            and normalized_candidate_direction in {"BUY", "SELL"}
            and not model_contract_violation
        ):
            increment_counter("llm_model_approve")
        elif model_decision == "HOLD" and not model_contract_violation:
            increment_counter("llm_model_hold")

    if (
        model_decision == "APPROVE"
        and normalized_candidate_direction in {"BUY", "SELL"}
        and not model_contract_violation
        and not request_error
    ):
        decision = normalized_candidate_direction
    else:
        decision = "HOLD"
        if request_error:
            logger.error(
                "🛑 [LLM REQUEST ERROR] %s: запрос к vLLM не завершён; "
                "type=%s candidate=%s; вход запрещён.",
                ticker,
                request_error_type,
                normalized_candidate_direction or "NONE",
            )
        elif model_decision != "HOLD" or model_contract_violation:
            model_contract_violation = True
            increment_counter("llm_contract_violation")
            logger.error(
                "🛑 [LLM CONTRACT] %s: ответ не прошёл контракт APPROVE/HOLD; "
                "raw=%s error_type=%s retry_attempted=%s retry_failed=%s candidate=%s; вход запрещён.",
                ticker,
                ai_analysis.get("model_raw_decision", model_decision),
                ai_analysis.get("contract_error_type", "unknown"),
                ai_analysis.get("contract_retry_attempted", False),
                ai_analysis.get("contract_retry_failed", False),
                normalized_candidate_direction or "NONE",
            )

    ai_analysis["model_decision"] = model_decision
    ai_analysis["final_decision"] = decision
    confidence_valid = bool(
        ai_analysis.get("confidence_valid", "confidence_score" in ai_analysis)
    )
    try:
        confidence = float(ai_analysis.get("confidence_score", 0.0)) * 100.0
        if not math.isfinite(confidence) or not 0.0 <= confidence <= 100.0:
            confidence_valid = False
            confidence = 0.0
    except (TypeError, ValueError):
        confidence_valid = False
        confidence = 0.0
    latency_val = float(ai_analysis.get("latency", 0.0) or 0.0)
    if decision not in {"BUY", "SELL", "HOLD"}:
        decision = "HOLD"
        confidence = 0.0
    candidate_confidence = confidence if confidence_valid else None

    if decision == "BUY":
        increment_counter("llm_approve_buy")
    elif decision == "SELL":
        increment_counter("llm_approve_sell")
    else:
        if not request_error:
            increment_counter("llm_hold")
            if normalized_candidate_direction == "BUY":
                increment_counter("llm_hold_buy")
            elif normalized_candidate_direction == "SELL":
                increment_counter("llm_hold_sell")

    return ai_analysis, {
        "request_error": request_error,
        "request_error_type": request_error_type,
        "model_decision": model_decision,
        "candidate_direction": normalized_candidate_direction,
        "model_reason_code": model_reason_code,
        "candidate_decision": candidate_decision,
        "model_contract_violation": model_contract_violation,
        "decision": decision,
        "confidence_valid": confidence_valid,
        "confidence": confidence,
        "candidate_confidence": candidate_confidence,
        "latency_val": latency_val,
    }
