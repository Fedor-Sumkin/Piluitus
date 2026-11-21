"""Final post-LLM market-evidence refresh and deterministic entry validation."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from strategy.candidate_recovery import refresh_post_llm_evidence
from strategy.decision_diagnostics import build_llm_feature_snapshot
from strategy.signal_guard import (
    classify_direction_evidence,
    validate_entry_evidence,
)


def validate_post_llm_entry(
    *,
    screener: Any,
    ticker: str,
    metrics: object,
    decision: str,
    candidate_direction: str,
    paper_mode: bool,
    queue_flow_conflict_reason: Callable[[str, dict[str, Any]], str],
    parse_finite_float: Callable[[object], float | None],
) -> dict[str, Any]:
    """Refresh executable evidence and apply the same ordered final entry guards.

    The result keeps pre-LLM metrics attributable while using refreshed BBO and
    trade-flow data for the actual post-inference entry decision.
    """
    evidence = metrics if isinstance(metrics, dict) else {}
    post_llm_refresh: dict[str, Any] = {}
    refreshed_price: float | None = None

    if candidate_direction in {"BUY", "SELL"}:
        evidence, post_llm_refresh = refresh_post_llm_evidence(
            screener, ticker, evidence
        )
        evidence["direction_evidence_state"] = classify_direction_evidence(
            candidate_direction, evidence
        )
        if (
            post_llm_refresh.get("trade_refresh_status") == "PASS"
            and post_llm_refresh.get("price_refreshed")
        ):
            refreshed_price = parse_finite_float(evidence.get("current_price"))
            if refreshed_price is not None and refreshed_price > 0.0:
                pass
            else:
                refreshed_price = None
        if isinstance(metrics, dict):
            metrics.update(evidence)

    valid_evidence = True
    block_reason = ""
    if (
        decision in {"BUY", "SELL"}
        and candidate_direction in {"BUY", "SELL"}
        and decision != candidate_direction
    ):
        valid_evidence = False
        block_reason = (
            f"Противоположное направление запрещено: локально подтверждён {candidate_direction}, "
            f"модель предложила {decision}."
        )
    elif decision in {"BUY", "SELL"}:
        conflict_reason = queue_flow_conflict_reason(decision, evidence)
        if conflict_reason:
            valid_evidence, block_reason = False, conflict_reason
        else:
            valid_evidence, block_reason = validate_entry_evidence(
                decision,
                evidence,
                paper_mode=paper_mode,
            )

    post_llm_features = build_llm_feature_snapshot(evidence)
    post_llm_evidence_validation = (
        "NOT_APPLICABLE"
        if decision not in {"BUY", "SELL"}
        else "PASS" if valid_evidence else "BLOCKED"
    )
    post_llm_evidence_reason = (
        block_reason if block_reason
        else "PASS" if decision in {"BUY", "SELL"}
        else "NOT_APPLICABLE"
    )
    return {
        "evidence": evidence,
        "post_llm_refresh": post_llm_refresh,
        "refreshed_price": refreshed_price,
        "valid_evidence": valid_evidence,
        "block_reason": block_reason,
        "post_llm_features": post_llm_features,
        "post_llm_evidence_validation": post_llm_evidence_validation,
        "post_llm_evidence_reason": post_llm_evidence_reason,
    }
