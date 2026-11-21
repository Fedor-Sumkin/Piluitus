from __future__ import annotations

import asyncio

import pytest

import strategy.llm_context as llm_context
from strategy.llm_context import attach_llm_rule_context
from strategy.llm_response import (
    build_llm_error_response,
    normalize_llm_response,
    process_llm_response,
)
import strategy.post_llm_validation as post_validation
from strategy.post_llm_validation import validate_post_llm_entry


class _Logger:
    def __init__(self):
        self.infos = []
        self.errors = []
        self.warnings = []

    def info(self, *args, **kwargs):
        self.infos.append((args, kwargs))

    def error(self, *args, **kwargs):
        self.errors.append((args, kwargs))

    def warning(self, *args, **kwargs):
        self.warnings.append((args, kwargs))


def test_llm_context_attaches_retrieved_and_directional_rule_ids(monkeypatch):
    async def get_context(**kwargs):
        assert kwargs["category"] == "ScalpingRules"
        assert kwargs["market_evidence"] == {"price_change_pct": 0.2}
        return "[RISK / R1] always\n[BUY / B1] buy-only\n[SELL / S1] sell-only"

    monkeypatch.setattr(llm_context, "_get_knowledge_context", get_context)
    monkeypatch.setattr(
        llm_context,
        "select_prompt_rules",
        lambda direction, context, max_chars: "[RISK / R1] always\n[BUY / B1] buy-only",
    )
    event = {"metrics": {"price_change_pct": 0.2}}
    logger = _Logger()

    result = asyncio.run(attach_llm_rule_context(
        event,
        db=object(),
        ticker="SBER",
        candidate_direction="BUY",
        logger=logger,
    ))

    assert result == {
        "retrieved_rule_ids": ["R1", "B1", "S1"],
        "prompt_rule_ids": ["R1", "B1"],
    }
    assert event["rag_context"] == [
        "[RISK / R1] always\n[BUY / B1] buy-only\n[SELL / S1] sell-only"
    ]
    assert len(logger.infos) == 1
    assert not logger.errors


def test_llm_context_fails_closed_when_knowledge_retrieval_raises(monkeypatch):
    async def fail(**kwargs):
        raise RuntimeError("knowledge store unavailable")

    monkeypatch.setattr(llm_context, "_get_knowledge_context", fail)
    event = {"metrics": {}}
    logger = _Logger()

    result = asyncio.run(attach_llm_rule_context(
        event,
        db=object(),
        ticker="VTBR",
        candidate_direction="SELL",
        logger=logger,
    ))

    assert result == {"retrieved_rule_ids": [], "prompt_rule_ids": []}
    assert event["rag_context"] == [
        "Нет проверенного контекста правил; направленный вход должен быть HOLD."
    ]
    assert len(logger.errors) == 1


def test_llm_error_response_is_fail_closed_for_timeout_and_worker_error():
    timeout = build_llm_error_response(
        TimeoutError("deadline"), error_type="worker_timeout"
    )
    worker = build_llm_error_response(
        RuntimeError("failed"), error_type="worker_error"
    )
    assert timeout["request_error"] is True
    assert timeout["request_error_type"] == "worker_timeout"
    assert "Превышено" in timeout["tactic_opinion"]
    assert timeout["critic_opinion"] == "deadline"
    assert worker["request_error_type"] == "worker_error"
    assert "Ошибка обработки" in worker["tactic_opinion"]


def test_llm_response_normalizer_selects_first_valid_item_or_hold_default():
    item = {"model_decision": "APPROVE", "confidence_score": 0.8}
    assert normalize_llm_response([item, {"model_decision": "HOLD"}]) is item
    assert normalize_llm_response({"model_decision": "HOLD"})["model_decision"] == "HOLD"
    assert normalize_llm_response([None, item])["final_decision"] == "HOLD"
    assert normalize_llm_response(None) == {
        "final_decision": "HOLD",
        "confidence_score": 0.0,
        "confidence_valid": False,
        "tactic_opinion": "",
        "critic_opinion": "",
    }


def test_llm_response_contract_approves_only_the_local_candidate():
    counts = []
    logger = _Logger()
    analysis, state = process_llm_response(
        {
            "model_decision": "APPROVE",
            "confidence_score": 0.82,
            "confidence_valid": True,
            "model_reason_code": "MOMENTUM",
            "latency": 0.12,
        },
        candidate_direction="buy",
        ticker="SBER",
        increment_counter=counts.append,
        logger=logger,
    )
    assert state["decision"] == "BUY"
    assert state["candidate_decision"] == "BUY"
    assert state["confidence"] == pytest.approx(82.0)
    assert state["candidate_confidence"] == pytest.approx(82.0)
    assert state["model_contract_violation"] is False
    assert analysis["final_decision"] == "BUY"
    assert "llm_model_approve" in counts
    assert "llm_approve_buy" in counts
    assert not logger.errors


def test_llm_response_contract_rejects_directionless_approval_and_bad_confidence():
    counts = []
    logger = _Logger()
    _, no_candidate = process_llm_response(
        {"model_decision": "APPROVE", "confidence_score": 0.9},
        candidate_direction="NONE",
        ticker="SBER",
        increment_counter=counts.append,
        logger=logger,
    )
    assert no_candidate["decision"] == "HOLD"
    assert no_candidate["candidate_decision"] == "HOLD"
    assert no_candidate["model_contract_violation"] is True
    assert len(logger.errors) == 1

    _, invalid_confidence = process_llm_response(
        {"model_decision": "APPROVE", "confidence_score": 1.5},
        candidate_direction="SELL",
        ticker="GAZP",
        increment_counter=counts.append,
        logger=logger,
    )
    # The confidence threshold is enforced later, after fresh evidence is read.
    assert invalid_confidence["decision"] == "SELL"
    assert invalid_confidence["confidence_valid"] is False
    assert invalid_confidence["confidence"] == 0.0
    assert invalid_confidence["candidate_confidence"] is None


def test_llm_response_request_error_never_approves_candidate():
    counts = []
    logger = _Logger()
    analysis, state = process_llm_response(
        build_llm_error_response(
            TimeoutError("late"), error_type="worker_timeout"
        ),
        candidate_direction="BUY",
        ticker="SBER",
        increment_counter=counts.append,
        logger=logger,
    )
    assert state["decision"] == "HOLD"
    assert state["request_error"] is True
    assert state["request_error_type"] == "worker_timeout"
    assert state["model_contract_violation"] is False
    assert analysis["final_decision"] == "HOLD"
    assert "llm_request_error" in counts
    assert "llm_timeout" in counts
    assert "llm_approve_buy" not in counts
    assert len(logger.errors) == 1


def test_post_llm_guard_blocks_direction_reversal_before_other_checks(monkeypatch):
    called = []
    monkeypatch.setattr(
        post_validation,
        "validate_entry_evidence",
        lambda *args, **kwargs: (called.append("evidence") or (True, "PASS")),
    )
    result = validate_post_llm_entry(
        screener=object(),
        ticker="SBER",
        metrics={"bid": 99.0, "ask": 100.0},
        decision="SELL",
        candidate_direction="BUY",
        paper_mode=True,
        queue_flow_conflict_reason=lambda *args: called.append("queue") or "",
        parse_finite_float=lambda value: float(value),
    )
    assert result["valid_evidence"] is False
    assert result["block_reason"].startswith("Противоположное направление запрещено")
    assert called == []


def test_post_llm_guard_keeps_queue_conflict_before_signal_evidence(monkeypatch):
    called = []
    monkeypatch.setattr(
        post_validation,
        "validate_entry_evidence",
        lambda *args, **kwargs: (called.append("evidence") or (True, "PASS")),
    )
    result = validate_post_llm_entry(
        screener=object(),
        ticker="SBER",
        metrics={"bid": 99.0, "ask": 100.0},
        decision="BUY",
        candidate_direction="BUY",
        paper_mode=True,
        queue_flow_conflict_reason=lambda *args: "QUEUE_CONFLICT",
        parse_finite_float=lambda value: float(value),
    )
    assert result["valid_evidence"] is False
    assert result["block_reason"] == "QUEUE_CONFLICT"
    assert called == []


def test_post_llm_guard_refreshes_evidence_and_returns_new_price(monkeypatch):
    def refresh(screener, ticker, metrics):
        assert ticker == "SBER"
        return (
            {**metrics, "current_price": 101.25, "bid": 101.2, "ask": 101.3},
            {"trade_refresh_status": "PASS", "price_refreshed": True},
        )

    monkeypatch.setattr(post_validation, "refresh_post_llm_evidence", refresh)
    monkeypatch.setattr(
        post_validation,
        "classify_direction_evidence",
        lambda direction, metrics: "ALIGNED",
    )
    monkeypatch.setattr(
        post_validation,
        "validate_entry_evidence",
        lambda decision, metrics, paper_mode=False: (True, "PASS"),
    )
    metrics = {"current_price": 100.0}
    result = validate_post_llm_entry(
        screener=object(),
        ticker="SBER",
        metrics=metrics,
        decision="BUY",
        candidate_direction="BUY",
        paper_mode=True,
        queue_flow_conflict_reason=lambda *args: "",
        parse_finite_float=lambda value: float(value),
    )
    assert result["valid_evidence"] is True
    assert result["post_llm_evidence_validation"] == "PASS"
    assert result["refreshed_price"] == pytest.approx(101.25)
    assert metrics["current_price"] == pytest.approx(101.25)
    assert metrics["direction_evidence_state"] == "ALIGNED"


def test_post_llm_guard_marks_hold_as_not_applicable_without_refresh():
    metrics = {"price_change_pct": 0.1}
    result = validate_post_llm_entry(
        screener=object(),
        ticker="SBER",
        metrics=metrics,
        decision="HOLD",
        candidate_direction="NONE",
        paper_mode=True,
        queue_flow_conflict_reason=lambda *args: "",
        parse_finite_float=lambda value: float(value),
    )
    assert result["valid_evidence"] is True
    assert result["post_llm_refresh"] == {}
    assert result["post_llm_evidence_validation"] == "NOT_APPLICABLE"
    assert result["post_llm_evidence_reason"] == "NOT_APPLICABLE"
