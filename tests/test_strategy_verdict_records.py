from __future__ import annotations

from datetime import datetime, timezone

from strategy.verdict_records import (
    build_pre_llm_risk_block_verdict,
    build_default_signal_reason,
    classify_local_evidence_diagnostic,
    build_llm_verdict_record,
    link_shadow_candidate_verdict,
    enrich_ai_analysis_with_verdict,
)


def test_default_hold_signal_reason_preserves_existing_branch_order():
    assert build_default_signal_reason(
        decision="HOLD", candidate_decision="BUY",
        confidence_valid=False, signal_reason="", model_decision="HOLD",
    ) == "Ответ модели не содержит корректной числовой оценки; вход запрещён."
    assert build_default_signal_reason(
        decision="HOLD", candidate_decision="BUY",
        confidence_valid=True, signal_reason="", model_decision="HOLD",
    ) == "Модель отклонила локальный кандидат: вердикт HOLD."
    assert build_default_signal_reason(
        decision="HOLD", candidate_decision="BUY",
        confidence_valid=True, signal_reason="", model_decision="APPROVE",
    ) == "Кандидат ИИ не прошёл локальные проверки входа."
    assert build_default_signal_reason(
        decision="HOLD", candidate_decision="HOLD",
        confidence_valid=True, signal_reason="", model_decision="APPROVE",
    ) == "Модель не предложила направленный вход."
    assert build_default_signal_reason(
        decision="HOLD", candidate_decision="BUY",
        confidence_valid=True, signal_reason="explicit", model_decision="HOLD",
    ) == "explicit"
    assert build_default_signal_reason(
        decision="BUY", candidate_decision="BUY",
        confidence_valid=True, signal_reason="", model_decision="APPROVE",
    ) == ""


def test_local_evidence_diagnostic_preserves_guard_precedence():
    args = dict(
        block_reason="BLOCK_REASON",
        candidate_direction="NONE",
        request_error=True,
        request_error_type="timeout",
        model_contract_violation=True,
        model_decision="HOLD",
        model_reason_code="NO_EDGE",
        confidence_valid=False,
        decision="HOLD",
        confidence=20.0,
    )
    assert classify_local_evidence_diagnostic(**args) == "BLOCK_REASON"
    args["block_reason"] = ""
    assert classify_local_evidence_diagnostic(**args) == "NO_DIRECTIONAL_CANDIDATE"
    args["candidate_direction"] = "BUY"
    assert classify_local_evidence_diagnostic(**args) == "LLM_REQUEST_ERROR:timeout"
    args["request_error"] = False
    assert classify_local_evidence_diagnostic(**args) == "LLM_CONTRACT_VIOLATION"
    args["model_contract_violation"] = False
    assert classify_local_evidence_diagnostic(**args) == "MODEL_HOLD:NO_EDGE"
    args["model_decision"] = "APPROVE"
    assert classify_local_evidence_diagnostic(**args) == "CONFIDENCE_INVALID"
    args["confidence_valid"] = True
    assert classify_local_evidence_diagnostic(**args) == "CONFIDENCE_BELOW_THRESHOLD:20.0"
    args["confidence"] = 75.0
    assert classify_local_evidence_diagnostic(**args) == "POST_LLM_GUARD_BLOCK"
    args["decision"] = "BUY"
    assert classify_local_evidence_diagnostic(**args) == "PRE_LLM_VALIDATED"


def test_pre_llm_risk_block_record_is_built_from_local_candidate_data():
    moment = datetime(2026, 11, 22, 10, 11, 12, 345000, tzinfo=timezone.utc)
    calls = []

    def explain(decision, candidate, metrics, reason):
        calls.append((decision, candidate, metrics, reason))
        return "tactic", "critic"

    metrics = {"price_change_pct": 0.12}
    verdict = build_pre_llm_risk_block_verdict(
        {"ticker": " sber ", "price": 100.5, "candidate_direction": "BUY", "metrics": metrics},
        now_local=moment,
        verdict_id="risk-1",
        explanation_builder=explain,
    )
    assert verdict == {
        "verdict_id": "risk-1",
        "timestamp": "10:11:12",
        "timestamp_iso": "2026-11-22T10:11:12.345+00:00",
        "ticker": "SBER",
        "price": 100.5,
        "decision": "HOLD",
        "candidate_decision": "BUY",
        "confidence": None,
        "decision_source": "RISK_MANAGER",
        "signal_status": "BLOCKED",
        "signal_reason": "RiskManager остановил торговлю до запроса к ИИ; исполнитель не вызывался.",
        "execution_status": "NOT_REQUESTED",
        "execution_message": "Не запрашивалось: торговля остановлена RiskManager.",
        "tactic_opinion": "tactic",
        "critic_opinion": "critic",
    }
    assert calls == [(
        "HOLD", "BUY", metrics,
        "RiskManager остановил торговлю до запроса к ИИ; исполнитель не вызывался.",
    )]


def test_pre_llm_risk_block_handles_missing_metrics_and_no_direction():
    moment = datetime(2026, 11, 22, tzinfo=timezone.utc)
    seen = []
    verdict = build_pre_llm_risk_block_verdict(
        {"ticker": "vtbr", "metrics": None, "candidate_direction": "HOLD"},
        now_local=moment,
        verdict_id="risk-2",
        explanation_builder=lambda *args: (seen.append(args) or ("t", "c")),
    )
    assert verdict["ticker"] == "VTBR"
    assert verdict["signal_status"] == "NO_ENTRY"
    assert seen[0][2] == {}


def _llm_record(**overrides):
    args = dict(
        verdict_id="v-1",
        event_payload={"shadow_candidate_id": "shadow-1"},
        now_local=datetime(2026, 11, 22, 10, 11, 12, tzinfo=timezone.utc),
        ticker="SBER",
        price=100.25,
        decision="BUY",
        candidate_decision="BUY",
        model_decision="APPROVE",
        candidate_confidence=82.456,
        confidence=83.456,
        confidence_valid=True,
        signal_reason="",
        execution_status="PENDING",
        execution_message="queued",
        requested_lots=3,
        requested_notional=300.75,
        tactic_text="tactic",
        critic_text="critic",
        local_evidence_diagnostic="PRE_LLM_VALIDATED",
        llm_input_features={"a": 1},
        post_llm_features={"b": 2},
        post_llm_evidence_validation="VALID",
        post_llm_evidence_reason="OK",
        post_llm_refresh={"fresh": True},
        ai_analysis={
            "model_reason_code": "MOMENTUM",
            "model_reason_code_raw": "momentum",
            "model_reason_validation": "VALID",
            "m1_evidence_state": "READY",
            "prompt_chars": 200,
            "prompt_fingerprint": "abc",
        },
    )
    args.update(overrides)
    return args


def test_llm_verdict_record_keeps_model_and_final_confidence_separate():
    record = build_llm_verdict_record(**_llm_record())
    assert record["confidence"] == 82.5
    assert record["model_confidence"] == 82.5
    assert record["final_confidence"] == 83.5
    assert record["signal_status"] == "APPROVED"
    assert record["shadow_candidate_id"] == "shadow-1"
    assert record["llm_reason_code"] == "MOMENTUM"
    assert record["llm_input_features"] == record["pre_llm_features"] == {"a": 1}
    assert record["post_llm_refresh"] == {"fresh": True}


def test_llm_verdict_record_handles_missing_confidence_and_no_entry():
    record = build_llm_verdict_record(**_llm_record(
        decision="HOLD", candidate_decision="HOLD",
        candidate_confidence=None, confidence_valid=False, confidence=0.0,
        requested_lots=0, requested_notional=0.0, ai_analysis={},
    ))
    assert record["signal_status"] == "NO_ENTRY"
    assert record["confidence"] is None and record["final_confidence"] is None
    assert record["requested_lots"] is None
    assert record["requested_notional_rub"] is None
    assert record["llm_reason_code"] == "UNKNOWN"


def test_shadow_verdict_link_records_success_failure_and_missing_candidate():
    class Tracker:
        def __init__(self, linked):
            self.linked, self.calls = linked, []

        def attach_llm_verdict(self, candidate_id, **kwargs):
            self.calls.append((candidate_id, kwargs))
            return self.linked

    class Logger:
        def __init__(self):
            self.warnings = []

        def warning(self, *args):
            self.warnings.append(args)

    base = dict(
        verdict_id="v-1", model_decision="APPROVE", final_decision="BUY",
        signal_status="APPROVED", confidence=80.0, reason_code="MOMENTUM",
        local_guard="OK", execution_status="PENDING", ticker="SBER",
    )
    for linked in (True, False):
        record, tracker, logger = {}, Tracker(linked), Logger()
        result = link_shadow_candidate_verdict(
            record, shadow_candidate_id="shadow-1", shadow_tracker=tracker,
            logger=logger, **base,
        )
        assert result is linked
        assert record["shadow_linked"] is linked
        assert len(tracker.calls) == 1
        assert len(logger.warnings) == (0 if linked else 1)

    record, tracker, logger = {}, Tracker(True), Logger()
    assert link_shadow_candidate_verdict(
        record, shadow_candidate_id="", shadow_tracker=tracker,
        logger=logger, **base,
    ) is False
    assert record["shadow_linked"] is False
    assert tracker.calls == []


def test_ai_analysis_enrichment_preserves_point_in_time_regime_defaults():
    analysis = {}
    enrich_ai_analysis_with_verdict(
        analysis,
        verdict_data={"shadow_linked": True},
        shadow_candidate_id="shadow-1",
        verdict_id="v-1",
        decision="BUY",
        confidence=83.0,
        confidence_valid=True,
        candidate_confidence=82.0,
        signal_status="APPROVED",
        model_decision="APPROVE",
        model_reason_code="MOMENTUM",
        local_evidence_diagnostic="PRE_LLM_VALIDATED",
        candidate_decision="BUY",
        llm_input_features={"pre": 1},
        post_llm_features={"post": 2},
        post_llm_evidence_validation="VALID",
        post_llm_evidence_reason="OK",
        post_llm_refresh={"updated": True},
        metrics={"regime": "TRENDING", "volatility_regime": "HIGH"},
        tactic_text="tactic",
        critic_text="critic",
    )
    assert analysis["final_decision"] == "BUY"
    assert analysis["confidence_score"] == 0.83
    assert analysis["market_regime"] == "TRENDING"
    assert analysis["volatility_regime"] == "HIGH"
    assert analysis["liquidity_regime"] == "UNKNOWN"
    assert analysis["directional_regime"] == "UNKNOWN"
    assert analysis["llm_shadow_linked"] is True
    assert analysis["tactic_opinion"] == "tactic"
    assert analysis["critic_opinion"] == "critic"
