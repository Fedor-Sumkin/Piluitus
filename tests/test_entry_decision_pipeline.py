from __future__ import annotations

import pytest

from strategy.entry_decision_pipeline import prepare_entry_execution_plan


class _Logger:
    def __init__(self):
        self.infos = []
        self.warnings = []

    def info(self, *args, **kwargs):
        self.infos.append((args, kwargs))

    def warning(self, *args, **kwargs):
        self.warnings.append((args, kwargs))


class _Execution:
    def __init__(self, stops=(90.0, 120.0)):
        self.stops = stops
        self.stop_calls = []

    def calculate_atr_stop(self, *args, **kwargs):
        self.stop_calls.append((args, kwargs))
        return self.stops


class _Risk:
    def __init__(self, lots=2):
        self.lots = lots
        self.calls = []

    def calculate_lot_size(self, *args):
        self.calls.append(args)
        return self.lots


def _prepare(**overrides):
    counters = []
    guard_blocks = []
    logger = _Logger()
    execution = _Execution()
    risk = _Risk()
    values = {
        "ticker": "SBER",
        "price": 100.0,
        "decision": "BUY",
        "confidence": 82.0,
        "confidence_valid": True,
        "valid_evidence": True,
        "block_reason": "",
        "signal_reason": "",
        "metrics": {
            "bar_atr": 2.0,
            "bar_atr_periods": 5,
            "bar_atr_age_sec": 1.2,
            "spread_available": 1.0,
            "bid": 99.9,
            "ask": 100.1,
            "spread_pct": 0.2,
            "bbo_age_sec": 0.1,
        },
        "ai_analysis": {},
        "lot_multiplier": 10,
        "execution_engine": execution,
        "risk_manager": risk,
        "paper_trading": True,
        "increment_counter": counters.append,
        "guard_block": guard_blocks.append,
        "finite_float": lambda value: float(value) if value is not None else None,
        "nonnegative_int": lambda value: int(value) if value is not None else None,
        "logger": logger,
        "min_atr_periods_raw": "5",
        "atr_multiplier_raw": "1.7",
    }
    values.update(overrides)
    result = prepare_entry_execution_plan(**values)
    return result, values, counters, guard_blocks, logger, execution, risk


def test_post_llm_evidence_gate_precedes_confidence_and_atr():
    result, values, counters, guard_blocks, logger, execution, risk = _prepare(
        valid_evidence=False,
        block_reason="BBO стал неактуальным после LLM",
    )
    assert result["decision"] == "HOLD"
    assert result["confidence"] == 0.0
    assert result["signal_reason"] == "BBO стал неактуальным после LLM"
    assert result["execution_status"] == "NOT_REQUESTED"
    assert values["ai_analysis"]["final_decision"] == "HOLD"
    assert values["ai_analysis"]["confidence_score"] == 0.0
    assert counters == ["post_llm_evidence_blocked"]
    assert guard_blocks == ["BBO стал неактуальным после LLM"]
    assert not execution.stop_calls
    assert not risk.calls
    assert len(logger.warnings) == 1


def test_evidence_gate_uses_existing_fallback_reason_when_block_reason_missing():
    result, _, counters, guard_blocks, _, execution, _ = _prepare(
        valid_evidence=False,
        block_reason="",
    )
    assert result["signal_reason"] == "Измеряемые условия входа не пройдены."
    assert counters == ["post_llm_evidence_blocked"]
    assert guard_blocks == ["Измеряемые условия входа не пройдены."]
    assert not execution.stop_calls


@pytest.mark.parametrize(
    ("confidence", "confidence_valid", "expected_reason"),
    [
        (74.9, True, "ниже порога входа 75%"),
        (0.0, False, "корректную числовую confidence_score"),
    ],
)
def test_confidence_gate_blocks_without_changing_atr_or_sizing(
    confidence, confidence_valid, expected_reason
):
    result, values, counters, _, _, execution, risk = _prepare(
        confidence=confidence,
        confidence_valid=confidence_valid,
    )
    assert result["decision"] == "HOLD"
    assert expected_reason in result["signal_reason"]
    assert result["execution_status"] == "NOT_REQUESTED"
    assert values["ai_analysis"]["final_decision"] == "HOLD"
    assert values["ai_analysis"]["confidence_score"] == pytest.approx(confidence / 100.0)
    assert counters == ["confidence_blocked", "post_llm_confidence_blocked"]
    assert not execution.stop_calls
    assert not risk.calls


def test_invalid_atr_protection_blocks_before_risk_sizing():
    result, values, counters, _, logger, execution, risk = _prepare(
        execution_engine=_Execution(stops=(0.0, 120.0)),
        min_atr_periods_raw="invalid",
    )
    assert result["decision"] == "BUY"
    assert result["execution_status"] == "BLOCKED"
    assert result["requested_lots"] == 0
    assert "5/5" in result["execution_message"]
    assert values["ai_analysis"]["atr_entry_ready"] is False
    assert counters == ["atr_history_blocked"]
    assert len(execution.stop_calls) == 1
    assert not risk.calls
    assert len(logger.warnings) == 2


@pytest.mark.parametrize(
    ("paper_trading", "expected_lot_label"),
    [(True, "биржевой lot_size"), (False, "Finam account trade_lot_size")],
)
def test_unknown_venue_lot_size_blocks_even_when_risk_manager_returns_zero(
    paper_trading, expected_lot_label
):
    result, _, counters, _, _, execution, risk = _prepare(
        lot_multiplier=0,
        paper_trading=paper_trading,
        risk_manager=_Risk(lots=0),
    )
    assert result["execution_status"] == "BLOCKED"
    assert expected_lot_label in result["execution_message"]
    assert counters == ["zero_lot", "finam_lot_unavailable"]
    assert len(execution.stop_calls) == 1
    assert len(risk.calls) == 1


def test_zero_lot_with_known_multiplier_is_reported_as_risk_limit():
    result, _, counters, _, _, _, _ = _prepare(
        risk_manager=_Risk(lots=0),
        lot_multiplier=10,
    )
    assert result["execution_status"] == "BLOCKED"
    assert result["execution_message"] == "Риск на минимальный лот превышает установленный лимит."
    assert counters == ["zero_lot"]


def test_approved_candidate_keeps_atr_metadata_and_returns_execution_plan():
    result, values, counters, _, logger, execution, risk = _prepare(
        decision="SELL",
    )
    analysis = values["ai_analysis"]
    assert result == {
        "decision": "SELL",
        "confidence": 82.0,
        "signal_reason": "",
        "execution_status": "PENDING",
        "execution_message": "Заявка передана в контур исполнения; ожидается результат.",
        "requested_lots": 2,
        "requested_notional": 2000.0,
        "lot_multiplier": 10,
    }
    assert execution.stop_calls == [
        (
            ("SBER", 100.0, "SELL"),
            {
                "atr_value": 2.0,
                "atr_periods": 5,
                "bid": 99.9,
                "ask": 100.1,
            },
        )
    ]
    assert risk.calls == [("SBER", 100.0, 90.0)]
    assert analysis["atr_entry_ready"] is True
    assert analysis["stop_loss"] == 90.0
    assert analysis["take_profit"] == 120.0
    assert analysis["atr_at_entry"] == 2.0
    assert analysis["atr_periods_at_entry"] == 5
    assert analysis["atr_age_sec_at_entry"] == 1.2
    assert analysis["atr_multiplier"] == 1.7
    assert analysis["entry_bid"] == 99.9
    assert analysis["entry_ask"] == 100.1
    assert analysis["entry_spread_pct"] == 0.2
    assert analysis["entry_bbo_age_sec"] == 0.1
    assert counters == []
    assert len(logger.warnings) == 1


def test_bbo_metadata_remains_zero_when_spread_is_not_available():
    _, values, _, _, _, execution, _ = _prepare(
        metrics={
            "bar_atr": 1.0,
            "bar_atr_periods": 5,
            "spread_available": 0.0,
            "bid": 99.0,
            "ask": 101.0,
        }
    )
    assert execution.stop_calls[0][1]["bid"] == 0.0
    assert execution.stop_calls[0][1]["ask"] == 0.0
    assert values["ai_analysis"]["entry_spread_pct"] == 0.0
    assert values["ai_analysis"]["entry_bbo_age_sec"] == 0.0


def test_non_directional_hold_does_not_touch_execution_dependencies():
    result, _, counters, _, _, execution, risk = _prepare(
        decision="HOLD",
        confidence=99.0,
    )
    assert result["execution_status"] == "NOT_REQUESTED"
    assert result["decision"] == "HOLD"
    assert result["requested_lots"] == 0
    assert not counters
    assert not execution.stop_calls
    assert not risk.calls
