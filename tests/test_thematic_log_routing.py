"""Regression tests for thematic run logging and per-file rotation."""
from __future__ import annotations

import logging

from monitoring.log_routing import (
    DiagnosticLogFilter,
    ErrorLogFilter,
    SessionLogFilter,
    ThematicLogFilter,
    VerdictLogFilter,
)


def _record(name: str, message: str, level: int = logging.INFO) -> logging.LogRecord:
    return logging.LogRecord(name, level, __file__, 1, message, (), None)


def test_diagnostic_records_stay_in_diagnostic_and_not_thematic_files():
    diagnostic = DiagnosticLogFilter()
    shadow = ThematicLogFilter("shadow", diagnostic)
    vllm = ThematicLogFilter("vllm", diagnostic)
    record = _record("Piluitus.Main", "[LLM DECISION] ticker=SBER decision=HOLD")

    assert not diagnostic.filter(record)
    assert not shadow.filter(record)
    assert vllm.filter(record)


def test_quality_diagnostics_remain_in_diagnostic_only():
    diagnostic = DiagnosticLogFilter()
    execution = ThematicLogFilter("execution", diagnostic)
    record = _record("Piluitus.Main", "[SIGNAL GUARD] evidence=weak")

    assert diagnostic.filter(record)
    assert not execution.filter(record)


def test_subsystem_logger_names_route_to_expected_theme():
    diagnostic = DiagnosticLogFilter()
    shadow = ThematicLogFilter("shadow", diagnostic)
    vllm = ThematicLogFilter("vllm", diagnostic)
    market = ThematicLogFilter("market", diagnostic)
    execution = ThematicLogFilter("execution", diagnostic)

    assert shadow.filter(_record("Piluitus.StrategyShadow", "outcome recorded"))
    assert vllm.filter(_record("Piluitus.AIEngine", "request completed"))
    assert market.filter(_record("Piluitus.FinamGateway", "websocket reconnect"))
    assert execution.filter(_record("Piluitus.ExecutionLayer", "position updated"))
    assert execution.filter(_record("Piluitus.Main", "[PAPER FILL] ticker=SBER"))

    shadow_be = _record("Piluitus.ExecutionLayer", "[SHADOW BE ARMED] SBER BUY")
    assert shadow.filter(shadow_be)
    assert not execution.filter(shadow_be)


def test_general_session_excludes_diagnostic_and_thematic_records():
    diagnostic = DiagnosticLogFilter()
    themes = tuple(
        ThematicLogFilter(kind, diagnostic)
        for kind in ("shadow", "vllm", "market", "execution")
    )
    session = SessionLogFilter(diagnostic, themes)

    assert session.filter(_record("Piluitus.Main", "PILUITUS startup complete"))
    assert not session.filter(_record("Piluitus.AIEngine", "request completed"))
    assert not session.filter(_record("Piluitus.Main", "[TRADE FLOW 60s] trades=10"))
    assert not session.filter(_record("Piluitus.Main", "[PAPER FILL] ticker=SBER"))


def test_verdict_and_error_streams_are_independent():
    verdicts = VerdictLogFilter()
    errors = ErrorLogFilter()

    assert verdicts.filter(_record("Piluitus.Verdicts", "verdict block"))
    assert not verdicts.filter(_record("Piluitus.Main", "ordinary message"))
    assert errors.filter(_record("Piluitus.AIEngine", "request failed", logging.ERROR))
    assert not errors.filter(_record("Piluitus.Main", "warning", logging.WARNING))
