"""Tests for periodic TRADE FLOW report formatting."""
from __future__ import annotations

import logging

from monitoring.trade_flow_report import TradeFlowReportLogger


def test_trade_flow_report_preserves_candidate_and_llm_metrics(caplog) -> None:
    reporter = TradeFlowReportLogger(logging.getLogger("trade-flow-report-test"))
    counts = {
        "quotes_received": 120,
        "selected_quotes": 85,
        "local_candidate_buy": 3,
        "local_candidate_sell": 1,
        "prefilter_pass_buy": 2,
        "prefilter_pass_sell": 1,
        "llm_calls": 3,
        "orders_opened_buy": 1,
    }

    with caplog.at_level(logging.INFO, logger="trade-flow-report-test"):
        reporter.log(counts, pending_bbo=4)

    message = caplog.text
    assert "[TRADE FLOW 60s]" in message
    assert "котировки=120, котировки_top15=85" in message
    assert "локально(BUY=3,SELL=1)" in message
    assert "до_LLM(BUY=2,SELL=1;доля=75.0%)" in message
    assert "LLM=3" in message
    assert "открыто(BUY=1,SELL=0)" in message
    assert "pending_now=4" in message


def test_trade_flow_report_defaults_missing_counters_to_zero(caplog) -> None:
    reporter = TradeFlowReportLogger(logging.getLogger("trade-flow-report-empty"))

    with caplog.at_level(logging.INFO, logger="trade-flow-report-empty"):
        reporter.log({}, pending_bbo=0)

    message = caplog.text
    assert "[TRADE FLOW 60s]" in message
    assert "котировки=0, котировки_top15=0" in message
    assert "до_LLM(BUY=0,SELL=0;доля=0.0%)" in message
    assert "LLM=0" in message
    assert "pending_now=0" in message
