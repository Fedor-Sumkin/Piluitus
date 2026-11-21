"""Tests for research-only strategy shadow scorecard logging."""
from __future__ import annotations

import logging

from monitoring.shadow_scorecard_logger import StrategyShadowScorecardLogger


class _Tracker:
    def __init__(self, snapshot):
        self.snapshot = snapshot

    def statistics_snapshot(self):
        return self.snapshot

    def pending_count(self):
        return 2


def _row(*, labels=0, ok=0):
    return {
        "labels": labels,
        "ok": ok,
        "positive_rate_pct": 50.0,
        "average_gross_return_pct": 0.12,
        "average_net_return_pct": 0.08,
        "average_mfe_pct": 0.2,
        "average_time_to_mfe_sec": 1.5,
        "average_mae_pct": -0.1,
        "average_time_to_mae_sec": 0.7,
        "average_valid_bbo_observations": 4.0,
        "average_bbo_observation_span_sec": 3.0,
        "profit_factor": 1.4,
    }


def _snapshot(segments=None):
    row = _row(labels=5, ok=5)
    return {
        "eligible": dict(row),
        "ineligible": dict(row),
        "cost_model": {
            "status": "READY",
            "commission_pct_per_side": 0.0354,
            "slippage_bps": 1.0,
        },
        "segments": segments or {},
    }


def test_scorecard_logger_emits_summary_and_pending_count(caplog) -> None:
    tracker = _Tracker(_snapshot())
    reporter = StrategyShadowScorecardLogger(logging.getLogger("shadow-scorecard-test"))

    with caplog.at_level(logging.INFO, logger="shadow-scorecard-test"):
        reporter.log(tracker)

    assert "[STRATEGY SHADOW SCORECARD]" in caplog.text
    assert "cost_status=READY" in caplog.text
    assert "eligible labels=5 ok=5" in caplog.text
    assert "ineligible labels=5 ok=5" in caplog.text
    assert "pending=2" in caplog.text


def test_scorecard_logger_reports_side_and_qualified_ticker_segments(caplog) -> None:
    segments = {
        "side=BUY": _row(labels=6, ok=6),
        "ticker=SBER": _row(labels=5, ok=5),
        "ticker=GAZP": _row(labels=4, ok=4),
        "ticker=VTBR": _row(labels=3, ok=3),
        "regime=TREND": _row(labels=3, ok=3),
    }
    reporter = StrategyShadowScorecardLogger(logging.getLogger("shadow-scorecard-segments"))

    with caplog.at_level(logging.INFO, logger="shadow-scorecard-segments"):
        reporter.log(_Tracker(_snapshot(segments)))

    assert "[STRATEGY SHADOW SIDE] side=BUY" in caplog.text
    assert "[STRATEGY SHADOW TICKER] ticker=SBER" in caplog.text
    assert "[STRATEGY SHADOW SEGMENT] regime=TREND" in caplog.text


def test_scorecard_logger_omits_unqualified_tickers_and_segments(caplog) -> None:
    segments = {
        "ticker=LOW": _row(labels=4, ok=4),
        "regime=LOW": _row(labels=2, ok=2),
    }
    reporter = StrategyShadowScorecardLogger(logging.getLogger("shadow-scorecard-thresholds"))

    with caplog.at_level(logging.INFO, logger="shadow-scorecard-thresholds"):
        reporter.log(_Tracker(_snapshot(segments)))

    assert "[STRATEGY SHADOW TICKER]" not in caplog.text
    assert "[STRATEGY SHADOW SEGMENT]" not in caplog.text
