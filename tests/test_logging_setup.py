"""Regression tests for the extracted runtime logging subsystem."""
from __future__ import annotations

import logging

from monitoring import logging_setup


def _record(
    message: str,
    *,
    level: int = logging.INFO,
    name: str = "Piluitus.Test",
) -> logging.LogRecord:
    return logging.LogRecord(
        name=name,
        level=level,
        pathname=__file__,
        lineno=1,
        msg=message,
        args=(),
        exc_info=None,
    )


def test_privacy_formatter_redacts_telegram_ids(monkeypatch) -> None:
    monkeypatch.setattr(logging_setup, "LOG_REDACT_TELEGRAM_ID", True)
    monkeypatch.setattr(logging_setup, "LOG_REDACT_PROJECT_PATH", False)
    formatter = logging_setup._PrivacyFormatter("%(message)s")

    rendered = formatter.format(_record("request id=123456", name="aiogram.event"))

    assert rendered == "request id=<REDACTED>"


def test_privacy_formatter_redacts_project_root(monkeypatch, tmp_path) -> None:
    monkeypatch.setattr(logging_setup, "LOG_REDACT_TELEGRAM_ID", False)
    monkeypatch.setattr(logging_setup, "LOG_REDACT_PROJECT_PATH", True)
    monkeypatch.setattr(logging_setup, "PROJECT_ROOT", tmp_path)
    formatter = logging_setup._PrivacyFormatter("%(message)s")

    rendered = formatter.format(_record(f"failed at {tmp_path / 'data.json'}"))

    assert rendered == "failed at <PROJECT_ROOT>/data.json"


def test_log_filters_route_diagnostics_session_events_and_errors() -> None:
    diagnostic = _record("[TRADE FLOW 60s] ticks=100")
    session = _record("PAPER position opened")
    error = _record("execution failed", level=logging.ERROR)

    assert logging_setup._DiagnosticLogFilter().filter(diagnostic) is True
    assert logging_setup._SessionLogFilter().filter(diagnostic) is False
    assert logging_setup._SessionLogFilter().filter(session) is True
    assert logging_setup._DiagnosticLogFilter().filter(session) is False
    assert logging_setup._ErrorLogFilter().filter(error) is True
    assert logging_setup._ErrorLogFilter().filter(session) is False


def test_run_id_filter_attaches_run_identifier() -> None:
    record = _record("test")

    assert logging_setup._RunIdFilter("run-123").filter(record) is True
    assert record.run_id == "run-123"


def test_durable_file_handler_fsyncs_each_record_when_interval_is_zero(
    monkeypatch, tmp_path
) -> None:
    sync_calls: list[int] = []
    monkeypatch.setattr(logging_setup.os, "fsync", lambda descriptor: sync_calls.append(descriptor))
    handler = logging_setup._DurableFileHandler(
        tmp_path / "durable.log",
        encoding="utf-8",
        sync_interval_seconds=0.0,
    )

    try:
        handler.emit(_record("durable record"))
        assert sync_calls
    finally:
        handler.close()


def test_fsync_interval_uses_safe_default_for_invalid_values(monkeypatch) -> None:
    monkeypatch.setenv("PILUITUS_LOG_FSYNC_INTERVAL_SECONDS", "not-a-number")

    assert logging_setup._log_fsync_interval_seconds() == 1.0

    monkeypatch.setenv("PILUITUS_LOG_FSYNC_INTERVAL_SECONDS", "0")
    assert logging_setup._log_fsync_interval_seconds() == 0.0
