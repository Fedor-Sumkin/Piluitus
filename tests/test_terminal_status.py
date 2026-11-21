"""Tests for the extracted terminal status/logging components."""
from __future__ import annotations

import io
import logging

from monitoring.terminal_status import StatusAwareStreamHandler, TerminalStatusLine


class _TTYBuffer(io.StringIO):
    def isatty(self) -> bool:
        return True


def test_terminal_status_line_uses_terminal_cell_width_for_cyrillic_and_cjk() -> None:
    assert TerminalStatusLine._cell_width("AБ中") == 4


def test_terminal_status_line_does_not_write_control_sequences_without_tty() -> None:
    stream = io.StringIO()
    status = TerminalStatusLine(stream=stream)

    status.update("Finam ticks 100")

    assert status.enabled is False
    assert stream.getvalue() == ""


def test_status_handler_compacts_finam_stream_status() -> None:
    stream = _TTYBuffer()
    status = TerminalStatusLine(stream=stream)
    handler = StatusAwareStreamHandler(stream, status)
    record = logging.LogRecord(
        name="test", level=logging.INFO, pathname=__file__, lineno=1,
        msg="Поток Finam активен: 1200 котировок в очередь=15; последний тик SBER",
        args=(), exc_info=None,
    )

    assert handler._capture_status_message(record) is True
    assert status._finam_text == "📡 Finam 1200 Q=15 SBER"


def test_status_handler_compacts_vllm_latency() -> None:
    stream = _TTYBuffer()
    status = TerminalStatusLine(stream=stream)
    handler = StatusAwareStreamHandler(stream, status)
    record = logging.LogRecord(
        name="test", level=logging.WARNING, pathname=__file__, lineno=1,
        msg="[vLLM RAW LATENCY] inference took 3.912 сек", args=(), exc_info=None,
    )

    assert handler._capture_status_message(record) is True
    assert status._latency_text == "⏱ vLLM 3.912s"
