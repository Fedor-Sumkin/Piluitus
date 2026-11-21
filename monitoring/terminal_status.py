"""Reusable terminal status rendering and status-aware console logging."""
from __future__ import annotations

import logging
import re
import shutil
import sys
import threading
import time as time_module
import unicodedata


class TerminalStatusLine:
    """One in-place terminal status line that is redrawn after log messages.

    Uses stderr, the same stream as logging.StreamHandler. Updates are throttled
    to reduce console I/O during high-frequency market data bursts. If output is
    redirected rather than attached to an interactive terminal, it stays silent
    instead of injecting carriage-return control characters into log files.
    """

    def __init__(self, stream=None, refresh_interval: float = 0.20):
        self.stream = stream if stream is not None else sys.stderr
        try:
            self.enabled = bool(self.stream.isatty())
        except (AttributeError, OSError):
            self.enabled = False
        self.refresh_interval = max(0.05, float(refresh_interval))
        self._lock = threading.RLock()
        self._text = ""
        self._market_text = ""
        self._funnel_text = ""
        self._finam_text = ""
        self._subscription_text = ""
        self._latency_text = ""
        self._impulse_text = ""
        self._visible = False
        self._last_draw = 0.0
        self._last_width = 0

    def _terminal_width(self) -> int:
        try:
            return max(20, shutil.get_terminal_size(fallback=(120, 24)).columns)
        except OSError:
            return 120

    @staticmethod
    def _cell_width(value: str) -> int:
        """Approximate terminal columns (not Python code points) for Unicode text."""
        width = 0
        for char in value:
            if char == "\u200d" or "\ufe00" <= char <= "\ufe0f":
                continue
            if unicodedata.combining(char):
                continue
            width += 2 if unicodedata.east_asian_width(char) in {"W", "F"} else 1
        return width

    @classmethod
    def _truncate_cells(cls, value: str, max_cells: int) -> str:
        result: list[str] = []
        used = 0
        for char in value:
            char_width = cls._cell_width(char)
            if used + char_width > max_cells:
                break
            result.append(char)
            used += char_width
        return "".join(result)

    def _clear_locked(self) -> None:
        if self.enabled and self._visible:
            # Erase the whole terminal row: no stale suffix survives a shorter update.
            self.stream.write("\r\x1b[2K")
            self.stream.flush()
        self._visible = False
        self._last_width = 0

    def _draw_locked(self, force: bool = False) -> None:
        if not self.enabled or not self._text:
            return
        now = time_module.monotonic()
        if not force and now - self._last_draw < self.refresh_interval:
            return

        self._clear_locked()
        # Reserve columns to avoid wrapping; truncate by terminal cells, not code points.
        max_cells = max(16, self._terminal_width() - 8)
        text = self._truncate_cells(self._text, max_cells)
        self.stream.write(text)
        self.stream.flush()
        self._visible = True
        self._last_width = self._cell_width(text)
        self._last_draw = now

    def _compose_text_locked(self) -> None:
        """Compose market, Finam, vLLM latency and NumPy status in one row."""
        market_text = self._market_text.strip()
        # Keep the live tick counter, but remove the repeated long label to save columns.
        market_text = market_text.replace("[WS ПОТОК ФИНАМ]", "Finam ticks")
        segments = [
            market_text,
            self._funnel_text.strip(),
            self._finam_text.strip(),
            self._subscription_text.strip(),
            self._latency_text.strip(),
            self._impulse_text.strip(),
        ]
        segments = [segment for segment in segments if segment]
        max_cells = max(32, self._terminal_width() - 8)
        if not segments:
            self._text = ""
            return

        # Reserve space for all status sources using rendered terminal-cell widths.
        separator = "  |  "
        available = max_cells - self._cell_width(separator) * (len(segments) - 1)
        while sum(self._cell_width(segment) for segment in segments) > available:
            longest_index = max(range(len(segments)), key=lambda i: self._cell_width(segments[i]))
            segment = segments[longest_index]
            segment_width = self._cell_width(segment)
            if segment_width <= 12:
                segments.pop()
                if not segments:
                    self._text = ""
                    return
                available = max_cells - self._cell_width(separator) * (len(segments) - 1)
                continue
            excess = sum(self._cell_width(item) for item in segments) - available
            target_width = max(12, segment_width - max(1, excess))
            shortened = self._truncate_cells(segment, target_width - 1)
            segments[longest_index] = shortened + "…"
        self._text = separator.join(segments)

    def update_funnel(self, text: str) -> None:
        """Update the local 88-to-top-N screener status in the persistent row."""
        with self._lock:
            self._funnel_text = str(text)
            self._compose_text_locked()
            self._draw_locked()

    def update_finam(self, text: str) -> None:
        """Update latest Finam stream counters without emitting a new log line."""
        with self._lock:
            self._finam_text = str(text)
            self._compose_text_locked()
            self._draw_locked()

    def update_subscription(self, text: str) -> None:
        """Update the latest QUOTES subscription batch in the persistent status row."""
        with self._lock:
            self._subscription_text = str(text)
            self._compose_text_locked()
            self._draw_locked()

    def update_latency(self, text: str) -> None:
        """Update the latest vLLM inference latency in the persistent status row."""
        with self._lock:
            self._latency_text = str(text)
            self._compose_text_locked()
            self._draw_locked()

    def update(self, text: str) -> None:
        """Update the live market part without clearing the latest impulse."""
        with self._lock:
            self._market_text = str(text)
            self._compose_text_locked()
            self._draw_locked()

    def update_impulse(self, text: str) -> None:
        """Update the NumPy impulse segment in-place instead of logging each tick."""
        with self._lock:
            self._impulse_text = str(text)
            self._compose_text_locked()
            # Respect the same redraw throttle as the market-tick status.
            self._draw_locked()

    def before_log(self) -> None:
        # Keep the lock held through log emission so a tick update cannot overwrite it.
        self._lock.acquire()
        self._clear_locked()

    def after_log(self) -> None:
        try:
            self._draw_locked(force=True)
        finally:
            self._lock.release()


class StatusAwareStreamHandler(logging.StreamHandler):
    """Console handler that folds high-frequency status messages into one row.

    This avoids assigning a function to ``handler.emit`` at runtime, which is
    incompatible with the method's descriptor type under Pylance.
    """

    def __init__(self, stream, status_line: TerminalStatusLine) -> None:
        super().__init__(stream)
        self.status_line = status_line

    def _capture_status_message(self, record: logging.LogRecord) -> bool:
        """Update the status row and return True when the log line is consumed."""
        if not self.status_line.enabled:
            return False

        try:
            message = record.getMessage()
        except Exception:
            message = ""

        if "Поток Finam активен:" in message:
            compact = message.split("Поток Finam активен:", 1)[1].strip()
            compact = compact.replace("котировок в очередь=", "Q=")
            compact = compact.replace("последний тик ", "")
            compact = compact.replace("; ", " ")
            self.status_line.update_finam("📡 Finam " + compact)
            return True

        if "Подписка QUOTES отправлена:" in message:
            batch_match = re.search(
                r"пакет\s+(\d+)/(\d+),\s*символов\s+(\d+)\s*\((.*?)\)\.?$",
                message,
            )
            if batch_match:
                batch_no, batch_total, symbols_count, symbols = batch_match.groups()
                symbols = symbols.strip().rstrip(".")
                self.status_line.update_subscription(
                    f"📨 QUOTES {batch_no}/{batch_total} {symbols_count} sym {symbols}"
                )
            else:
                self.status_line.update_subscription("📨 QUOTES подписка отправлена")
            return True

        if "[vLLM RAW LATENCY]" in message:
            latency_match = re.search(r"([0-9]+(?:\.[0-9]+)?)\s*сек", message)
            latency = latency_match.group(1) if latency_match else "?"
            self.status_line.update_latency(f"⏱ vLLM {latency}s")
            return True

        return False

    def _colorize_control_record(self, record: logging.LogRecord) -> logging.LogRecord:
        """Return a colored copy for selected console diagnostics only."""
        # Do not inject ANSI escape sequences into redirected output or log files.
        if not self.status_line.enabled:
            return record

        try:
            message = record.getMessage()
        except Exception:
            return record

        upper = message.upper()
        color = ""
        reset = "\x1b[0m"

        if "[PAPER BBO ENTRY RETRY]" in upper:
            color = "\x1b[96m"  # bright cyan
        elif "[PAPER BBO ENTRY BLOCK]" in upper:
            color = "\x1b[91m"  # bright red
        elif "[DASHBOARD EXECUTION]" in upper:
            failure_tokens = ("BLOCKED", "FAILED", "ERROR", "SKIPPED", "ОТКЛОН", "ОШИБК", "БЛОК")
            success_tokens = ("EXECUTED", "FILLED", "OPENED", "SUCCESS", "ИСПОЛНЕН", "ОТКРЫТ", "УСПЕШ")
            if any(token in upper for token in failure_tokens):
                color = "\x1b[91m"  # bright red
            elif any(token in upper for token in success_tokens):
                color = "\x1b[92m"  # bright green
            else:
                color = "\x1b[93m"  # bright yellow
        elif "TRADE FLOW 60S" in upper:
            color = "\x1b[95m"  # bright magenta

        if not color:
            return record

        # Clone the LogRecord: another handler (especially a file handler)
        # must receive the original, uncolored message.
        colored = logging.makeLogRecord(record.__dict__.copy())
        colored.msg = f"{color}{message}{reset}"
        colored.args = ()
        return colored

    def emit(self, record: logging.LogRecord) -> None:
        """Emit ordinary logs around the persistent status line."""
        if self._capture_status_message(record):
            return

        self.status_line.before_log()
        try:
            console_record = self._colorize_control_record(record)
            super().emit(console_record)
        finally:
            self.status_line.after_log()
