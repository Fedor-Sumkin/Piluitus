"""Centralized runtime logging configuration for PILUITUS.

This module owns privacy formatting, durable run-file handlers, log routing,
and terminal status integration. Import it after environment loading.
"""
from __future__ import annotations

import logging
import math
import os
import re
import sys
from time import monotonic as _monotonic
from datetime import datetime
from pathlib import Path
from typing import cast
from uuid import uuid4

from config import LOG_REDACT_PROJECT_PATH, LOG_REDACT_TELEGRAM_ID, PROJECT_ROOT
from monitoring.terminal_status import StatusAwareStreamHandler, TerminalStatusLine

# --------------------------------------------------------------
#  LOGGING REGIME
# --------------------------------------------------------------
class _PrivacyFormatter(logging.Formatter):
    """Redact machine-specific paths and Telegram bot IDs from every log output."""

    _telegram_id_re = re.compile(r"(?i)(\bid\s*=\s*)\d+\b")

    @staticmethod
    def _redact_project_path(message: str) -> str:
        if not LOG_REDACT_PROJECT_PATH:
            return message
        root = str(PROJECT_ROOT).rstrip("\\/")
        if not root:
            return message
        variants = {
            root,
            root.replace("\\", "/"),
            root.replace("/", "\\"),
        }
        flags = re.IGNORECASE if os.name == "nt" else 0
        for candidate in sorted(variants, key=len, reverse=True):
            if candidate:
                message = re.sub(re.escape(candidate), "<PROJECT_ROOT>", message, flags=flags)
        return message

    def format(self, record: logging.LogRecord) -> str:
        rendered = super().format(record)
        if LOG_REDACT_TELEGRAM_ID and record.name.lower().startswith("aiogram"):
            rendered = self._telegram_id_re.sub(r"\1<REDACTED>", rendered)
        return self._redact_project_path(rendered)

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
# basicConfig may be a no-op when a server has already installed handlers;
# replace the formatter on every existing root handler either way.
for _root_handler in logging.getLogger().handlers:
    _old_formatter = _root_handler.formatter
    _format = getattr(_old_formatter, "_fmt", "%(message)s")
    _datefmt = getattr(_old_formatter, "datefmt", None)
    _root_handler.setFormatter(_PrivacyFormatter(_format, datefmt=_datefmt))

logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.WARNING)

logger = logging.getLogger("Piluitus.Main")


logger.setLevel(logging.INFO)


class _DurableFileHandler(logging.FileHandler):
    """Flush every record and periodically force it from OS cache to storage.

    Session/diagnostic logs are fsync'ed at a configurable interval to limit
    disk I/O. The error log uses interval=0 and is fsync'ed on every record.
    """

    def __init__(
        self,
        filename,
        mode: str = "a",
        encoding: str | None = None,
        delay: bool = False,
        errors: str | None = None,
        *,
        sync_interval_seconds: float = 1.0,
    ) -> None:
        super().__init__(
            filename,
            mode=mode,
            encoding=encoding,
            delay=delay,
            errors=errors,
        )
        self.sync_interval_seconds = max(0.0, float(sync_interval_seconds))
        self._last_fsync_at = 0.0
        self._last_fsync_warning_at = 0.0
        self._closing = False

    def flush(self) -> None:
        # StreamHandler.emit() calls flush after every formatted log record.
        # This first drains Python's text buffer into the OS.
        super().flush()
        stream = self.stream
        if stream is None:
            return

        now = _monotonic()
        if (
            not self._closing
            and self.sync_interval_seconds > 0
            and now - self._last_fsync_at < self.sync_interval_seconds
        ):
            return

        try:
            # Ask the operating system to commit the file data to the device.
            os.fsync(stream.fileno())
            self._last_fsync_at = now
        except (OSError, ValueError) as exc:
            # Do not recursively log from a handler that may itself be failing.
            # Report at most once per minute directly to stderr.
            if now - self._last_fsync_warning_at >= 60.0:
                try:
                    sys.stderr.write(
                        "[PILUITUS LOG WARNING] Не удалось синхронизировать лог с диском: "
                        f"{type(exc).__name__}\n"
                    )
                    sys.stderr.flush()
                except Exception:
                    pass
                self._last_fsync_warning_at = now

    def close(self) -> None:
        # Force one final fsync before FileHandler closes the stream.
        self._closing = True
        try:
            super().close()
        finally:
            self._closing = False


def _log_fsync_interval_seconds() -> float:
    raw_value = os.getenv("PILUITUS_LOG_FSYNC_INTERVAL_SECONDS", "1.0").strip()
    try:
        value = float(raw_value)
    except (TypeError, ValueError, OverflowError):
        return 1.0
    return value if math.isfinite(value) and value >= 0.0 else 1.0


class _RunIdFilter(logging.Filter):
    """Attach the current process run ID to every record written to a file."""

    def __init__(self, run_id: str):
        super().__init__()
        self.run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        setattr(record, "run_id", self.run_id)
        return True


class _DiagnosticLogFilter(logging.Filter):
    """Keep high-volume market/telemetry diagnostics out of the session narrative."""

    _markers = (
        "[TRADE FLOW 60s]",
        "[ORDER_BOOK 60s]",
        "[OPEN POSITION BBO 60s]",
        "[ORDER_BOOK PARSER SAMPLE]",
        "[ORDER_BOOK UNATTRIBUTED SAMPLE]",
        "[vLLM RAW LATENCY]",
        "[PRE-LLM FILTER]",
        "[PRE-LLM WAIT]",
        "[LLM DECISION]",
        "[LLM REASON MISMATCH]",
        "[LLM CONTRACT RETRY]",
        "[LLM CONTRACT RESULT]",
        "[M1 HISTORY WARMUP]",
        "[ORDER_BOOK POSITION POLICY]",
        "[ORDER_BOOK POSITION RESYNC]",
        "[ORDER_BOOK REST CONTROL]",
        "[PAPER EXIT GUARD]",
        "[CANDIDATE TTL]",
        "[POSITION SIZE]",
        "[SIGNAL GUARD]",
        "[RISK FINAL GATE]",
        "[EXECUTION RESULT]",
        "[EXECUTION OUTCOME]",
        "[EXECUTION STALE SIGNAL BLOCK]",
        "[PAPER BBO ENTRY BLOCK]",
        "[PAPER BBO ENTRY RETRY]",
        "[PAPER FILL]",
        "[PAPER PARTIAL ENTRY]",
        "[PAPER EXIT TRIGGER]",
        "[PAPER POSITION RESUMED]",
        "[PAPER BBO GAP RECOVERED]",
        "[PORTFOLIO GUARD]",
        "[ENTRY SESSION GUARD]",
        "[STOP COOLDOWN HYDRATION FAILED]",
        "[PAPER CLOSE ALL]",
        "[PAPER JOURNAL]",
        "[PAPER EXIT TRIGGER WAIT]",
        "[PAPER EXIT GUARD]",
        "[PAPER STALE POSITION DETAIL]",
        "[PAPER RECONCILIATION GUARD]",
        "[STOP COOLDOWN]",
        "[SHADOW BE ARMED]",
        "[SHADOW BE TRIGGERED]",
        "[M1 PATTERN]",
        "[DIAGNOSTIC LOGGER READY]",
        "[NUMPY ИМПУЛЬС]",
        "[WS ПОТОК ФИНАМ]",
        "Поток Finam активен:",
        "Finam ORDER_BOOK подписки синхронизированы:",
    )

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.ERROR:
            return False
        try:
            message = record.getMessage()
        except Exception:
            return False
        return any(marker in message for marker in self._markers)


class _SessionLogFilter(logging.Filter):
    """Keep ordinary session events and human-readable technical conclusions."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.ERROR:
            return False
        return not _DiagnosticLogFilter().filter(record)


class _ErrorLogFilter(logging.Filter):
    """Route errors and critical failures to the dedicated error file."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= logging.ERROR


def _configure_run_file_logging() -> tuple[str, dict[str, str]]:
    """Create session, diagnostic and error files for one uniquely named run."""
    root_logger = logging.getLogger()
    existing_handlers = [
        handler for handler in root_logger.handlers
        if getattr(handler, "_piluitus_run_log", False)
    ]
    if len(existing_handlers) >= 3:
        paths = {
            str(getattr(handler, "_piluitus_log_kind", "")):
                str(getattr(handler, "_piluitus_run_path", ""))
            for handler in existing_handlers
        }
        run_id = str(getattr(existing_handlers[0], "_piluitus_run_id", "existing"))
        if all(paths.get(kind) for kind in ("session", "diagnostic", "errors")):
            return run_id, paths

    now = datetime.now().astimezone()
    run_id = f"{now.strftime('%Y%m%d_%H%M%S')}_{os.getpid()}_{uuid4().hex[:8]}"
    filename_stamp = now.strftime("%d%m%Y_%H.%M")
    log_dir_value = os.getenv("PILUITUS_LOG_DIR", "trading_logs").strip() or "trading_logs"
    log_dir = Path(log_dir_value).expanduser()
    if not log_dir.is_absolute():
        log_dir = PROJECT_ROOT / log_dir

    names = {
        "session": f"session_{filename_stamp}",
        "diagnostic": f"diagnostic_{filename_stamp}",
        "errors": f"errors_log_{filename_stamp}",
    }
    suffix_number = 1
    while True:
        suffix = "" if suffix_number == 1 else f"_{suffix_number:02d}"
        candidate_paths = {
            kind: log_dir / f"{stem}{suffix}.log"
            for kind, stem in names.items()
        }
        if not any(path.exists() for path in candidate_paths.values()):
            break
        suffix_number += 1

    created_handlers: list[logging.Handler] = []
    filters: dict[str, logging.Filter] = {
        "session": _SessionLogFilter(),
        "diagnostic": _DiagnosticLogFilter(),
        "errors": _ErrorLogFilter(),
    }
    paths: dict[str, str] = {}
    try:
        log_dir.mkdir(parents=True, exist_ok=True)
        for kind in ("session", "diagnostic", "errors"):
            path = candidate_paths[kind]
            handler = _DurableFileHandler(
                path,
                mode="x",
                encoding="utf-8",
                sync_interval_seconds=(
                    0.0 if kind == "errors" else _log_fsync_interval_seconds()
                ),
            )
            created_handlers.append(handler)
            handler.setLevel(logging.ERROR if kind == "errors" else logging.INFO)
            handler.setFormatter(_PrivacyFormatter(
                "%(asctime)s,%(msecs)03d [%(levelname)s] [RUN=%(run_id)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            ))
            handler.addFilter(_RunIdFilter(run_id))
            handler.addFilter(filters[kind])
            setattr(handler, "_piluitus_run_log", True)
            setattr(handler, "_piluitus_log_kind", kind)
            setattr(handler, "_piluitus_run_id", run_id)
            setattr(handler, "_piluitus_run_path", str(path))
            root_logger.addHandler(handler)
            paths[kind] = str(path)
        return run_id, paths
    except OSError as exc:
        for handler in created_handlers:
            root_logger.removeHandler(handler)
            handler.close()
        root_logger.warning(
            "Не удалось создать файловые журналы запуска (каталог=%s): %s",
            log_dir.name, type(exc).__name__,
        )
        return run_id, {}


RUN_ID, RUN_LOG_PATHS = _configure_run_file_logging()
logger.info(
    "🧾 [RUN START] id=%s; session=%s; diagnostic=%s; errors=%s",
    RUN_ID,
    Path(RUN_LOG_PATHS["session"]).name if RUN_LOG_PATHS.get("session") else "unavailable",
    Path(RUN_LOG_PATHS["diagnostic"]).name if RUN_LOG_PATHS.get("diagnostic") else "unavailable",
    Path(RUN_LOG_PATHS["errors"]).name if RUN_LOG_PATHS.get("errors") else "unavailable",
)
logger.info(
    "🛡️ [LOG DURABILITY] session/diagnostic fsync interval=%.2fs; errors=fsync per record",
    _log_fsync_interval_seconds(),
)
logger.info(
    "[DIAGNOSTIC LOGGER READY] path=%s",
    Path(RUN_LOG_PATHS["diagnostic"]).name if RUN_LOG_PATHS.get("diagnostic") else "unavailable",
)

terminal_status = TerminalStatusLine()
_last_numpy_impulse_fallback_log_at = 0.0


def update_numpy_impulse_status(ticker: str) -> None:
    """Show the latest impulse in the status row; rate-limit logs without a TTY."""
    global _last_numpy_impulse_fallback_log_at
    message = f"🚨 [NUMPY ИМПУЛЬС] {ticker} → сигнал в очереди"
    terminal_status.update_impulse(message)

    # Preserve visibility in redirected/non-interactive logs without flooding them.
    if not terminal_status.enabled:
        now = _monotonic()
        if now - _last_numpy_impulse_fallback_log_at >= 5.0:
            logger.warning("%s (повторные сообщения подавляются на 5 сек.)", message)
            _last_numpy_impulse_fallback_log_at = now


def _install_status_aware_logging_handlers() -> None:
    """Replace console handlers with typed handlers that preserve live status."""
    loggers: list[logging.Logger] = [cast(logging.Logger, logging.root)]
    for candidate in logging.Logger.manager.loggerDict.values():
        if isinstance(candidate, logging.Logger):
            loggers.append(candidate)

    replacements: dict[int, StatusAwareStreamHandler] = {}
    for current_logger in loggers:
        # Iterate over a snapshot because handlers are replaced in-place.
        for index, handler in enumerate(list(current_logger.handlers)):
            if not isinstance(handler, logging.StreamHandler):
                continue
            if getattr(handler, "stream", None) not in (sys.stderr, sys.stdout):
                continue
            if isinstance(handler, StatusAwareStreamHandler):
                continue

            replacement = replacements.get(id(handler))
            if replacement is None:
                replacement = StatusAwareStreamHandler(handler.stream, terminal_status)
                replacement.setLevel(handler.level)
                replacement.setFormatter(handler.formatter)
                replacement.terminator = handler.terminator
                for current_filter in handler.filters:
                    replacement.addFilter(current_filter)
                replacements[id(handler)] = replacement
            current_logger.handlers[index] = replacement

_install_status_aware_logging_handlers()
