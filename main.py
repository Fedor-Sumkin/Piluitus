import asyncio
import json
import math
import logging
import re
import os
import sys
import time as time_module
import threading
import shutil
import unicodedata
from itertools import count
from contextlib import asynccontextmanager
from typing import Any, List, Union, cast
from aiogram import Bot 

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Body, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, time
from uuid import uuid4
from types import SimpleNamespace
from fastapi.templating import Jinja2Templates
from config import (
    Settings,
    PROJECT_ROOT,
    LOG_REDACT_TELEGRAM_ID,
    LOG_REDACT_PROJECT_PATH,
    TICKERS_TO_TRADE,
    FINAM_TRADE_LOT_SIZES,
    LOT_SIZES,
)
from risk_limits import validate_daily_loss_limit

# --- PILUITUS‑V3 modules ------------------------------------------------
from ai_engine import AIEngine
from db_manager import DBManager
from finam_client import FinamBrokerClient
from screener_funnel import HFTMemoryScreener
from execution_layer import BaseExecutionEngine
from risk_manager import RiskManager
from signal_guard import validate_signal_evidence, infer_candidate_direction, validate_atr_readiness
from rag_prompt_utils import select_prompt_rules
from candidate_recovery import coalesce_pending_bbo_entry, refresh_candidate_bbo_metrics
from scalping_strategy import evaluate_scalping_candidate
from shadow_outcomes import ShadowForwardTracker
from dashboard_verdicts import (
    build_verdict_explanation,
    finite_float,
    nonnegative_int,
    normalise_execution_status,
)
import tg_bot
from news_parser import MarketNewsParser

# --------------------------------------------------------------
#  ENVIRONMENT & INITIALIZATION
# --------------------------------------------------------------
load_dotenv()

FINAM_TOKEN = os.getenv("FINAM_TOKEN", "")
# PAPER orders are recorded against an internal simulator identity, never a Finam client/account ID.
CLIENT_ID = "PILUITUS_PAPER" if Settings.PAPER_TRADING else os.getenv("FINAM_CLIENT_ID", "UNKNOWN_ACCOUNT")
DEPOSIT = float(os.getenv("TRADING_DEPOSIT", 100000.0))

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


def _parse_finite_float(value: object) -> float | None:
    """Convert an untrusted JSON scalar to a finite float."""
    if isinstance(value, bool):
        return None
    if isinstance(value, str):
        raw = value
    elif isinstance(value, (int, float)):
        raw = value
    else:
        return None
    try:
        number = float(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None
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

        now = time_module.monotonic()
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


terminal_status = TerminalStatusLine()
_last_numpy_impulse_fallback_log_at = 0.0


def update_numpy_impulse_status(ticker: str) -> None:
    """Show the latest impulse in the status row; rate-limit logs without a TTY."""
    global _last_numpy_impulse_fallback_log_at
    message = f"🚨 [NUMPY ИМПУЛЬС] {ticker} → сигнал в очереди"
    terminal_status.update_impulse(message)

    # Preserve visibility in redirected/non-interactive logs without flooding them.
    if not terminal_status.enabled:
        now = time_module.monotonic()
        if now - _last_numpy_impulse_fallback_log_at >= 5.0:
            logger.warning("%s (повторные сообщения подавляются на 5 сек.)", message)
            _last_numpy_impulse_fallback_log_at = now


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

# --------------------------------------------------------------
#  1️⃣  GLOBAL OBJECTS & PRIORITIZED CONTOUR
# --------------------------------------------------------------
db_manager = DBManager()
ai_engine = AIEngine()
broker = FinamBrokerClient(token=FINAM_TOKEN, is_demo=Settings.FINAM_DEMO)
screener = HFTMemoryScreener(window_size=30)
execution_engine = BaseExecutionEngine(
    finam_broker_client=broker,
    screener=screener,
    initial_capital=DEPOSIT,
    max_open_stop_risk_pct=Settings.MAX_OPEN_STOP_RISK_PCT,
)
# Keep Finam ORDER_BOOK subscriptions for every active PAPER position, even
# when its ticker rotates out of the screener's Top-N list.
broker.open_positions_provider = execution_engine.get_open_position_tickers
risk_manager = RiskManager(
    max_risk_per_trade=Settings.MAX_RISK_PCT,
    daily_loss_limit=Settings.DAILY_LOSS_LIMIT_PCT,
    initial_deposit=DEPOSIT,
    drawdown_warning_pct=Settings.DRAWDOWN_WARNING_PCT,
    max_drawdown_pct=Settings.MAX_DRAWDOWN_PCT,
    loss_streak_3_pause_sec=Settings.LOSS_STREAK_3_PAUSE_SEC,
    loss_streak_5_pause_sec=Settings.LOSS_STREAK_5_PAUSE_SEC,
    loss_streak_hard_stop=Settings.LOSS_STREAK_HARD_STOP,
)

if not math.isclose(
    float(execution_engine.initial_capital),
    float(risk_manager.initial_deposit),
    rel_tol=0.0,
    abs_tol=0.01,
):
    raise RuntimeError(
        "Несогласованный стартовый капитал: ExecutionLayer и RiskManager должны использовать TRADING_DEPOSIT."
    )
logger.info(
    "💰 [CAPITAL SYNC] TRADING_DEPOSIT=%.2f ₽; ExecutionLayer=%.2f ₽; RiskManager=%.2f ₽.",
    DEPOSIT, execution_engine.initial_capital, risk_manager.initial_deposit,
)


def _current_paper_open_pnl() -> float:
    """PnL of in-memory PAPER positions, updated on every monitored market tick."""
    if not getattr(execution_engine, "paper_mode", True):
        return 0.0
    total = 0.0
    for position in getattr(execution_engine, "paper_positions", {}).values():
        if not isinstance(position, dict) or str(position.get("status", "OPEN")).upper() != "OPEN":
            continue
        try:
            value = _parse_finite_float(position.get("pnl", 0.0))
            if value is not None:
                total += value
        except (TypeError, ValueError):
            continue
    return total


def _current_paper_assets_cost() -> float:
    """Cost basis of active and quarantined PAPER positions, never mark-to-market."""
    if not getattr(execution_engine, "paper_mode", True):
        return 0.0
    from portfolio_metrics import calculate_assets_used

    positions = list(getattr(execution_engine, "paper_positions", {}).values())
    unresolved = getattr(execution_engine, "_unreconciled_paper_positions", {})
    if isinstance(unresolved, dict):
        positions.extend(unresolved.values())
    return calculate_assets_used(positions, lot_sizes=LOT_SIZES)


# In PAPER, in-memory positions are the live source of floating PnL and book cost.
# Avoid adding the same OPEN journal rows a second time from Redis.
if execution_engine.paper_mode:
    risk_manager.open_pnl_provider = _current_paper_open_pnl
    risk_manager.open_assets_cost_provider = _current_paper_assets_cost

# Глобальные кэши и лимиты ОЗУ под High-Load тики
last_ai_verdicts_cache: list[dict[str, Any]] = []
MAX_CACHE_SIZE = 100  # Защита ОЗУ от утечек памяти
tick_counter = 0
spinner_frames = ["|", "/", "-", "\\"]

# Асинхронная очередь с приоритетами. Второй элемент — уникальный sequence ID:
# PriorityQueue сравнивает tuple поэлементно; без уникального ID при совпадении
# priority/timestamp/event_type Python пытался сравнивать payload-словари и падал
# с TypeError: '<' not supported between instances of 'dict' and 'dict'.
PriorityItem = tuple[int, int, float, str, dict[str, Any]]
anomaly_queue: asyncio.PriorityQueue[PriorityItem] = asyncio.PriorityQueue(maxsize=1000)
_priority_sequence = count()
# Serializes order publication with PAPER maintenance; generation invalidates old queued/in-flight AI events.
_paper_session_lock = asyncio.Lock()
_paper_session_generation = 0
bg_market_task = None
_last_funnel_log_at = 0.0
_last_funnel_log_key: tuple[str, ...] | None = None
_last_funnel_ready = False

# Счётчики диагностируют, на каком этапе останавливаются сигналы. Окно — 60 секунд.
_trade_flow_lock = threading.Lock()
_trade_flow_counts: dict[str, int] = {}
_trade_flow_last_log_at = time_module.monotonic()
# Forward labels are research-only and bounded in memory; all observations are logged.
scalping_shadow_tracker = ShadowForwardTracker()

# One outstanding AI event per ticker prevents repeated nearly identical
# anomaly events from filling the queue while inference is still running.
_ai_ticker_pending: set[str] = set()
_ai_ticker_last_enqueued_at: dict[str, float] = {}
_ai_ticker_pending_lock = threading.Lock()
try:
    _AI_TICKER_COOLDOWN_SEC = max(0.5, float(os.getenv("PILUITUS_AI_TICKER_COOLDOWN_SEC", "2.0")))
except (TypeError, ValueError):
    _AI_TICKER_COOLDOWN_SEC = 2.0
_prefilter_last_logged: dict[tuple[str, str], float] = {}
_prefilter_log_lock = threading.Lock()
_pending_bbo_candidates: dict[str, dict[str, Any]] = {}
_pending_bbo_lock = threading.Lock()
try:
    _PENDING_BBO_CANDIDATE_TTL_SEC = min(5.0, max(0.25, float(os.getenv("PILUITUS_PENDING_BBO_TTL_SEC", "1.5"))))
except (TypeError, ValueError):
    _PENDING_BBO_CANDIDATE_TTL_SEC = 1.5


# Per-open-position ORDER_BOOK diagnostics are reported in the same 60-second
# windows as TRADE FLOW. These counters do not influence entries or exits.
_position_order_book_diag_lock = threading.Lock()
_position_order_book_diag: dict[str, dict[str, Any]] = {}


def _record_position_order_book_event(ticker: str, result: dict[str, Any]) -> None:
    engine = globals().get("execution_engine")
    positions = getattr(engine, "paper_positions", {}) if engine is not None else {}
    trade = positions.get(ticker) if isinstance(positions, dict) else None
    if not isinstance(trade, dict) or str(trade.get("status", "OPEN")).upper() not in {"OPEN", "ACTIVE"}:
        return

    reason = str(result.get("reason") or result.get("book_reason") or "").strip().upper()
    if not reason:
        reason = "OK" if result.get("valid", True) else "UNKNOWN"
    now = time_module.monotonic()
    with _position_order_book_diag_lock:
        item = _position_order_book_diag.setdefault(ticker, {
            "window_events": 0,
            "window_valid": 0,
            "window_invalid": 0,
            "window_out_of_order": 0,
            "window_unusable": 0,
            "window_invalid_reasons": {},
            "last_event_monotonic": 0.0,
            "last_result": "",
        })
        item["window_events"] += 1
        if result.get("updated") and result.get("valid", True):
            item["window_valid"] += 1
        elif result.get("updated"):
            item["window_invalid"] += 1
            reasons = item["window_invalid_reasons"]
            reasons[reason] = reasons.get(reason, 0) + 1
        elif result.get("out_of_order"):
            item["window_out_of_order"] += 1
        else:
            item["window_unusable"] += 1
        item["last_event_monotonic"] = now
        item["last_result"] = reason


def _log_open_position_bbo_diagnostics() -> None:
    engine = globals().get("execution_engine")
    current_screener = globals().get("screener")
    positions = getattr(engine, "paper_positions", {}) if engine is not None else {}
    quotes = getattr(current_screener, "quotes", {}) if current_screener is not None else {}
    if not isinstance(positions, dict) or not positions:
        return

    now = time_module.monotonic()
    for raw_ticker, trade in sorted(positions.items()):
        if not isinstance(trade, dict) or str(trade.get("status", "OPEN")).upper() not in {"OPEN", "ACTIVE"}:
            continue
        ticker = str(trade.get("ticker") or raw_ticker).strip().upper().split("@")[0]
        quote = quotes.get(ticker, {}) if isinstance(quotes, dict) else {}
        if not isinstance(quote, dict):
            quote = {}

        def age_text(field: str) -> str:
            observed = _parse_finite_float(quote.get(field))
            if observed is None or observed <= 0.0:
                return "нет"
            age = now - observed
            return f"{age:.2f}s" if math.isfinite(age) and age >= 0.0 else "будущее"

        side = str(trade.get("side", "")).strip().upper()
        executable_side = "ASK" if side == "SELL" else "BID"
        executable_age = age_text(
            "last_ask_confirmed_seen" if executable_side == "ASK" else "last_bid_confirmed_seen"
        )
        with _position_order_book_diag_lock:
            item = _position_order_book_diag.setdefault(ticker, {
                "window_events": 0,
                "window_valid": 0,
                "window_invalid": 0,
                "window_out_of_order": 0,
                "window_unusable": 0,
                "window_invalid_reasons": {},
                "last_event_monotonic": 0.0,
                "last_result": "",
            })
            window_events = item["window_events"]
            window_valid = item["window_valid"]
            window_invalid = item["window_invalid"]
            window_out_of_order = item["window_out_of_order"]
            window_unusable = item["window_unusable"]
            invalid_reasons = dict(item["window_invalid_reasons"])
            last_event_monotonic = float(item["last_event_monotonic"] or 0.0)
            last_result = str(item.get("last_result") or "-")
            item["window_events"] = 0
            item["window_valid"] = 0
            item["window_invalid"] = 0
            item["window_out_of_order"] = 0
            item["window_unusable"] = 0
            item["window_invalid_reasons"] = {}

        invalid_summary = ",".join(
            f"{reason}:{count}" for reason, count in sorted(invalid_reasons.items())
        ) or "-"
        last_event_age = (
            f"{now - last_event_monotonic:.2f}s"
            if last_event_monotonic > 0.0 and now >= last_event_monotonic else "нет"
        )
        logger.info(
            "📘 [OPEN POSITION BBO 60s] %s side=%s exec=%s exec_age=%s "
            "Bid=%.6f Ask=%.6f bid_age=%s ask_age=%s book_event_age=%s "
            "book_receive_age=%s book_valid=%s book_reason=%s last_position_event_age=%s "
            "last_position_result=%s events=%d valid=%d invalid=%d out_of_order=%d unusable=%d invalid_reasons=%s",
            ticker, side, executable_side, executable_age,
            _parse_finite_float(quote.get("bid")) or 0.0,
            _parse_finite_float(quote.get("ask")) or 0.0,
            age_text("last_bid_confirmed_seen"),
            age_text("last_ask_confirmed_seen"),
            age_text("last_order_book_event_seen"),
            age_text("last_order_book_received"),
            bool(quote.get("order_book_valid", False)),
            str(quote.get("order_book_last_reason", "NO_ORDER_BOOK")),
            last_event_age, last_result, window_events, window_valid, window_invalid,
            window_out_of_order, window_unusable, invalid_summary,
        )


def _trade_flow_inc(key: str, amount: int = 1) -> None:
    with _trade_flow_lock:
        _trade_flow_counts[key] = _trade_flow_counts.get(key, 0) + amount
    _trade_flow_maybe_log()


def _trade_flow_maybe_log(force: bool = False) -> None:
    global _trade_flow_last_log_at, _trade_flow_counts
    now = time_module.monotonic()
    with _trade_flow_lock:
        if not force and now - _trade_flow_last_log_at < 60.0:
            return
        counts = dict(_trade_flow_counts)
        _trade_flow_counts = {}
        _trade_flow_last_log_at = now

    names = (
        "quotes_received", "selected_quotes", "selected_anomalies", "anomalies_queued",
        "queue_dropped", "direction_prefilter_skipped", "prefilter_blocked", "candidate_deduplicated",
        "bbo_deferred", "bbo_resumed", "bbo_expired", "bbo_superseded", "bbo_coalesced", "bbo_cancelled",
        "bbo_resume_attempted", "bbo_resume_from_book", "bbo_resume_from_quotes", "bbo_resume_expired",
        "bbo_resume_session_cancelled", "bbo_resume_invalid_payload", "bbo_resume_funnel_not_ready",
        "bbo_resume_not_selected", "bbo_resume_active_position", "bbo_resume_quote_missing",
        "bbo_resume_book_unconfirmed", "bbo_resume_invalid_bbo", "bbo_resume_book_stale",
        "bbo_resume_confirmation_stale", "bbo_resume_other_book_failure",
        "bbo_resume_direction_changed", "bbo_resume_evidence_quotes", "bbo_resume_evidence_blocked",
        "bbo_resume_atr_blocked", "bbo_resume_payload_changed", "bbo_resume_deduplicated",
        "bbo_resume_queue_full",
        "local_candidate_buy", "local_candidate_sell",
        "prefilter_pass_buy", "prefilter_pass_sell",
        "llm_calls", "llm_model_approve", "llm_model_hold",
        "llm_request_error", "llm_timeout",
        "post_llm_evidence_blocked", "post_llm_confidence_blocked",
        "llm_approve_buy", "llm_approve_sell",
        "llm_hold", "llm_hold_buy", "llm_hold_sell", "llm_contract_violation",
        "llm_contract_retry", "llm_contract_retry_recovered", "llm_contract_retry_failed",
        "orders_opened_buy", "orders_opened_sell", "guard_blocked",
        "guard_quotes", "guard_spread", "guard_volume", "guard_direction", "guard_size",
        "guard_text", "guard_other", "confidence_blocked", "risk_blocked", "zero_lot",
        "strategy_shadow_evaluated", "strategy_shadow_pass", "strategy_shadow_reject",
        "shadow_block_bbo_unavailable", "shadow_block_bbo_stale",
        "shadow_block_order_book_not_confirmed", "shadow_block_atr_not_ready",
        "shadow_block_spread", "shadow_block_other",
        "orders_scheduled", "orderbook_updates", "orderbook_unusable",
        "orderbook_side_touches", "orderbook_heartbeats", "orderbook_invalid",
        "orderbook_out_of_order", "active_position_skipped", "atr_unready_candidates",
        "execution_bbo_blocked", "atr_history_blocked", "portfolio_cap_blocked",
        "portfolio_size_reduced", "entry_session_blocked", "execution_other_blocked",
    )
    values = {key: counts.get(key, 0) for key in names}
    with _pending_bbo_lock:
        values["bbo_pending"] = len(_pending_bbo_candidates)
    local_total = counts.get("local_candidate_buy", 0) + counts.get("local_candidate_sell", 0)
    pass_total = counts.get("prefilter_pass_buy", 0) + counts.get("prefilter_pass_sell", 0)
    values["prefilter_pass_pct"] = round(100.0 * pass_total / local_total, 1) if local_total else 0.0
    logger.info(
        "📊 [TRADE FLOW 60s] котировки=%(quotes_received)d, котировки_top15=%(selected_quotes)d, "
        "аномалии_top15=%(selected_anomalies)d, в_очереди=%(anomalies_queued)d, очередь_потери=%(queue_dropped)d; "
        "направление_не_совпало=%(direction_prefilter_skipped)d, prefilter_block=%(prefilter_blocked)d, "
        "duplicate=%(candidate_deduplicated)d; локально(BUY=%(local_candidate_buy)d,SELL=%(local_candidate_sell)d), "
        "до_LLM(BUY=%(prefilter_pass_buy)d,SELL=%(prefilter_pass_sell)d;доля=%(prefilter_pass_pct).1f%%); "
        "LLM=%(llm_calls)d, model(APPROVE=%(llm_model_approve)d,HOLD=%(llm_model_hold)d), "
        "после_LLM(guard_block=%(post_llm_evidence_blocked)d,confidence_block=%(post_llm_confidence_blocked)d), "
        "итог(APPROVE_BUY=%(llm_approve_buy)d,APPROVE_SELL=%(llm_approve_sell)d,"
        "HOLD_BUY-кандидат=%(llm_hold_buy)d,HOLD_SELL-кандидат=%(llm_hold_sell)d), "
        "request_error=%(llm_request_error)d (timeout=%(llm_timeout)d); "
        "contract_error=%(llm_contract_violation)d (retry=%(llm_contract_retry)d,"
        "recovered=%(llm_contract_retry_recovered)d,failed=%(llm_contract_retry_failed)d); "
        "открыто(BUY=%(orders_opened_buy)d,SELL=%(orders_opened_sell)d); "
        "Guard=%(guard_blocked)d (BBO=%(guard_quotes)d, спред=%(guard_spread)d, объём=%(guard_volume)d, "
        "направление=%(guard_direction)d, размер=%(guard_size)d, текст=%(guard_text)d, прочее=%(guard_other)d); "
        "ORDER_BOOK: принятых_состояний=%(orderbook_updates)d, затронуто_сторон_BBO=%(orderbook_side_touches)d, "
        "heartbeat_без_движения_BBO=%(orderbook_heartbeats)d, некорректных_состояний=%(orderbook_invalid)d, "
        "событий_не_по_порядку=%(orderbook_out_of_order)d, ошибок_обработки=%(orderbook_unusable)d; "
        "повторные кандидаты по открытым позициям=%(active_position_skipped)d; "
        "ATR ожидание до LLM (кандидаты отложены)=%(atr_unready_candidates)d; "
        "блок ATR после APPROVE=%(atr_history_blocked)d; "
        "BBO wait=%(bbo_deferred)d, resumed=%(bbo_resumed)d, expired=%(bbo_expired)d, "
        "superseded=%(bbo_superseded)d, coalesced=%(bbo_coalesced)d, cancelled=%(bbo_cancelled)d, pending_now=%(bbo_pending)d; "
        "BBO resume: attempts=%(bbo_resume_attempted)d (ORDER_BOOK=%(bbo_resume_from_book)d, QUOTES=%(bbo_resume_from_quotes)d), "
        "expired=%(bbo_resume_expired)d, session_cancel=%(bbo_resume_session_cancelled)d, "
        "funnel_not_ready=%(bbo_resume_funnel_not_ready)d, not_selected=%(bbo_resume_not_selected)d, "
        "active_position=%(bbo_resume_active_position)d, invalid_payload=%(bbo_resume_invalid_payload)d, "
        "book_wait(missing=%(bbo_resume_quote_missing)d, unconfirmed=%(bbo_resume_book_unconfirmed)d, "
        "invalid=%(bbo_resume_invalid_bbo)d, stale=%(bbo_resume_book_stale)d, "
        "confirmation_stale=%(bbo_resume_confirmation_stale)d, other=%(bbo_resume_other_book_failure)d), "
        "direction_changed=%(bbo_resume_direction_changed)d, evidence(quotes=%(bbo_resume_evidence_quotes)d, "
        "blocked=%(bbo_resume_evidence_blocked)d), ATR=%(bbo_resume_atr_blocked)d, "
        "payload_changed=%(bbo_resume_payload_changed)d, deduplicated=%(bbo_resume_deduplicated)d, "
        "queue_full=%(bbo_resume_queue_full)d; "
        "после_LLM_исполнение: portfolio_cap_block=%(portfolio_cap_blocked)d, "
        "portfolio_size_reduced=%(portfolio_size_reduced)d, entry_session_block=%(entry_session_blocked)d, "
        "other_block=%(execution_other_blocked)d; "
        "strategy_shadow=%(strategy_shadow_pass)d/%(strategy_shadow_evaluated)d pass "
        "(reject=%(strategy_shadow_reject)d; hard_blocks: BBO_missing=%(shadow_block_bbo_unavailable)d, "
        "BBO_stale=%(shadow_block_bbo_stale)d, book_unconfirmed=%(shadow_block_order_book_not_confirmed)d, "
        "ATR=%(shadow_block_atr_not_ready)d, spread=%(shadow_block_spread)d, other=%(shadow_block_other)d); "
        "confidence=%(confidence_blocked)d, RiskManager_pre_LLM=%(risk_blocked)d, "
        "минимальный_лот_по_риску=%(zero_lot)d, BBO_entry_block=%(execution_bbo_blocked)d, "
        "ордеров_принято/открыто=%(orders_scheduled)d",
        values,
    )
    shadow = scalping_shadow_tracker.statistics_snapshot()
    eligible = shadow["eligible"]
    ineligible = shadow["ineligible"]
    logger.info(
        "[STRATEGY SHADOW SCORECARD] eligible labels=%d ok=%d positive=%.1f%% avg_gross=%+.5f%% avg_net=%+.5f%% "
        "MFE=%+.5f%% MAE=%+.5f%%; ineligible labels=%d ok=%d positive=%.1f%% avg_gross=%+.5f%% avg_net=%+.5f%% "
        "MFE=%+.5f%% MAE=%+.5f%%; pending=%d",
        eligible["labels"], eligible["ok"], eligible["positive_rate_pct"],
        eligible["average_gross_return_pct"], eligible["average_net_return_pct"],
        eligible["average_mfe_pct"], eligible["average_mae_pct"],
        ineligible["labels"], ineligible["ok"], ineligible["positive_rate_pct"],
        ineligible["average_gross_return_pct"], ineligible["average_net_return_pct"],
        ineligible["average_mfe_pct"], ineligible["average_mae_pct"],
        scalping_shadow_tracker.pending_count(),
    )
    segments = shadow.get("segments", {})
    for side in ("BUY", "SELL"):
        row = segments.get(f"side={side}")
        if row and int(row.get("labels", 0)) > 0:
            logger.info(
                "[STRATEGY SHADOW SIDE] side=%s labels=%d ok=%d positive=%.1f%% "
                "avg_gross=%+.5f%% avg_net=%+.5f%% MFE=%+.5f%% MAE=%+.5f%%",
                side, row["labels"], row["ok"], row["positive_rate_pct"],
                row["average_gross_return_pct"], row["average_net_return_pct"],
                row["average_mfe_pct"], row["average_mae_pct"],
            )
    ticker_rows = [
        (key, row) for key, row in segments.items()
        if key.startswith("ticker=") and int(row.get("ok", 0)) >= 5
    ]
    for key, row in sorted(ticker_rows, key=lambda item: int(item[1].get("ok", 0)), reverse=True)[:5]:
        logger.info(
            "[STRATEGY SHADOW TICKER] %s labels=%d ok=%d positive=%.1f%% avg_net=%+.5f%%",
            key, row["labels"], row["ok"], row["positive_rate_pct"], row["average_net_return_pct"],
        )
    for prefix in (
        "regime=", "score_band=", "horizon=", "hour_msk=", "hard_block=",
        "spread_atr_bucket=", "trade_count_bucket=", "flow_alignment=", "side_flow_alignment=",
    ):
        grouped = [
            (key, row) for key, row in segments.items()
            if key.startswith(prefix) and int(row.get("labels", 0)) >= 3
        ]
        for key, row in sorted(grouped, key=lambda item: int(item[1].get("labels", 0)), reverse=True)[:3]:
            logger.info(
                "[STRATEGY SHADOW SEGMENT] %s labels=%d ok=%d positive=%.1f%% avg_gross=%+.5f%% avg_net=%+.5f%%",
                key, row["labels"], row["ok"], row["positive_rate_pct"],
                row["average_gross_return_pct"], row["average_net_return_pct"],
            )
    _log_open_position_bbo_diagnostics()


def _trade_flow_guard_block(reason: str) -> None:
    _trade_flow_inc("guard_blocked")
    lowered = str(reason or "").lower()
    if "bid/ask" in lowered or "котировк" in lowered or "актуальн" in lowered:
        bucket = "guard_quotes"
    elif "спред" in lowered:
        bucket = "guard_spread"
    elif "размер лучшей заявки" in lowered or "сокращение" in lowered or "истории размера" in lowered:
        bucket = "guard_size"
    elif "всплеск" in lowered or "объём" in lowered or "объем" in lowered:
        bucket = "guard_volume"
    elif "импульс" in lowered or "vwap" in lowered or "цена не" in lowered:
        bucket = "guard_direction"
    elif "объяснение" in lowered or "текст" in lowered or "поврежд" in lowered or "неподтверждён" in lowered:
        bucket = "guard_text"
    else:
        bucket = "guard_other"
    _trade_flow_inc(bucket)


def _guard_reason_bucket(reason: str) -> str:
    lowered = str(reason or "").lower()
    if "bid/ask" in lowered or "котировк" in lowered or "актуальн" in lowered:
        return "quotes"
    if "спред" in lowered:
        return "spread"
    if "atr" in lowered or "m1-диапазон" in lowered or "m1-истори" in lowered:
        return "atr"
    if "размер лучшей заявки" in lowered or "сокращение" in lowered or "истории размера" in lowered:
        return "size"
    if "всплеск" in lowered or "объём" in lowered or "объем" in lowered:
        return "volume"
    if "импульс" in lowered or "vwap" in lowered or "цена не" in lowered:
        return "direction"
    return "other"


def _log_prefilter_blocked(ticker: str, reason: str, *, interval_sec: float = 15.0) -> None:
    """Rate-limit identical prefilter warnings per ticker and reason category."""
    bucket = _guard_reason_bucket(reason)
    key = (str(ticker).upper(), bucket)
    now = time_module.monotonic()
    with _prefilter_log_lock:
        last = _prefilter_last_logged.get(key, 0.0)
        if now - last < interval_sec:
            return
        _prefilter_last_logged[key] = now
    logger.warning(
        "⏭️ [PRE-LLM FILTER] %s пропущен до запроса LLM (%s): %s",
        ticker, bucket, reason,
    )


def _log_scalping_shadow(ticker: str, direction: str, metrics: dict[str, Any]) -> dict[str, Any]:
    """Log a research-only strategy score; it never changes the baseline order path."""
    result = evaluate_scalping_candidate(direction, metrics)
    observation_id = scalping_shadow_tracker.record_candidate(ticker, direction, metrics, result)
    _trade_flow_inc("strategy_shadow_evaluated")
    _trade_flow_inc("strategy_shadow_pass" if result.get("eligible") else "strategy_shadow_reject")
    shadow_block_keys = {
        "bbo_unavailable": "shadow_block_bbo_unavailable",
        "bbo_stale": "shadow_block_bbo_stale",
        "order_book_not_confirmed": "shadow_block_order_book_not_confirmed",
        "atr_not_ready": "shadow_block_atr_not_ready",
        "spread_above_0.50_pct": "shadow_block_spread",
    }
    observed_blocks = set(result.get("hard_blocks", []))
    counted_blocks = set()
    for block in observed_blocks:
        counter = shadow_block_keys.get(block)
        if counter:
            _trade_flow_inc(counter)
            counted_blocks.add(block)
    if observed_blocks - counted_blocks:
        _trade_flow_inc("shadow_block_other", len(observed_blocks - counted_blocks))
    logger.info(
        "🧪 [STRATEGY SHADOW] %s id=%s side=%s eligible=%s score=%.1f/%.0f regime=%s "
        "queue_imbalance=%s spread_ATR=%s hard_blocks=%s reasons=%s",
        ticker, observation_id or "-", direction, bool(result.get("eligible")), float(result.get("score", 0.0)),
        float(result.get("threshold", 70.0)), result.get("regime", "UNKNOWN"),
        result.get("queue_imbalance"), result.get("spread_atr_ratio"),
        ",".join(result.get("hard_blocks", [])) or "-",
        ",".join(result.get("reasons", [])) or "-",
    )
    return result


def _reserve_ai_ticker(ticker: str) -> bool:
    now = time_module.monotonic()
    with _ai_ticker_pending_lock:
        if ticker in _ai_ticker_pending:
            return False
        last = _ai_ticker_last_enqueued_at.get(ticker, 0.0)
        if now - last < _AI_TICKER_COOLDOWN_SEC:
            return False
        _ai_ticker_pending.add(ticker)
        _ai_ticker_last_enqueued_at[ticker] = now
        # Keep the per-ticker cooldown map bounded during long sessions.
        if len(_ai_ticker_last_enqueued_at) > 500:
            cutoff = now - 300.0
            for old_ticker, stamp in list(_ai_ticker_last_enqueued_at.items()):
                if stamp < cutoff and old_ticker not in _ai_ticker_pending:
                    _ai_ticker_last_enqueued_at.pop(old_ticker, None)
        return True


def _release_ai_ticker(ticker: str) -> None:
    if not ticker:
        return
    with _ai_ticker_pending_lock:
        _ai_ticker_pending.discard(str(ticker).upper())


def _drop_pending_bbo_candidate(ticker: str, expected: dict[str, Any] | None = None) -> bool:
    key = str(ticker or "").strip().upper()
    with _pending_bbo_lock:
        current = _pending_bbo_candidates.get(key)
        if current is not None and (expected is None or current is expected):
            _pending_bbo_candidates.pop(key, None)
            return True
    return False


def _cancel_pending_bbo_candidate(ticker: str, expected: dict[str, Any] | None = None) -> bool:
    removed = _drop_pending_bbo_candidate(ticker, expected)
    if removed:
        _trade_flow_inc("bbo_cancelled")
    return removed


def _expire_pending_bbo_candidates(now: float | None = None) -> None:
    current_time = time_module.monotonic() if now is None else now
    expired: list[str] = []
    with _pending_bbo_lock:
        for ticker, item in list(_pending_bbo_candidates.items()):
            created_at = float(item.get("created_at", 0.0) or 0.0)
            if created_at <= 0.0 or current_time - created_at > _PENDING_BBO_CANDIDATE_TTL_SEC:
                _pending_bbo_candidates.pop(ticker, None)
                expired.append(ticker)
    for ticker in expired:
        _trade_flow_inc("bbo_expired")
        logger.debug("⌛ Отложенный кандидат %s истёк: свежий BBO не поступил за %.2f сек.", ticker, _PENDING_BBO_CANDIDATE_TTL_SEC)


def _store_pending_bbo_candidate(
    ticker: str,
    payload: dict[str, Any],
    received_monotonic: object | None = None,
) -> None:
    # TTL starts when the candidate is actually stored. Repeated ticks for
    # the same ticker/side refresh evidence but do not restart the TTL or replace
    # the waiter object; this prevents the resume path from chasing a moving target.
    now = time_module.monotonic()
    key = str(ticker).strip().upper().split("@")[0]
    with _pending_bbo_lock:
        previous = _pending_bbo_candidates.get(key)
        entry, status = coalesce_pending_bbo_entry(
            previous, payload, now=now, ttl_sec=_PENDING_BBO_CANDIDATE_TTL_SEC
        )
        if status == "coalesced" and previous is not None:
            previous["payload"] = entry["payload"]
            _pending_bbo_candidates[key] = previous
        else:
            _pending_bbo_candidates[key] = entry
    if status == "expired":
        _trade_flow_inc("bbo_expired")
    elif status == "coalesced":
        _trade_flow_inc("bbo_coalesced")
        logger.debug("♻️ [PRE-LLM COALESCE] %s: повторный сигнал обновил существующее ожидание.", key)
        return
    elif status == "superseded":
        _trade_flow_inc("bbo_superseded")
    _trade_flow_inc("bbo_deferred")
    logger.info(
        "⏳ [PRE-LLM WAIT] %s: кандидат ожидает целостный свежий ORDER_BOOK (TTL=%.2fs; status=%s).",
        key, _PENDING_BBO_CANDIDATE_TTL_SEC, status,
    )


def _bbo_resume_reason_bucket(reason: object) -> str:
    """Map a human-readable BBO recovery failure to a stable diagnostic counter."""
    lowered = str(reason or "").strip().lower()
    if "состояние котировок" in lowered or "состояния стакана" in lowered:
        return "quote_missing"
    if "не подтверждён" in lowered or "не подтвержден" in lowered or "целостный" in lowered:
        return "book_unconfirmed"
    if "непротиворечив" in lowered or "двухсторон" in lowered:
        return "invalid_bbo"
    if "order_book устарело" in lowered:
        return "book_stale"
    if "свежего подтверждения" in lowered:
        return "confirmation_stale"
    return "other_book_failure"


async def _resume_pending_bbo_candidate(
    ticker: str,
    *,
    trigger: str = "ORDER_BOOK",
) -> None:
    """Revalidate a short-lived candidate after a coherent book or fresh quote event."""
    key = str(ticker or "").strip().upper().split("@")[0]
    with _pending_bbo_lock:
        pending = _pending_bbo_candidates.get(key)
    if pending is None:
        return

    normalized_trigger = str(trigger or "ORDER_BOOK").strip().upper()
    _trade_flow_inc("bbo_resume_attempted")
    _trade_flow_inc(
        "bbo_resume_from_quotes" if normalized_trigger == "QUOTES" else "bbo_resume_from_book"
    )

    now = time_module.monotonic()
    created_at = float(pending.get("created_at", 0.0) or 0.0)
    if created_at <= 0.0 or now - created_at > _PENDING_BBO_CANDIDATE_TTL_SEC:
        _drop_pending_bbo_candidate(key, pending)
        _trade_flow_inc("bbo_expired")
        _trade_flow_inc("bbo_resume_expired")
        return

    payload = pending.get("payload")
    if not isinstance(payload, dict):
        _trade_flow_inc("bbo_resume_invalid_payload")
        _cancel_pending_bbo_candidate(key, pending)
        return
    if (
        getattr(execution_engine, "_maintenance_active", False)
        or payload.get("paper_session_generation") != _paper_session_generation
    ):
        _trade_flow_inc("bbo_resume_session_cancelled")
        _cancel_pending_bbo_candidate(key, pending)
        return

    funnel_status = screener.get_status()
    if not funnel_status.get("ready"):
        _trade_flow_inc("bbo_resume_funnel_not_ready")
        _cancel_pending_bbo_candidate(key, pending)
        return
    if key not in set(funnel_status.get("selected_tickers", [])):
        _trade_flow_inc("bbo_resume_not_selected")
        _cancel_pending_bbo_candidate(key, pending)
        return

    active_tickers = execution_engine.get_open_position_tickers()
    if key in active_tickers or key in getattr(execution_engine, "_opening_tickers", set()):
        _trade_flow_inc("bbo_resume_active_position")
        _cancel_pending_bbo_candidate(key, pending)
        _trade_flow_inc("active_position_skipped")
        return

    metrics = payload.get("metrics")
    if not isinstance(metrics, dict):
        _trade_flow_inc("bbo_resume_invalid_payload")
        _cancel_pending_bbo_candidate(key, pending)
        return

    book_ready, book_reason = refresh_candidate_bbo_metrics(screener, key, metrics, now=now)
    if not book_ready:
        _trade_flow_inc("bbo_resume_" + _bbo_resume_reason_bucket(book_reason))
        logger.debug(
            "[PRE-LLM BBO RESUME WAIT] %s trigger=%s: %s",
            key, normalized_trigger, book_reason,
        )
        return

    direction = infer_candidate_direction(metrics)
    original_direction = str(payload.get("candidate_direction", "")).upper()
    if direction not in {"BUY", "SELL"} or direction != original_direction:
        _trade_flow_inc("bbo_resume_direction_changed")
        _cancel_pending_bbo_candidate(key, pending)
        _trade_flow_inc("direction_prefilter_skipped")
        return

    _log_scalping_shadow(key, direction, metrics)
    evidence_ok, evidence_reason = validate_signal_evidence(direction, metrics)
    if not evidence_ok:
        if _guard_reason_bucket(evidence_reason) == "quotes":
            _trade_flow_inc("bbo_resume_evidence_quotes")
            return
        _trade_flow_inc("bbo_resume_evidence_blocked")
        _cancel_pending_bbo_candidate(key, pending)
        _trade_flow_inc("prefilter_blocked")
        _trade_flow_guard_block(evidence_reason)
        _log_prefilter_blocked(key, evidence_reason)
        return

    atr_ready, atr_reason = validate_atr_readiness(metrics)
    if not atr_ready:
        _trade_flow_inc("bbo_resume_atr_blocked")
        _trade_flow_inc("atr_unready_candidates")
        _cancel_pending_bbo_candidate(key, pending)
        _log_prefilter_blocked(key, atr_reason, interval_sec=60.0)
        return

    # A same-side tick may have refreshed the pending payload while this
    # coroutine was validating the book. Never enqueue stale evidence or remove
    # the newer payload; a later event will revalidate that newer candidate.
    with _pending_bbo_lock:
        still_current = (
            _pending_bbo_candidates.get(key) is pending
            and pending.get("payload") is payload
        )
    if not still_current:
        _trade_flow_inc("bbo_resume_payload_changed")
        return

    if not _reserve_ai_ticker(key):
        _trade_flow_inc("bbo_resume_deduplicated")
        _cancel_pending_bbo_candidate(key, pending)
        _trade_flow_inc("candidate_deduplicated")
        return

    payload["bid"] = metrics.get("bid", 0.0)
    payload["ask"] = metrics.get("ask", 0.0)
    payload["bid_size"] = metrics.get("bid_size", 0.0)
    payload["ask_size"] = metrics.get("ask_size", 0.0)
    try:
        anomaly_queue.put_nowait(_make_priority_item(1, "MARKET_TICK", payload))
        _drop_pending_bbo_candidate(key, pending)
        _trade_flow_inc("bbo_resumed")
        _trade_flow_inc("anomalies_queued")
        _trade_flow_inc("prefilter_pass_buy" if direction == "BUY" else "prefilter_pass_sell")
        logger.info(
            "✅ [PRE-LLM RESUME] %s: свежий BBO и ORDER_BOOK подтверждены; "
            "кандидат %s передан в LLM-очередь (trigger=%s).",
            key, direction, normalized_trigger,
        )
        update_numpy_impulse_status(key)
    except asyncio.QueueFull:
        _release_ai_ticker(key)
        _drop_pending_bbo_candidate(key, pending)
        _trade_flow_inc("bbo_resume_queue_full")
        _trade_flow_inc("queue_dropped")
        logger.error("Очередь AI-сигналов заполнена; восстановленный кандидат %s отброшен.", key)


# Последний рассчитанный срез баланса для UI. Не изменяет внутреннюю риск-модель.
_dashboard_balance_snapshot = {
    "equity": round(DEPOSIT, 2),
    "initial_deposit": round(DEPOSIT, 2),
    "free_margin": round(DEPOSIT, 2),
    "margin_used": 0.0,
    "exposure_cap": round(DEPOSIT * 0.15, 2),
    "exposure_cap_pct": 0.15,
    "single_position_cap": round(DEPOSIT * 0.05, 2),
    "unreconciled_positions_count": 0,
    "pnl": 0.0,
    "realized_pnl": 0.0,
    "open_pnl": 0.0,
    "active_positions_count": 0,
}
_balance_guard_last_signature: tuple[int, bool] | None = None

def _latest_screener_prices() -> dict[str, float]:
    """Последние известные цены по инструментам из локального рыночного скринера."""
    prices: dict[str, float] = {}
    for ticker, data in getattr(screener, "history", {}).items():
        try:
            count = int(data.get("count", 0))
            values = data.get("prices")
            if count > 0 and values is not None:
                price = float(values[min(count, len(values)) - 1])
                if price > 0 and price == price and price != float("inf"):
                    prices[str(ticker).strip().upper().split("@")[0]] = price
        except (TypeError, ValueError, IndexError, AttributeError):
            continue
    return prices

async def _get_dashboard_open_positions() -> list[dict]:
    """Read open positions from the authoritative source for the current execution mode."""
    if getattr(execution_engine, "paper_mode", True):
        raw = getattr(execution_engine, "paper_positions", getattr(execution_engine, "_paper_positions", {}))
        values = list(raw.values()) if isinstance(raw, dict) else []
    else:
        # In LIVE mode, execute_order persists accepted orders in the trade journal.
        # Read this low-frequency snapshot here (one-second sync), not for every market tick.
        try:
            active = await db_manager.get_active_positions()
            values = list(active.values()) if isinstance(active, dict) else []
        except Exception as exc:
            logger.debug("Не удалось получить LIVE-позиции для расчёта капитала: %s", exc)
            values = []
    return [dict(item) for item in values if isinstance(item, dict) and str(item.get("status", "OPEN")).upper() == "OPEN"]

async def _refresh_dashboard_balance_snapshot(positions: list[dict] | None = None) -> dict:
    """Build a non-mark-to-market dashboard snapshot.

    Free cash = configured initial deposit + realized net PnL - acquisition cost
    of currently open assets. The assets field uses entry_price * quantity and
    therefore stays unchanged when market quotes move. Total PnL also includes
    floating PnL from open positions. The original deposit remains unchanged for
    risk limits; equity reports deposit plus total (realized + floating) PnL.
    """
    global _dashboard_balance_snapshot
    from portfolio_metrics import calculate_assets_used, calculate_free_balance

    if positions is None:
        positions = await _get_dashboard_open_positions()

    try:
        initial_deposit = float(DEPOSIT)
        if not math.isfinite(initial_deposit) or initial_deposit < 0:
            raise ValueError("invalid configured deposit")
    except (TypeError, ValueError, OverflowError):
        initial_deposit = float(getattr(risk_manager, "initial_deposit", 0.0) or 0.0)

    try:
        pnl = float(getattr(risk_manager, "total_session_pnl", getattr(risk_manager, "session_pnl", 0.0)))
        if not math.isfinite(pnl):
            pnl = 0.0
    except (TypeError, ValueError, OverflowError):
        pnl = 0.0

    try:
        realized_pnl = float(getattr(risk_manager, "realized_pnl", 0.0) or 0.0)
        if not math.isfinite(realized_pnl):
            realized_pnl = 0.0
    except (TypeError, ValueError, OverflowError):
        realized_pnl = 0.0

    try:
        open_pnl = float(getattr(risk_manager, "open_pnl", pnl - realized_pnl) or 0.0)
        if not math.isfinite(open_pnl):
            open_pnl = pnl - realized_pnl
    except (TypeError, ValueError, OverflowError):
        open_pnl = pnl - realized_pnl

    # Old PAPER positions are not restored as live simulated positions after a
    # restart. Until an operator reviews them, their saved acquisition cost stays
    # reserved and all new entries are blocked by ExecutionLayer.
    unresolved_map = (
        getattr(execution_engine, "_unreconciled_paper_positions", {})
        if getattr(execution_engine, "paper_mode", True) else {}
    )
    unresolved_positions = list(unresolved_map.values()) if isinstance(unresolved_map, dict) else []
    capital_positions = list(positions)
    seen_ids = {str(p.get("id", "")) for p in capital_positions if isinstance(p, dict)}
    for unresolved in unresolved_positions:
        if isinstance(unresolved, dict) and str(unresolved.get("id", "")) not in seen_ids:
            capital_positions.append(unresolved)

    assets_used = calculate_assets_used(
        positions=capital_positions,
        # The helper intentionally ignores live prices and uses purchase price.
        latest_prices=None,
        lot_sizes=LOT_SIZES,
    )
    free_balance = calculate_free_balance(initial_deposit + realized_pnl, assets_used)
    max_gross_pct = min(max(0.0, float(getattr(execution_engine, "max_gross_exposure_pct", 0.15))), 0.15)
    max_single_pct = min(max(0.0, float(getattr(execution_engine, "max_single_position_pct", 0.05))), 0.05)
    exposure_cap = round(initial_deposit * max_gross_pct, 2)
    single_position_cap = round(initial_deposit * max_single_pct, 2)
    _dashboard_balance_snapshot = {
        "equity": round(initial_deposit + pnl, 2),
        "initial_deposit": round(initial_deposit, 2),
        "free_margin": free_balance,
        "margin_used": round(assets_used, 2),
        "exposure_cap": exposure_cap,
        "exposure_cap_pct": max_gross_pct,
        "single_position_cap": single_position_cap,
        "unreconciled_positions_count": len(unresolved_positions),
        "pnl": round(pnl, 2),
        "realized_pnl": round(realized_pnl, 2),
        "open_pnl": round(open_pnl, 2),
        "active_positions_count": len(positions),
    }

    global _balance_guard_last_signature
    over_cap = assets_used > exposure_cap
    signature = (len(unresolved_positions), over_cap)
    if signature != _balance_guard_last_signature:
        if unresolved_positions:
            logger.critical(
                "🛑 [PAPER RECONCILIATION GUARD] %d старых PAPER-позиций на сверке; их себестоимость %.2f ₽ учтена в активах. Новые входы заблокированы.",
                len(unresolved_positions), calculate_assets_used(unresolved_positions, lot_sizes=LOT_SIZES),
            )
        if over_cap:
            logger.error(
                "🚨 [BALANCE GUARD] Себестоимость активов %.2f ₽ превышает лимит задействования %.2f ₽ (%.0f%% депозита). Новые входы будут заблокированы.",
                assets_used, exposure_cap, max_gross_pct * 100,
            )
        _balance_guard_last_signature = signature
    return dict(_dashboard_balance_snapshot)


def _make_priority_item(priority: int, event_type: str, payload: dict[str, Any]) -> PriorityItem:
    """Создаёт единый формат элемента PriorityQueue со стабильным tie-breaker."""
    loop = asyncio.get_running_loop()
    return (int(priority), next(_priority_sequence), loop.time(), event_type, payload)



# Все 88 инструментов остаются подписанными для сбора сравнимой статистики.
# В LLM разрешается передавать только события от выбранного локальным рейтингом top-N.
screener.set_universe(TICKERS_TO_TRADE)

# --------------------------------------------------------------
#  2️⃣  НЕБЛОКИРУЮЩИЙ ДИСКОВЫЙ БОРТОВОЙ ЖУРНАЛ ТЕСТОВ
# --------------------------------------------------------------
def _sync_write_trading_log(filename: str, log_line: str):
    """Синхронная запись блока лога на диск, изолированная в пуле потоков ОС."""
    try:
        with open(filename, "a", encoding="utf-8") as f:
            f.write(log_line + "\n")
    except Exception as e:
        logger.error(f"❌ Ошибка записи в текстовый лог: {e}")

async def log_trading_event_to_file(ticker: str, price: float, decision: str, confidence: float, tactic: str, critic: str):
    """Асинхронный неблокирующий регистратор торговых сессий."""
    os.makedirs("trading_logs", exist_ok=True)
    
    date_str = datetime.now().strftime("%Y-%m-%d")
    filename = f"trading_logs/piluitus_session_{date_str}.txt"
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    log_line = (
        f"========================================================================\n"
        f"⏰ ВРЕМЯ: {timestamp} | АКТИВ: {ticker} | ЦЕНА: {price} ₽\n"
        f"🎯 РЕШЕНИЕ ИИ: {decision} (Уверенность: {confidence:.1f}%)\n"
        f"------------------------------------------------------------------------\n"
        f"📝 ТАКТИКА: {tactic}\n"
        f"🧐 КРИТИКА: {critic}\n"
        f"========================================================================\n"
    )
    
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, _sync_write_trading_log, filename, log_line)
# --------------------------------------------------------------
#  3️⃣  CONNECTION MANAGER (WebSocket)
# --------------------------------------------------------------
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info("🔌 WS‑клиент отключён")
        
    async def broadcast(self, message: str):
        for conn in self.active_connections:
            try:
                await conn.send_text(message)
            except Exception:
                pass

manager = ConnectionManager()

# --------------------------------------------------------------
#  4️⃣  ПОЛИМОРФНЫЙ ТИКЕРНЫЙ ОБРАБОТЧИК С АВТО-ЗАВЕРШЕНИЕМ ПОЗИЦИЙ
# --------------------------------------------------------------
async def process_market_event(event: Union[SimpleNamespace, dict]):
    """Обновляет локальную воронку по каждой котировке и направляет в LLM только top-N."""
    global tick_counter, _last_funnel_log_at, _last_funnel_log_key, _last_funnel_ready
    event_session_generation = _paper_session_generation
    if getattr(execution_engine, "_maintenance_active", False):
        return
    _expire_pending_bbo_candidates()
    try:
        # Instrument trades update the trade-flow accumulator independently of QUOTES.
        if isinstance(event, dict) and str(event.get("event_type", "")).upper() == "INSTRUMENT_TRADE":
            trade_ticker = str(event.get("ticker") or "").strip().upper().split("@")[0]
            if not trade_ticker:
                _trade_flow_inc("instrument_trades_invalid")
                return
            trade_result = screener.push_trade(
                ticker=trade_ticker, price=event.get("price", 0.0),
                size=event.get("size", 0.0), trade_id=event.get("trade_id"),
                side=event.get("side"), event_timestamp=event.get("timestamp"),
                received_monotonic=event.get("received_monotonic"),
            )
            if trade_result.get("accepted"):
                _trade_flow_inc("instrument_trades_received")
            else:
                trade_reason = str(trade_result.get("reason") or "rejected")
                _trade_flow_inc("instrument_trades_" + trade_reason)
                # Rejected trade events never invalidate the independent QUOTES-driven M1 history.
            return

        # ORDER_BOOK events have no last-trade price. Route them before the QUOTES
        # normalizer, otherwise price<=0 would silently discard every book update.
        if isinstance(event, dict) and str(event.get("event_type", "")).upper() == "ORDER_BOOK":
            book_ticker = str(event.get("ticker") or event.get("security_code") or "").strip().upper().split("@")[0]
            if not book_ticker:
                _trade_flow_inc("orderbook_unusable")
                return
            try:
                book_result = await screener.push_order_book(
                    ticker=book_ticker,
                    bid=float(event.get("bid", 0.0) or 0.0),
                    ask=float(event.get("ask", 0.0) or 0.0),
                    bid_size=float(event.get("bid_size", 0.0) or 0.0),
                    ask_size=float(event.get("ask_size", 0.0) or 0.0),
                    bid_present=bool(event.get("bid_present", True)),
                    ask_present=bool(event.get("ask_present", True)),
                    bid_size_present=bool(event.get("bid_size_present", True)),
                    ask_size_present=bool(event.get("ask_size_present", True)),
                    bid_timestamp=event.get("bid_timestamp") or event.get("order_book_timestamp"),
                    ask_timestamp=event.get("ask_timestamp") or event.get("order_book_timestamp"),
                    order_book_timestamp=event.get("order_book_timestamp"),
                    received_monotonic=event.get("received_monotonic"),
                    book_valid=bool(event.get("book_valid", True)),
                    book_reason=str(event.get("book_reason") or ""),
                    bid_levels=event.get("bid_levels") if isinstance(event.get("bid_levels"), list) else None,
                    ask_levels=event.get("ask_levels") if isinstance(event.get("ask_levels"), list) else None,
                    bid_levels_present=bool(event.get("bid_levels_present", False)),
                    ask_levels_present=bool(event.get("ask_levels_present", False)),
                )
            except (TypeError, ValueError, KeyError):
                _trade_flow_inc("orderbook_unusable")
                return
            _record_position_order_book_event(book_ticker, book_result)
            if book_result.get("updated") and book_result.get("valid", True):
                _trade_flow_inc("orderbook_updates")
                _trade_flow_inc("orderbook_side_touches", int(book_result.get("bbo_side_touch_count", 0) or 0))
                if book_result.get("heartbeat_only"):
                    _trade_flow_inc("orderbook_heartbeats")
                shadow_metrics: dict[str, Any] = {}
                shadow_bbo_ready, _shadow_bbo_reason = refresh_candidate_bbo_metrics(
                    screener, book_ticker, shadow_metrics, now=time_module.monotonic()
                )
                if shadow_bbo_ready:
                    scalping_shadow_tracker.observe_market_tick(book_ticker, shadow_metrics)
                await _resume_pending_bbo_candidate(book_ticker, trigger="ORDER_BOOK")
                # ORDER_BOOK can refresh BBO independently of QUOTES. Re-run the
                # PAPER exit monitor only after a coherent book confirmation.
                if execution_engine.paper_mode and book_ticker in execution_engine.paper_positions:
                    last_trade_price = execution_engine._latest_price(
                        book_ticker, execution_engine.paper_positions.get(book_ticker)
                    )
                    execution_engine.schedule_paper_position_monitor(
                        book_ticker,
                        last_trade_price if last_trade_price > 0.0 else None,
                        db_manager,
                        event_source="ORDER_BOOK",
                        event_timestamp=event.get("order_book_timestamp"),
                    )
            elif book_result.get("updated"):
                _trade_flow_inc("orderbook_invalid")
                logger.debug(
                    "ORDER_BOOK %s заблокирован: %s",
                    book_ticker, book_result.get("reason", book_result.get("book_reason", "invalid")),
                )
            elif book_result.get("out_of_order"):
                _trade_flow_inc("orderbook_out_of_order")
            else:
                _trade_flow_inc("orderbook_unusable")
                if book_result.get("reason"):
                    logger.debug("ORDER_BOOK %s не принят: %s", book_ticker, book_result["reason"])
            return

        if isinstance(event, dict):
            ticker = event.get("security_code") or event.get("ticker", "SBER")
            price = float(event.get("price", 0.0) or 0.0)
            volume = float(event.get("volume", 0.0) or 0.0)
            bid = float(event.get("bid", 0.0) or 0.0)
            ask = float(event.get("ask", 0.0) or 0.0)
            bid_size = float(event.get("bid_size", event.get("bidSize", 0.0)) or 0.0)
            ask_size = float(event.get("ask_size", event.get("askSize", 0.0)) or 0.0)
            bid_present = bool(event.get("bid_present", bid > 0.0))
            ask_present = bool(event.get("ask_present", ask > 0.0))
            bid_size_present = bool(event.get("bid_size_present", bid_size > 0.0))
            ask_size_present = bool(event.get("ask_size_present", ask_size > 0.0))
            day_volume = float(event.get("day_volume", 0.0) or 0.0)
            supplied_metrics = event.get("metrics", {})
            event_timestamp = event.get("timestamp") or event.get("datetime") or event.get("time")
            received_monotonic = event.get("received_monotonic")
        else:
            ticker = getattr(event, "security_code", getattr(event, "ticker", "SBER"))
            price = float(getattr(event, "price", 0.0) or 0.0)
            volume = float(getattr(event, "volume", 0.0) or 0.0)
            bid = float(getattr(event, "bid", 0.0) or 0.0)
            ask = float(getattr(event, "ask", 0.0) or 0.0)
            bid_size = float(getattr(event, "bid_size", getattr(event, "bidSize", 0.0)) or 0.0)
            ask_size = float(getattr(event, "ask_size", getattr(event, "askSize", 0.0)) or 0.0)
            bid_present = bool(getattr(event, "bid_present", bid > 0.0))
            ask_present = bool(getattr(event, "ask_present", ask > 0.0))
            bid_size_present = bool(getattr(event, "bid_size_present", bid_size > 0.0))
            ask_size_present = bool(getattr(event, "ask_size_present", ask_size > 0.0))
            day_volume = float(getattr(event, "day_volume", 0.0) or 0.0)
            supplied_metrics = getattr(event, "metrics", {})
            event_timestamp = (
                getattr(event, "timestamp", None)
                or getattr(event, "datetime", None)
                or getattr(event, "time", None)
            )
            received_monotonic = getattr(event, "received_monotonic", None)

        ticker = str(ticker or "").strip().upper().split("@")[0]
        if price <= 0.0 or not ticker or volume < 0.0:
            return
        # Quote ticks do not cancel deferred candidates. They may refresh the
        # two-sided quote confirmation clocks; a pending candidate can resume on
        # QUOTES only while a recently confirmed coherent ORDER_BOOK is still fresh.

        tick_counter += 1
        current_frame = spinner_frames[tick_counter % len(spinner_frames)]
        terminal_status.update(
            f"⚡ [WS ПОТОК ФИНАМ] {current_frame} "
            f"Тиков: {tick_counter:,} | {ticker} {price}₽"
        )

        # Учитываем каждый инструмент в рейтинге, а не только аномальные тики.
        analysis = await screener.push_tick(
            ticker=ticker,
            price=price,
            volume=volume,
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            day_volume=day_volume,
            bid_present=bid_present,
            ask_present=ask_present,
            bid_size_present=bid_size_present,
            ask_size_present=ask_size_present,
            event_timestamp=event_timestamp,
            received_monotonic=received_monotonic,
        )

        _trade_flow_inc("quotes_received")
        # ORDER_BOOK and QUOTES use independent provider streams. A valid recent
        # book can be confirmed by the newest two-sided QUOTES packet between
        # book deltas; this prevents a pending candidate from expiring merely
        # because no new best-level ORDER_BOOK event arrived within its 1.5s TTL.
        with _pending_bbo_lock:
            has_pending_bbo = ticker in _pending_bbo_candidates
        if has_pending_bbo:
            await _resume_pending_bbo_candidate(ticker, trigger="QUOTES")
        quote_metrics_for_shadow = analysis.get("metrics", {})
        if isinstance(quote_metrics_for_shadow, dict):
            scalping_shadow_tracker.observe_market_tick(ticker, quote_metrics_for_shadow)
        funnel_status = screener.get_status()
        terminal_status.update_funnel(screener.get_status_text())
        if analysis.get("selected") and analysis.get("selection_ready"):
            _trade_flow_inc("selected_quotes")
        now_mono = time_module.monotonic()
        selected_snapshot = tuple(funnel_status.get("selected_tickers", []))
        if not funnel_status["ready"]:
            _last_funnel_ready = False
            if _last_funnel_log_at == 0.0 or now_mono - _last_funnel_log_at >= 30.0:
                logger.info(
                    "🎯 Воронка прогревается: котировки получены по %d/%d инструментам; пригодны для рейтинга %d; цель top-%d.",
                    funnel_status["observed_count"], funnel_status["universe_count"], funnel_status["eligible_count"], funnel_status["top_n"],
                )
                _last_funnel_log_at = now_mono
        elif selected_snapshot:
            first_ready = not _last_funnel_ready
            selection_changed = _last_funnel_log_key is None or selected_snapshot != _last_funnel_log_key
            cooldown_elapsed = _last_funnel_log_at == 0.0 or now_mono - _last_funnel_log_at >= 30.0
            if first_ready or (selection_changed and cooldown_elapsed):
                logger.info(
                    "🎯 ВОРОНКА ГОТОВА: %d → %d тикеров для LLM: %s",
                    funnel_status["universe_count"], len(selected_snapshot), ", ".join(selected_snapshot),
                )
                _last_funnel_log_at = now_mono
                _last_funnel_log_key = selected_snapshot
            _last_funnel_ready = True

        logger.debug(
            "📥 [ВХОДЯЩИЙ ТИК] %s price=%s anomaly=%s selected=%s rank=%s score=%s",
            ticker, price, analysis["is_anomaly"], analysis.get("selected", False),
            analysis.get("metrics", {}).get("funnel_rank", 0),
            analysis.get("metrics", {}).get("funnel_score", 0.0),
        )

        # Аномалия сама по себе не запускает LLM. Сначала локально определяем
        # направление, затем проверяем измеряемые условия входа. Модель вызывается
        # только для кандидата, который не был заранее отклонён численным guard.
        if analysis["is_anomaly"] and analysis.get("selection_ready") and analysis.get("selected"):
            _trade_flow_inc("selected_anomalies")
            metrics_payload = analysis.get("metrics", supplied_metrics if isinstance(supplied_metrics, dict) else {})
            if not isinstance(metrics_payload, dict):
                metrics_payload = {}

            # Existing positions are monitored independently on every market event.
            # Do not spend another LLM call proposing a second entry in the same ticker.
            active_tickers = execution_engine.get_open_position_tickers()
            ticker_opening = ticker in getattr(execution_engine, "_opening_tickers", set())
            if ticker in active_tickers or ticker_opening:
                _trade_flow_inc("active_position_skipped")
            else:
                candidate_direction = infer_candidate_direction(metrics_payload)
                if candidate_direction is None:
                    _trade_flow_inc("direction_prefilter_skipped")
                    _log_prefilter_blocked(
                        ticker,
                        "Направление не подтверждено: краткосрочное изменение цены и отклонение от VWAP не совпадают по знаку.",
                    )
                else:
                    _trade_flow_inc("local_candidate_buy" if candidate_direction == "BUY" else "local_candidate_sell")
                    _log_scalping_shadow(ticker, candidate_direction, metrics_payload)
                    evidence_ok, evidence_reason = validate_signal_evidence(candidate_direction, metrics_payload)
                    if not evidence_ok:
                        if _guard_reason_bucket(evidence_reason) == "quotes":
                            pending_payload = {
                                "ticker": ticker,
                                "price": price,
                                "volume": volume,
                                "bid": bid,
                                "ask": ask,
                                "bid_size": bid_size,
                                "ask_size": ask_size,
                                "day_volume": day_volume,
                                "candidate_direction": candidate_direction,
                                "paper_session_generation": event_session_generation,
                                "metrics": {
                                    key: value
                                    for key, value in metrics_payload.items()
                                    if key not in {"selected_for_ai", "funnel_ready"}
                                },
                            }
                            _store_pending_bbo_candidate(ticker, pending_payload, received_monotonic)
                        else:
                            _trade_flow_inc("prefilter_blocked")
                            _trade_flow_guard_block(evidence_reason)
                            _log_prefilter_blocked(ticker, evidence_reason)
                    else:
                        atr_ready, atr_reason = validate_atr_readiness(metrics_payload)
                        if not atr_ready:
                            # ATR is a hard pre-LLM prerequisite. Re-evaluate on a
                            # later fresh tick instead of spending inference only to
                            # reject an APPROVE after the model has answered.
                            _trade_flow_inc("atr_unready_candidates")
                            _log_prefilter_blocked(ticker, atr_reason, interval_sec=60.0)
                        elif not _reserve_ai_ticker(ticker):
                            _trade_flow_inc("candidate_deduplicated")
                        else:
                            event_payload = {
                                "ticker": ticker,
                                "price": price,
                                "volume": volume,
                                "bid": bid,
                                "ask": ask,
                                "bid_size": bid_size,
                                "ask_size": ask_size,
                                "day_volume": day_volume,
                                "candidate_direction": candidate_direction,
                                "paper_session_generation": event_session_generation,
                                "metrics": {
                                    key: value
                                    for key, value in metrics_payload.items()
                                    if key not in {"selected_for_ai", "funnel_ready"}
                                },
                            }
                            try:
                                anomaly_queue.put_nowait(_make_priority_item(1, "MARKET_TICK", event_payload))
                                _trade_flow_inc("anomalies_queued")
                                _trade_flow_inc("prefilter_pass_buy" if candidate_direction == "BUY" else "prefilter_pass_sell")
                                update_numpy_impulse_status(ticker)
                            except asyncio.QueueFull:
                                _release_ai_ticker(ticker)
                                _trade_flow_inc("queue_dropped")
                                logger.error("Очередь AI-сигналов заполнена; аномалия %s отброшена.", ticker)

        # Обновление позиции продолжается по всем 88 котировкам, а не только по top-N.
        execution_engine.schedule_paper_position_monitor(
            ticker,
            price,
            db_manager,
            event_source="LAST_TRADE",
            event_timestamp=event_timestamp,
        )

    except Exception as e:
        logger.error("💥 Ошибка в обработчике тиков: %s", e, exc_info=True)
# --------------------------------------------------------------
#  5️⃣  MARKET WORKER С ПРИОРИТЕТНЫМ ПЕРЕХВАТОМ И КОНТУРОМ RAG
# --------------------------------------------------------------
class MarketWorker:
    def __init__(self, db: DBManager, ai: AIEngine, broker_client):
        self.db = db
        self.ai = ai
        self.broker = broker_client
        self.client_id = CLIENT_ID
        self._last_risk_block_verdict_at = 0.0

    async def _execute_and_publish(
        self,
        *,
        verdict_id: str,
        ticker: str,
        lots: int,
        side: str,
        price: float,
        ai_analysis: dict,
        session_generation: int,
    ) -> None:
        """Serialize order execution with PAPER reset and reject pre-reset signals."""
        async with _paper_session_lock:
            if (
                getattr(execution_engine, "_maintenance_active", False)
                or session_generation != _paper_session_generation
            ):
                logger.info("⏭️ Старый сигнал %s по %s отброшен: PAPER-сессия изменилась/на обслуживании.", verdict_id, ticker)
                return
            await self._execute_and_publish_locked(
                verdict_id=verdict_id, ticker=ticker, lots=lots, side=side,
                price=price, ai_analysis=ai_analysis,
            )

    async def _execute_and_publish_locked(
        self,
        *,
        verdict_id: str,
        ticker: str,
        lots: int,
        side: str,
        price: float,
        ai_analysis: dict,
    ) -> None:
        """Execute asynchronously, then publish and persist the actual result."""
        try:
            # Re-read the complete risk state immediately before execution.
            # A candidate may have waited in an async task while another trade
            # closed and activated the session hard stop.
            if not await risk_manager.is_trading_allowed(self.db, force_refresh=True):
                result = {
                    "status": "BLOCKED",
                    "message": (
                        "RISK_FINAL_GATE: RiskManager запретил исполнение после "
                        "повторной проверки; ранее одобренный сигнал отброшен."
                    ),
                }
                _trade_flow_inc("risk_final_gate_blocked")
                logger.warning(
                    "🛑 [RISK FINAL GATE] %s %s: отложенный сигнал %s заблокирован перед исполнением.",
                    side, ticker, verdict_id,
                )
            else:
                result = await execution_engine.execute_order(
                    ticker=ticker,
                    lots=lots,
                    side=side,
                    price=price,
                    client_id=self.client_id,
                    db=self.db,
                    ai_analysis=ai_analysis,
                )
            if not isinstance(result, dict):
                result = {"status": "FAILED", "message": "Исполнитель вернул некорректный ответ."}
        except Exception as exc:
            logger.error("Ошибка исполнения %s %s: %s", side, ticker, exc, exc_info=True)
            result = {"status": "FAILED", "message": str(exc)}

        raw_status = str(result.get("status", "FAILED")).upper()
        result_mode = str(result.get("mode") or ("PAPER" if getattr(execution_engine, "paper_mode", True) else "LIVE")).upper()
        execution_status = normalise_execution_status(raw_status, result_mode)
        result_message = str(result.get("message", ""))
        if "BBO_ENTRY_BLOCK" in result_message:
            _trade_flow_inc("execution_bbo_blocked")
        if execution_status in {"OPENED", "ACCEPTED"}:
            # Count actual executor outcomes, not merely queued AI decisions.
            _trade_flow_inc("orders_scheduled")
            _trade_flow_inc("orders_opened_buy" if side == "BUY" else "orders_opened_sell")
        order_id = str(result.get("orderId") or result.get("order_id") or "")
        raw_actual_lots = result.get("lots")
        raw_actual_shares = result.get("shares")
        raw_actual_notional = result.get("notional_entry_rub")
        actual_lots = nonnegative_int(raw_actual_lots)
        actual_shares = nonnegative_int(raw_actual_shares)
        actual_notional = finite_float(raw_actual_notional)
        requested_lots_from_executor = nonnegative_int(result.get("requested_lots"))
        if execution_status in {"SKIPPED", "BLOCKED", "FAILED"}:
            if "Лимиты портфеля:" in result_message:
                _trade_flow_inc("portfolio_cap_blocked")
            elif "ENTRY_SESSION_BLOCK" in result_message:
                _trade_flow_inc("entry_session_blocked")
            elif "BBO_ENTRY_BLOCK" not in result_message and "AI Decision is HOLD" not in result_message:
                _trade_flow_inc("execution_other_blocked")
        if (
            execution_status in {"OPENED", "ACCEPTED"}
            and requested_lots_from_executor is not None
            and actual_lots is not None
            and actual_lots < requested_lots_from_executor
        ):
            _trade_flow_inc("portfolio_size_reduced")
        if execution_status in {"OPENED", "ACCEPTED"} and raw_actual_lots is None:
            actual_lots = int(lots)
        if execution_status in {"OPENED", "ACCEPTED"}:
            logger.info(
                "📌 [EXECUTION RESULT] %s %s: requested=%s lots; actual=%s lots/%s shares; notional=%.2f ₽; status=%s",
                side, ticker, requested_lots_from_executor if requested_lots_from_executor is not None else lots,
                actual_lots if actual_lots is not None else lots,
                actual_shares if actual_shares is not None else 0,
                actual_notional if actual_notional is not None else 0.0, execution_status,
            )
        if (
            execution_status in {"OPENED", "ACCEPTED"}
            and raw_actual_shares is None
            and actual_lots is not None
        ):
            actual_shares = actual_lots * max(int(FINAM_TRADE_LOT_SIZES.get(ticker, 0) or 0), 0)
        if (
            execution_status in {"OPENED", "ACCEPTED"}
            and raw_actual_notional is None
            and actual_shares is not None
        ):
            actual_notional = round(actual_shares * price, 2)

        update = {
            "verdict_id": verdict_id,
            "ticker": ticker,
            "execution_status": execution_status,
            "execution_message": str(result.get("message") or (
                "PAPER-позиция зарегистрирована." if execution_status == "OPENED"
                else "Брокер принял заявку; факт исполнения должен быть подтверждён отдельно." if execution_status == "ACCEPTED"
                else "Заявка не открыта."
            )),
            "order_id": order_id or None,
            "executed_lots": actual_lots,
            "executed_shares": actual_shares,
            "actual_notional_rub": actual_notional,
            "execution_timestamp": datetime.now().astimezone().isoformat(timespec="milliseconds"),
        }

        # Update in-memory history so a browser refresh shows the final outcome.
        for cached in last_ai_verdicts_cache:
            if isinstance(cached, dict) and cached.get("verdict_id") == verdict_id:
                cached.update(update)
                break

        # Persist execution outcome separately. This avoids list-index races when
        # several asynchronous orders finish while newer verdicts are LPUSH'ed.
        try:
            redis_inst = getattr(self.db, "redis", getattr(self.db, "_redis", None))
            if redis_inst is not None:
                await redis_inst.set(
                    f"piluitus:verdict:execution:{verdict_id}",
                    json.dumps(update, ensure_ascii=False),
                    ex=7 * 24 * 60 * 60,
                )
        except Exception as persist_exc:
            logger.warning("Не удалось сохранить статус исполнения вердикта %s: %s", verdict_id, persist_exc)

        try:
            positions = await _get_dashboard_open_positions()
            balance = await _refresh_dashboard_balance_snapshot(positions)
            update["active_positions"] = positions
            update["balance"] = balance
            await manager.broadcast(json.dumps({"type": "EXECUTION_UPDATE", **update}, ensure_ascii=False))
            await manager.broadcast(json.dumps({
                "type": "TABLES_UPDATE",
                "active_positions": positions,
                "closed_trades": getattr(execution_engine, "closed_trades", [])[-30:],
                "balance": balance,
            }, ensure_ascii=False))
        except Exception as broadcast_exc:
            logger.error("Не удалось отправить статус исполнения в веб-интерфейс: %s", broadcast_exc, exc_info=True)

        if execution_status in {"OPENED", "ACCEPTED"}:
            logger.info(
                "📡 [DASHBOARD EXECUTION] %s %s %s: %s лот(а), %s ₽, order=%s",
                side, ticker, execution_status, actual_lots, actual_notional, order_id or "N/A",
            )
        else:
            logger.warning(
                "📡 [DASHBOARD EXECUTION] %s %s: %s — %s",
                side, ticker, execution_status, update["execution_message"],
            )

    async def _publish_pre_llm_risk_block(self, event_payload: dict, session_generation: int) -> None:
        """Publish a local candidate rejected by RiskManager if its PAPER session is still current."""
        async with _paper_session_lock:
            if (
                getattr(execution_engine, "_maintenance_active", False)
                or session_generation != _paper_session_generation
            ):
                return
            await self._publish_pre_llm_risk_block_locked(event_payload)

    async def _publish_pre_llm_risk_block_locked(self, event_payload: dict) -> None:
        """Persist and broadcast the pre-LLM risk block."""
        now_mono = time_module.monotonic()
        # RiskManager is a global gate. Avoid filling the verdict panel with one
        # identical card per ticker while the same global stop remains active.
        if now_mono - self._last_risk_block_verdict_at < 10.0:
            return
        self._last_risk_block_verdict_at = now_mono
        ticker = str(event_payload.get("ticker", "UNKNOWN")).strip().upper()
        price = event_payload.get("price", 0.0)
        metrics = event_payload.get("metrics", {})
        if not isinstance(metrics, dict):
            metrics = {}
        candidate = str(event_payload.get("candidate_direction", "HOLD")).upper()
        reason = "RiskManager остановил торговлю до запроса к ИИ; исполнитель не вызывался."
        tactic, critic = build_verdict_explanation("HOLD", candidate, metrics, reason)
        now_local = datetime.now().astimezone()
        verdict_id = uuid4().hex
        verdict = {
            "verdict_id": verdict_id,
            "timestamp": now_local.strftime("%H:%M:%S"),
            "timestamp_iso": now_local.isoformat(timespec="milliseconds"),
            "ticker": ticker,
            "price": price,
            "decision": "HOLD",
            "candidate_decision": candidate,
            "confidence": None,
            "decision_source": "RISK_MANAGER",
            "signal_status": "BLOCKED" if candidate in {"BUY", "SELL"} else "NO_ENTRY",
            "signal_reason": reason,
            "execution_status": "NOT_REQUESTED",
            "execution_message": "Не запрашивалось: торговля остановлена RiskManager.",
            "tactic_opinion": tactic,
            "critic_opinion": critic,
        }
        last_ai_verdicts_cache.append(verdict)
        if len(last_ai_verdicts_cache) > MAX_CACHE_SIZE:
            last_ai_verdicts_cache.pop(0)
        try:
            redis_inst = getattr(self.db, "redis", getattr(self.db, "_redis", None))
            if redis_inst is not None:
                await redis_inst.lpush("piluitus:verdicts:history", json.dumps(verdict, ensure_ascii=False))
                await redis_inst.ltrim("piluitus:verdicts:history", 0, MAX_CACHE_SIZE - 1)
        except Exception as persist_exc:
            logger.warning("Не удалось сохранить RiskManager-вердикт %s: %s", ticker, persist_exc)
        try:
            payload = {"type": "MARKET_UPDATE", **verdict, "balance": dict(_dashboard_balance_snapshot)}
            await manager.broadcast(json.dumps(payload, ensure_ascii=False))
        except Exception as broadcast_exc:
            logger.error("Не удалось отправить RiskManager-вердикт в веб-интерфейс: %s", broadcast_exc)

    async def start_processing(self):
        global last_ai_verdicts_cache
        logger.info("🤖 ИИ‑конвейер воркеров Piluitus v35.2 успешно запущен.")

        while True:
            queue_item_acquired = False
            pending_ticker = ""
            try:
                priority, _sequence_id, _timestamp_id, event_type, event_payload = await anomaly_queue.get()
                queue_item_acquired = True
                if event_type == "MARKET_TICK" and isinstance(event_payload, dict):
                    pending_ticker = str(event_payload.get("ticker", "")).strip().upper()
                    if (
                        getattr(execution_engine, "_maintenance_active", False)
                        or event_payload.get("paper_session_generation") != _paper_session_generation
                    ):
                        continue
                
                if priority == 0:
                    logger.warning(f"🚨 [ПРИОРИТЕТНЫЙ ПЕРЕХВАТ] Выполнение экстренной команды: {event_type}")
                    if event_type == "CMD_CLOSE_ALL":
                        await execution_engine.close_all_positions(db=self.db) 
                    elif event_type == "CMD_SET_RISK_LIMIT":
                        try:
                            risk_manager.daily_loss_limit = validate_daily_loss_limit(
                                event_payload.get("value", Settings.DAILY_LOSS_LIMIT_PCT)
                            )
                        except ValueError as exc:
                            logger.error("[RISK LIMIT] Некорректное значение в очереди: %s", exc)
                    continue

                ticker   = event_payload.get("ticker") or event_payload.get("security_code") or "UNKNOWN"
                price    = event_payload.get("price") or 0.0
                metrics  = event_payload.get("metrics", {})
                if not isinstance(metrics, dict):
                    metrics = {}
                candidate_direction_for_diag = str(event_payload.get("candidate_direction") or "").upper()
                llm_input_features = {
                    "price_change_pct": _parse_finite_float(metrics.get("price_change_pct")),
                    "price_vs_vwap_pct": _parse_finite_float(metrics.get("price_vs_vwap_pct")),
                    "volume_spike": _parse_finite_float(metrics.get("volume_spike")),
                    "trade_data_mode": _parse_finite_float(metrics.get("trade_data_mode")),
                    "trade_data_ready": _parse_finite_float(metrics.get("trade_data_ready")),
                    "trade_vwap_60s": _parse_finite_float(metrics.get("trade_vwap_60s")),
                    "trade_volume_5s": _parse_finite_float(metrics.get("trade_volume_5s")),
                    "trade_volume_baseline_5s": _parse_finite_float(metrics.get("trade_volume_baseline_5s")),
                    "trade_flow_imbalance_30s": _parse_finite_float(metrics.get("trade_flow_imbalance_30s")),
                    "trade_buy_volume_30s": _parse_finite_float(metrics.get("trade_buy_volume_30s")),
                    "trade_sell_volume_30s": _parse_finite_float(metrics.get("trade_sell_volume_30s")),
                    "trade_count_60s": _parse_finite_float(metrics.get("trade_count_60s")),
                    "trade_history_coverage_sec": _parse_finite_float(metrics.get("trade_history_coverage_sec")),
                    "trade_metrics_age_sec": _parse_finite_float(metrics.get("trade_metrics_age_sec")),
                    "trade_price_change_30s_pct": _parse_finite_float(metrics.get("trade_price_change_30s_pct")),
                    "trade_realized_volatility_60s_pct": _parse_finite_float(metrics.get("trade_realized_volatility_60s_pct")),
                    "spread_pct": _parse_finite_float(metrics.get("spread_pct")),
                    "m1_pattern": str(metrics.get("bar_pattern_name") or ""),
                    "m1_pattern_confirmed": _parse_finite_float(metrics.get("bar_pattern_confirmed")),
                    "m1_pattern_direction_code": _parse_finite_float(metrics.get("bar_pattern_direction_code")),
                    "m1_breakout_up": _parse_finite_float(metrics.get("bar_breakout_up")),
                    "m1_breakout_down": _parse_finite_float(metrics.get("bar_breakout_down")),
                    "m1_pullback_buy": _parse_finite_float(metrics.get("bar_pullback_buy")),
                    "m1_pullback_sell": _parse_finite_float(metrics.get("bar_pullback_sell")),
                    "m1_trend_up": _parse_finite_float(metrics.get("bar_trend_up")),
                    "m1_trend_down": _parse_finite_float(metrics.get("bar_trend_down")),
                    "m1_signal_gap_atr": _parse_finite_float(metrics.get("bar_signal_gap_atr")),
                    "bid_size_drop_pct": _parse_finite_float(metrics.get("bid_size_drop_pct")),
                    "ask_size_drop_pct": _parse_finite_float(metrics.get("ask_size_drop_pct")),
                    "top_bid_history_samples": _parse_finite_float(metrics.get("top_bid_history_samples")),
                    "top_ask_history_samples": _parse_finite_float(metrics.get("top_ask_history_samples")),
                    "bid_size_drop_age_sec": _parse_finite_float(metrics.get("bid_size_drop_age_sec")),
                    "ask_size_drop_age_sec": _parse_finite_float(metrics.get("ask_size_drop_age_sec")),
                    "bid_size_available": _parse_finite_float(metrics.get("bid_size_available")),
                    "ask_size_available": _parse_finite_float(metrics.get("ask_size_available")),
                    "bid_ask_available": _parse_finite_float(metrics.get("bid_ask_available")),
                    "spread_available": _parse_finite_float(metrics.get("spread_available")),
                    "m1_atr": _parse_finite_float(metrics.get("bar_atr")),
                    "m1_atr_periods": _parse_finite_float(metrics.get("bar_atr_periods")),
                    "m1_close_position": _parse_finite_float(metrics.get("bar_close_position")),
                    "m1_body_ratio": _parse_finite_float(metrics.get("bar_body_ratio")),
                    "queue_imbalance": _parse_finite_float(metrics.get("queue_imbalance")),
                    "queue_imbalance_available": _parse_finite_float(metrics.get("queue_imbalance_available")),
                    "queue_imbalance_age_sec": _parse_finite_float(metrics.get("queue_imbalance_age_sec")),
                    "bid_quote_age_sec": _parse_finite_float(metrics.get("bid_quote_age_sec")),
                    "ask_quote_age_sec": _parse_finite_float(metrics.get("ask_quote_age_sec")),
                }
                volume_spike = _parse_finite_float(metrics.get("volume_spike"))
                volume_5s = _parse_finite_float(metrics.get("trade_volume_5s"))
                volume_baseline_5s = _parse_finite_float(metrics.get("trade_volume_baseline_5s"))
                trade_count_60s = _parse_finite_float(metrics.get("trade_count_60s"))
                if volume_5s is not None and volume_baseline_5s is not None:
                    volume_text = (
                        f"Всплеск объёма: {volume_spike:.2f}×; "
                        f"5с={volume_5s:.3f} ед. провайдера, база={volume_baseline_5s:.3f}; "
                        f"сделок за 60с={int(trade_count_60s or 0)}."
                    )
                else:
                    volume_text = (
                        f"Всплеск объёма: {volume_spike:.2f}×."
                        if volume_spike is not None else "Объём сделок: Н/Д."
                    )

                trade_realized_volatility = _parse_finite_float(
                    metrics.get("trade_realized_volatility_60s_pct")
                )
                m1_atr_pct = _parse_finite_float(metrics.get("bar_atr_pct"))
                if trade_realized_volatility is not None:
                    volatility_text = (
                        f"Реализованная волатильность по сделкам за 60с: "
                        f"{trade_realized_volatility:.5f}%."
                    )
                elif m1_atr_pct is not None:
                    volatility_text = f"M1 ATR: {m1_atr_pct:.5f}%."
                else:
                    volatility_text = "Волатильность: Н/Д — нет подтверждённой истории."

                asyncio.create_task(
                    log_trading_event_to_file(
                        ticker=ticker,
                        price=price,
                        decision="В ОЧЕРЕДИ (ПРОВЕРКА РИСКОВ)",
                        confidence=0.0,
                        tactic=volume_text,
                        critic=volatility_text,
                    )
                )

                trading_allowed = await risk_manager.is_trading_allowed(self.db)
                if (
                    getattr(execution_engine, "_maintenance_active", False)
                    or event_payload.get("paper_session_generation") != _paper_session_generation
                ):
                    continue
                if not trading_allowed:
                    _trade_flow_inc("risk_blocked")
                    asyncio.create_task(
                        log_trading_event_to_file(
                            ticker, price, "RISK_BLOCK", 0.0,
                            "Торговля заблокирована.", "RiskManager запретил торговлю до запроса к ИИ."
                        )
                    )
                    await self._publish_pre_llm_risk_block(
                        event_payload, int(event_payload.get("paper_session_generation", -1))
                    )
                    continue

                try:
                    from knowledge_parser import TechnicalKnowledgeParser

                    rag_context_str = await TechnicalKnowledgeParser.get_knowledge_context(
                        db=self.db,
                        category="ScalpingRules",
                        market_evidence=metrics if isinstance(metrics, dict) else {},
                    )
                    event_payload["rag_context"] = [rag_context_str]
                    retrieved_rule_ids = re.findall(
                        r"\[(?:RISK|BUY|SELL)\s*/\s*([A-Z0-9_]+)\]",
                        rag_context_str,
                    )
                    # The prompt receives always-on RISK rules plus only the
                    # selected local candidate's directional rules. They remain
                    # advisory context; deterministic guards own hard entry gates.
                    prompt_rule_text = select_prompt_rules(
                        candidate_direction_for_diag, rag_context_str, max_chars=900
                    )
                    prompt_rule_ids = re.findall(
                        r"\[(?:RISK|BUY|SELL)\s*/\s*([A-Z0-9_]+)\]",
                        prompt_rule_text,
                    )
                    logger.info(
                        "[LLM CONTEXT] ticker=%s candidate=%s retrieved_rule_ids=%s prompt_rule_ids=%s",
                        ticker, candidate_direction_for_diag,
                        ",".join(retrieved_rule_ids) if retrieved_rule_ids else "NONE",
                        ",".join(prompt_rule_ids) if prompt_rule_ids else "NONE",
                    )
                except Exception as exc:
                    logger.error("⚠️ Ошибка сборки RAG-контекста в main.py: %s", exc, exc_info=True)
                    event_payload["rag_context"] = [
                        "Нет проверенного контекста правил; направленный вход должен быть HOLD."
                    ]

                logger.info(
                    "🧭 [LLM INPUT] ticker=%s candidate=%s features=%s",
                    ticker, candidate_direction_for_diag,
                    json.dumps(llm_input_features, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                )
                try:
                    _trade_flow_inc("llm_calls")
                    ai_response = await asyncio.wait_for(
                        self.ai.analyze_market_state(event_payload),
                        timeout=float(Settings.VLLM_TIMEOUT) * 2.0 + 1.0,
                    )
                except asyncio.TimeoutError as exc:
                    ai_response = {
                        "final_decision": "HOLD",
                        "model_decision": "HOLD",
                        "model_raw_decision": "INVALID_RESPONSE",
                        "model_contract_violation": False,
                        "contract_error_type": None,
                        "error_type": "worker_timeout",
                        "request_error": True,
                        "request_error_type": "worker_timeout",
                        "confidence_score": 0.0,
                        "confidence_valid": False,
                        "tactic_opinion": "Превышено общее время ожидания ИИ; торговый вход заблокирован.",
                        "critic_opinion": str(exc),
                    }
                except Exception as exc:
                    ai_response = {
                        "final_decision": "HOLD",
                        "model_decision": "HOLD",
                        "model_raw_decision": "INVALID_RESPONSE",
                        "model_contract_violation": False,
                        "contract_error_type": None,
                        "error_type": "worker_error",
                        "request_error": True,
                        "request_error_type": "worker_error",
                        "confidence_score": 0.0,
                        "confidence_valid": False,
                        "tactic_opinion": "Ошибка обработки запроса ИИ; торговый вход заблокирован.",
                        "critic_opinion": str(exc),
                    }

                if (
                    getattr(execution_engine, "_maintenance_active", False)
                    or event_payload.get("paper_session_generation") != _paper_session_generation
                ):
                    continue

                ai_analysis = {"final_decision": "HOLD", "confidence_score": 0.0, "confidence_valid": False, "tactic_opinion": "", "critic_opinion": ""}
                if isinstance(ai_response, list) and len(ai_response) > 0:
                    first_item = next(iter(ai_response), None)
                    if isinstance(first_item, dict):
                        ai_analysis = first_item
                elif isinstance(ai_response, dict):
                    ai_analysis = ai_response

                if ai_analysis.get("contract_retry_attempted"):
                    _trade_flow_inc("llm_contract_retry")
                if ai_analysis.get("contract_retry_recovered"):
                    _trade_flow_inc("llm_contract_retry_recovered")
                if ai_analysis.get("contract_retry_failed"):
                    _trade_flow_inc("llm_contract_retry_failed")

                request_error = bool(ai_analysis.get("request_error", False))
                request_error_type = str(ai_analysis.get("request_error_type") or "request_error")
                if request_error:
                    _trade_flow_inc("llm_request_error")
                    if "timeout" in request_error_type:
                        _trade_flow_inc("llm_timeout")

                # LLM returns only APPROVE/HOLD. Trade direction comes exclusively
                # from the deterministic local candidate and cannot be changed by the model.
                model_decision = str(
                    ai_analysis.get("model_decision", ai_analysis.get("final_decision", "HOLD"))
                ).strip().upper()
                candidate_direction = str(event_payload.get("candidate_direction", "")).strip().upper()
                model_reason_code = str(
                    ai_analysis.get("model_reason_code")
                    or ("NO_LOCAL_CANDIDATE" if candidate_direction not in {"BUY", "SELL"} else "NOT_PROVIDED")
                ).strip().upper()
                candidate_decision = candidate_direction if candidate_direction in {"BUY", "SELL"} else "HOLD"
                model_contract_violation = bool(ai_analysis.get("model_contract_violation", False))
                if not request_error:
                    if (
                        model_decision == "APPROVE"
                        and candidate_direction in {"BUY", "SELL"}
                        and not model_contract_violation
                    ):
                        _trade_flow_inc("llm_model_approve")
                    elif model_decision == "HOLD" and not model_contract_violation:
                        _trade_flow_inc("llm_model_hold")
                if (
                    model_decision == "APPROVE"
                    and candidate_direction in {"BUY", "SELL"}
                    and not model_contract_violation
                    and not request_error
                ):
                    decision = candidate_direction
                else:
                    decision = "HOLD"
                    if request_error:
                        logger.error(
                            "🛑 [LLM REQUEST ERROR] %s: запрос к vLLM не завершён; "
                            "type=%s candidate=%s; вход запрещён.",
                            ticker, request_error_type, candidate_direction or "NONE",
                        )
                    elif model_decision != "HOLD" or model_contract_violation:
                        model_contract_violation = True
                        _trade_flow_inc("llm_contract_violation")
                        logger.error(
                            "🛑 [LLM CONTRACT] %s: ответ не прошёл контракт APPROVE/HOLD; "
                            "raw=%s error_type=%s retry_attempted=%s retry_failed=%s candidate=%s; вход запрещён.",
                            ticker, ai_analysis.get("model_raw_decision", model_decision),
                            ai_analysis.get("contract_error_type", "unknown"),
                            ai_analysis.get("contract_retry_attempted", False),
                            ai_analysis.get("contract_retry_failed", False),
                            candidate_direction or "NONE",
                        )
                ai_analysis["model_decision"] = model_decision
                ai_analysis["final_decision"] = decision
                confidence_valid = bool(ai_analysis.get("confidence_valid", "confidence_score" in ai_analysis))
                try:
                    confidence = float(ai_analysis.get("confidence_score", 0.0)) * 100.0
                    if not math.isfinite(confidence) or not 0.0 <= confidence <= 100.0:
                        confidence_valid = False
                        confidence = 0.0
                except (TypeError, ValueError):
                    confidence_valid = False
                    confidence = 0.0
                latency_val = float(ai_analysis.get("latency", 0.0) or 0.0)
                if decision not in {"BUY", "SELL", "HOLD"}:
                    decision = "HOLD"
                    confidence = 0.0
                candidate_confidence = confidence if confidence_valid else None
                if decision == "BUY":
                    _trade_flow_inc("llm_approve_buy")
                elif decision == "SELL":
                    _trade_flow_inc("llm_approve_sell")
                else:
                    if not request_error:
                        _trade_flow_inc("llm_hold")
                        if candidate_direction == "BUY":
                            _trade_flow_inc("llm_hold_buy")
                        elif candidate_direction == "SELL":
                            _trade_flow_inc("llm_hold_sell")
                # Независимый численный фильтр: свободный текст и confidence LLM
                # не заменяют обязательную проверку измеряемых условий входа.
                evidence = metrics if isinstance(metrics, dict) else {}
                if isinstance(evidence, dict) and _parse_finite_float(evidence.get("trade_data_mode")) >= 0.5:
                    # Recompute the trade window after the model response. A candidate
                    # can become stale while vLLM is thinking; never authorize it from
                    # the original pre-LLM snapshot alone.
                    fresh_trade = screener.refresh_trade_metrics(ticker)
                    evidence = dict(evidence)
                    for key in (
                        "trade_data_ready", "trade_vwap_60s", "trade_flow_imbalance_30s",
                        "trade_count_60s", "trade_history_coverage_sec", "trade_metrics_age_sec", "volume_spike",
                        "price_change_pct", "trade_price_change_30s_pct",
                        "trade_realized_volatility_60s_pct", "current_price", "vwap",
                        "price_vs_vwap_pct",
                    ):
                        if key in fresh_trade:
                            evidence[key] = fresh_trade[key]
                    if isinstance(metrics, dict):
                        metrics.update(evidence)
                valid_evidence = True
                block_reason = ""
                if decision in {"BUY", "SELL"} and candidate_direction in {"BUY", "SELL"} and decision != candidate_direction:
                    valid_evidence = False
                    block_reason = (
                        f"Противоположное направление запрещено: локально подтверждён {candidate_direction}, "
                        f"модель предложила {decision}."
                    )
                elif decision in {"BUY", "SELL"}:
                    valid_evidence, block_reason = validate_signal_evidence(decision, evidence)
                signal_reason = (
                    f"Ошибка запроса к vLLM ({request_error_type}); вход запрещён."
                    if request_error else
                    "LLM нарушила контракт APPROVE/HOLD; вход запрещён."
                    if model_contract_violation else ""
                )
                if decision in {"BUY", "SELL"} and not valid_evidence:
                    requested_decision = decision
                    signal_reason = block_reason or "Измеряемые условия входа не пройдены."
                    _trade_flow_inc("post_llm_evidence_blocked")
                    _trade_flow_guard_block(signal_reason)
                    logger.warning(
                        "🛑 [SIGNAL GUARD] Заблокирован кандидат %s по %s: %s",
                        requested_decision, ticker, signal_reason,
                    )
                    decision = "HOLD"
                    confidence = 0.0
                    ai_analysis["final_decision"] = decision
                    ai_analysis["confidence_score"] = 0.0

                if decision in {"BUY", "SELL"} and (not confidence_valid or confidence < 75.0):
                    _trade_flow_inc("confidence_blocked")
                    _trade_flow_inc("post_llm_confidence_blocked")
                    requested_decision = decision
                    if not confidence_valid:
                        reason = "Модель не вернула корректную числовую confidence_score; вход заблокирован."
                    else:
                        reason = f"Оценка модели {confidence:.0f}% ниже порога входа 75%; новая позиция не открыта."
                    signal_reason = reason
                    decision = "HOLD"
                    # Keep the model's original candidate score visible; the critic
                    # text explains that the candidate fell below the entry threshold.
                    ai_analysis["final_decision"] = decision
                    ai_analysis["confidence_score"] = confidence / 100.0

                execution_status = "NOT_REQUESTED"
                execution_message = "Новая позиция не запрашивалась."
                requested_lots = 0
                requested_notional = 0.0
                lot_multiplier = max(int(FINAM_TRADE_LOT_SIZES.get(ticker, 0) or 0), 0)
                verdict_id = uuid4().hex

                if decision in ("BUY", "SELL") and confidence >= 75.0:
                    logger.warning(f"🎯 СИГНАЛ УТВЕРЖДЁН ИИ: {ticker} → {decision} ({confidence:.1f}%)")
                    atr_value = finite_float(metrics.get("bar_atr")) or 0.0
                    atr_periods = nonnegative_int(metrics.get("bar_atr_periods")) or 0
                    entry_bbo_available = (finite_float(metrics.get("spread_available")) or 0.0) >= 0.5
                    entry_bid = (finite_float(metrics.get("bid")) or 0.0) if entry_bbo_available else 0.0
                    entry_ask = (finite_float(metrics.get("ask")) or 0.0) if entry_bbo_available else 0.0
                    entry_spread_pct = (finite_float(metrics.get("spread_pct")) or 0.0) if entry_bbo_available else 0.0
                    entry_bbo_age_sec = (finite_float(metrics.get("bbo_age_sec")) or 0.0) if entry_bbo_available else 0.0
                    stop_loss, take_profit = execution_engine.calculate_atr_stop(
                        ticker,
                        price,
                        decision,
                        atr_value=atr_value,
                        atr_periods=atr_periods,
                        bid=entry_bid,
                        ask=entry_ask,
                    )
                    if stop_loss <= 0.0 or take_profit <= 0.0:
                        try:
                            min_atr_periods = min(14, max(2, int(os.getenv("PILUITUS_ATR_MIN_PERIODS", "5"))))
                        except (TypeError, ValueError):
                            min_atr_periods = 5
                        execution_status = "BLOCKED"
                        execution_message = (
                            f"Недостаточно закрытых M1-диапазонов для ATR: {atr_periods}/{min_atr_periods}; "
                            "новая позиция не открыта."
                        )
                        _trade_flow_inc("atr_history_blocked")
                        ai_analysis["atr_entry_ready"] = False
                        logger.warning("Сигнал %s по %s заблокирован после LLM: %s", decision, ticker, execution_message)
                    else:
                        ai_analysis["atr_entry_ready"] = True
                        ai_analysis["stop_loss"] = stop_loss
                        ai_analysis["take_profit"] = take_profit
                        ai_analysis["atr_at_entry"] = atr_value
                        ai_analysis["atr_periods_at_entry"] = atr_periods
                        ai_analysis["atr_multiplier"] = finite_float(os.getenv("ATR_MULTIPLIER", "1.5")) or 1.5
                        ai_analysis["entry_bid"] = entry_bid
                        ai_analysis["entry_ask"] = entry_ask
                        ai_analysis["entry_spread_pct"] = entry_spread_pct
                        ai_analysis["entry_bbo_age_sec"] = entry_bbo_age_sec
                        requested_lots = risk_manager.calculate_lot_size(ticker, price, stop_loss)
                        if requested_lots <= 0:
                            _trade_flow_inc("zero_lot")
                            execution_status = "BLOCKED"
                            if lot_multiplier <= 0:
                                _trade_flow_inc("finam_lot_unavailable")
                                execution_message = (
                                    f"{'биржевой lot_size' if Settings.PAPER_TRADING else 'Finam account trade_lot_size'} для {ticker} не подтверждён; "
                                    "новая позиция не открыта."
                                )
                                logger.warning(
                                    "Сигнал %s по %s заблокирован: lot_size не подтверждён.",
                                    decision, ticker,
                                )
                            else:
                                execution_message = "Риск на минимальный лот превышает установленный лимит."
                                logger.warning(
                                    "Сигнал %s по %s пропущен: риск на минимальный лот превышает лимит.",
                                    decision, ticker,
                                )
                        else:
                            requested_notional = requested_lots * lot_multiplier * price
                            logger.info(
                                "🧮 [POSITION SIZE] %s %s: lots=%d shares=%d price=%.4f notional=%.2f ₽ planned_risk=%.2f ₽ ATR=%.6f (%d M1 ranges) stop=%.4f%%",
                                decision, ticker, requested_lots, requested_lots * lot_multiplier, price, requested_notional,
                                abs(price - stop_loss) * requested_lots * lot_multiplier,
                                atr_value, atr_periods, abs(price - stop_loss) / price * 100.0,
                            )
                            execution_status = "PENDING"
                            execution_message = "Заявка передана в контур исполнения; ожидается результат."

                # Do not expose free-form LLM reasoning as evidence. User-facing text
                # is generated only from locally measured values and guard outcomes.
                if decision == "HOLD" and not signal_reason:
                    if not confidence_valid:
                        signal_reason = "Ответ модели не содержит корректной числовой оценки; вход запрещён."
                    elif model_decision == "HOLD" and candidate_decision in {"BUY", "SELL"}:
                        signal_reason = "Модель отклонила локальный кандидат: вердикт HOLD."
                    elif candidate_decision in {"BUY", "SELL"}:
                        signal_reason = "Кандидат ИИ не прошёл локальные проверки входа."
                    else:
                        signal_reason = "Модель не предложила направленный вход."
                tactic_text, critic_text = build_verdict_explanation(
                    decision, candidate_decision, evidence, signal_reason
                )
                if block_reason:
                    local_evidence_diagnostic = block_reason
                elif candidate_direction not in {"BUY", "SELL"}:
                    local_evidence_diagnostic = "NO_DIRECTIONAL_CANDIDATE"
                elif request_error:
                    local_evidence_diagnostic = f"LLM_REQUEST_ERROR:{request_error_type}"
                elif model_contract_violation:
                    local_evidence_diagnostic = "LLM_CONTRACT_VIOLATION"
                elif model_decision == "HOLD":
                    local_evidence_diagnostic = f"MODEL_HOLD:{model_reason_code}"
                elif not confidence_valid:
                    local_evidence_diagnostic = "CONFIDENCE_INVALID"
                elif decision == "HOLD" and confidence < 75.0:
                    local_evidence_diagnostic = f"CONFIDENCE_BELOW_THRESHOLD:{confidence:.1f}"
                elif decision == "HOLD":
                    local_evidence_diagnostic = "POST_LLM_GUARD_BLOCK"
                else:
                    local_evidence_diagnostic = "PRE_LLM_VALIDATED"
                logger.info(
                    "[LLM DECISION] %s candidate=%s model=%s final=%s confidence=%s "
                    "confidence_valid=%s contract_violation=%s model_reason_code=%s "
                    "reason=%s local_evidence=%s features=%s",
                    ticker,
                    candidate_direction or "NONE",
                    model_decision,
                    decision,
                    f"{confidence:.1f}%" if confidence_valid else "invalid",
                    confidence_valid,
                    model_contract_violation,
                    model_reason_code,
                    signal_reason or ("APPROVED" if decision in {"BUY", "SELL"} else "MODEL_HOLD"),
                    local_evidence_diagnostic,
                    json.dumps(llm_input_features, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                )

                signal_status = (
                    "APPROVED" if decision in {"BUY", "SELL"}
                    else "BLOCKED" if candidate_decision in {"BUY", "SELL"}
                    else "NO_ENTRY"
                )
                now_local = datetime.now().astimezone()
                verdict_data = {
                    "verdict_id": verdict_id,
                    "timestamp": now_local.strftime("%H:%M:%S"),
                    "timestamp_iso": now_local.isoformat(timespec="milliseconds"),
                    "ticker": ticker,
                    "price": price,
                    "decision": decision,
                    "candidate_decision": candidate_decision,
                    "model_decision": model_decision,
                    "confidence": round(candidate_confidence, 1) if candidate_confidence is not None else None,
                    "signal_status": signal_status,
                    "signal_reason": signal_reason,
                    "execution_status": execution_status,
                    "execution_message": execution_message,
                    "requested_lots": requested_lots or None,
                    "requested_notional_rub": round(requested_notional, 2) if requested_notional else None,
                    "executed_lots": None,
                    "executed_shares": None,
                    "actual_notional_rub": None,
                    "order_id": None,
                    "tactic_opinion": tactic_text,
                    "critic_opinion": critic_text,
                    "local_evidence_reason": local_evidence_diagnostic,
                    "llm_input_features": llm_input_features,
                    "llm_candidate_direction": candidate_decision,
                    "llm_model_decision": model_decision,
                    "llm_reason_code": model_reason_code,
                }

                ai_analysis["final_decision"] = decision
                ai_analysis["confidence_score"] = (confidence / 100.0) if confidence_valid else 0.0
                ai_analysis["tactic_opinion"] = tactic_text
                ai_analysis["critic_opinion"] = critic_text

                asyncio.create_task(
                    log_trading_event_to_file(
                        ticker=ticker,
                        price=price,
                        decision=f"ВЕРДИКТ ИИ: {decision} (кандидат={candidate_decision}, исполнение={execution_status})",
                        confidence=round(confidence, 1) if confidence_valid else 0.0,
                        tactic=tactic_text,
                        critic=critic_text,
                    )
                )

                async with _paper_session_lock:
                    if (
                        getattr(execution_engine, "_maintenance_active", False)
                        or event_payload.get("paper_session_generation") != _paper_session_generation
                    ):
                        continue
                    last_ai_verdicts_cache.append(verdict_data)
                    if len(last_ai_verdicts_cache) > MAX_CACHE_SIZE:
                        last_ai_verdicts_cache.pop(0)

                    try:
                        redis_inst = getattr(self.db, "redis", getattr(self.db, "_redis", None))
                        if redis_inst is not None:
                            await redis_inst.lpush("piluitus:verdicts:history", json.dumps(verdict_data, ensure_ascii=False))
                            await redis_inst.ltrim("piluitus:verdicts:history", 0, MAX_CACHE_SIZE - 1)
                    except Exception as redis_err:
                        logger.error(f"❌ Ошибка буферизации вердикта в Redis: {redis_err}")

                    current_balance = dict(_dashboard_balance_snapshot)
                    payload_for_ws = {
                        "type": "MARKET_UPDATE",
                        **verdict_data,
                        "balance": current_balance,
                    }

                    async def _safe_broadcast(payload: dict):
                        try:
                            json_str = json.dumps(payload, ensure_ascii=False)
                            await manager.broadcast(json_str)
                        except Exception as broadcast_err:
                            logger.error(f"Ошибка WebSocket бродкаста: {broadcast_err}")

                    await _safe_broadcast(payload_for_ws)

                    if execution_status == "PENDING":
                        asyncio.create_task(
                            self._execute_and_publish(
                                verdict_id=verdict_id,
                                ticker=ticker,
                                lots=requested_lots,
                                side=decision,
                                price=price,
                                ai_analysis=dict(ai_analysis),
                                session_generation=int(event_payload.get("paper_session_generation", -1)),
                            )
                        )

            except Exception as outer_exc:
                logger.error("[MarketWorker] КРИТИЧЕСКИЙ СБОЙ КОНВЕЙЕРА: %r", outer_exc, exc_info=True)

            finally:
                # Подтверждаем только элемент, который worker действительно извлёк.
                # ACK выполняется ровно один раз на всех путях (включая continue).
                if queue_item_acquired:
                    anomaly_queue.task_done()
                if pending_ticker:
                    _release_ai_ticker(pending_ticker)
                await asyncio.sleep(0.001)
# --------------------------------------------------------------
#  6️⃣  ФОНОВЫЕ ЦИКЛЫ ОБНОВЛЕНИЯ И СИНХРОНИЗАЦИИ ТАБЛИЦ
# --------------------------------------------------------------
async def background_broker_init():
    """
    Поддерживает Finam market-data stream независимо от дня недели.
    Торговые интервалы каждого инструмента проверяются отдельно через Finam schedule API;
    это также позволяет получать данные в предусмотренные MOEX сессии выходного дня.
    """
    logger.info("🔌 Проверка брокерского соединения Finam; календарь сессий будет проверяться по каждому инструменту...")
    try:
        await broker.connect()
        if bool(getattr(execution_engine, "paper_mode", True)):
            logger.info(
                "🧪 [FINAM ACCOUNT] PAPER работает на внутреннем псевдосчёте; "
                "Finam используется только для авторизации и рыночных данных. Account discovery не требуется."
            )
        else:
            selected_account_id = await broker.discover_accounts()
            if selected_account_id:
                logger.info(
                    "🏦 [FINAM ACCOUNT] account_id успешно определён из текущей сессии; "
                    "source=%s; account_id=%s.",
                    getattr(broker, "account_id_source", "session"),
                    broker._mask_account_id(selected_account_id),
                )
            else:
                logger.error(
                    "⛔ [FINAM ACCOUNT] LIVE-счёт не определён; LIVE-входы заблокированы."
                )

        # Verify both the authenticated endpoint and the schedule parser before
        # the first candidate reaches the executor. SBER is used only as a probe;
        # every actual order is checked against its own instrument schedule.
        try:
            schedule_probe = await execution_engine._market_session_status("SBER")
            logger.info(
                "📅 [SESSION CALENDAR PROBE] SBER@MISX: known=%s, open=%s, entry_allowed=%s, "
                "reason=%s, next_session=%s; actual entries are checked per instrument.",
                schedule_probe.get("known"),
                schedule_probe.get("is_open"),
                schedule_probe.get("entry_allowed"),
                schedule_probe.get("reason"),
                schedule_probe.get("next_session_start", "-"),
            )
            if not schedule_probe.get("known"):
                logger.warning(
                    "⛔ [SESSION CALENDAR] расписание Finam не подтверждено; "
                    "новые входы останутся заблокированы до успешной проверки."
                )
        except Exception as schedule_probe_exc:
            logger.error(
                "❌ [SESSION CALENDAR PROBE] ошибка проверки расписания Finam: %s",
                type(schedule_probe_exc).__name__,
                exc_info=True,
            )

        bridge_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=10000)
        async def _run_market_stream():
            await broker.start_market_data_stream(
                tickers=TICKERS_TO_TRADE,
                queue=bridge_queue,
                screener=screener,
            )

        async def _bridge_worker():
            while True:
                try:
                    raw_event = await bridge_queue.get()
                    try:
                        await process_market_event(raw_event)
                    finally:
                        bridge_queue.task_done()
                except Exception as bridge_err:
                    logger.error("❌ Ошибка моста WebSocket-котировок: %s", bridge_err, exc_info=True)
                    await asyncio.sleep(0.1)

        # TaskGroup ties both stream tasks to this parent task.
        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(_run_market_stream(), name="finam-market-stream")
            task_group.create_task(_bridge_worker(), name="finam-bridge-worker")
            logger.info("⏳ Задача Finam WebSocket запущена; ожидаем HANDSHAKE_SUCCESS.")
    except Exception as exc:
        logger.error("❌ Ошибка инициализации API Finam: %s", exc, exc_info=True)

    while True:
        await asyncio.sleep(3600)

# --------------------------------------------------------------
#  6️⃣.5️⃣ ВСПОМОГАТЕЛЬНЫЕ ФОНОВЫЕ ИНИЦИАЛИЗАТОРЫ (Перемещены вверх для линтера)
# --------------------------------------------------------------
async def _start_news_updater() -> None:
    async def _broadcast(message: str) -> None:
        await manager.broadcast(message)
    await MarketNewsParser.update_news_cycle(db_manager, broadcast_callback=_broadcast)

async def _finam_metadata_refresh_loop() -> None:
    """Refresh exchange lots for PAPER or account-specific lots for LIVE."""
    from risk_manager import RiskManager

    try:
        refresh_sec = max(300.0, float(os.getenv("FINAM_METADATA_REFRESH_SEC", "900")))
    except (TypeError, ValueError, OverflowError):
        refresh_sec = 900.0
    try:
        account_retry_sec = min(
            300.0,
            max(15.0, float(os.getenv("FINAM_ACCOUNT_DISCOVERY_RETRY_SEC", "60"))),
        )
    except (TypeError, ValueError, OverflowError):
        account_retry_sec = 60.0
    while True:
        # background_broker_init owns authentication. Do not send REST requests
        # until a JWT has been obtained successfully.
        if not str(getattr(broker, "jwt_token", "") or "").strip():
            await asyncio.sleep(1.0)
            continue
        paper_mode = bool(getattr(execution_engine, "paper_mode", True))
        account_id = str(getattr(broker, "account_id", "") or "").strip()
        if paper_mode:
            # GetAsset requires account_id even for exchange-level lot metadata.
            # Resolve a read-only context from the JWT session; do not populate the
            # LIVE execution account or route PAPER orders through Finam.
            metadata_context = str(getattr(broker, "metadata_account_id", "") or "").strip()
            discover_metadata = getattr(broker, "discover_metadata_account_id", None)
            if callable(discover_metadata) and not metadata_context:
                try:
                    metadata_context = str(await discover_metadata() or "").strip()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "[FINAM METADATA WORKER] read-only metadata context discovery failed: %s",
                        type(exc).__name__,
                    )
            if callable(discover_metadata) and not metadata_context:
                logger.warning(
                    "⏳ [FINAM METADATA WORKER] PAPER GetAsset refresh deferred: "
                    "no session-authorized read-only account context; retry in %.0fs.",
                    account_retry_sec,
                )
                await asyncio.sleep(account_retry_sec)
                continue
        elif not account_id:
            discover = getattr(broker, "discover_accounts", None)
            if callable(discover):
                try:
                    account_id = str(
                        await discover() or getattr(broker, "account_id", "") or ""
                    ).strip()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "[FINAM METADATA WORKER] account discovery failed: %s",
                        type(exc).__name__,
                    )
        if not paper_mode and (not account_id or account_id.upper() == "UNKNOWN_ACCOUNT"):
            logger.warning(
                "⛔ [FINAM METADATA WORKER] LIVE account_id не получен; LIVE-входы остаются заблокированы."
            )
            await asyncio.sleep(account_retry_sec)
            continue
        try:
            await RiskManager.update_finam_lot_sizes(
                finam_broker_client=broker, paper_mode=paper_mode
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "❌ [FINAM METADATA WORKER] refresh failed: %s",
                type(exc).__name__, exc_info=True,
            )
        confirmed = sum(
            1 for ticker in TICKERS_TO_TRADE
            if int(FINAM_TRADE_LOT_SIZES.get(str(ticker).strip().upper(), 0) or 0) > 0
        )
        # Avoid repeating 88 requests every 180 seconds after persistent provider-side HTTP 400.
        retry_delay = refresh_sec
        if confirmed:
            logger.info(
                "📦 [FINAM METADATA WORKER] confirmed %s lots=%d/%d; next refresh in %.0fs.",
                "PAPER exchange" if paper_mode else "LIVE account",
                confirmed, len(TICKERS_TO_TRADE), refresh_sec,
            )
        await asyncio.sleep(retry_delay)


async def _start_tables_sync_loop():
    """Периодически обновляет позиции и фактическую оценку задействованного капитала."""
    logger.info("📊 Воркер синхронизации таблиц и капитала запущен.")
    while True:
        try:
            await asyncio.sleep(1.0)

            active_positions = await _get_dashboard_open_positions()
            balance = await _refresh_dashboard_balance_snapshot(active_positions)
            raw_closed = getattr(execution_engine, "closed_trades", getattr(execution_engine, "_closed_trades", []))

            payload = {
                "type": "TABLES_UPDATE",
                "active_positions": active_positions,
                "closed_trades": raw_closed[-30:] if isinstance(raw_closed, list) else [],
                "balance": balance,
            }
            await manager.broadcast(json.dumps(payload, ensure_ascii=False))
        except Exception as e:
            logger.error("❌ Ошибка воркера синхронизации таблиц/капитала: %s", e, exc_info=True)
# --------------------------------------------------------------
#  7️⃣  FastAPI LIFESPAN
# --------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global bg_market_task, last_ai_verdicts_cache
    background_tasks: set[asyncio.Task] = set()

    def _spawn_background(coro, *, name: str) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        background_tasks.add(task)
        task.add_done_callback(background_tasks.discard)
        return task
    gross_exposure_pct = execution_engine.max_gross_exposure_pct * 100.0
    single_position_pct = execution_engine.max_single_position_pct * 100.0
    logger.info(
        "🚀 Запуск PILUITUS v35.2 | депозит=%.2f ₽ | общий лимит активов=%.1f%% (%.2f ₽), "
        "одна позиция=%.1f%% (%.2f ₽).",
        DEPOSIT, gross_exposure_pct, DEPOSIT * gross_exposure_pct / 100.0,
        single_position_pct, DEPOSIT * single_position_pct / 100.0,
    )

    try:
        await db_manager.connect()
        maintenance_state = await db_manager.get_json("piluitus:paper:maintenance")
        if isinstance(maintenance_state, dict) and bool(maintenance_state.get("active")):
            execution_engine._maintenance_active = True
            execution_engine._maintenance_reason = str(maintenance_state.get("reason") or "PAPER-сессия приостановлена для обслуживания.")
            logger.warning("⏸️ Восстановлена сохранённая пауза PAPER-сессии. Нажмите «Возобновить PAPER» на странице обслуживания.")
    except Exception as exc:
        logger.critical(f"❌ Критическая ошибка старта БД в Lifespan: {exc}")

    # В v17 функция существовала, но не вызывалась при запуске: JSON мог не попасть в Redis.
    try:
        from knowledge_parser import TechnicalKnowledgeParser
        await TechnicalKnowledgeParser.warm_up_rag_cache(db_manager)
    except Exception as rag_start_err:
        logger.error("⚠️ Не удалось загрузить базу ai_market_knowledge.json в RAG: %s", rag_start_err, exc_info=True)

    try:
        # PAPER-позиции из прежних запусков не восстанавливаются как активные автоматически:
        # они переводятся в STALE_REVIEW без фиктивной цены выхода/PnL.
        quarantined = await execution_engine.quarantine_unreconciled_paper_positions(db_manager)
        if quarantined:
            logger.warning("В старом PAPER-журнале найдено позиций для ручной проверки: %d", quarantined)
    except Exception as stale_paper_err:
        logger.error("Не удалось проверить старые PAPER-позиции: %s", stale_paper_err, exc_info=True)

    try:
        redis_inst = getattr(db_manager, "redis", getattr(db_manager, "_redis", None))
        if redis_inst is not None:
            cached_verdicts = await redis_inst.lrange("piluitus:verdicts:history", 0, MAX_CACHE_SIZE - 1)
            if cached_verdicts:
                last_ai_verdicts_cache = [json.loads(v) for v in cached_verdicts][::-1]
                logger.info(f"📚 Из Redis успешно восстановлено {len(last_ai_verdicts_cache)} вердиктов.")
    except Exception as redis_restore_err:
        logger.error(f"⚠️ Не удалось восстановить историю вердиктов из Redis: {redis_restore_err}")

    worker = MarketWorker(db=db_manager, ai=ai_engine, broker_client=broker)
    async def _run_worker_safely():
        try:
            await worker.start_processing()
        except Exception as e:
            logger.critical(f"❌ КРИТИЧЕСКОЕ ПАДЕНИЕ MarketWorker В ЦИКЛЕ ТОРГОВЛИ: {e}", exc_info=True)

    # Запускаем строго по одной асинхронной задаче на каждый воркер (дублирование удалено)
    _spawn_background(_run_worker_safely(), name="market-worker")
    _spawn_background(_finam_metadata_refresh_loop(), name="finam-metadata-refresh")
    _spawn_background(_start_news_updater(), name="news-updater")
    _spawn_background(_start_tables_sync_loop(), name="tables-sync")

    import tg_bot as tb
    tb.global_broker_ref = broker
    tb.global_execution_engine_ref = execution_engine

    if tg_bot.bot and tg_bot.dp:
        logger.info("🤖 Подключаем Telegram‑бот к API...")
        bot: Bot = cast(Bot, tg_bot.bot)          
        dp = tg_bot.dp                             

        async def _start_bot_safely() -> None:
            try:
                await dp.start_polling(bot, skip_updates=True)
            except Exception as exc:
                logger.error(f"❌ Telegram‑бот не смог запуститься: {exc}")

        _spawn_background(_start_bot_safely(), name="telegram-polling")
        logger.info("🟢 Telegram‑бот успешно инициализирован.")
    else:
        logger.warning("⚠️ Токен Telegram не найден – режим управления отключён.")

    bg_market_task = _spawn_background(background_broker_init(), name="broker-init-and-streams")

    try:
        yield
    finally:
        logger.info("🛑 Начинаем штатную остановку фоновых задач PILUITUS...")
        for task in tuple(background_tasks):
            if not task.done():
                task.cancel()
        if background_tasks:
            results = await asyncio.gather(*tuple(background_tasks), return_exceptions=True)
            for result in results:
                if isinstance(result, BaseException) and not isinstance(result, asyncio.CancelledError):
                    logger.warning("Ошибка при завершении фоновой задачи: %r", result)

        _trade_flow_maybe_log(force=True)
        try:
            await ai_engine.close()
        except Exception as close_err:
            logger.warning("Ошибка закрытия AIEngine: %s", close_err, exc_info=True)
        logger.info("🛑 Фоновые задачи остановлены; AIEngine закрыт.")


app = FastAPI(title="PILUITUS v35.2 — AI Trading Core", lifespan=lifespan)

# 🔥 HTTP-ЭНДПОИНТ ДЛЯ ПЕРЕХВАТА ИНТЕРАКТИВНЫХ КОМАНД С ВЫСШИМ ПРИОРИТЕТОМ (Priority 0)
@app.post("/api/v1/control/close_all")
async def http_emergency_close_all():
    """Экстренное принудительное закрытие всех открытых позиций фронтендом с обходом HFT-очереди."""
    await anomaly_queue.put(_make_priority_item(0, "CMD_CLOSE_ALL", {}))
    return {"status": "queued", "priority": "high_emergency"}

@app.post("/api/v1/control/risk_limit")
async def http_update_risk_limit(payload: dict = Body(...)):
    """Динамическое изменение лимитов просадки робота с высшим приоритетом обработки."""
    try:
        new_limit = validate_daily_loss_limit(payload.get("value", Settings.DAILY_LOSS_LIMIT_PCT))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    await anomaly_queue.put(_make_priority_item(0, "CMD_SET_RISK_LIMIT", {"value": new_limit}))
    return {"status": "queued", "priority": "high_emergency", "new_target_limit": new_limit}

# --------------------------------------------------------------
#  8️⃣  FastAPI ROUTING & WEBSOCKET ENDPOINT
# --------------------------------------------------------------
BASE_DIR = PROJECT_ROOT
TEMPLATE_DIR = BASE_DIR / "interface" / "templates"
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))
PAPER_CLEAR_CONFIRMATION = "ОЧИСТИТЬ PAPER-СЕССИЮ"


def _require_paper_mode() -> None:
    if not getattr(execution_engine, "paper_mode", True):
        raise HTTPException(status_code=403, detail="Операция доступна только в PAPER_TRADING=True.")


def _paper_trade_rows(snapshot: dict) -> list[dict]:
    rows = snapshot.get("paper_trades", [])
    return [dict(row) for row in rows if isinstance(row, dict)] if isinstance(rows, list) else []


async def _broadcast_paper_tables() -> None:
    positions = await _get_dashboard_open_positions()
    balance = await _refresh_dashboard_balance_snapshot(positions)
    raw_closed = getattr(execution_engine, "closed_trades", [])
    await manager.broadcast(json.dumps({
        "type": "TABLES_UPDATE",
        "active_positions": positions,
        "closed_trades": raw_closed[-30:] if isinstance(raw_closed, list) else [],
        "balance": balance,
    }, ensure_ascii=False))


@app.get("/maintenance", response_class=HTMLResponse)
async def read_maintenance_page():
    page_path = TEMPLATE_DIR / "maintenance.html"
    if not page_path.is_file():
        return HTMLResponse("<h1>maintenance.html не найден</h1>", status_code=404)
    return FileResponse(page_path)


@app.get("/api/v1/paper/reconciliation")
async def get_paper_reconciliation():
    _require_paper_mode()
    snapshot = await db_manager.get_paper_session_snapshot()
    paper_rows = _paper_trade_rows(snapshot)
    stale_rows = [
        row for row in paper_rows
        if str(row.get("status", "")).upper() == "STALE_REVIEW"
        and str(row.get("reconciliation_status", "")).upper() == "REVIEW_REQUIRED"
    ]
    stale_rows.sort(key=lambda row: str(row.get("entry_timestamp") or row.get("entry_time") or ""))
    summary = {key: value for key, value in snapshot.items() if key != "paper_trades"}
    return {
        "paper_mode": True,
        "maintenance_active": bool(getattr(execution_engine, "_maintenance_active", False)),
        "summary": summary,
        "stale_positions": stale_rows,
        "memory_open_positions": [
            dict(row) for row in getattr(execution_engine, "paper_positions", {}).values()
            if isinstance(row, dict)
        ],
    }


@app.post("/api/v1/paper/reconciliation/{order_id}")
async def reconcile_paper_position(order_id: str, payload: dict[str, object] = Body(...)):
    _require_paper_mode()
    action = str(payload.get("action", "")).strip().lower()
    if action not in {"restore_open", "close", "void"}:
        raise HTTPException(status_code=400, detail="Неизвестное действие сверки.")

    async with _paper_session_lock:
        if execution_engine._opening_tickers or execution_engine._lock_closing:
            raise HTTPException(status_code=409, detail="Есть незавершённая операция исполнения. Повторите сверку после её завершения.")

        all_trades = await db_manager.get_all_daily_trades()
        trade = all_trades.get(order_id)
        if not isinstance(trade, dict) or not DBManager.is_paper_trade(trade, order_id):
            raise HTTPException(status_code=404, detail="PAPER-запись не найдена в постоянном журнале.")
        trade = dict(trade)
        if str(trade.get("status", "")).upper() != "STALE_REVIEW" or str(trade.get("reconciliation_status", "")).upper() != "REVIEW_REQUIRED":
            raise HTTPException(status_code=409, detail="Запись уже не находится в статусе STALE_REVIEW.")

        ticker = str(trade.get("ticker", "")).strip().upper().split("@")[0]
        side = str(trade.get("side", "")).strip().upper()
        try:
            entry_price = float(trade.get("entry_price", 0.0))
            volume = int(float(trade.get("volume") or 0))
            if volume <= 0:
                lots = int(float(trade.get("lots") or 0))
                lot_size = int(float(trade.get("lot_size") or LOT_SIZES.get(ticker, 1) or 1))
                volume = lots * lot_size
        except (TypeError, ValueError, OverflowError):
            entry_price = 0.0
            volume = 0
        if not ticker or side not in {"BUY", "SELL"} or not math.isfinite(entry_price) or entry_price <= 0 or volume <= 0:
            raise HTTPException(status_code=422, detail="В журнале недостаточно корректных данных для безопасной сверки.")

        now = datetime.now().astimezone()
        note = str(payload.get("note", "")).strip()
        if action in {"close", "void"} and len(note) < 3:
            raise HTTPException(status_code=422, detail="Добавьте короткое пояснение к ручной операции.")

        if action == "restore_open":
            if ticker in execution_engine.paper_positions:
                raise HTTPException(status_code=409, detail=f"По {ticker} уже есть активная PAPER-позиция.")
            trade.update({
                "status": "OPEN",
                "mode": "PAPER",
                "current_price": float(trade.get("current_price") or entry_price),
                "volume": volume,
                "reconciliation_status": "CONFIRMED_OPEN",
                "reconciled_at": now.isoformat(timespec="seconds"),
                "reconciliation_note": note or "Оператор подтвердил, что PAPER-позиция остаётся открытой.",
            })
            await db_manager.set_json(f"piluitus:trade:{order_id}", trade)
            execution_engine.paper_positions[ticker] = trade
            execution_engine._unreconciled_paper_positions.pop(order_id, None)
            message = f"Позиция {ticker} возвращена в активный PAPER-мониторинг. После следующего перезапуска потребуется повторная сверка."

        elif action == "close":
            raw_exit_price: object = payload.get("exit_price")
            exit_price = _parse_finite_float(raw_exit_price)
            if exit_price is None or exit_price <= 0.0:
                raise HTTPException(
                    status_code=422,
                    detail="Укажите корректную положительную цену выхода.",
                )
            sign = 1 if side == "BUY" else -1
            pnl = (exit_price - entry_price) * volume * sign
            pnl_pct = ((exit_price - entry_price) / entry_price) * 100.0 * sign
            trade.update({
                "status": "CLOSED",
                "mode": "PAPER",
                "volume": volume,
                "current_price": exit_price,
                "exit_price": exit_price,
                "exit_time": now.strftime("%H:%M:%S"),
                "exit_timestamp": now.isoformat(timespec="milliseconds"),
                "exit_reason": "MANUAL_RECONCILIATION",
                "pnl": round(pnl, 2),
                "pnl_percent": round(pnl_pct, 4),
                "final_outcome": "MANUALLY_RECONCILED",
                "reconciliation_status": "CLOSED_CONFIRMED",
                "reconciled_at": now.isoformat(timespec="seconds"),
                "reconciliation_note": note,
            })
            await db_manager.set_json(f"piluitus:trade:{order_id}", trade)
            await db_manager.push_to_list("piluitus:trade_history", trade, max_len=100)
            execution_engine._unreconciled_paper_positions.pop(order_id, None)
            execution_engine.closed_trades.append(dict(trade))
            message = f"Закрытие {ticker} зафиксировано по {exit_price:.4f} ₽; расчётный PnL {pnl:+.2f} ₽."

        else:  # void
            trade.update({
                "status": "VOIDED",
                "mode": "PAPER",
                "final_outcome": "VOIDED",
                "reconciliation_status": "VOIDED_CONFIRMED",
                "reconciled_at": now.isoformat(timespec="seconds"),
                "reconciliation_note": note,
            })
            await db_manager.set_json(f"piluitus:trade:{order_id}", trade)
            execution_engine._unreconciled_paper_positions.pop(order_id, None)
            message = f"Запись {order_id} помечена VOIDED. Она сохранена в журнале для аудита, но больше не блокирует торговлю и капитал."

    await _broadcast_paper_tables()
    return {"status": "ok", "message": message, "trade": trade}


@app.post("/api/v1/paper/session/clear")
async def clear_paper_session(payload: dict[str, object] = Body(...)):
    global _paper_session_generation, last_ai_verdicts_cache, _balance_guard_last_signature
    _require_paper_mode()
    if str(payload.get("confirmation", "")).strip() != PAPER_CLEAR_CONFIRMATION:
        raise HTTPException(status_code=400, detail=f"Для подтверждения введите точную фразу: {PAPER_CLEAR_CONFIRMATION}")

    async with _paper_session_lock:
        execution_engine._maintenance_active = True
        execution_engine._maintenance_reason = "PAPER-сессия приостановлена для полной очистки. Проверьте результат и вручную возобновите PAPER."
        _paper_session_generation += 1
        # Persist the pause first. Any later failure must leave the bot paused
        # across process restarts rather than silently resuming a partial reset.
        await db_manager.set_json("piluitus:paper:maintenance", {
            "active": True,
            "reason": execution_engine._maintenance_reason,
            "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        })

        # Purge queued market events but retain emergency priority-zero commands.
        emergency_items = []
        while True:
            try:
                queued_item = anomaly_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                if isinstance(queued_item, tuple) and queued_item and queued_item[0] == 0:
                    emergency_items.append(queued_item)
            finally:
                anomaly_queue.task_done()
        for item in emergency_items:
            anomaly_queue.put_nowait(item)

        with _ai_ticker_pending_lock:
            _ai_ticker_pending.clear()
            _ai_ticker_last_enqueued_at.clear()

        if execution_engine._opening_tickers or execution_engine._lock_closing:
            raise HTTPException(
                status_code=409,
                detail="PAPER-операция ещё выполняется. Торговля оставлена на паузе; обновите страницу и повторите очистку.",
            )

        monitor_task = execution_engine._monitor_worker_task
        if monitor_task is not None and not monitor_task.done():
            monitor_task.cancel()
            await asyncio.gather(monitor_task, return_exceptions=True)
        execution_engine._monitor_worker_task = None
        execution_engine._pending_monitor_prices.clear()

        try:
            storage_result = await db_manager.clear_paper_session()
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        removed_memory_positions = len(execution_engine.paper_positions)
        removed_memory_closed = len(execution_engine.closed_trades)
        removed_memory_stale = len(execution_engine._unreconciled_paper_positions)
        execution_engine.paper_positions.clear()
        execution_engine.closed_trades.clear()
        execution_engine._unreconciled_paper_positions.clear()
        execution_engine._ticker_cooldowns.clear()
        execution_engine._reserved_notional.clear()
        execution_engine._reserved_stop_risk.clear()

        risk_manager._balance_initialized = False
        risk_manager.block_trading = False
        risk_manager._blocked_date = None
        risk_manager.total_session_pnl = 0.0
        risk_manager.current_equity = DEPOSIT
        risk_manager.free_margin = DEPOSIT
        risk_manager.high_water_mark = DEPOSIT
        risk_manager.current_drawdown_pct = 0.0
        risk_manager._drawdown_warning_active = False
        risk_manager._loss_streak_count = 0
        risk_manager._loss_pause_signature = None
        risk_manager._cached_trading_allowed = None
        risk_manager._last_risk_snapshot_monotonic = 0.0

        last_ai_verdicts_cache.clear()
        with _trade_flow_lock:
            _trade_flow_counts.clear()
        _balance_guard_last_signature = None
        balance = await _refresh_dashboard_balance_snapshot([])

        await manager.broadcast(json.dumps({
            "type": "PAPER_SESSION_RESET",
            "balance": balance,
            "active_positions": [],
            "closed_trades": [],
        }, ensure_ascii=False))

        result = {
            "status": "ok",
            "maintenance_active": True,
            "message": "PAPER-сессия очищена. Для защиты от старых сигналов торговый конвейер оставлен на паузе.",
            "storage": storage_result,
            "memory": {
                "removed_open_positions": removed_memory_positions,
                "removed_closed_trades": removed_memory_closed,
                "removed_stale_positions": removed_memory_stale,
            },
            "balance": balance,
        }
        logger.warning("🧹 Полная очистка PAPER-сессии завершена; торговля приостановлена до ручного возобновления.")
        return result


@app.post("/api/v1/paper/session/cleanup-stale-and-resume")
async def cleanup_stale_and_resume_paper_session():
    """Remove only quarantined STALE_REVIEW PAPER rows, then resume PAPER atomically."""
    global _paper_session_generation, _balance_guard_last_signature
    _require_paper_mode()

    async with _paper_session_lock:
        if execution_engine._opening_tickers or execution_engine._lock_closing:
            raise HTTPException(
                status_code=409,
                detail="Есть незавершённая операция исполнения. Очистка и запуск отменены.",
            )

        snapshot_before = await db_manager.get_paper_session_snapshot()
        if (
            snapshot_before.get("live_trade_count", 0)
            or snapshot_before.get("live_history_count", 0)
            or snapshot_before.get("live_active_order_count", 0)
        ):
            raise HTTPException(
                status_code=409,
                detail="В хранилище обнаружены LIVE-записи. Автоматический запуск PAPER заблокирован; данные не изменены.",
            )

        execution_engine._maintenance_active = True
        execution_engine._maintenance_reason = (
            "PAPER приостановлен на время удаления только STALE_REVIEW-записей."
        )
        _paper_session_generation += 1
        await db_manager.set_json("piluitus:paper:maintenance", {
            "active": True,
            "reason": execution_engine._maintenance_reason,
            "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        })

        # Drop queued market events so old candidates cannot run immediately after resume.
        # Priority-zero emergency commands are retained.
        emergency_items = []
        while True:
            try:
                queued_item = anomaly_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                if isinstance(queued_item, tuple) and queued_item and queued_item[0] == 0:
                    emergency_items.append(queued_item)
            finally:
                anomaly_queue.task_done()
        for item in emergency_items:
            anomaly_queue.put_nowait(item)

        with _ai_ticker_pending_lock:
            _ai_ticker_pending.clear()
            _ai_ticker_last_enqueued_at.clear()

        try:
            storage_result = await db_manager.clear_unreconciled_paper_positions()
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        removed_ids = {
            str(order_id) for order_id in storage_result.get("position_ids", [])
        }
        removed_memory_stale = sum(
            1 for order_id in removed_ids
            if order_id in execution_engine._unreconciled_paper_positions
        )
        for order_id in removed_ids:
            execution_engine._unreconciled_paper_positions.pop(order_id, None)

        snapshot_after = await db_manager.get_paper_session_snapshot()
        if snapshot_after.get("paper_stale_count", 0) > 0:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Осталось STALE_REVIEW-позиций: {snapshot_after['paper_stale_count']}. "
                    "PAPER оставлен на паузе; проверьте журнал."
                ),
            )
        if (
            snapshot_after.get("live_trade_count", 0)
            or snapshot_after.get("live_history_count", 0)
            or snapshot_after.get("live_active_order_count", 0)
        ):
            raise HTTPException(
                status_code=409,
                detail="После очистки обнаружены LIVE-записи. PAPER оставлен на паузе.",
            )

        # No unresolved PAPER records remain in persistent storage. Clear any
        # matching in-memory quarantine remnants but preserve current open positions,
        # closed history, verdicts and risk state.
        removed_memory_stale += len(execution_engine._unreconciled_paper_positions)
        execution_engine._unreconciled_paper_positions.clear()
        # Persist the resume before lifting the in-memory maintenance lock.
        # If storage fails, this process must remain paused.
        await db_manager.set_json("piluitus:paper:maintenance", {
            "active": False,
            "reason": "",
            "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        })
        execution_engine._maintenance_active = False
        execution_engine._maintenance_reason = ""

        _balance_guard_last_signature = None
        await _broadcast_paper_tables()
        result = {
            "status": "ok",
            "maintenance_active": False,
            "message": (
                "STALE_REVIEW-записи удалены. PAPER-конвейер автоматически возобновлён; "
                "закрытая история, вердикты, риск-состояние и текущие позиции в памяти сохранены."
            ),
            "storage": storage_result,
            "memory": {"removed_stale_positions": removed_memory_stale},
            "preserved": {
                "paper_history_rows": snapshot_after.get("paper_history_count", 0),
                "verdict_history_rows": snapshot_after.get("verdict_history_count", 0),
                "verdict_execution_keys": snapshot_after.get("verdict_execution_count", 0),
                "active_memory_positions": len(execution_engine.paper_positions),
            },
        }
        logger.warning(
            "🧹 [PAPER STALE CLEANUP + RESUME] deleted=%d; memory_stale=%d; PAPER resumed.",
            storage_result.get("deleted_stale_positions", 0), removed_memory_stale,
        )
        return result


@app.post("/api/v1/paper/session/resume")
async def resume_paper_session():
    _require_paper_mode()
    async with _paper_session_lock:
        if execution_engine._opening_tickers or execution_engine._lock_closing:
            raise HTTPException(status_code=409, detail="Выполняется операция исполнения; PAPER пока нельзя возобновить.")
        snapshot = await db_manager.get_paper_session_snapshot()
        if snapshot.get("paper_stale_count", 0) > 0:
            raise HTTPException(status_code=409, detail="Сначала завершите сверку всех STALE_REVIEW-позиций.")
        if snapshot.get("live_trade_count", 0) or snapshot.get("live_history_count", 0) or snapshot.get("live_active_order_count", 0):
            raise HTTPException(status_code=409, detail="В хранилище есть LIVE-записи. Возобновление PAPER заблокировано до проверки хранилища.")
        execution_engine._maintenance_active = False
        execution_engine._maintenance_reason = ""
        await db_manager.set_json("piluitus:paper:maintenance", {
            "active": False,
            "reason": "",
            "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        })
        logger.warning("▶️ PAPER-сессия вручную возобновлена оператором.")
        await _broadcast_paper_tables()
        return {"status": "ok", "maintenance_active": False, "message": "PAPER-торговля возобновлена. Новые сигналы будут приниматься."}


@app.get("/", response_class=HTMLResponse)
async def read_root():
    index_path = TEMPLATE_DIR / "index.html"          
    if not index_path.is_file():
        return HTMLResponse(
            content="<h1>404 – index.html не найден</h1><p>Поместите файл в папку interface/templates.</p>",
            status_code=404,
        )
    return FileResponse(index_path)

STATIC_DIR = BASE_DIR / "static"
if STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.websocket("/ws/frontend")
async def websocket_frontend_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        logger.info("🔌 К WS‑шлюзу подключился веб-интерфейс. Отправка стартового пакета...")
        
        initial_positions = await _get_dashboard_open_positions()
        initial_balance = await _refresh_dashboard_balance_snapshot(initial_positions)
        
        cached_news = []
        try:
            redis_inst = getattr(db_manager, "redis", getattr(db_manager, "_redis", None))
            if redis_inst is not None:
                news_raw = await redis_inst.get("piluitus:news:latest")
                if news_raw:
                    cached_news = json.loads(news_raw)
        except Exception:
            pass

        raw_closed = getattr(execution_engine, "closed_trades", getattr(execution_engine, "_closed_trades", []))
        verdicts_for_client = [dict(item) for item in last_ai_verdicts_cache if isinstance(item, dict)]
        try:
            redis_inst = getattr(db_manager, "redis", getattr(db_manager, "_redis", None))
            if redis_inst is not None:
                for verdict in verdicts_for_client:
                    verdict_id = verdict.get("verdict_id")
                    if not verdict_id:
                        continue
                    execution_raw = await redis_inst.get(f"piluitus:verdict:execution:{verdict_id}")
                    if execution_raw:
                        execution_update = json.loads(execution_raw)
                        if isinstance(execution_update, dict):
                            verdict.update(execution_update)
        except Exception as history_exc:
            logger.debug("Не удалось обогатить историю статусами исполнения: %s", history_exc)

        await websocket.send_json({
            "type": "INIT_DATA",
            "portfolio": {},
            "balance": initial_balance,
            "news": cached_news,
            "verdicts_history": verdicts_for_client,
            "active_positions": initial_positions,
            "closed_trades": raw_closed[-30:] if isinstance(raw_closed, list) else []
        })

        while True:
            data = await websocket.receive_text()
            message = json.loads(data)

            if isinstance(message, list):
                for single_message in message:
                    if isinstance(single_message, dict):
                        target_ticker = single_message.get("security_code") or single_message.get("ticker")
                        price_val     = single_message.get("price") or single_message.get("last_price")

                        if target_ticker and price_val is not None:
                            mock_event = SimpleNamespace(
                                security_code=str(target_ticker),
                                price=float(price_val),
                                volume=int(single_message.get("volume", 1000)),
                            )
                            asyncio.create_task(process_market_event(mock_event))
                await asyncio.sleep(0.0001)
            
            elif isinstance(message, dict):
                # ПЕРЕХВАТ ИНТЕРАКТИВНЫХ WS-КОМАНД ИЗ ИНТЕРФЕЙСА (Мгновенный перевод на Priority 0)
                msg_type = message.get("type")
                if msg_type == "USER_COMMAND_CLOSE_ALL":
                    await anomaly_queue.put(_make_priority_item(0, "CMD_CLOSE_ALL", {}))
                    continue
                elif msg_type == "USER_COMMAND_SET_RISK":
                    await anomaly_queue.put(_make_priority_item(0, "CMD_SET_RISK_LIMIT", {"value": message.get("value", 0.10)}))
                    continue

                target_ticker = message.get("security_code") or message.get("ticker")
                price_val     = message.get("price") or message.get("last_price")

                if target_ticker and price_val is not None:
                    mock_event = SimpleNamespace(
                        security_code=str(target_ticker),
                        price=float(price_val),
                        volume=int(message.get("volume", 1000)),
                    )
                    asyncio.create_task(process_market_event(mock_event))
            else:
                logger.error(f"❌ Получено вебсокет-сообщение неизвестного типа: {type(message)}")

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as ws_err:
        logger.error(f"⚠️ Ошибка WebSocket: {ws_err}")


if __name__ == "__main__":
    import uvicorn
    try:
        uvicorn.run(
            app,
            # Keep Uvicorn from reconfiguring/closing PILUITUS' durable file handlers.
            log_config=None,
            host="127.0.0.1",
            port=8080,
            reload=False,
            workers=1,
        )
    except KeyboardInterrupt:
        # Ctrl+C is an expected operator shutdown, not an application failure.
        logger.info("🛑 PILUITUS остановлен оператором (Ctrl+C).")
