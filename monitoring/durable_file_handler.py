from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path


class DurableFileHandler(logging.FileHandler):
    """Durable UTF-8 file logging with optional per-run size rotation.

    Every record is flushed from Python's text buffer. The stream is fsync'ed
    at the configured interval; close and rollover force a final fsync.
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
        max_bytes: int = 0,
    ) -> None:
        super().__init__(
            filename,
            mode=mode,
            encoding=encoding,
            delay=delay,
            errors=errors,
        )
        self.sync_interval_seconds = max(0.0, float(sync_interval_seconds))
        self.max_bytes = max(0, int(max_bytes))
        self._initial_filename = Path(self.baseFilename)
        self._rollover_index = 1
        self._last_fsync_at = 0.0
        self._last_fsync_warning_at = 0.0
        self._last_rollover_warning_at = 0.0
        self._closing = False

    def flush(self) -> None:
        # StreamHandler.emit() normally flushes after every record.
        super().flush()
        stream = self.stream
        if stream is None:
            return

        now = time.monotonic()
        if (
            not self._closing
            and self.sync_interval_seconds > 0
            and now - self._last_fsync_at < self.sync_interval_seconds
        ):
            return

        try:
            os.fsync(stream.fileno())
            self._last_fsync_at = now
        except (OSError, ValueError) as exc:
            self._write_handler_warning(
                "Не удалось синхронизировать лог с диском",
                exc,
                attribute="_last_fsync_warning_at",
            )

    def _write_handler_warning(
        self, message: str, exc: BaseException, *, attribute: str
    ) -> None:
        now = time.monotonic()
        previous = float(getattr(self, attribute, 0.0))
        if now - previous < 60.0:
            return
        try:
            sys.stderr.write(
                f"[PILUITUS LOG WARNING] {message}: {type(exc).__name__}\n"
            )
            sys.stderr.flush()
        except Exception:
            pass
        setattr(self, attribute, now)

    def _next_rollover_target(self) -> tuple[int, Path]:
        index = self._rollover_index + 1
        base = self._initial_filename
        while True:
            candidate = base.with_name(
                f"{base.stem}_part{index:02d}{base.suffix}"
            )
            if not candidate.exists():
                return index, candidate
            index += 1

    def _rollover(self) -> bool:
        """Open the next file before closing the current one to avoid data loss."""
        index, next_path = self._next_rollover_target()
        try:
            next_stream = open(
                next_path,
                self.mode,
                encoding=self.encoding,
                errors=self.errors,
            )
        except OSError as exc:
            self._write_handler_warning(
                f"Не удалось открыть следующий сегмент {next_path.name}",
                exc,
                attribute="_last_rollover_warning_at",
            )
            return False

        old_stream = self.stream
        self._closing = True
        try:
            # Persist the old segment before switching to the next one.
            self.flush()
        finally:
            self._closing = False

        self.stream = next_stream
        self.baseFilename = os.path.abspath(os.fspath(next_path))
        self._rollover_index = index
        self._last_fsync_at = 0.0
        if old_stream is not None:
            try:
                old_stream.close()
            except OSError as exc:
                self._write_handler_warning(
                    f"Не удалось закрыть предыдущий сегмент {self._initial_filename.name}",
                    exc,
                    attribute="_last_rollover_warning_at",
                )
        return True

    def emit(self, record: logging.LogRecord) -> None:
        """Rotate before writing a record that would exceed the configured cap."""
        try:
            if self.stream is None:
                self.stream = self._open()

            message = self.format(record)
            payload = message + self.terminator
            if self.max_bytes > 0 and self.stream is not None:
                # TextIOWrapper translates newline characters to os.linesep
                # on Windows, so estimate the bytes that will actually hit disk.
                disk_payload = payload.replace("\\n", os.linesep)
                encoded_size = len(
                    disk_payload.encode(
                        self.stream.encoding or "utf-8",
                        errors=self.stream.errors or "strict",
                    )
                )
                current_size = os.fstat(self.stream.fileno()).st_size
                if current_size > 0 and current_size + encoded_size > self.max_bytes:
                    # If rollover fails, keep writing to the existing file.
                    # Preserving the record is more important than enforcing the cap.
                    self._rollover()

            self.stream.write(payload)
            self.flush()
        except Exception:
            self.handleError(record)

    def close(self) -> None:
        # Force one final fsync before FileHandler closes the active segment.
        self._closing = True
        try:
            super().close()
        finally:
            self._closing = False
