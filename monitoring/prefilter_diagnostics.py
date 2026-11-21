"""Rate-limited logging for repeated pre-LLM candidate filter blocks."""
from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable

from monitoring.trade_flow_diagnostics import guard_reason_bucket


class PrefilterBlockLogger:
    """Suppress repeated warnings for the same ticker and reason bucket."""

    def __init__(
        self,
        logger: logging.Logger,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._logger = logger
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._last_logged: dict[tuple[str, str], float] = {}

    def log(
        self, ticker: str, reason: str, *, interval_sec: float = 15.0
    ) -> bool:
        """Log a warning if its ticker/category key is outside the cooldown."""
        bucket = guard_reason_bucket(reason)
        key = (str(ticker).upper(), bucket)
        now = self._monotonic()
        with self._lock:
            last = self._last_logged.get(key, 0.0)
            if now - last < interval_sec:
                return False
            self._last_logged[key] = now
        self._logger.warning(
            "⏭️ [PRE-LLM FILTER] %s пропущен до запроса LLM (%s): %s",
            ticker, bucket, reason,
        )
        return True

    def clear(self) -> None:
        """Reset rate-limit history, for isolated sessions and tests."""
        with self._lock:
            self._last_logged.clear()
