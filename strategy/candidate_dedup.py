"""Side-aware, bounded deduplication for repeated market candidates."""
from __future__ import annotations

import threading
import time


class CandidateDeduplicator:
    def __init__(self, cooldown_sec: float = 8.0, retention_sec: float = 600.0) -> None:
        self.cooldown_sec = max(0.5, float(cooldown_sec))
        self.retention_sec = max(self.cooldown_sec, float(retention_sec))
        self._pending: set[str] = set()
        self._last: dict[tuple[str, str, str], float] = {}
        self._lock = threading.RLock()

    def reserve(
        self,
        ticker: str,
        direction: str = "NONE",
        pattern: str = "FLOW",
        *,
        now: float | None = None,
    ) -> bool:
        symbol = str(ticker or "").strip().upper().split("@")[0]
        side = str(direction or "NONE").strip().upper()
        signal = str(pattern or "FLOW").strip().upper()
        if not symbol or side not in {"BUY", "SELL", "NONE"}:
            return False
        stamp = time.monotonic() if now is None else float(now)
        key = (symbol, side, signal[:48])
        with self._lock:
            if symbol in self._pending:
                return False
            last = self._last.get(key)
            if last is not None and stamp - last < self.cooldown_sec:
                return False
            self._pending.add(symbol)
            self._last[key] = stamp
            cutoff = stamp - self.retention_sec
            if len(self._last) > 1000:
                self._last = {
                    old_key: old_stamp for old_key, old_stamp in self._last.items()
                    if old_stamp >= cutoff
                }
            return True

    def release(self, ticker: str) -> None:
        symbol = str(ticker or "").strip().upper().split("@")[0]
        if not symbol:
            return
        with self._lock:
            self._pending.discard(symbol)

    def clear(self) -> None:
        with self._lock:
            self._pending.clear()
            self._last.clear()
