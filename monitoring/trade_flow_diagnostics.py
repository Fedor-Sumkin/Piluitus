"""Pure reason bucketing for trade-flow diagnostics.

These helpers only normalize diagnostic labels. They do not approve, reject,
size, schedule, or execute trading candidates.
"""
from __future__ import annotations

import threading


def guard_reason_bucket(reason: object) -> str:
    """Map a pre-LLM guard explanation to a bounded diagnostic category."""
    lowered = str(reason or "").lower()
    if (
        "bid/ask" in lowered
        or "bbo" in lowered
        or "котировк" in lowered
        or "актуальн" in lowered
    ):
        return "quotes"
    if "спред" in lowered:
        return "spread"
    if "atr" in lowered or "m1-диапазон" in lowered or "m1-истори" in lowered:
        return "atr"
    if "m1" in lowered or "минутн" in lowered or "закрытыми свечами" in lowered:
        return "m1"
    if "размер лучшей заявки" in lowered or "сокращение" in lowered or "истории размера" in lowered:
        return "size"
    if "всплеск" in lowered or "объём" in lowered or "объем" in lowered:
        return "volume"
    if "импульс" in lowered or "vwap" in lowered or "цена не" in lowered:
        return "direction"
    return "other"


def bbo_resume_reason_bucket(reason: object) -> str:
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


class TradeFlowCounters:
    """Thread-safe rolling-window counter storage for TRADE FLOW telemetry.

    Snapshotting atomically drains the current window. This class deliberately
    knows nothing about logging, trading decisions, or report formatting.
    """

    def __init__(self, started_at: float) -> None:
        self._lock = threading.Lock()
        self._counts: dict[str, int] = {}
        self._last_snapshot_at = float(started_at)

    def increment(self, key: str, amount: int = 1) -> None:
        normalized_key = str(key)
        increment = int(amount)
        with self._lock:
            self._counts[normalized_key] = self._counts.get(normalized_key, 0) + increment

    def snapshot_if_due(
        self, *, now: float, interval_sec: float = 60.0, force: bool = False
    ) -> dict[str, int] | None:
        timestamp = float(now)
        interval = max(0.0, float(interval_sec))
        with self._lock:
            if not force and timestamp - self._last_snapshot_at < interval:
                return None
            snapshot = dict(self._counts)
            self._counts.clear()
            self._last_snapshot_at = timestamp
            return snapshot

    def reset(self, now: float) -> None:
        with self._lock:
            self._counts.clear()
            self._last_snapshot_at = float(now)
