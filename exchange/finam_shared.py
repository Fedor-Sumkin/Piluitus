import asyncio
import logging
import math
from datetime import datetime, timezone, timedelta
from urllib.parse import quote as url_quote
from collections.abc import Callable, Iterable
from typing import Any
import json
import inspect
import os
import re
import time
import websockets
import httpx

logger = logging.getLogger("Piluitus.FinamGateway")
JOURNAL_FILE = "trading_journal.json"
_MOSCOW_TZ = timezone(timedelta(hours=3), name="MSK")


def _websocket_close_code(exc: BaseException) -> int | None:
    """Read a WebSocket close code across websockets versions."""
    candidates = (
        getattr(exc, "code", None),
        getattr(getattr(exc, "rcvd", None), "code", None),
        getattr(getattr(exc, "sent", None), "code", None),
    )
    for value in candidates:
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    return None


def _websocket_disconnect_log_level(exc: BaseException) -> int:
    """A normal 1000/1001 close is informational; abnormal closes remain errors."""
    return (
        logging.INFO
        if _websocket_close_code(exc) in {1000, 1001}
        else logging.ERROR
    )


def _coalesce_queued_market_event(queue: asyncio.Queue, event: dict[str, Any]) -> bool:
    """Replace an older queued snapshot for the same symbol without changing queue accounting.

    ORDER_BOOK events contain the gateway's fully reconstructed depth snapshot and
    QUOTES are latest-value state. Replacing an older same-symbol snapshot is safer
    than allowing the queue to accumulate stale market state. Trade prints are
    deliberately never coalesced.
    """
    event_type = str(event.get("event_type") or "QUOTES").strip().upper()
    if event_type not in {"ORDER_BOOK", "QUOTES"}:
        return False
    ticker = str(event.get("ticker") or event.get("security_code") or "").strip().upper().split("@")[0]
    if not ticker:
        return False
    pending = getattr(queue, "_queue", None)
    if pending is None:
        return False
    for index in range(len(pending) - 1, -1, -1):
        queued = pending[index]
        if not isinstance(queued, dict):
            continue
        queued_type = str(queued.get("event_type") or "QUOTES").strip().upper()
        queued_ticker = str(queued.get("ticker") or queued.get("security_code") or "").strip().upper().split("@")[0]
        if queued_type == event_type and queued_ticker == ticker:
            pending[index] = event
            return True
    return False


def _event_timestamp_to_monotonic(value: object, receive_monotonic: float) -> float:
    """Map provider event time to the local monotonic clock without aging it."""
    event_dt: datetime | None = None
    try:
        if isinstance(value, datetime):
            event_dt = value
        elif isinstance(value, dict):
            seconds_raw = value.get("seconds")
            nanos_raw = value.get("nanos", 0)
            if seconds_raw is not None:
                seconds = float(seconds_raw)
                nanos = float(nanos_raw or 0.0)
                if math.isfinite(seconds) and math.isfinite(nanos):
                    event_dt = datetime.fromtimestamp(
                        seconds + nanos / 1_000_000_000.0, tz=timezone.utc
                    )
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            epoch = float(value)
            if not math.isfinite(epoch):
                return receive_monotonic
            if epoch >= 1e18:
                epoch /= 1_000_000_000.0
            elif epoch >= 1e15:
                epoch /= 1_000_000.0
            elif epoch >= 1e12:
                epoch /= 1_000.0
            event_dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        elif isinstance(value, str) and value.strip():
            text = value.strip()
            try:
                epoch = float(text)
            except ValueError:
                event_dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
            else:
                if not math.isfinite(epoch):
                    return receive_monotonic
                if epoch >= 1e18:
                    epoch /= 1_000_000_000.0
                elif epoch >= 1e15:
                    epoch /= 1_000_000.0
                elif epoch >= 1e12:
                    epoch /= 1_000.0
                event_dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
        if event_dt is None:
            return receive_monotonic
        if event_dt.tzinfo is None:
            event_dt = event_dt.replace(tzinfo=timezone.utc)
        age_seconds = (datetime.now(timezone.utc) - event_dt.astimezone(timezone.utc)).total_seconds()
        if age_seconds < -2.0:
            return receive_monotonic
        return receive_monotonic - max(0.0, age_seconds)
    except (TypeError, ValueError, OverflowError, OSError):
        return receive_monotonic


def _should_resync_position_order_book(
    bid_age_sec: object,
    ask_age_sec: object,
    book_event_age_sec: object,
    *,
    stale_after_sec: float,
    stream_idle_sec: float,
) -> bool:
    """Resubscribe only when BBO is old and the ORDER_BOOK event stream is idle.

    A resting best level may remain unchanged while valid deeper-level deltas
    continue to maintain the reconstructed book. Its level-change timestamp
    alone must not cause repeated subscription resets.
    """
    try:
        bid_age = float(bid_age_sec)
        ask_age = float(ask_age_sec)
        event_age = float(book_event_age_sec)
        stale_after = max(0.0, float(stale_after_sec))
        stream_idle = max(0.0, float(stream_idle_sec))
    except (TypeError, ValueError, OverflowError):
        return True
    if not math.isfinite(stale_after) or not math.isfinite(stream_idle):
        return True
    if max(bid_age, ask_age) < stale_after:
        return False
    # A recent, in-order provider event keeps the delta-chain alive even when
    # the best price level itself has not changed. Execution freshness guards
    # remain independent and continue to require a confirmed executable BBO.
    return not (
        math.isfinite(event_age)
        and 0.0 <= event_age <= stream_idle
    )
