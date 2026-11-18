"""Bounded PAPER best-bid/offer event journal for deterministic exit research.

The recorder stores accepted ORDER_BOOK states for selected symbols and open
PAPER positions. It is deliberately observational: it never feeds prices back
into the execution path.
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import uuid4

logger = logging.getLogger("Piluitus.BBOReplay")

BBO_REPLAY_STORAGE_KEY = "piluitus:paper:bbo_replay"
BBO_REPLAY_STORAGE_LIMIT = 100_000


def _positive_env_int(name: str, default: int, minimum: int = 1) -> int:
    try:
        return max(minimum, int(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


def _finite(value: object, default: float = 0.0) -> float:
    if isinstance(value, bool):
        return default
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return result if math.isfinite(result) else default


def _timestamp_epoch(value: object) -> float | None:
    """Parse Finam timestamps without confusing seconds, ms, us or ns."""
    try:
        if isinstance(value, datetime):
            parsed = value
        elif isinstance(value, Mapping):
            seconds = value.get("seconds")
            if seconds is None:
                return None
            epoch = _finite(seconds, math.nan)
            nanos = _finite(value.get("nanos", 0), 0.0)
            if not math.isfinite(epoch) or not math.isfinite(nanos):
                return None
            epoch += nanos / 1_000_000_000.0
            parsed = datetime.fromtimestamp(epoch, tz=timezone.utc)
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            epoch = float(value)
            if not math.isfinite(epoch):
                return None
            if epoch >= 1e18:
                epoch /= 1e9
            elif epoch >= 1e15:
                epoch /= 1e6
            elif epoch >= 1e12:
                epoch /= 1e3
            return epoch
        elif isinstance(value, str) and value.strip():
            raw = value.strip()
            try:
                epoch = float(raw)
            except ValueError:
                parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            else:
                if not math.isfinite(epoch):
                    return None
                if epoch >= 1e18:
                    epoch /= 1e9
                elif epoch >= 1e15:
                    epoch /= 1e6
                elif epoch >= 1e12:
                    epoch /= 1e3
                return epoch
        else:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(timezone.utc).timestamp()
    except (TypeError, ValueError, OverflowError, OSError):
        return None


class BBOReplayRecorder:
    """Non-blocking bounded queue + batched durable journal for PAPER BBO events."""

    def __init__(
        self,
        *,
        queue_limit: int | None = None,
        batch_size: int | None = None,
        flush_interval_sec: float | None = None,
        heartbeat_interval_sec: float | None = None,
        storage_limit: int = BBO_REPLAY_STORAGE_LIMIT,
    ) -> None:
        self.queue_limit = max(100, int(queue_limit or _positive_env_int(
            "PILUITUS_BBO_REPLAY_QUEUE_LIMIT", 8192
        )))
        self.batch_size = max(1, int(batch_size or _positive_env_int(
            "PILUITUS_BBO_REPLAY_BATCH_SIZE", 250
        )))
        self.flush_interval_sec = max(
            0.05,
            float(flush_interval_sec if flush_interval_sec is not None else
                  _finite(os.getenv("PILUITUS_BBO_REPLAY_FLUSH_SEC", "1.0"), 1.0)),
        )
        self.heartbeat_interval_sec = max(
            0.1,
            float(heartbeat_interval_sec if heartbeat_interval_sec is not None else
                  _finite(os.getenv("PILUITUS_BBO_REPLAY_HEARTBEAT_SEC", "1.0"), 1.0)),
        )
        self.storage_limit = max(1, int(storage_limit))
        self.session_id = uuid4().hex
        self._sequence = 0
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=self.queue_limit)
        self._last_state: dict[str, tuple[float, float, float, float, bool, float]] = {}
        self._task: asyncio.Task | None = None
        self._stopping = False
        self.accepted = 0
        self.persisted = 0
        self.dropped_queue_full = 0
        self.persistence_errors = 0
        self.skipped_unchanged = 0

    def record(
        self,
        ticker: str,
        book_result: Mapping[str, Any],
        event: Mapping[str, Any] | None = None,
    ) -> bool:
        """Queue a normalized accepted book event; never awaits storage/network I/O."""
        symbol = str(ticker or "").strip().upper().split("@")[0]
        if not symbol or not isinstance(book_result, Mapping):
            return False
        if not bool(book_result.get("updated")):
            return False

        valid = bool(book_result.get("valid", False))
        bid = _finite(book_result.get("bid"))
        ask = _finite(book_result.get("ask"))
        bid_size = _finite(book_result.get("bid_size"))
        ask_size = _finite(book_result.get("ask_size"))
        if valid and (bid <= 0.0 or ask <= 0.0 or ask < bid):
            return False

        now_mono = time.monotonic()
        prior = self._last_state.get(symbol)
        state = (bid, ask, bid_size, ask_size, valid)
        changed = prior is None or state != prior[:5]
        heartbeat_due = prior is None or now_mono - prior[5] >= self.heartbeat_interval_sec
        if not changed and not heartbeat_due:
            self.skipped_unchanged += 1
            return False

        source_event = event if isinstance(event, Mapping) else {}
        raw_timestamp = (
            source_event.get("order_book_timestamp")
            or source_event.get("timestamp")
        )
        received_epoch = datetime.now(timezone.utc).timestamp()
        event_epoch = _timestamp_epoch(raw_timestamp)
        timestamp_epoch = event_epoch if event_epoch is not None else received_epoch
        received_monotonic = _finite(
            source_event.get("received_monotonic"), now_mono
        )
        reason = str(book_result.get("reason") or source_event.get("book_reason") or "")
        event_kind = (
            "INVALIDATION" if not valid
            else "STATE_CHANGE" if changed
            else "HEARTBEAT"
        )

        self._sequence += 1
        row = {
            "schema_version": 1,
            "session_id": self.session_id,
            "sequence": self._sequence,
            "ticker": symbol,
            "source": "FINAM_ORDER_BOOK",
            "event_kind": event_kind,
            "event_time_epoch": round(timestamp_epoch, 6),
            "event_time_source": "EXCHANGE" if event_epoch is not None else "RECEIVE_FALLBACK",
            "received_at_utc": datetime.fromtimestamp(
                received_epoch, tz=timezone.utc
            ).isoformat(timespec="milliseconds"),
            "received_monotonic": round(received_monotonic, 6),
            "bid": bid,
            "ask": ask,
            "bid_size": bid_size,
            "ask_size": ask_size,
            "book_valid": valid,
            "book_reason": reason[:120],
        }
        try:
            self._queue.put_nowait(row)
        except asyncio.QueueFull:
            self.dropped_queue_full += 1
            return False
        # Advance the deduplication watermark only after enqueue succeeds. If the
        # queue is full, the next repeated state can still be retried.
        self._last_state[symbol] = (*state, now_mono)
        self.accepted += 1
        return True

    def start(self, db_manager: Any) -> None:
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(
            self._writer(db_manager), name="paper-bbo-replay-writer"
        )

    async def close(self) -> None:
        """Drain queued observations before application shutdown."""
        self._stopping = True
        task = self._task
        if task is not None:
            await task
        self._task = None

    async def _persist_batch(self, db_manager: Any, batch: list[dict[str, Any]]) -> None:
        if not batch:
            return
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                await db_manager.push_many_to_list(
                    BBO_REPLAY_STORAGE_KEY, batch, max_len=self.storage_limit
                )
                last_error = None
                break
            except Exception as exc:
                last_error = exc
                if attempt < 2:
                    await asyncio.sleep(0.1 * (attempt + 1))
        if last_error is not None:
            self.persistence_errors += len(batch)
            logger.error(
                "[PAPER BBO REPLAY] persist failed after retries batch=%d errors=%d: %s",
                len(batch), self.persistence_errors, last_error, exc_info=True,
            )
            return
        self.persisted += len(batch)
        logger.debug(
            "[PAPER BBO REPLAY] persisted=%d batch=%d queue=%d dropped=%d",
            self.persisted, len(batch), self._queue.qsize(), self.dropped_queue_full,
        )

    async def _writer(self, db_manager: Any) -> None:
        while not self._stopping or not self._queue.empty():
            try:
                first = await asyncio.wait_for(
                    self._queue.get(), timeout=self.flush_interval_sec
                )
            except asyncio.TimeoutError:
                continue
            batch = [first]
            while len(batch) < self.batch_size:
                try:
                    batch.append(self._queue.get_nowait())
                except asyncio.QueueEmpty:
                    break
            await self._persist_batch(db_manager, batch)
            for _ in batch:
                self._queue.task_done()

    def status(self) -> dict[str, int | str]:
        return {
            "session_id": self.session_id,
            "accepted": self.accepted,
            "persisted": self.persisted,
            "queued": self._queue.qsize(),
            "dropped_queue_full": self.dropped_queue_full,
            "persistence_errors": self.persistence_errors,
            "skipped_unchanged": self.skipped_unchanged,
        }
