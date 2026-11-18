"""Bounded PAPER market-event journal for deterministic exit research.

The recorder stores accepted ORDER_BOOK/QUOTES states, production exit-trigger
events and the first fresh executable BBO after a pending trigger. It is
observational and never feeds prices back into the execution path.
"""
from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Mapping
from uuid import uuid4

logger = logging.getLogger("Piluitus.BBOReplay")

BBO_REPLAY_STORAGE_KEY = "piluitus:paper:bbo_replay"
BBO_REPLAY_STORAGE_LIMIT = 250_000


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
        storage_limit: int | None = None,
    ) -> None:
        self.queue_limit = min(100_000, max(100, int(queue_limit or _positive_env_int(
            "PILUITUS_BBO_REPLAY_QUEUE_LIMIT", 8192
        ))))
        self.batch_size = min(5_000, max(1, int(batch_size or _positive_env_int(
            "PILUITUS_BBO_REPLAY_BATCH_SIZE", 250
        ))))
        self.flush_interval_sec = min(60.0, max(
            0.05,
            float(flush_interval_sec if flush_interval_sec is not None else
                  _finite(os.getenv("PILUITUS_BBO_REPLAY_FLUSH_SEC", "1.0"), 1.0)),
        ))
        self.heartbeat_interval_sec = min(60.0, max(
            0.1,
            float(heartbeat_interval_sec if heartbeat_interval_sec is not None else
                  _finite(os.getenv("PILUITUS_BBO_REPLAY_HEARTBEAT_SEC", "1.0"), 1.0)),
        ))
        self.storage_limit = min(1_000_000, max(1, int(
            storage_limit if storage_limit is not None else _positive_env_int(
                "PILUITUS_BBO_REPLAY_STORAGE_LIMIT", BBO_REPLAY_STORAGE_LIMIT
            )
        )))
        self.session_id = uuid4().hex
        self._sequence = 0
        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=self.queue_limit)
        self._last_state: dict[str, tuple[float, float, float, float, bool, str, float]] = {}
        self._pending_exit_tickers: dict[str, str] = {}
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
        if self._stopping:
            return False
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
        if valid and (
            (bid <= 0.0 and ask <= 0.0)
            or (bid > 0.0 and ask > 0.0 and ask < bid)
        ):
            return False

        now_mono = time.monotonic()
        source_event = event if isinstance(event, Mapping) else {}
        source_name = str(source_event.get("source") or "FINAM_ORDER_BOOK").upper()
        received_monotonic = _finite(
            source_event.get("received_monotonic"), now_mono
        )
        bid_confirmed = _finite(
            source_event.get("bid_confirmed_monotonic"), received_monotonic
        )
        ask_confirmed = _finite(
            source_event.get("ask_confirmed_monotonic"), received_monotonic
        )
        bid_age = now_mono - bid_confirmed if bid_confirmed > 0.0 else None
        ask_age = now_mono - ask_confirmed if ask_confirmed > 0.0 else None
        max_age = _finite(source_event.get("max_age_sec"), 2.0)
        if not math.isfinite(max_age) or max_age <= 0.0:
            max_age = 2.0
        max_age = min(5.0, max(0.25, max_age))
        pending_side = self._pending_exit_tickers.get(symbol)
        pending_age = bid_age if pending_side == "BUY" else ask_age
        pending_price = bid if pending_side == "BUY" else ask
        pending_fresh = (
            bool(pending_side) and valid and pending_price > 0.0
            and pending_age is not None and math.isfinite(pending_age)
            and 0.0 <= pending_age <= max_age
        )
        prior = self._last_state.get(symbol)
        state = (bid, ask, bid_size, ask_size, valid, source_name)
        price_state_changed = prior is None or (
            bid != prior[0] or ask != prior[1]
            or valid != prior[4] or source_name != prior[5]
        )
        heartbeat_due = prior is None or now_mono - prior[6] >= self.heartbeat_interval_sec
        if not price_state_changed and not heartbeat_due and not pending_fresh:
            # Keep the latest sizes for the next price transition/heartbeat,
            # but do not persist every queue-size-only update.
            self._last_state[symbol] = (*state, prior[6])
            self.skipped_unchanged += 1
            return False

        raw_timestamp = (
            source_event.get("order_book_timestamp")
            or source_event.get("timestamp")
        )
        received_epoch = datetime.now(timezone.utc).timestamp()
        event_epoch = _timestamp_epoch(raw_timestamp)
        # Match the screener's timestamp trust policy: provider times more than
        # two seconds ahead of local UTC are treated as receive-time fallbacks.
        if event_epoch is not None and event_epoch > received_epoch + 2.0:
            event_epoch = None
        timestamp_epoch = event_epoch if event_epoch is not None else received_epoch
        reason = str(book_result.get("reason") or source_event.get("book_reason") or "")
        event_kind = (
            "INVALIDATION" if not valid
            else "STATE_CHANGE" if price_state_changed
            else "HEARTBEAT"
        )

        self._sequence += 1
        row = {
            "schema_version": 1,
            "session_id": self.session_id,
            "sequence": self._sequence,
            "ticker": symbol,
            "source": source_name,
            "event_kind": event_kind,
            "event_time_epoch": round(timestamp_epoch, 6),
            "event_time_source": "EXCHANGE" if event_epoch is not None else "RECEIVE_FALLBACK",
            "received_at_utc": datetime.fromtimestamp(
                received_epoch, tz=timezone.utc
            ).isoformat(timespec="milliseconds"),
            "received_monotonic": round(received_monotonic, 6),
            "bid_confirmed_monotonic": round(bid_confirmed, 6) if bid_confirmed > 0.0 else None,
            "ask_confirmed_monotonic": round(ask_confirmed, 6) if ask_confirmed > 0.0 else None,
            "bid_age_sec_at_capture": round(bid_age, 6) if bid_age is not None else None,
            "ask_age_sec_at_capture": round(ask_age, 6) if ask_age is not None else None,
            "bid": bid,
            "ask": ask,
            "bid_size": bid_size,
            "ask_size": ask_size,
            "bbo_valid": valid,
            "book_valid": bool(book_result.get("order_book_valid", valid)),
            "book_reason": reason[:120],
        }
        try:
            self._queue.put_nowait(row)
        except asyncio.QueueFull:
            self.dropped_queue_full += 1
            if self.dropped_queue_full == 1 or self.dropped_queue_full % 100 == 0:
                logger.warning(
                    "[PAPER BBO REPLAY] queue full; dropped=%d limit=%d",
                    self.dropped_queue_full, self.queue_limit,
                )
            return False
        # Advance the deduplication watermark only after enqueue succeeds. If the
        # queue is full, the next repeated state can still be retried.
        self._last_state[symbol] = (*state, now_mono)
        if pending_fresh:
            self._pending_exit_tickers.pop(symbol, None)
        self.accepted += 1
        return True

    def record_exit_trigger(
        self,
        ticker: str,
        trade: Mapping[str, Any],
        executable_quote: Mapping[str, Any] | None = None,
    ) -> bool:
        """Persist the production monitor's first latched SL/TP trigger."""
        if self._stopping or not isinstance(trade, Mapping):
            return False
        symbol = str(ticker or trade.get("ticker") or "").strip().upper().split("@")[0]
        reason = str(trade.get("pending_exit_reason") or "").strip().upper()
        if not symbol or reason not in {"STOP_LOSS", "TAKE_PROFIT"}:
            return False
        now_mono = time.monotonic()
        received_epoch = datetime.now(timezone.utc).timestamp()
        market_epoch = _timestamp_epoch(trade.get("exit_trigger_market_timestamp"))
        if market_epoch is not None and market_epoch > received_epoch + 2.0:
            market_epoch = None
        quote = executable_quote if isinstance(executable_quote, Mapping) else {}
        quote_price = _finite(quote.get("executable_price"), math.nan)
        quote_age = _finite(quote.get("age_sec"), math.inf)
        quote_max_age = _finite(quote.get("max_age_sec"), 0.0)
        quote_fresh = (
            math.isfinite(quote_price) and quote_price > 0.0
            and math.isfinite(quote_age) and 0.0 <= quote_age <= quote_max_age
            and quote_max_age > 0.0
        )
        self._sequence += 1
        row = {
            "schema_version": 1,
            "session_id": self.session_id,
            "sequence": self._sequence,
            "ticker": symbol,
            "source": "PAPER_MONITOR",
            "event_kind": "EXIT_TRIGGER",
            # Sequence/event time is the local trigger-observation time.
            # Preserve the original market timestamp separately for audit.
            "event_time_epoch": round(received_epoch, 6),
            "event_time_source": "RECEIVE_FALLBACK",
            "trigger_market_time_epoch": (
                round(market_epoch, 6) if market_epoch is not None else None
            ),
            "trigger_market_time_source": (
                "EXCHANGE" if market_epoch is not None else "UNAVAILABLE"
            ),
            "received_at_utc": datetime.fromtimestamp(
                received_epoch, tz=timezone.utc
            ).isoformat(timespec="milliseconds"),
            "received_monotonic": round(now_mono, 6),
            "trade_id": str(trade.get("id") or "")[:96],
            "position_side": str(trade.get("side") or "").upper(),
            "trigger_reason": reason,
            "trigger_source": str(trade.get("exit_trigger_source") or "UNKNOWN").upper(),
            "trigger_price": _finite(trade.get("pending_exit_trigger_price")),
            "observed_trigger_price": _finite(trade.get("exit_trigger_observed_price")),
            "trigger_bbo_valid": bool(quote_fresh),
            "trigger_bbo_price": round(quote_price, 8) if quote_fresh else None,
            "trigger_bbo_age_sec": round(quote_age, 6) if math.isfinite(quote_age) else None,
            "trigger_bbo_max_age_sec": quote_max_age if math.isfinite(quote_max_age) else None,
        }
        try:
            self._queue.put_nowait(row)
        except asyncio.QueueFull:
            self.dropped_queue_full += 1
            if self.dropped_queue_full == 1 or self.dropped_queue_full % 100 == 0:
                logger.warning(
                    "[PAPER BBO REPLAY] queue full; dropped=%d limit=%d",
                    self.dropped_queue_full, self.queue_limit,
                )
            return False
        if not quote_fresh:
            self._pending_exit_tickers[symbol] = str(trade.get("side") or "").upper()
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
                len(batch), self.persistence_errors, last_error,
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
