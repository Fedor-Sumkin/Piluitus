"""Screener funnel OrderBook methods."""
from __future__ import annotations

import math
import time
from collections import deque
from typing import Any

import numpy as np

from strategy.screener_funnel_primitives import (
    _best_quote_ofi_delta,
    logger,
)



class ScreenerFunnelOrderBookMixin:
    """Composable behavior over the single HFTMemoryScreener state owner."""

    @staticmethod
    def _top_size_drop_pct(history: deque, current_size: float, now: float) -> tuple[float, int]:
        """Снижение объёма лучшей котировки относительно медианы прошлых наблюдений.

        Это только best-bid/best-ask proxy, а не доказательство отмены лимитных
        ордеров или исчезновения плотности во всём стакане.
        """
        prior = [
            float(size) for ts, size in list(history)[-5:]
            if now - float(ts) <= 60.0 and float(size) > 0.0
        ]
        if current_size <= 0.0 or len(prior) < 3:
            return 0.0, len(prior)
        baseline = float(np.median(prior))
        if baseline <= 0.0:
            return 0.0, len(prior)
        drop_pct = max(0.0, (baseline - current_size) / baseline * 100.0)
        return min(100.0, drop_pct), len(prior)

    def _observe_top_size(self, quote: dict, side: str, size: float, now: float) -> None:
        """Record one real observation for the visible best Bid/Ask size."""
        history = quote[f"{side}_size_history"]
        drop_key = f"{side}_size_drop_pct"
        drop_at_key = f"{side}_size_drop_at"
        seen_key = f"last_{side}_size_seen"
        samples_key = "top_bid_history_samples" if side == "bid" else "top_ask_history_samples"
        size_key = f"{side}_size"
        size = max(0.0, float(size))
        old_drop = float(quote.get(drop_key, 0.0) or 0.0)
        old_drop_at = float(quote.get(drop_at_key, 0.0) or 0.0)
        fresh_old_drop = old_drop >= 50.0 and old_drop_at > 0.0 and now - old_drop_at <= self.size_signal_ttl_sec

        if size <= 0.0:
            prior_sizes = [
                float(value) for ts, value in history
                if now - float(ts) <= 60.0 and float(value) > 0.0
            ]
            quote[size_key] = 0.0
            quote[f"{side}_size_current"] = 0.0
            quote[f"{side}_size_baseline"] = float(np.median(prior_sizes)) if prior_sizes else 0.0
            quote[seen_key] = 0.0
            quote[drop_key] = 0.0
            quote[drop_at_key] = 0.0
            quote[samples_key] = len([1 for ts, value in history if now - float(ts) <= 60.0 and float(value) > 0.0])
            return

        drop, samples = self._top_size_drop_pct(history, size, now)
        prior_sizes = [
            float(value) for ts, value in list(history)[-5:]
            if now - float(ts) <= 60.0 and float(value) > 0.0
        ]
        quote[f"{side}_size_baseline"] = float(np.median(prior_sizes)) if prior_sizes else 0.0
        quote[f"{side}_size_current"] = size
        quote[size_key] = size
        quote[seen_key] = now
        quote[samples_key] = int(samples)
        if drop >= 50.0:
            quote[drop_key] = float(drop)
            quote[drop_at_key] = now
        elif fresh_old_drop:
            # Preserve a measured sudden drop for a short alignment window so the
            # next price/volume tick can validate the same event.
            quote[drop_key] = old_drop
            quote[drop_at_key] = old_drop_at
        else:
            quote[drop_key] = float(drop)
            quote[drop_at_key] = 0.0
        history.append((now, size))

    def _current_top_size(self, quote: dict, side: str, now: float) -> float:
        size = float(quote.get(f"{side}_size", 0.0) or 0.0)
        seen = float(quote.get(f"last_{side}_size_seen", 0.0) or 0.0)
        if size <= 0.0 or seen <= 0.0 or now - seen > self.bbo_max_age_sec:
            return 0.0
        return size

    def _current_size_drop(self, quote: dict, side: str, now: float) -> float:
        drop = float(quote.get(f"{side}_size_drop_pct", 0.0) or 0.0)
        observed_at = float(quote.get(f"{side}_size_drop_at", 0.0) or 0.0)
        if drop <= 0.0 or observed_at <= 0.0 or now - observed_at > self.size_signal_ttl_sec:
            return 0.0
        return drop

    def _sync_push_order_book(
        self,
        ticker: str,
        bid: float,
        ask: float,
        bid_size: float,
        ask_size: float,
        *,
        bid_present: bool = True,
        ask_present: bool = True,
        bid_size_present: bool = True,
        ask_size_present: bool = True,
        bid_timestamp: object | None = None,
        ask_timestamp: object | None = None,
        order_book_timestamp: object | None = None,
        received_monotonic: float | None = None,
        book_valid: bool = True,
        book_reason: str = "",
        bid_levels: list[list[float]] | None = None,
        ask_levels: list[list[float]] | None = None,
        bid_levels_present: bool = False,
        ask_levels_present: bool = False,
    ) -> dict[str, Any]:
        """Update BBO values and freshness independently for each observed side."""
        ticker = self.normalize_ticker(ticker)
        values = (bid, ask, bid_size, ask_size)
        if not ticker or not all(np.isfinite(float(value)) for value in values):
            return {"updated": False, "reason": "Некорректные значения ORDER_BOOK"}
        now = time.monotonic()
        received_at = now
        if received_monotonic is not None:
            try:
                supplied_received = float(received_monotonic)
                if math.isfinite(supplied_received) and 0.0 < supplied_received <= now + 0.05:
                    received_at = min(now, supplied_received)
            except (TypeError, ValueError, OverflowError):
                pass
        with self._lock:
            if self.universe and ticker not in self._universe_set:
                return {"updated": False, "reason": "Тикер вне вселенной"}
            self.register_ticker(ticker)
            quote = self.quotes[ticker]
            bid = float(bid)
            ask = float(ask)
            bid_seen_at = self._event_timestamp_to_monotonic(bid_timestamp, received_at) if bid_present else 0.0
            ask_seen_at = self._event_timestamp_to_monotonic(ask_timestamp, received_at) if ask_present else 0.0
            # ORDER_BOOK envelope time is the ordering/freshness clock for the
            # complete reconstructed state. Level timestamps describe when an
            # individual price level changed and must not age the whole book.
            book_seen_at = self._event_timestamp_to_monotonic(order_book_timestamp, received_at)
            confirmation_seen_at = min(received_at, book_seen_at)
            prior_book_event = float(quote.get("last_order_book_event_seen", 0.0) or 0.0)
            accept_book_event = prior_book_event <= 0.0 or book_seen_at + 0.001 >= prior_book_event
            if not accept_book_event:
                return {
                    "updated": False,
                    "reason": "Устаревшее событие ORDER_BOOK",
                    "out_of_order": True,
                    "bbo_side_updated": False,
                }

            quote["last_order_book_event_seen"] = max(prior_book_event, book_seen_at)
            quote["last_order_book_seen"] = book_seen_at
            quote["last_order_book_received"] = received_at
            old_bid = float(quote.get("bid", 0.0) or 0.0)
            old_ask = float(quote.get("ask", 0.0) or 0.0)
            old_bid_size = float(quote.get("bid_size", 0.0) or 0.0)
            old_ask_size = float(quote.get("ask_size", 0.0) or 0.0)

            prior_bid_event = float(quote.get("last_bid_book_event_seen", 0.0) or 0.0)
            prior_ask_event = float(quote.get("last_ask_book_event_seen", 0.0) or 0.0)
            # Every emitted parser event contains a complete reconstructed book.
            # Its envelope clock orders this stream against earlier ORDER_BOOK
            # events only. Cross-stream arbitration uses socket-ingress time;
            # each stream retains its own event-time watermark.
            bid_state_present = bool(bid_present or bid_levels_present or order_book_timestamp is not None)
            ask_state_present = bool(ask_present or ask_levels_present or order_book_timestamp is not None)
            # The envelope timestamp orders the complete ORDER_BOOK message.
            # Without it, fall back to the captured socket-ingress monotonic time.
            # Per-level timestamps are diagnostic timestamps of the last change
            # at that price; they must never be used as the message ordering clock.
            bid_ordering_time = book_seen_at
            ask_ordering_time = book_seen_at
            accept_bid_event = bool(
                bid_state_present and (prior_bid_event <= 0.0 or bid_ordering_time + 0.001 >= prior_bid_event)
            )
            accept_ask_event = bool(
                ask_state_present and (prior_ask_event <= 0.0 or ask_ordering_time + 0.001 >= prior_ask_event)
            )
            if accept_bid_event:
                quote["last_bid_book_event_seen"] = max(prior_bid_event, bid_ordering_time)
            if accept_ask_event:
                quote["last_ask_book_event_seen"] = max(prior_ask_event, ask_ordering_time)
            accept_bid_update = bool(
                accept_bid_event
                and received_at >= float(quote.get("last_bid_source_received_seen", 0.0) or 0.0)
            )
            accept_ask_update = bool(
                accept_ask_event
                and received_at >= float(quote.get("last_ask_source_received_seen", 0.0) or 0.0)
            )

            old_bid = float(quote.get("bid", 0.0) or 0.0)
            old_ask = float(quote.get("ask", 0.0) or 0.0)
            candidate_bid = (bid if bid > 0.0 else 0.0) if accept_bid_update else old_bid
            candidate_ask = (ask if ask > 0.0 else 0.0) if accept_ask_update else old_ask

            # Crossed or incomplete snapshots are explicit invalidations. They
            # revoke executable confirmations immediately; prices are retained
            # only when the message authoritatively supplied that side.
            if not book_valid:
                quote.setdefault("queue_imbalance_history", deque(maxlen=1024)).clear()
                quote.setdefault("ofi_history", deque(maxlen=4096)).clear()
                if bid_state_present and accept_bid_update:
                    quote["bid"] = candidate_bid
                    quote["last_bid_source_received_seen"] = received_at
                    quote["last_bid_event_seen"] = bid_ordering_time
                    if bid_present:
                        quote["last_bid_seen"] = max(
                            float(quote.get("last_bid_seen", 0.0) or 0.0),
                            bid_seen_at if bid_seen_at > 0.0 else book_seen_at,
                        )
                if ask_state_present and accept_ask_update:
                    quote["ask"] = candidate_ask
                    quote["last_ask_source_received_seen"] = received_at
                    quote["last_ask_event_seen"] = ask_ordering_time
                    if ask_present:
                        quote["last_ask_seen"] = max(
                            float(quote.get("last_ask_seen", 0.0) or 0.0),
                            ask_seen_at if ask_seen_at > 0.0 else book_seen_at,
                        )
                quote["last_bid_confirmed_seen"] = 0.0
                quote["last_ask_confirmed_seen"] = 0.0
                quote["last_bbo_seen"] = 0.0
                quote["order_book_valid"] = False
                quote["order_book_last_reason"] = str(
                    book_reason or "INVALID_ORDER_BOOK"
                )
                return {
                    "updated": True,
                    "valid": False,
                    "reason": quote["order_book_last_reason"],
                    "ticker": ticker,
                    "bid": quote["bid"],
                    "ask": quote["ask"],
                    "bbo_side_updated": bool(bid_present or ask_present),
                    "bbo_side_touch_count": int(bool(bid_present)) + int(bool(ask_present)),
                    "confirmed_sides": 0,
                    "bbo_price_changed": bool(old_bid != quote["bid"] or old_ask != quote["ask"]),
                }

            current_bid = candidate_bid
            current_ask = candidate_ask
            crossed = current_bid > 0.0 and current_ask > 0.0 and current_ask < current_bid
            two_sided = current_bid > 0.0 and current_ask > 0.0 and current_ask >= current_bid
            if not two_sided or crossed:
                quote.setdefault("queue_imbalance_history", deque(maxlen=1024)).clear()
                quote.setdefault("ofi_history", deque(maxlen=4096)).clear()
                quote["bid"] = current_bid
                quote["ask"] = current_ask
                if accept_bid_update:
                    quote["last_bid_source_received_seen"] = received_at
                    quote["last_bid_event_seen"] = bid_ordering_time
                if accept_ask_update:
                    quote["last_ask_source_received_seen"] = received_at
                    quote["last_ask_event_seen"] = ask_ordering_time
                quote["last_bid_confirmed_seen"] = 0.0
                quote["last_ask_confirmed_seen"] = 0.0
                quote["last_bbo_seen"] = 0.0
                quote["order_book_valid"] = False
                quote["order_book_last_reason"] = "CROSSED_BBO" if crossed else "MISSING_BBO_SIDE"
                return {
                    "updated": True,
                    "valid": False,
                    "reason": quote["order_book_last_reason"],
                    "ticker": ticker,
                    "bid": current_bid,
                    "ask": current_ask,
                    "bbo_side_updated": bool(bid_present or ask_present),
                    "bbo_side_touch_count": int(bool(bid_present)) + int(bool(ask_present)),
                    "confirmed_sides": 0,
                    "bbo_price_changed": bool(old_bid != current_bid or old_ask != current_ask),
                }

            quote["order_book_valid"] = True
            quote["order_book_last_reason"] = "OK"
            quote["bid"] = current_bid
            quote["ask"] = current_ask
            if accept_bid_update:
                # Advance the side's event watermark even for a heartbeat. This
                # prevents a delayed QUOTES packet from rolling the maintained
                # BBO backwards. `last_bid_seen` remains the last actual best-
                # level update, separate from the book-confirmation clock.
                quote["last_bid_source_received_seen"] = received_at
                quote["last_bid_event_seen"] = bid_ordering_time
                if bid_present:
                    quote["last_bid_seen"] = max(
                        float(quote.get("last_bid_seen", 0.0) or 0.0),
                        bid_seen_at if bid_seen_at > 0.0 else book_seen_at,
                    )
            if accept_ask_update:
                quote["last_ask_source_received_seen"] = received_at
                quote["last_ask_event_seen"] = ask_ordering_time
                if ask_present:
                    quote["last_ask_seen"] = max(
                        float(quote.get("last_ask_seen", 0.0) or 0.0),
                        ask_seen_at if ask_seen_at > 0.0 else book_seen_at,
                    )
            # A coherent, in-order delta confirms both sides of the maintained
            # book even if neither best level changed. Record this book's actual
            # event time; never borrow a newer QUOTES timestamp for an older book.
            if current_bid > 0.0 and accept_bid_update:
                # When the envelope clock is absent, `book_seen_at` is the
                # captured socket receive time. A level timestamp may be old
                # simply because that resting price has not changed recently.
                bid_confirmation_at = book_seen_at
                quote["last_bid_confirmed_seen"] = bid_confirmation_at
            elif current_bid <= 0.0:
                quote["last_bid_confirmed_seen"] = 0.0
            if current_ask > 0.0 and accept_ask_update:
                ask_confirmation_at = book_seen_at
                quote["last_ask_confirmed_seen"] = ask_confirmation_at
            elif current_ask <= 0.0:
                quote["last_ask_confirmed_seen"] = 0.0
            bid_confirmed = float(quote.get("last_bid_confirmed_seen", 0.0) or 0.0)
            ask_confirmed = float(quote.get("last_ask_confirmed_seen", 0.0) or 0.0)
            quote["last_bbo_seen"] = (
                min(bid_confirmed, ask_confirmed)
                if bid_confirmed > 0.0 and ask_confirmed > 0.0 else 0.0
            )
            if bid_size_present and accept_bid_update:
                quote["last_bid_size_event_seen"] = max(
                    float(quote.get("last_bid_size_event_seen", 0.0) or 0.0),
                    bid_seen_at if bid_seen_at > 0.0 else book_seen_at,
                )
                self._observe_top_size(quote, "bid", float(bid_size), confirmation_seen_at)
            if ask_size_present and accept_ask_update:
                quote["last_ask_size_event_seen"] = max(
                    float(quote.get("last_ask_size_event_seen", 0.0) or 0.0),
                    ask_seen_at if ask_seen_at > 0.0 else book_seen_at,
                )
                self._observe_top_size(quote, "ask", float(ask_size), confirmation_seen_at)
            # The parser emits the complete depth on each heartbeat. Refresh the
            # size timestamp only for sides whose source event is not older than
            # the current side watermark; never roll size/depth backwards from a
            # delayed ORDER_BOOK frame. Heartbeats refresh availability without
            # manufacturing a new size-history sample.
            if accept_bid_update:
                quote["bid_size"] = max(0.0, float(bid_size))
                quote["last_bid_size_seen"] = confirmation_seen_at if bid_size > 0.0 else 0.0
                if bid_levels_present and bid_levels is not None:
                    quote["bid_levels"] = bid_levels
            if accept_ask_update:
                quote["ask_size"] = max(0.0, float(ask_size))
                quote["last_ask_size_seen"] = confirmation_seen_at if ask_size > 0.0 else 0.0
                if ask_levels_present and ask_levels is not None:
                    quote["ask_levels"] = ask_levels

            bid_queue_size = float(self._current_top_size(quote, "bid", now))
            ask_queue_size = float(self._current_top_size(quote, "ask", now))
            queue_history = quote.setdefault("queue_imbalance_history", deque(maxlen=1024))
            if bid_queue_size > 0.0 and ask_queue_size > 0.0:
                queue_total = bid_queue_size + ask_queue_size
                queue_value = (bid_queue_size - ask_queue_size) / queue_total
                if not queue_history or book_seen_at > float(queue_history[-1][0]):
                    queue_history.append((book_seen_at, queue_value))
            else:
                queue_history.clear()

            # OFI is computed only from two consecutive accepted, coherent
            # ORDER_BOOK states. Heartbeats with unchanged BBO/depth are ignored.
            ofi_history = quote.setdefault("ofi_history", deque(maxlen=4096))
            new_bid_size = float(self._current_top_size(quote, "bid", now))
            new_ask_size = float(self._current_top_size(quote, "ask", now))
            if new_bid_size <= 0.0 or new_ask_size <= 0.0:
                ofi_history.clear()
            elif accept_bid_update and accept_ask_update and (
                old_bid > 0.0 and old_ask > 0.0
                and (
                    old_bid != current_bid or old_ask != current_ask
                    or old_bid_size != new_bid_size or old_ask_size != new_ask_size
                )
            ):
                delta = _best_quote_ofi_delta(
                    old_bid, old_bid_size, old_ask, old_ask_size,
                    current_bid, new_bid_size, current_ask, new_ask_size,
                )
                if delta is not None and (
                    not ofi_history or book_seen_at > float(ofi_history[-1][0])
                ):
                    average_depth = (
                        old_bid_size + old_ask_size + new_bid_size + new_ask_size
                    ) / 4.0
                    ofi_history.append((book_seen_at, delta, average_depth))
            return {
                "updated": True,
                "valid": True,
                "book_event_accepted": True,
                "bid_side_updated": bool(accept_bid_update),
                "ask_side_updated": bool(accept_ask_update),
                "bbo_side_updated": bool(bid_present or ask_present),
                "bbo_side_touch_count": int(bool(bid_present)) + int(bool(ask_present)),
                "confirmed_sides": int(bool(accept_bid_update)) + int(bool(accept_ask_update)),
                "bbo_price_changed": bool(old_bid != current_bid or old_ask != current_ask),
                "heartbeat_only": not bool(bid_present or ask_present),
                "ticker": ticker,
                "bid": quote["bid"],
                "ask": quote["ask"],
                "bid_size": self._current_top_size(quote, "bid", now),
                "ask_size": self._current_top_size(quote, "ask", now),
                "bid_size_drop_pct": self._current_size_drop(quote, "bid", now),
                "ask_size_drop_pct": self._current_size_drop(quote, "ask", now),
            }

    async def push_order_book(
        self,
        ticker: str,
        bid: float,
        ask: float,
        bid_size: float,
        ask_size: float,
        *,
        bid_present: bool = True,
        ask_present: bool = True,
        bid_size_present: bool = True,
        ask_size_present: bool = True,
        bid_timestamp: object | None = None,
        ask_timestamp: object | None = None,
        order_book_timestamp: object | None = None,
        received_monotonic: float | None = None,
        book_valid: bool = True,
        book_reason: str = "",
        bid_levels: list[list[float]] | None = None,
        ask_levels: list[list[float]] | None = None,
        bid_levels_present: bool = False,
        ask_levels_present: bool = False,
    ) -> dict[str, Any]:
        """Accept a normalized best-level state from the Finam ORDER_BOOK stream."""
        return self._sync_push_order_book(
            ticker, float(bid), float(ask), float(bid_size), float(ask_size),
            bid_present=bid_present, ask_present=ask_present,
            bid_size_present=bid_size_present, ask_size_present=ask_size_present,
            bid_timestamp=bid_timestamp, ask_timestamp=ask_timestamp,
            order_book_timestamp=order_book_timestamp,
            received_monotonic=received_monotonic,
            book_valid=book_valid,
            book_reason=book_reason,
            bid_levels=bid_levels, ask_levels=ask_levels,
            bid_levels_present=bid_levels_present, ask_levels_present=ask_levels_present,
        )

