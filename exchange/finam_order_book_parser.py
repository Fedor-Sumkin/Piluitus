"""Stateful Finam ORDER_BOOK snapshot and delta reconstruction."""
from __future__ import annotations
import time
from datetime import datetime, timezone
from typing import Any
from exchange.finam_shared import _event_timestamp_to_monotonic

class FinamOrderBookParserMixin:
    def _extract_order_book_updates(self, envelope: dict) -> list[dict[str, Any]]:
        """Apply Finam ORDER_BOOK level updates and return current top-of-book states.

        Handles the documented payload.order_book[].rows form and common direct
        bids/asks snapshots. Rows are treated as absolute price-level sizes: a
        zero size removes that side at the price. If the stream provides a
        deletion without a side or size, it is ignored rather than guessing.
        """
        self._last_order_book_payload_valid = False
        self._last_order_book_out_of_order = False
        self._last_order_book_out_of_order_tickers.clear()
        self._last_order_book_unrecognized_tickers.clear()
        self._last_order_book_out_of_order_details.clear()
        self._last_order_book_snapshot_wait_tickers.clear()
        try:
            received_monotonic = float(envelope.get("_received_monotonic", time.monotonic()))
        except (TypeError, ValueError, OverflowError):
            received_monotonic = time.monotonic()
        payload = self._decode_payload(envelope.get("payload"))
        if not isinstance(payload, dict):
            payload = self._decode_payload(envelope.get("data"))
        if not isinstance(payload, dict):
            return []

        raw_books: Any = (
            payload.get("order_book")
            or payload.get("orderBook")
            or payload.get("orderbook")
            or payload.get("books")
        )
        if raw_books is None and (payload.get("symbol") or payload.get("ticker")):
            payload_snapshot_flag = bool(
                payload.get(
                    "is_data_snapshot",
                    payload.get("is_snapshot", payload.get("isDataSnapshot", False)),
                )
            )
            if any(key in payload for key in ("rows", "bids", "asks", "levels")) or payload_snapshot_flag:
                raw_books = [payload]
        if isinstance(raw_books, dict):
            raw_books = [raw_books]
        if not isinstance(raw_books, list):
            return []

        updates: list[dict[str, Any]] = []
        for book in raw_books:
            if not isinstance(book, dict):
                continue
            symbol = str(book.get("symbol") or book.get("ticker") or book.get("security_code") or "").strip()
            if not symbol:
                continue
            ticker = symbol.split("@")[0].upper()
            if not ticker:
                continue

            raw_rows_obj = book.get("rows")
            if not isinstance(raw_rows_obj, list):
                raw_rows_obj = book.get("levels")
            raw_rows = raw_rows_obj if isinstance(raw_rows_obj, list) else []
            raw_bids = book.get("bids")
            raw_asks = book.get("asks")
            snapshot_flag = bool(
                book.get(
                    "is_data_snapshot",
                    book.get(
                        "is_snapshot",
                        book.get("isDataSnapshot", payload.get("is_data_snapshot", payload.get("is_snapshot", False))),
                    ),
                )
            )
            has_level_payload = bool(
                isinstance(raw_rows_obj, list)
                or isinstance(raw_bids, list)
                or isinstance(raw_asks, list)
            )
            # Finam may send an explicit empty snapshot after a session closes.
            # It is authoritative evidence that the reconstructed book is empty,
            # unlike a malformed/empty delta which must remain unrecognized.
            empty_snapshot = bool(snapshot_flag and not has_level_payload)
            # A forced resubscription breaks the previous delta chain. Until a
            # fresh provider snapshot arrives, deltas cannot safely rebuild BBO.
            if ticker in self._order_book_snapshot_required and not snapshot_flag:
                self._last_order_book_snapshot_wait_tickers.add(ticker)
                continue
            if snapshot_flag:
                self._order_book_snapshot_required.discard(ticker)
            if empty_snapshot:
                self._crossed_bbo_since.pop(ticker, None)
            if not has_level_payload and not empty_snapshot:
                self._last_order_book_unrecognized_tickers.add(ticker)
                continue
            self._last_order_book_payload_valid = True
            state = self._order_book_state.setdefault(
                ticker,
                {
                    "bids": {}, "asks": {},
                    "bid_timestamps": {}, "ask_timestamps": {},
                    "initialized": False,
                    # Last BBO state actually delivered to the screener. Internal
                    # book mutations can temporarily form a crossed market; do not
                    # lose those changes while waiting for a coherent pair.
                    "published_bid": None, "published_ask": None,
                    "published_bid_timestamp": "", "published_ask_timestamp": "",
                    "bid_bbo_timestamp": "", "ask_bbo_timestamp": "",
                    "last_event_monotonic": 0.0,
                },
            )
            # Reject out-of-order DATA messages before mutating reconstructed
            # depth. Screener-side rejection alone is insufficient: a stale
            # delta must not poison parser state and later reappear in a heartbeat.
            message_timestamp = (
                envelope.get("timestamp") or envelope.get("datetime") or envelope.get("time")
                or payload.get("timestamp")
            )
            message_event_seen = _event_timestamp_to_monotonic(
                message_timestamp, received_monotonic
            )
            prior_message_seen = float(state.get("last_event_monotonic", 0.0) or 0.0)
            if prior_message_seen > 0.0 and message_event_seen + 0.001 < prior_message_seen:
                self._last_order_book_out_of_order = True
                self._last_order_book_out_of_order_tickers.add(ticker)
                self._last_order_book_out_of_order_details[ticker] = {
                    "event_timestamp": str(message_timestamp or ""),
                    "event_monotonic": message_event_seen,
                    "prior_monotonic": prior_message_seen,
                    "age_delta_ms": round((prior_message_seen - message_event_seen) * 1000.0, 1),
                }
                continue
            state["last_event_monotonic"] = max(prior_message_seen, message_event_seen)
            bids: dict[float, float] = state["bids"]
            asks: dict[float, float] = state["asks"]
            bid_timestamps: dict[float, object] = state.setdefault("bid_timestamps", {})
            ask_timestamps: dict[float, object] = state.setdefault("ask_timestamps", {})
            # Capture the actual best levels before applying this message. A delta
            # to a deeper level must not make the unchanged BBO side look fresh.
            previous_bid = (max(bids), bids[max(bids)]) if bids else None
            previous_ask = (min(asks), asks[min(asks)]) if asks else None

            first_payload_for_symbol = not bool(state.get("initialized"))
            if snapshot_flag or first_payload_for_symbol:
                bids.clear()
                asks.clear()
                bid_timestamps.clear()
                ask_timestamps.clear()

            bid_rows_touched: dict[float, object] = {}
            ask_rows_touched: dict[float, object] = {}

            # Some API versions expose explicit arrays rather than rows.
            if isinstance(raw_bids, list):
                bids.clear()
                bid_timestamps.clear()
                for level in raw_bids:
                    if not isinstance(level, dict):
                        continue
                    level_price = self._decimal(level.get("price"))
                    level_size = self._decimal(
                        level.get("buy_size", level.get("buySize", level.get("bid_size", level.get("bidSize", level.get("size", level.get("quantity", level.get("volume")))))))
                    )
                    if level_price > 0 and level_size > 0:
                        bids[level_price] = level_size
                        level_timestamp = level.get("timestamp") or book.get("timestamp") or envelope.get("timestamp")
                        if level_timestamp is not None and level_timestamp != "":
                            bid_timestamps[level_price] = level_timestamp
                        bid_rows_touched[level_price] = level_timestamp
            if isinstance(raw_asks, list):
                asks.clear()
                ask_timestamps.clear()
                for level in raw_asks:
                    if not isinstance(level, dict):
                        continue
                    level_price = self._decimal(level.get("price"))
                    level_size = self._decimal(
                        level.get("sell_size", level.get("sellSize", level.get("ask_size", level.get("askSize", level.get("size", level.get("quantity", level.get("volume")))))))
                    )
                    if level_price > 0 and level_size > 0:
                        asks[level_price] = level_size
                        level_timestamp = level.get("timestamp") or book.get("timestamp") or envelope.get("timestamp")
                        if level_timestamp is not None and level_timestamp != "":
                            ask_timestamps[level_price] = level_timestamp
                        ask_rows_touched[level_price] = level_timestamp

            for row in raw_rows:
                if not isinstance(row, dict):
                    continue
                level_price = self._decimal(row.get("price"))
                if level_price <= 0:
                    continue
                action = str(row.get("action", "")).strip().upper()
                side = str(row.get("side", row.get("book_side", ""))).strip().upper()

                # Finam's current JSON WebSocket representation uses camelCase
                # (`buySize` / `sellSize`) for protobuf oneof fields, while REST
                # and some gateways use snake_case. Decimal values may be {value: ...}.
                bid_keys = ("buy_size", "buySize", "bid_size", "bidSize")
                ask_keys = ("sell_size", "sellSize", "ask_size", "askSize")
                has_bid_size = any(key in row and row.get(key) not in (None, "") for key in bid_keys)
                has_ask_size = any(key in row and row.get(key) not in (None, "") for key in ask_keys)
                is_remove = any(token in action for token in ("REMOVE", "DELETE", "CANCEL"))
                row_timestamp = row.get("timestamp") or book.get("timestamp") or envelope.get("timestamp")

                if has_bid_size:
                    bid_rows_touched[level_price] = row_timestamp
                    raw_size = next(row[key] for key in bid_keys if key in row and row.get(key) not in (None, ""))
                    level_size = self._decimal(raw_size)
                    if is_remove or level_size <= 0:
                        bids.pop(level_price, None)
                        bid_timestamps.pop(level_price, None)
                    else:
                        bids[level_price] = level_size
                        if row_timestamp:
                            bid_timestamps[level_price] = row_timestamp
                elif ("BID" in side or "BUY" in side) and is_remove:
                    bid_rows_touched[level_price] = row_timestamp
                    bids.pop(level_price, None)
                    bid_timestamps.pop(level_price, None)

                if has_ask_size:
                    ask_rows_touched[level_price] = row_timestamp
                    raw_size = next(row[key] for key in ask_keys if key in row and row.get(key) not in (None, ""))
                    level_size = self._decimal(raw_size)
                    if is_remove or level_size <= 0:
                        asks.pop(level_price, None)
                        ask_timestamps.pop(level_price, None)
                    else:
                        asks[level_price] = level_size
                        if row_timestamp:
                            ask_timestamps[level_price] = row_timestamp
                elif ("ASK" in side or "SELL" in side or "OFFER" in side) and is_remove:
                    ask_rows_touched[level_price] = row_timestamp
                    asks.pop(level_price, None)
                    ask_timestamps.pop(level_price, None)

            # Initial delta payloads may contain only one side. Retain state until
            # both sides are actually known; never fabricate the missing side.
            state["initialized"] = bool(state.get("initialized") or raw_rows or isinstance(raw_bids, list) or isinstance(raw_asks, list))
            best_bid = max(bids) if bids else 0.0
            best_ask = min(asks) if asks else 0.0
            current_bid = (float(best_bid), float(bids[best_bid])) if best_bid > 0 else None
            current_ask = (float(best_ask), float(asks[best_ask])) if best_ask > 0 else None
            # The WebSocket envelope timestamp is the message/event clock. The
            # per-level timestamp is only the last update time of that price
            # level and can legitimately remain unchanged across deeper deltas.
            # The envelope timestamp is the only reliable event clock for this
            # WebSocket DATA message. book.timestamp / row.timestamp can describe
            # an individual level's last change and may remain old during a fresh
            # deeper-level delta. When the envelope clock is absent, use the
            # receive monotonic captured at socket ingress instead of promoting a
            # stale per-level timestamp to message freshness.
            level_fallback_timestamp = book.get("timestamp") or message_timestamp
            fallback_timestamp = level_fallback_timestamp
            if fallback_timestamp is None or fallback_timestamp == "":
                fallback_timestamp = datetime.now(timezone.utc).isoformat()

            if empty_snapshot:
                # An explicit empty snapshot clears both sides and invalidates the
                # executable BBO. Never let the previous top-of-book survive a
                # server-confirmed empty book (commonly sent after market close).
                state["initialized"] = True
                state["published_bid"] = None
                state["published_ask"] = None
                state["published_bid_timestamp"] = ""
                state["published_ask_timestamp"] = ""
                state["bid_bbo_timestamp"] = fallback_timestamp
                state["ask_bbo_timestamp"] = fallback_timestamp
                updates.append({
                    "event_type": "ORDER_BOOK",
                    "ticker": ticker,
                    "bid": 0.0,
                    "ask": 0.0,
                    "bid_size": 0.0,
                    "ask_size": 0.0,
                    "bid_present": True,
                    "ask_present": True,
                    "bid_size_present": True,
                    "ask_size_present": True,
                    "bid_timestamp": fallback_timestamp,
                    "ask_timestamp": fallback_timestamp,
                    "bid_levels": [],
                    "ask_levels": [],
                    "bid_levels_present": True,
                    "ask_levels_present": True,
                    "order_book_timestamp": message_timestamp,
                    "received_monotonic": received_monotonic,
                    "timestamp_source": "ENVELOPE" if message_timestamp not in (None, "") else "RECEIVE_FALLBACK",
                    "previous_bid": previous_bid[0] if previous_bid else None,
                    "previous_ask": previous_ask[0] if previous_ask else None,
                    "bid_side_touched": True,
                    "ask_side_touched": True,
                    "book_valid": False,
                    "book_reason": "EMPTY_SNAPSHOT",
                })
                continue

            # Remember when the current best level became the BBO (including when
            # deletion of a better level exposed an older resting level). This is
            # kept separately from per-price timestamps because BBO status itself
            # changed at the time of the deletion.
            bid_changed_in_book = current_bid != previous_bid
            ask_changed_in_book = current_ask != previous_ask
            best_bid_touched = best_bid > 0 and best_bid in bid_rows_touched
            best_ask_touched = best_ask > 0 and best_ask in ask_rows_touched
            if bid_changed_in_book or best_bid_touched or isinstance(raw_bids, list):
                bid_event_timestamp = list(bid_rows_touched.values())[-1] if bid_rows_touched else fallback_timestamp
                state["bid_bbo_timestamp"] = (
                    (bid_rows_touched.get(best_bid) or bid_event_timestamp or fallback_timestamp)
                    if best_bid > 0 else bid_event_timestamp or fallback_timestamp
                )
            elif not state.get("bid_bbo_timestamp") and best_bid > 0:
                state["bid_bbo_timestamp"] = bid_timestamps.get(best_bid) or fallback_timestamp
            if ask_changed_in_book or best_ask_touched or isinstance(raw_asks, list):
                ask_event_timestamp = list(ask_rows_touched.values())[-1] if ask_rows_touched else fallback_timestamp
                state["ask_bbo_timestamp"] = (
                    (ask_rows_touched.get(best_ask) or ask_event_timestamp or fallback_timestamp)
                    if best_ask > 0 else ask_event_timestamp or fallback_timestamp
                )
            elif not state.get("ask_bbo_timestamp") and best_ask > 0:
                state["ask_bbo_timestamp"] = ask_timestamps.get(best_ask) or fallback_timestamp

            # A crossed intermediate state is not executable. Publish an explicit
            # invalidation immediately, but do not advance the last-published
            # snapshot. The next coherent update will then publish all sides that
            # changed during the crossed interval.
            if best_bid > 0 and best_ask > 0 and best_ask < best_bid:
                crossed_started = ticker not in self._crossed_bbo_since
                crossed_since = self._crossed_bbo_since.setdefault(ticker, received_monotonic)
                crossed_duration = max(0.0, received_monotonic - crossed_since)
                bid_levels = [[float(px), float(sz)] for px, sz in sorted(bids.items(), reverse=True)]
                ask_levels = [[float(px), float(sz)] for px, sz in sorted(asks.items())]
                updates.append({
                    "event_type": "ORDER_BOOK",
                    "ticker": ticker,
                    "bid": current_bid[0] if current_bid else 0.0,
                    "ask": current_ask[0] if current_ask else 0.0,
                    "bid_size": current_bid[1] if current_bid else 0.0,
                    "ask_size": current_ask[1] if current_ask else 0.0,
                    "bid_present": True,
                    "ask_present": True,
                    "bid_size_present": True,
                    "ask_size_present": True,
                    "bid_timestamp": fallback_timestamp,
                    "ask_timestamp": fallback_timestamp,
                    "bid_levels": bid_levels,
                    "ask_levels": ask_levels,
                    "bid_levels_present": True,
                    "ask_levels_present": True,
                    "order_book_timestamp": message_timestamp,
                    "received_monotonic": received_monotonic,
                    "timestamp_source": "ENVELOPE" if message_timestamp not in (None, "") else "RECEIVE_FALLBACK",
                    "previous_bid": previous_bid[0] if previous_bid else None,
                    "previous_ask": previous_ask[0] if previous_ask else None,
                    "bid_changed_in_book": bid_changed_in_book,
                    "ask_changed_in_book": ask_changed_in_book,
                    "bid_side_touched": bool(bid_rows_touched),
                    "ask_side_touched": bool(ask_rows_touched),
                    "bid_best_level_touched": best_bid_touched,
                    "ask_best_level_touched": best_ask_touched,
                    "bid_touched_levels": sorted(bid_rows_touched.keys(), reverse=True)[:3],
                    "ask_touched_levels": sorted(ask_rows_touched.keys())[:3],
                    "book_valid": False,
                    "book_reason": "CROSSED_BBO",
                    "crossed_started": crossed_started,
                    "crossed_duration_sec": round(crossed_duration, 3),
                })
                continue

            published_bid = state.get("published_bid")
            published_ask = state.get("published_ask")
            published_bid_timestamp = state.get("published_bid_timestamp")
            published_ask_timestamp = state.get("published_ask_timestamp")
            bid_timestamp = state.get("bid_bbo_timestamp") or fallback_timestamp
            ask_timestamp = state.get("ask_bbo_timestamp") or fallback_timestamp

            # Compare against the last state delivered to the screener, not the
            # previous internal delta. This preserves pending changes across a
            # temporarily crossed BBO. Same-price/size best-level updates still
            # refresh that side's timestamp independently.
            bid_present = bool(
                current_bid != published_bid
                or (best_bid > 0 and bid_timestamp != published_bid_timestamp)
                or isinstance(raw_bids, list)
            )
            ask_present = bool(
                current_ask != published_ask
                or (best_ask > 0 and ask_timestamp != published_ask_timestamp)
                or isinstance(raw_asks, list)
            )
            # Do not drop a valid delta merely because it did not alter the
            # best level. The screener uses this event as proof that the ticker's
            # maintained order-book state is still being updated.

            if bid_present:
                state["published_bid"] = current_bid
                state["published_bid_timestamp"] = bid_timestamp
            if ask_present:
                state["published_ask"] = current_ask
                state["published_ask_timestamp"] = ask_timestamp
            crossed_since = self._crossed_bbo_since.get(ticker)
            coherent_bbo = bool(
                current_bid and current_ask and current_ask[0] >= current_bid[0]
            )
            crossed_recovered_after = (
                max(0.0, received_monotonic - crossed_since)
                if crossed_since is not None and coherent_bbo else None
            )
            if crossed_recovered_after is not None:
                self._crossed_bbo_since.pop(ticker, None)
            # Retain current full depth for diagnostics and later execution-model
            # calibration. The quantity unit must be verified before using it to
            # simulate multi-level fills.
            bid_levels = [[float(px), float(sz)] for px, sz in sorted(bids.items(), reverse=True)]
            ask_levels = [[float(px), float(sz)] for px, sz in sorted(asks.items())]
            updates.append({
                "event_type": "ORDER_BOOK",
                "ticker": ticker,
                "bid": current_bid[0] if current_bid else 0.0,
                "ask": current_ask[0] if current_ask else 0.0,
                "bid_size": current_bid[1] if current_bid else 0.0,
                "ask_size": current_ask[1] if current_ask else 0.0,
                "bid_present": bid_present,
                "ask_present": ask_present,
                "bid_size_present": bid_present,
                "ask_size_present": ask_present,
                "bid_timestamp": bid_timestamp or fallback_timestamp,
                "ask_timestamp": ask_timestamp or fallback_timestamp,
                "bid_levels": bid_levels,
                "ask_levels": ask_levels,
                # These arrays are the parser's full reconstructed depth, so
                # refresh them on every heartbeat as well as a best-level move.
                "bid_levels_present": True,
                "ask_levels_present": True,
                "order_book_timestamp": message_timestamp,
                "received_monotonic": received_monotonic,
                "timestamp_source": "ENVELOPE" if message_timestamp not in (None, "") else "RECEIVE_FALLBACK",
                "book_valid": bool(current_bid and current_ask and current_ask[0] >= current_bid[0]),
                "book_reason": (
                    "OK" if current_bid and current_ask and current_ask[0] >= current_bid[0]
                    else "MISSING_BBO_SIDE"
                ),
                "crossed_recovered": crossed_recovered_after is not None,
                "crossed_recovered_after_sec": (
                    round(crossed_recovered_after, 3)
                    if crossed_recovered_after is not None else None
                ),
            })
        return updates
