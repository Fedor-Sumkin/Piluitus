"""Finam QUOTES, trades, ORDER_BOOK parsing and WebSocket stream mixin."""
from __future__ import annotations

from exchange.finam_shared import (
    asyncio, logging, math, datetime, timezone, timedelta, url_quote,
    Callable, Iterable, Any, json, inspect, os, re, time, websockets, httpx,
    logger, JOURNAL_FILE, _MOSCOW_TZ,
    _websocket_close_code, _websocket_disconnect_log_level,
    _coalesce_queued_market_event, _event_timestamp_to_monotonic,
    _should_resync_position_order_book,
)

class FinamMarketDataMixin:
    @classmethod
    def _extract_quotes(cls, envelope: dict) -> list[dict]:
        """Normalize Finam QUOTES envelopes, including JSON-string payloads.

        Current Finam messages can look like:
        {"type":"DATA", "subscription_type":"QUOTES",
         "payload":"{\"quote\":[{\"symbol\":...,\"last\":{\"value\":...}}]}"}

        In that format payload itself is a JSON string (not an object), and
        decimal-like fields such as last/lastSize/volume are objects containing
        a `value` member.
        """
        payload = cls._decode_payload(envelope.get("payload"))
        if not payload:
            payload = cls._decode_payload(envelope.get("data"))
        if not payload:
            payload = {}

        if isinstance(payload, list):
            candidates: Any = payload
        elif isinstance(payload, dict):
            candidates = (
                payload.get("quote")
                or payload.get("quotes")
                or payload.get("trades")
                or payload.get("trade")
            )
            if candidates is None and any(
                key in payload for key in ("symbol", "ticker", "security_code", "last", "price")
            ):
                candidates = [payload]
        else:
            candidates = None

        # Compatibility with envelopes that place the quote outside payload.
        if candidates is None:
            top_quote = cls._decode_payload(envelope.get("quote") or envelope.get("quotes"))
            if top_quote not in (None, {}, []):
                candidates = top_quote

        if isinstance(candidates, dict):
            candidates = [candidates]
        if not isinstance(candidates, list):
            return []

        envelope_timestamp = (
            envelope.get("timestamp") or envelope.get("datetime") or envelope.get("time")
            or (payload.get("timestamp") if isinstance(payload, dict) else None)
        )
        try:
            received_monotonic = float(envelope.get("_received_monotonic", time.monotonic()))
        except (TypeError, ValueError, OverflowError):
            received_monotonic = time.monotonic()

        result: list[dict] = []
        for item in candidates:
            if not isinstance(item, dict):
                continue

            symbol = str(
                item.get("symbol")
                or item.get("ticker")
                or item.get("security_code")
                or ""
            ).strip()
            if not symbol:
                continue

            # Finam decimal fields can be plain values or {"value": "..."}.
            last_raw = item.get("last")
            if last_raw is None:
                last_raw = item.get("price")
            if last_raw is None:
                last_raw = item.get("last_price")
            if last_raw is None:
                last_raw = item.get("close")
            price = cls._decimal(last_raw)
            if price <= 0:
                continue

            # The screener's per-event `volume` must be the size of this
            # trade, never cumulative session volume. Falling back to `volume`
            # fabricates a trade-size spike when lastSize is absent and can
            # distort both anomaly detection and VWAP weighting.
            last_size_raw = item.get("lastSize")
            if last_size_raw is None:
                last_size_raw = item.get("last_size")
            if last_size_raw is None:
                last_size_raw = item.get("size")
            session_volume_raw = item.get("volume")
            last_size = cls._decimal(last_size_raw)
            session_volume = cls._decimal(session_volume_raw)

            ticker = symbol.split("@")[0]
            bid_raw = item.get("bid")
            ask_raw = item.get("ask")
            # Prefer documented snake_case keys; fall back to legacy camelCase.
            bid_size_raw = item.get("bid_size")
            if bid_size_raw is None:
                bid_size_raw = item.get("bidSize")
            ask_size_raw = item.get("ask_size")
            if ask_size_raw is None:
                ask_size_raw = item.get("askSize")

            # Presence flags distinguish a partial update (field omitted) from an
            # explicit zero quote (Finam documents bid=0 when no active bid).
            bid_present = "bid" in item and bid_raw is not None and bid_raw != ""
            ask_present = "ask" in item and ask_raw is not None and ask_raw != ""
            bid_size_present = (
                ("bid_size" in item or "bidSize" in item)
                and bid_size_raw is not None and bid_size_raw != ""
            )
            ask_size_present = (
                ("ask_size" in item or "askSize" in item)
                and ask_size_raw is not None and ask_size_raw != ""
            )

            result.append({
                "ticker": ticker,
                "security_code": ticker,
                "symbol": symbol,
                "price": price,
                "volume": max(0.0, last_size),
                "last_size": max(0.0, last_size),
                "day_volume": max(0.0, session_volume),
                "bid": cls._decimal(bid_raw),
                "ask": cls._decimal(ask_raw),
                "bid_size": max(0.0, cls._decimal(bid_size_raw)),
                "ask_size": max(0.0, cls._decimal(ask_size_raw)),
                "bid_present": bool(bid_present),
                "ask_present": bool(ask_present),
                "bid_size_present": bool(bid_size_present),
                "ask_size_present": bool(ask_size_present),
                # An explicit null/empty item timestamp must not mask the
                # envelope event timestamp. Both feed paths use the same clock.
                "timestamp": item.get("timestamp") or envelope_timestamp,
                "received_monotonic": received_monotonic,
                "is_snapshot": bool(item.get("isDataSnapshot", item.get("is_snapshot", False))),
            })
        return result

    @classmethod
    def _extract_instrument_trades(cls, envelope: dict) -> list[dict[str, Any]]:
        """Normalize public Finam INSTRUMENT_TRADES events; never infer a trade from QUOTES."""
        payload = cls._decode_payload(envelope.get("payload"))
        if not payload:
            payload = cls._decode_payload(envelope.get("data"))
        if isinstance(payload, list):
            candidates: Any = payload
            parent_symbol = str(envelope.get("symbol") or envelope.get("ticker") or "").strip()
        elif isinstance(payload, dict):
            candidates = (
                payload.get("trades")
                or payload.get("trade")
                or payload.get("instrument_trades")
                or payload.get("instrumentTrades")
            )
            parent_symbol = str(
                payload.get("symbol")
                or payload.get("ticker")
                or payload.get("securityCode")
                or envelope.get("symbol")
                or envelope.get("ticker")
                or ""
            ).strip()
            if candidates is None and any(
                key in payload
                for key in (
                    "trade_id", "tradeId", "price", "tradePrice",
                    "size", "tradeSize",
                )
            ):
                candidates = [payload]
        else:
            return []
        if isinstance(candidates, dict):
            candidates = [candidates]
        if not isinstance(candidates, list):
            return []
        envelope_timestamp = envelope.get("timestamp") or envelope.get("datetime") or envelope.get("time")
        received = envelope.get("_received_monotonic", time.monotonic())
        try:
            received = float(received)
        except (TypeError, ValueError, OverflowError):
            received = time.monotonic()
        result: list[dict[str, Any]] = []
        for item in candidates:
            if not isinstance(item, dict):
                continue
            symbol = str(
                item.get("symbol")
                or item.get("ticker")
                or item.get("securityCode")
                or parent_symbol
            ).strip()
            if not symbol:
                continue
            raw_price = item.get("price")
            if raw_price is None:
                raw_price = item.get("tradePrice")
            raw_size = (
                item.get("size")
                or item.get("quantity")
                or item.get("volume")
                or item.get("tradeSize")
            )
            price = cls._decimal(raw_price)
            size = cls._decimal(raw_size)
            if price <= 0.0 or size <= 0.0:
                continue
            result.append({
                "event_type": "INSTRUMENT_TRADE",
                "ticker": symbol.split("@", 1)[0].upper(),
                "symbol": symbol if "@" in symbol else f"{symbol}@MISX",
                "trade_id": item.get("trade_id") or item.get("tradeId") or item.get("id"),
                "price": price,
                "size": size,
                "side": item.get("side") or item.get("tradeSide"),
                "timestamp": (
                    item.get("timestamp")
                    or item.get("tradeTimestamp")
                    or item.get("tradeTime")
                    or envelope_timestamp
                ),
                "received_monotonic": received,
            })
        return result

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

    async def start_market_data_stream(self, tickers: list, queue: asyncio.Queue, screener=None):
        """Подключиться к Finam WS и подписать инструменты небольшими группами.

        Размер одного запроса задаётся переменной FINAM_WS_SYMBOL_BATCH_SIZE
        (по умолчанию 10). Одно WebSocket-соединение может обслуживать несколько
        подписок, поэтому большой список не отправляется одним запросом.
        """
        if not self.jwt_token:
            raise RuntimeError("Сначала вызовите broker.connect() для получения JWT.")

        try:
            connect_parameters = inspect.signature(websockets.connect).parameters
        except (TypeError, ValueError) as exc:
            raise RuntimeError(
                "Не удалось определить API установленного пакета websockets. "
                "Обновите пакет командой: python -m pip install -U websockets"
            ) from exc

        if "additional_headers" in connect_parameters:
            header_argument = "additional_headers"
        elif "extra_headers" in connect_parameters:
            header_argument = "extra_headers"
        else:
            raise RuntimeError(
                "Установленная версия websockets не поддерживает дополнительные HTTP-заголовки. "
                "Выполните: python -m pip install -U websockets"
            )

        # Нормализуем MIC и удаляем дубликаты, чтобы не тратить лимит символов.
        symbols = []
        seen = set()
        for raw_ticker in tickers:
            symbol = str(raw_ticker).strip()
            if not symbol:
                continue
            if "@" not in symbol:
                symbol = f"{symbol}@MISX"
            if symbol not in seen:
                seen.add(symbol)
                symbols.append(symbol)

        if not symbols:
            raise ValueError("Список TICKERS_TO_TRADE пуст: нечего подписывать на Finam QUOTES.")

        try:
            batch_size = int(os.getenv("FINAM_WS_SYMBOL_BATCH_SIZE", "10"))
        except ValueError as exc:
            raise ValueError(
                "FINAM_WS_SYMBOL_BATCH_SIZE должен быть целым числом, например 10."
            ) from exc
        if batch_size < 1:
            raise ValueError("FINAM_WS_SYMBOL_BATCH_SIZE должен быть не меньше 1.")

        batches = [symbols[i:i + batch_size] for i in range(0, len(symbols), batch_size)]
        logger.info(
            "Подготовлено %d уникальных инструментов Finam; подписка будет отправлена "
            "%d запросами по не более %d символов.",
            len(symbols), len(batches), batch_size,
        )

        self.is_ws_running = True
        backoff = 1
        refresh_token_before_retry = False

        while self.is_ws_running:
            try:
                if refresh_token_before_retry:
                    await self.connect()
                    refresh_token_before_retry = False

                # `websockets.connect` имеет разные типы заголовков в разных версиях:
                # extra_headers (v12) и additional_headers (новые версии).
                # Объявляем вызываемый объект как Any: динамический **kwargs иначе
                # ошибочно сопоставляется Pylance с create_protocol в старых stubs.
                header_kwargs: dict[str, Any] = {
                    header_argument: {"Authorization": self.jwt_token}
                }
                connect_fn: Any = websockets.connect
                self._order_book_state.clear()
                async with connect_fn(
                    self.ws_endpoint,
                    **header_kwargs,
                    ping_interval=20,
                    ping_timeout=10,
                    open_timeout=10,
                ) as ws:
                    logger.info(
                        "TCP/TLS WebSocket Finam установлен; ожидаем HANDSHAKE_SUCCESS (websockets %s).",
                        getattr(websockets, "__version__", "unknown"),
                    )

                    handshake_deadline = asyncio.get_running_loop().time() + 10.0
                    while True:
                        remaining = handshake_deadline - asyncio.get_running_loop().time()
                        if remaining <= 0:
                            raise TimeoutError(
                                "Finam WebSocket не прислал HANDSHAKE_SUCCESS в течение 10 секунд."
                            )
                        raw_handshake = await asyncio.wait_for(ws.recv(), timeout=remaining)
                        try:
                            handshake_message = json.loads(raw_handshake)
                        except (TypeError, json.JSONDecodeError):
                            logger.warning("Finam WS прислал не-JSON сообщение до handshake.")
                            continue

                        if not isinstance(handshake_message, dict):
                            continue

                        handshake_type = str(handshake_message.get("type", "")).upper()
                        if handshake_type == "ERROR":
                            error_info = handshake_message.get("error_info", handshake_message)
                            raise RuntimeError(f"Finam отклонил WebSocket handshake: {error_info}")

                        if handshake_type == "EVENT":
                            event_info = handshake_message.get("event_info") or {}
                            event_name = (
                                str(event_info.get("event", "")).upper()
                                if isinstance(event_info, dict) else ""
                            )
                            if event_name == "HANDSHAKE_SUCCESS":
                                logger.info("✅ Finam WS: авторизация успешна (HANDSHAKE_SUCCESS)")
                                break
                            logger.info("Событие Finam WS до подписки: %s", event_info)
                            if event_name == "CONNECTION_CLOSED":
                                raise RuntimeError(
                                    f"Finam закрыл соединение во время handshake: {event_info}"
                                )

                    # Отправляем несколько SUBSCRIBE на одном сокете.
                    # Документация Finam допускает несколько подписок на одном соединении.
                    for batch_number, symbol_batch in enumerate(batches, start=1):
                        request = {
                            "action": "SUBSCRIBE",
                            "type": "QUOTES",
                            "data": {"symbols": symbol_batch},
                            "token": self.jwt_token,
                        }
                        await ws.send(json.dumps(request, ensure_ascii=False))
                        logger.info(
                            "Подписка QUOTES отправлена: пакет %d/%d, символов %d (%s%s).",
                            batch_number,
                            len(batches),
                            len(symbol_batch),
                            ", ".join(symbol_batch[:3]),
                            "…" if len(symbol_batch) > 3 else "",
                        )
                        # Небольшой интервал, чтобы не отправлять все команды одним всплеском.
                        if batch_number < len(batches):
                            await asyncio.sleep(0.05)

                    logger.info(
                        "Все %d запросов подписки отправлены; ожидаем поток котировок.",
                        len(batches),
                    )
                    backoff = 1

                    # Enable strict trade-data mode before the first QUOTES event.
                    # Actual INSTRUMENT_TRADES subscriptions follow live Top-15 plus
                    # open positions, avoiding 88 unnecessary high-rate streams.
                    if screener is not None:
                        screener.enable_trade_data_mode()
                    logger.info("📈 Finam INSTRUMENT_TRADES: dynamic Top-15/position subscriptions enabled; QUOTES.lastSize excluded from trade volume.")
                    backoff = 1

                    # Диагностические счётчики: отправка SUBSCRIBE сама по себе не
                    # означает, что сервер уже прислал полезные данные.
                    data_messages = 0
                    enqueued_quotes = 0
                    enqueued_order_book_events = 0
                    enqueued_bbo_side_updates = 0
                    first_data_logged = False
                    queue_overflow_counts = {
                        "ORDER_BOOK": 0, "INSTRUMENT_TRADE": 0, "QUOTES": 0,
                    }
                    queue_overflow_last_log = 0.0
                    queue_overflow_last_ticker = "-"

                    def _record_queue_overflow(event_kind: str, ticker: object) -> None:
                        nonlocal queue_overflow_last_log, queue_overflow_last_ticker
                        queue_overflow_counts[event_kind] = queue_overflow_counts.get(event_kind, 0) + 1
                        queue_overflow_last_ticker = str(ticker or "-")
                        now = time.monotonic()
                        if now - queue_overflow_last_log < 5.0:
                            return
                        total_dropped = sum(queue_overflow_counts.values())
                        logger.warning(
                            "[FINAM QUEUE OVERFLOW] за последние %.1fс: всего=%d, "
                            "ORDER_BOOK=%d, INSTRUMENT_TRADE=%d, QUOTES=%d; последний тикер=%s. "
                            "Поток не успевает обрабатываться потребителем; события теряются.",
                            max(0.0, now - queue_overflow_last_log) if queue_overflow_last_log else 5.0,
                            total_dropped,
                            queue_overflow_counts["ORDER_BOOK"],
                            queue_overflow_counts["INSTRUMENT_TRADE"],
                            queue_overflow_counts["QUOTES"],
                            queue_overflow_last_ticker,
                        )
                        for key in queue_overflow_counts:
                            queue_overflow_counts[key] = 0
                        queue_overflow_last_log = now
                    first_order_book_logged = False
                    first_trade_logged = False
                    instrument_trade_messages = 0
                    instrument_trade_events = 0
                    instrument_trade_queue_dropped = 0
                    instrument_trade_queue_backpressure = 0
                    order_book_events_coalesced = 0
                    quote_events_coalesced = 0
                    last_trade_diag = asyncio.get_running_loop().time()
                    order_book_decode_warned = False
                    order_book_messages = 0
                    order_book_decode_failures = 0
                    order_book_valid_no_bbo_delta = 0
                    order_book_invalid_states = 0
                    order_book_crossed_episodes = 0
                    order_book_invalid_reasons: dict[str, int] = {}
                    order_book_invalid_by_ticker: dict[str, int] = {}
                    order_book_missing_envelope_timestamp = 0
                    order_book_queue_dropped = 0
                    order_book_out_of_order = 0
                    order_book_out_of_order_age_deltas_ms: list[float] = []
                    order_book_subscribed: set[str] = set()
                    order_book_subscribed_since: dict[str, float] = {}
                    trade_subscribed: set[str] = set()
                    trade_subscribed_since: dict[str, float] = {}
                    last_trade_subscription_sync = 0.0
                    try:
                        trade_subscription_sync_sec = max(2.0, float(os.getenv("FINAM_INSTRUMENT_TRADES_REFRESH_SEC", "5")))
                        trade_subscription_top_n = max(1, min(15, int(os.getenv("FINAM_INSTRUMENT_TRADES_TOP_N", "15"))))
                        trade_subscription_min_hold_sec = max(0.0, min(120.0, float(os.getenv("FINAM_INSTRUMENT_TRADES_MIN_HOLD_SEC", "60"))))
                        trade_subscription_max_overbook = max(0, min(10, int(os.getenv("FINAM_INSTRUMENT_TRADES_MAX_OVERBOOK", "5"))))
                    except (TypeError, ValueError, OverflowError):
                        trade_subscription_sync_sec, trade_subscription_top_n = 5.0, 15
                        trade_subscription_min_hold_sec, trade_subscription_max_overbook = 60.0, 5
                    order_book_symbol_diag: dict[str, dict[str, Any]] = {}
                    last_open_position_symbols: set[str] = set()
                    order_book_unattributed_messages = 0
                    order_book_unattributed_sample = ""
                    order_book_parser_sample = ""

                    def _order_book_symbol_stats(ticker: str) -> dict[str, Any]:
                        return order_book_symbol_diag.setdefault(ticker, {
                            "window_wire": 0,
                            "window_parse_no_update": 0,
                            "window_parse_out_of_order": 0,
                            "window_parse_unrecognized": 0,
                            "window_snapshot_wait": 0,
                            "window_parsed": 0,
                            "window_queued": 0,
                            "window_dropped": 0,
                            "last_wire_monotonic": 0.0,
                            "last_parsed_monotonic": 0.0,
                            "last_queued_monotonic": 0.0,
                        })

                    last_order_book_sync = 0.0
                    last_order_book_diag = asyncio.get_running_loop().time()
                    last_ws_data_at = last_order_book_diag
                    position_snapshot_inflight: set[str] = set()
                    position_snapshot_last_at: dict[str, float] = {}
                    try:
                        crossed_resubscribe_after_sec = max(2.0, float(os.getenv("FINAM_CROSSED_BBO_RESUBSCRIBE_AFTER_SEC", "5")))
                        crossed_resubscribe_cooldown_sec = max(10.0, float(os.getenv("FINAM_CROSSED_BBO_RESUBSCRIBE_COOLDOWN_SEC", "30")))
                        position_rest_refresh_interval_sec = max(3.0, float(os.getenv("FINAM_POSITION_REST_BBO_REFRESH_SEC", "15")))
                        position_rest_refresh_trigger_age_sec = max(0.5, float(os.getenv("FINAM_POSITION_REST_BBO_TRIGGER_AGE_SEC", "1.5")))
                        position_ws_idle_resync_sec = max(1.0, float(os.getenv("FINAM_POSITION_WS_IDLE_RESYNC_SEC", "3")))
                    except (TypeError, ValueError):
                        crossed_resubscribe_after_sec = 5.0
                        crossed_resubscribe_cooldown_sec = 30.0
                        position_rest_refresh_interval_sec = 15.0
                        position_rest_refresh_trigger_age_sec = 1.5
                        position_ws_idle_resync_sec = 3.0

                    logger.info(
                        "[ORDER_BOOK POSITION POLICY] stale_trigger=%.2fs ws_idle_resync=%.2fs "
                        "resync_cooldown=%.2fs rest_control_interval=%.2fs",
                        position_rest_refresh_trigger_age_sec,
                        position_ws_idle_resync_sec,
                        crossed_resubscribe_cooldown_sec,
                        position_rest_refresh_interval_sec,
                    )

                    async def _refresh_position_snapshot(symbol: str) -> None:
                        ticker = symbol.split("@", 1)[0].strip().upper()
                        try:
                            await self.refresh_order_book_snapshot(ticker, queue)
                        except Exception as refresh_exc:
                            logger.warning("Finam REST BBO refresh %s failed: %s", ticker, refresh_exc)
                        finally:
                            position_snapshot_inflight.discard(ticker)
                    last_no_data_warning_at = 0.0
                    try:
                        order_book_refresh_sec = max(1.0, float(os.getenv("FINAM_ORDER_BOOK_REFRESH_SEC", "1")))
                        order_book_limit = max(1, min(15, int(os.getenv("FINAM_ORDER_BOOK_TOP_N", "15"))))
                        order_book_min_hold_sec = max(0.0, min(120.0, float(os.getenv("FINAM_ORDER_BOOK_MIN_HOLD_SEC", "20"))))
                        order_book_max_overbook = max(0, min(15, int(os.getenv("FINAM_ORDER_BOOK_MAX_OVERBOOK", "5"))))
                    except (TypeError, ValueError):
                        order_book_refresh_sec, order_book_limit = 10.0, 15
                        order_book_min_hold_sec, order_book_max_overbook = 20.0, 5

                    while self.is_ws_running:
                        # Keep ORDER_BOOK subscriptions aligned with the current top-N.
                        # QUOTES for the full universe continue independently.
                        now_loop = asyncio.get_running_loop().time()
                        if screener is not None:
                            try:
                                funnel_status = screener.get_status()
                                # Keep currently eligible members subscribed during
                                # short coverage dips; screener readiness still gates LLM.
                                selected = funnel_status.get("selected_tickers", [])
                                # Recalculate the desired set on every receive-loop
                                # turn. A Top-15 rotation should not wait for the
                                # periodic refresh timer before its BBO subscription
                                # is requested (the timer is only a safety resync).
                                # Top-N remains the only screener/LLM candidate set.
                                # ORDER_BOOK additionally retains every open position for exit monitoring.
                                normalized_selected: list[str] = []
                                seen_selected: set[str] = set()
                                for raw_ticker in selected:
                                    ticker = str(raw_ticker).strip().upper().split("@")[0]
                                    if not ticker or ticker in seen_selected:
                                        continue
                                    seen_selected.add(ticker)
                                    normalized_selected.append(f"{ticker}@MISX")
                                    if len(normalized_selected) >= order_book_limit:
                                        break
                                # The screener/LLM universe remains strictly Top-N.
                                # Position monitoring has a separate technical requirement:
                                # every open position must keep receiving BBO after it leaves Top-N.
                                wanted = set(normalized_selected)
                                position_symbols: set[str] = set()
                                if self.open_positions_provider is not None:
                                    try:
                                        raw_positions = self.open_positions_provider()
                                        seen_positions: set[str] = set()
                                        for raw_ticker in raw_positions:
                                            ticker = str(raw_ticker).strip().upper().split("@")[0]
                                            if not ticker or ticker in seen_positions:
                                                continue
                                            seen_positions.add(ticker)
                                            position_symbols.add(f"{ticker}@MISX")
                                    except Exception as position_exc:
                                        logger.warning(
                                            "Finam ORDER_BOOK: не удалось получить тикеры открытых позиций; "
                                            "подписки Top-%d сохраняются: %s",
                                            order_book_limit, position_exc,
                                        )
                                # Position diagnostics use bare tickers because parser statistics
                                # are keyed by normalized ticker (for example, "LSRG", not "LSRG@MISX").
                                last_open_position_symbols = {
                                    symbol.split("@", 1)[0].strip().upper()
                                    for symbol in position_symbols
                                }
                                position_only_symbols = position_symbols - wanted
                                wanted.update(position_symbols)

                                # Trade flow is required only for active Top-15 and open
                                # positions. Keep a bounded overlap while symbols rotate.
                                trade_wanted = set(normalized_selected[:trade_subscription_top_n]) | position_symbols
                                obsolete_trade = trade_subscribed - trade_wanted
                                protected_trade = {
                                    symbol for symbol in obsolete_trade
                                    if now_loop - trade_subscribed_since.get(symbol, 0.0) < trade_subscription_min_hold_sec
                                }
                                if len(protected_trade) > trade_subscription_max_overbook:
                                    protected_trade = set(sorted(
                                        protected_trade,
                                        key=lambda item: trade_subscribed_since.get(item, 0.0),
                                        reverse=True,
                                    )[:trade_subscription_max_overbook])
                                effective_trade_wanted = trade_wanted | protected_trade
                                trade_sync_due = now_loop - last_trade_subscription_sync >= trade_subscription_sync_sec
                                if effective_trade_wanted != trade_subscribed or trade_sync_due:
                                    removed_trade = trade_subscribed - effective_trade_wanted
                                    added_trade = effective_trade_wanted - trade_subscribed
                                    for trade_symbol in sorted(removed_trade):
                                        await ws.send(json.dumps({
                                            "action": "UNSUBSCRIBE", "type": "INSTRUMENT_TRADES",
                                            "data": {"symbol": trade_symbol}, "token": self.jwt_token,
                                        }, ensure_ascii=False))
                                        if screener is not None:
                                            reset_subscription = getattr(screener, "reset_trade_subscription", None)
                                            if callable(reset_subscription):
                                                reset_subscription(trade_symbol)
                                            else:
                                                screener.reset_trade_history(trade_symbol)
                                        trade_subscribed.discard(trade_symbol)
                                        trade_subscribed_since.pop(trade_symbol, None)
                                    for trade_symbol in sorted(added_trade):
                                        if screener is not None:
                                            reset_subscription = getattr(screener, "reset_trade_subscription", None)
                                            if callable(reset_subscription):
                                                reset_subscription(trade_symbol)
                                            else:
                                                screener.reset_trade_history(trade_symbol)
                                        await ws.send(json.dumps({
                                            "action": "SUBSCRIBE", "type": "INSTRUMENT_TRADES",
                                            "data": {"symbol": trade_symbol}, "token": self.jwt_token,
                                        }, ensure_ascii=False))
                                        trade_subscribed.add(trade_symbol)
                                        trade_subscribed_since[trade_symbol] = now_loop
                                    last_trade_subscription_sync = now_loop
                                    if added_trade or removed_trade:
                                        logger.info(
                                            "📈 Finam INSTRUMENT_TRADES subscriptions: Top-%d=%d, positions=%d, active=%d, added=%d, removed=%d, held=%d.",
                                            trade_subscription_top_n, len(normalized_selected[:trade_subscription_top_n]),
                                            len(position_symbols), len(trade_subscribed), len(added_trade),
                                            len(removed_trade), len(protected_trade),
                                        )

                                # Positions need continuously executable BBO even outside Top-15.
                                # Refresh stale/missing sides with a coherent control snapshot.
                                for position_symbol in sorted(position_symbols):
                                    position_ticker = position_symbol.split("@", 1)[0].strip().upper()
                                    if (
                                        not position_ticker
                                        or position_ticker in position_snapshot_inflight
                                        or now_loop - position_snapshot_last_at.get(position_ticker, 0.0)
                                        < position_rest_refresh_interval_sec
                                    ):
                                        continue
                                    state = self._order_book_state.get(position_ticker, {})
                                    bid_stamp = state.get("bid_bbo_timestamp")
                                    ask_stamp = state.get("ask_bbo_timestamp")
                                    bid_seen = _event_timestamp_to_monotonic(bid_stamp, now_loop)
                                    ask_seen = _event_timestamp_to_monotonic(ask_stamp, now_loop)
                                    bid_age = now_loop - bid_seen if bid_stamp else math.inf
                                    ask_age = now_loop - ask_seen if ask_stamp else math.inf
                                    book_event_seen = float(state.get("last_event_monotonic", 0.0) or 0.0)
                                    book_event_age = (
                                        now_loop - book_event_seen if book_event_seen > 0.0 else math.inf
                                    )
                                    should_resync = _should_resync_position_order_book(
                                        bid_age,
                                        ask_age,
                                        book_event_age,
                                        stale_after_sec=position_rest_refresh_trigger_age_sec,
                                        stream_idle_sec=position_ws_idle_resync_sec,
                                    )

                                    # REST is diagnostic-only. A stale best-level change timestamp
                                    # is not sufficient to reset an otherwise active delta-chain:
                                    # resubscribe only after both the executable BBO age threshold
                                    # and a genuine ORDER_BOOK stream-idle interval are exceeded.
                                    last_resubscribe = self._crossed_bbo_resubscribe_at.get(
                                        position_ticker, 0.0
                                    )
                                    if (
                                        should_resync
                                        and position_symbol in order_book_subscribed
                                        and now_loop - last_resubscribe >= crossed_resubscribe_cooldown_sec
                                    ):
                                        self._order_book_snapshot_required.add(position_ticker)
                                        self._last_order_book_snapshot_wait_tickers.discard(position_ticker)
                                        self._order_book_state.pop(position_ticker, None)
                                        self._crossed_bbo_resubscribe_at[position_ticker] = now_loop
                                        try:
                                            await ws.send(json.dumps({
                                                "action": "UNSUBSCRIBE", "type": "ORDER_BOOK",
                                                "data": {"symbol": position_symbol}, "token": self.jwt_token,
                                            }))
                                            await asyncio.sleep(0.05)
                                            await ws.send(json.dumps({
                                                "action": "SUBSCRIBE", "type": "ORDER_BOOK",
                                                "data": {"symbol": position_symbol}, "token": self.jwt_token,
                                            }))
                                            order_book_subscribed_since[position_symbol] = now_loop
                                            logger.warning(
                                                "🔄 [ORDER_BOOK POSITION RESYNC] %s: BBO устарел "
                                                "(Bid age=%.2fs, Ask age=%.2fs); WS subscription restarted, "
                                                "ожидается полный provider snapshot; REST остаётся диагностическим.",
                                                position_ticker, bid_age, ask_age,
                                            )
                                        except Exception as resync_exc:
                                            # Let the regular subscription synchronizer retry
                                            # on the next loop turn if either WS command failed.
                                            order_book_subscribed.discard(position_symbol)
                                            order_book_subscribed_since.pop(position_symbol, None)
                                            logger.warning(
                                                "Не удалось перезапустить ORDER_BOOK для позиции %s: %s; "
                                                "подписка будет повторена синхронизатором.",
                                                position_ticker, type(resync_exc).__name__,
                                            )

                                    # Fresh BBO needs no REST control request. If the BBO is
                                    # stale but WS events are still flowing, keep the subscription
                                    # and use only the throttled diagnostic snapshot below.
                                    if max(bid_age, ask_age) < position_rest_refresh_trigger_age_sec:
                                        continue

                                    if (
                                        position_ticker not in position_snapshot_inflight
                                        and now_loop - position_snapshot_last_at.get(position_ticker, 0.0)
                                        >= position_rest_refresh_interval_sec
                                    ):
                                        position_snapshot_inflight.add(position_ticker)
                                        position_snapshot_last_at[position_ticker] = now_loop
                                        asyncio.create_task(_refresh_position_snapshot(position_symbol))

                                # Keep a small subscription overlap during Top-N rotation.
                                # It avoids tearing down a recently warmed book because of a
                                # marginal score change while still bounding provider load.
                                obsolete = order_book_subscribed - wanted
                                protected = {
                                    symbol for symbol in obsolete
                                    if now_loop - order_book_subscribed_since.get(symbol, 0.0) < order_book_min_hold_sec
                                }
                                if len(protected) > order_book_max_overbook:
                                    protected = set(sorted(
                                        protected,
                                        key=lambda item: order_book_subscribed_since.get(item, 0.0),
                                        reverse=True,
                                    )[:order_book_max_overbook])
                                effective_wanted = wanted | protected
                                sync_due = now_loop - last_order_book_sync >= order_book_refresh_sec
                                selection_changed = effective_wanted != order_book_subscribed
                                if selection_changed or sync_due:
                                    removed = order_book_subscribed - effective_wanted
                                    added = effective_wanted - order_book_subscribed
                                    for symbol in sorted(removed):
                                        await ws.send(json.dumps({
                                            "action": "UNSUBSCRIBE", "type": "ORDER_BOOK",
                                            "data": {"symbol": symbol}, "token": self.jwt_token,
                                        }))
                                        self._order_book_state.pop(symbol.split("@")[0].upper(), None)
                                        order_book_subscribed.discard(symbol)
                                        order_book_subscribed_since.pop(symbol, None)
                                    for symbol in sorted(added):
                                        self._order_book_state.pop(symbol.split("@")[0].upper(), None)
                                        await ws.send(json.dumps({
                                            "action": "SUBSCRIBE", "type": "ORDER_BOOK",
                                            "data": {"symbol": symbol}, "token": self.jwt_token,
                                        }))
                                        order_book_subscribed.add(symbol)
                                        order_book_subscribed_since[symbol] = now_loop
                                    last_order_book_sync = now_loop
                                    if added or removed:
                                        position_symbols_text = ",".join(sorted(
                                            symbol.split("@")[0] for symbol in position_symbols
                                        )) or "-"
                                        position_subscriptions_text = ",".join(sorted(
                                            symbol.split("@")[0]
                                            for symbol in position_symbols
                                            if symbol in order_book_subscribed
                                        )) or "-"
                                        logger.info(
                                            "📚 Finam ORDER_BOOK подписки синхронизированы: Top-%d=%d, "
                                            "позиций_вне_Top=%d, всего=%d, добавлено=%d, снято=%d, "
                                            "временно_удержано=%d; "
                                            "активные_позиции=%s; запрошенные_подписки_позиций=%s.",
                                            order_book_limit, len(normalized_selected),
                                            len(position_only_symbols), len(order_book_subscribed),
                                            len(added), len(removed), len(protected),
                                            position_symbols_text, position_subscriptions_text,
                                        )
                            except Exception as sub_exc:
                                last_order_book_sync = now_loop
                                logger.warning(
                                    "Finam ORDER_BOOK: не удалось синхронизировать подписки, "
                                    "QUOTES продолжается: %s", sub_exc,
                                )

                        if now_loop - last_trade_diag >= 60.0:
                            logger.info(
                                "📈 Finam INSTRUMENT_TRADES 60s: messages=%d, trades=%d, queue_dropped=%d, backpressure_waits=%d.",
                                instrument_trade_messages, instrument_trade_events,
                                instrument_trade_queue_dropped, instrument_trade_queue_backpressure,
                            )
                            instrument_trade_messages = 0
                            instrument_trade_events = 0
                            instrument_trade_queue_dropped = 0
                            instrument_trade_queue_backpressure = 0
                            last_trade_diag = now_loop

                        if now_loop - last_order_book_diag >= 60.0:
                            invalid_reason_summary = ", ".join(
                                f"{reason}:{count}"
                                for reason, count in sorted(
                                    order_book_invalid_reasons.items(),
                                    key=lambda item: item[1],
                                    reverse=True,
                                )[:8]
                            ) or "-"
                            invalid_ticker_summary = ", ".join(
                                f"{ticker}:{count}"
                                for ticker, count in sorted(
                                    order_book_invalid_by_ticker.items(),
                                    key=lambda item: item[1],
                                    reverse=True,
                                )[:12]
                            ) or "-"

                            position_diag_parts: list[str] = []
                            position_gaps: list[str] = []
                            for ticker in sorted(last_open_position_symbols):
                                symbol = f"{ticker}@MISX"
                                stats = _order_book_symbol_stats(ticker)
                                subscribe_requested = symbol in order_book_subscribed
                                subscribed_since = order_book_subscribed_since.get(symbol, 0.0)
                                last_wire = float(stats.get("last_wire_monotonic", 0.0) or 0.0)
                                last_parsed = float(stats.get("last_parsed_monotonic", 0.0) or 0.0)
                                last_queued = float(stats.get("last_queued_monotonic", 0.0) or 0.0)
                                wire_age = now_loop - last_wire if last_wire > 0.0 else None
                                sub_age = now_loop - subscribed_since if subscribed_since > 0.0 else None
                                parsed_age = now_loop - last_parsed if last_parsed > 0.0 else None
                                queued_age = now_loop - last_queued if last_queued > 0.0 else None

                                flags: list[str] = []
                                if not subscribe_requested:
                                    flags.append("NO_SUBSCRIBE_REQUEST")
                                    position_gaps.append(f"{ticker}:NO_SUBSCRIBE_REQUEST")
                                elif wire_age is None:
                                    flags.append("NO_WIRE_DATA")
                                    position_gaps.append(f"{ticker}:NO_WIRE_DATA")
                                elif wire_age >= 60.0:
                                    flags.append("WIRE_STALE")
                                    position_gaps.append(f"{ticker}:WIRE_STALE={wire_age:.1f}s")
                                if stats["window_snapshot_wait"] > 0:
                                    flags.append("WAIT_SNAPSHOT")
                                    position_gaps.append(f"{ticker}:WAIT_SNAPSHOT={stats['window_snapshot_wait']}")
                                if stats["window_wire"] > 0 and stats["window_parsed"] == 0:
                                    flags.append("PARSE_EMPTY")
                                    position_gaps.append(f"{ticker}:PARSE_EMPTY")
                                if stats["window_dropped"] > 0:
                                    flags.append("QUEUE_DROP")
                                    position_gaps.append(f"{ticker}:QUEUE_DROP={stats['window_dropped']}")

                                sub_age_text = f"{sub_age:.1f}s" if sub_age is not None else "нет"
                                wire_age_text = f"{wire_age:.1f}s" if wire_age is not None else "нет"
                                parsed_age_text = f"{parsed_age:.1f}s" if parsed_age is not None else "нет"
                                queued_age_text = f"{queued_age:.1f}s" if queued_age is not None else "нет"
                                position_diag_parts.append(
                                    f"{ticker}(sub_req_local={'Y' if subscribe_requested else 'N'},"
                                    f"sub_req_age={sub_age_text},wire={stats['window_wire']},"
                                    f"wire_age={wire_age_text},parsed={stats['window_parsed']},"
                                    f"parse_age={parsed_age_text},queued={stats['window_queued']},"
                                    f"queue_age={queued_age_text},"
                                    f"no_update={stats['window_parse_no_update']},"
                                    f"parse_ooo={stats['window_parse_out_of_order']},"
                                    f"unparsed={stats['window_parse_unrecognized']},"
                                    f"dropped={stats['window_dropped']},"
                                    f"flags={','.join(flags) or 'OK'})"
                                )

                            position_diag_summary = " | ".join(position_diag_parts) or "-"
                            logger.info(
                                "📚 [ORDER_BOOK 60s] подписок=%d, сообщений=%d, "
                                "событий_в_очередь=%d, затронуто_сторон_BBO=%d, "
                                "heartbeat_без_движения_BBO=%d, некорректных_состояний=%d "
                                "(причины=%s; тикеры=%s), CROSSED_BBO_эпизодов=%d, "
                                "без_timestamp_envelope=%d, out_of_order=%d, потерь_очереди=%d, "
                                "coalesced_ORDER_BOOK=%d, coalesced_QUOTES=%d, "
                                "ошибок_разбора=%d; позиции=[%s]; сообщений_без_тикера=%d.",
                                len(order_book_subscribed), order_book_messages,
                                enqueued_order_book_events, enqueued_bbo_side_updates,
                                order_book_valid_no_bbo_delta, order_book_invalid_states,
                                invalid_reason_summary, invalid_ticker_summary,
                                order_book_crossed_episodes, order_book_missing_envelope_timestamp,
                                order_book_out_of_order, order_book_queue_dropped,
                                order_book_events_coalesced, quote_events_coalesced,
                                order_book_decode_failures,
                                position_diag_summary, order_book_unattributed_messages,
                            )
                            if position_gaps:
                                logger.warning(
                                    "📚 [ORDER_BOOK POSITION GAP] %s",
                                    " | ".join(sorted(set(position_gaps))),
                                )
                            if order_book_parser_sample:
                                logger.warning(
                                    "📚 [ORDER_BOOK PARSER SAMPLE] %s",
                                    order_book_parser_sample,
                                )
                            if order_book_unattributed_sample:
                                logger.warning(
                                    "📚 [ORDER_BOOK UNATTRIBUTED SAMPLE] %s",
                                    order_book_unattributed_sample,
                                )

                            if order_book_out_of_order_age_deltas_ms:
                                ordered_ooo_ages = sorted(order_book_out_of_order_age_deltas_ms)
                                logger.info(
                                    "📚 [ORDER_BOOK OUT_OF_ORDER 60s] events=%d age_delta_ms_min=%.1f "
                                    "avg=%.1f max=%.1f",
                                    len(ordered_ooo_ages), ordered_ooo_ages[0],
                                    sum(ordered_ooo_ages) / len(ordered_ooo_ages), ordered_ooo_ages[-1],
                                )
                            else:
                                logger.info("[ORDER_BOOK OUT_OF_ORDER 60s] events=0")
                            order_book_out_of_order_age_deltas_ms.clear()
                            order_book_messages = 0
                            order_book_decode_failures = 0
                            order_book_valid_no_bbo_delta = 0
                            order_book_invalid_states = 0
                            order_book_crossed_episodes = 0
                            order_book_invalid_reasons.clear()
                            order_book_invalid_by_ticker.clear()
                            order_book_missing_envelope_timestamp = 0
                            order_book_out_of_order = 0
                            order_book_queue_dropped = 0
                            order_book_events_coalesced = 0
                            quote_events_coalesced = 0
                            enqueued_order_book_events = 0
                            enqueued_bbo_side_updates = 0
                            order_book_unattributed_messages = 0
                            order_book_unattributed_sample = ""
                            order_book_parser_sample = ""
                            # Keep every symbol's window counters on the same 60-second
                            # boundary, including symbols that become positions later.
                            for symbol_stats in order_book_symbol_diag.values():
                                for key in (
                                    "window_wire", "window_parse_no_update",
                                    "window_parse_out_of_order", "window_parse_unrecognized",
                                    "window_snapshot_wait", "window_parsed", "window_queued", "window_dropped",
                                ):
                                    symbol_stats[key] = 0
                            last_order_book_diag = now_loop

                        try:
                            # Wake periodically even during a quiet market so a newly
                            # selected Top-15 can be subscribed without waiting for
                            # the next unrelated market-data packet.
                            raw_message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            quiet_for = asyncio.get_running_loop().time() - last_ws_data_at
                            now_warning = asyncio.get_running_loop().time()
                            if quiet_for >= 30.0 and now_warning - last_no_data_warning_at >= 30.0:
                                logger.warning(
                                    "Finam WS: %.0f секунд без входящих сообщений; "
                                    "DATA=%d, котировок в очереди=%d, ORDER_BOOK сообщений=%d, "
                                    "ORDER_BOOK событий в очереди=%d, потерь очереди=%d, подписок стакана=%d. "
                                    "Соединение оставляем открытым.",
                                    quiet_for, data_messages, enqueued_quotes,
                                    order_book_messages, enqueued_order_book_events,
                                    order_book_queue_dropped, len(order_book_subscribed),
                                )
                                last_no_data_warning_at = now_warning
                            continue

                        try:
                            envelope = json.loads(raw_message)
                        except (TypeError, json.JSONDecodeError):
                            logger.warning("Пропущено некорректное JSON-сообщение Finam WS.")
                            continue
                        if not isinstance(envelope, dict):
                            logger.warning(
                                "Finam WS прислал JSON не-объектного типа: %s",
                                type(envelope).__name__,
                            )
                            continue
                        # Preserve the socket-ingress time so queue backlog cannot
                        # turn an old message without a provider timestamp into a
                        # fresh BBO confirmation when the worker finally handles it.
                        envelope["_received_monotonic"] = time.monotonic()

                        message_type = str(envelope.get("type", "")).upper()
                        if message_type == "ERROR" or envelope.get("error_info"):
                            error_info = envelope.get("error_info", envelope)
                            logger.error("Ошибка подписки/потока Finam: %s", error_info)
                            if "maximum number of symbols exceeded" in str(error_info).lower():
                                raise RuntimeError(
                                    f"Finam отклонил размер пакета символов: {error_info}"
                                )
                            continue

                        if message_type == "EVENT":
                            event_info = envelope.get("event_info", {})
                            logger.info("Событие Finam WS: %s", event_info)
                            if (
                                isinstance(event_info, dict)
                                and str(event_info.get("event", "")).upper() == "CONNECTION_CLOSED"
                            ):
                                raise RuntimeError(f"Finam закрыл WebSocket: {event_info}")
                            continue

                        if message_type != "DATA":
                            logger.debug("Неизвестное сообщение Finam WS: type=%r", message_type)
                            continue

                        data_messages += 1
                        last_ws_data_at = asyncio.get_running_loop().time()
                        subscription_type = str(envelope.get("subscription_type", "")).upper()
                        raw_payload = envelope.get("payload", {})
                        payload = self._decode_payload(raw_payload)

                        if not first_data_logged:
                            payload_keys = list(payload.keys()) if isinstance(payload, dict) else []
                            logger.info(
                                "Получено первое DATA от Finam: subscription_type=%s, "
                                "payload_type=%s, payload_keys=%s",
                                subscription_type or "<не указан>",
                                type(payload).__name__,
                                payload_keys,
                            )
                            first_data_logged = True

                        if subscription_type in {"ORDER_BOOK", "ORDERBOOK"}:
                            order_book_messages += 1
                            message_received_monotonic = float(
                                envelope.get("_received_monotonic", time.monotonic())
                            )
                            symbol_hints = self._extract_order_book_symbol_hints(payload)
                            book_updates = self._extract_order_book_updates(envelope)
                            for ooo_detail in self._last_order_book_out_of_order_details.values():
                                try:
                                    age_delta_ms = float(ooo_detail.get("age_delta_ms", 0.0))
                                except (TypeError, ValueError, OverflowError):
                                    continue
                                if math.isfinite(age_delta_ms) and age_delta_ms >= 0.0:
                                    order_book_out_of_order_age_deltas_ms.append(age_delta_ms)
                            # A rejected out-of-order delta breaks confidence in the local
                            # delta chain. Resubscribe under a per-symbol cooldown and wait
                            # for a provider snapshot before accepting another book update.
                            for stale_ticker in sorted(self._last_order_book_out_of_order_tickers):
                                stale_symbol = f"{stale_ticker}@MISX"
                                last_resubscribe = self._crossed_bbo_resubscribe_at.get(stale_ticker, 0.0)
                                if stale_symbol not in order_book_subscribed or message_received_monotonic - last_resubscribe < crossed_resubscribe_cooldown_sec:
                                    continue
                                self._order_book_snapshot_required.add(stale_ticker)
                                self._order_book_state.pop(stale_ticker, None)
                                self._crossed_bbo_resubscribe_at[stale_ticker] = message_received_monotonic
                                try:
                                    await ws.send(json.dumps({"action":"UNSUBSCRIBE","type":"ORDER_BOOK","data":{"symbol":stale_symbol},"token":self.jwt_token}))
                                    await asyncio.sleep(0.02)
                                    await ws.send(json.dumps({"action":"SUBSCRIBE","type":"ORDER_BOOK","data":{"symbol":stale_symbol},"token":self.jwt_token}))
                                    order_book_subscribed_since[stale_symbol] = message_received_monotonic
                                    detail = self._last_order_book_out_of_order_details.get(stale_ticker, {})
                                    logger.warning(
                                        "[ORDER_BOOK OUT_OF_ORDER RESYNC] %s: delta chain invalidated; "
                                        "age_delta_ms=%.1f event_ts=%s prior_event_monotonic=%.6f; waiting for full snapshot.",
                                        stale_ticker,
                                        float(detail.get("age_delta_ms", 0.0) or 0.0),
                                        str(detail.get("event_timestamp") or "-")[:48],
                                        float(detail.get("prior_monotonic", 0.0) or 0.0),
                                    )
                                except Exception as resync_exc:
                                    order_book_subscribed.discard(stale_symbol)
                                    order_book_subscribed_since.pop(stale_symbol, None)
                                    logger.warning("[ORDER_BOOK OUT_OF_ORDER RESYNC FAILED] %s: %s; subscription will retry.", stale_ticker, type(resync_exc).__name__)
                            update_tickers = {
                                str(item.get("ticker") or "").strip().upper().split("@")[0]
                                for item in book_updates
                                if isinstance(item, dict) and item.get("ticker")
                            }
                            observed_tickers = symbol_hints | update_tickers
                            parse_missing_tickers = symbol_hints - update_tickers
                            if not self._last_order_book_payload_valid:
                                self._last_order_book_unrecognized_tickers.update(
                                    parse_missing_tickers
                                    - self._last_order_book_out_of_order_tickers
                                    - self._last_order_book_snapshot_wait_tickers
                                )
                            if not observed_tickers:
                                order_book_unattributed_messages += 1
                                if not order_book_unattributed_sample:
                                    safe_payload = self._redact_for_log(payload)
                                    order_book_unattributed_sample = json.dumps(
                                        safe_payload, ensure_ascii=False, default=str
                                    )[:1200]
                            elif parse_missing_tickers and not order_book_parser_sample:
                                safe_payload = self._redact_for_log(payload)
                                out_of_order_symbols = parse_missing_tickers & self._last_order_book_out_of_order_tickers
                                unrecognized_symbols = parse_missing_tickers & self._last_order_book_unrecognized_tickers
                                snapshot_wait_symbols = parse_missing_tickers & self._last_order_book_snapshot_wait_tickers
                                classifications = []
                                if out_of_order_symbols:
                                    classifications.append("OUT_OF_ORDER=" + ",".join(sorted(out_of_order_symbols)))
                                if unrecognized_symbols:
                                    classifications.append("UNRECOGNIZED=" + ",".join(sorted(unrecognized_symbols)))
                                if snapshot_wait_symbols:
                                    classifications.append("WAIT_SNAPSHOT=" + ",".join(sorted(snapshot_wait_symbols)))
                                if not classifications:
                                    classifications.append("NO_UPDATE=unknown")
                                detail = {
                                    symbol: self._last_order_book_out_of_order_details[symbol]
                                    for symbol in sorted(out_of_order_symbols)
                                    if symbol in self._last_order_book_out_of_order_details
                                }
                                order_book_parser_sample = (
                                    f"classification={'/'.join(classifications)}; "
                                    f"out_of_order_detail={json.dumps(detail, ensure_ascii=False, default=str)}; "
                                    f"symbols={','.join(sorted(parse_missing_tickers))}; payload="
                                    + json.dumps(safe_payload, ensure_ascii=False, default=str)[:1000]
                                )
                            for observed_ticker in observed_tickers:
                                symbol_stats = _order_book_symbol_stats(observed_ticker)
                                symbol_stats["window_wire"] += 1
                                symbol_stats["last_wire_monotonic"] = message_received_monotonic
                                if observed_ticker not in update_tickers:
                                    symbol_stats["window_parse_no_update"] += 1
                                    if observed_ticker in self._last_order_book_snapshot_wait_tickers:
                                        symbol_stats["window_snapshot_wait"] += 1
                                    elif observed_ticker in self._last_order_book_out_of_order_tickers:
                                        symbol_stats["window_parse_out_of_order"] += 1
                                    elif (
                                        observed_ticker in self._last_order_book_unrecognized_tickers
                                        or not self._last_order_book_payload_valid
                                    ):
                                        symbol_stats["window_parse_unrecognized"] += 1
                            # Count per-symbol drops even when another symbol in the
                            # same envelope produced a valid update.
                            order_book_out_of_order += len(self._last_order_book_out_of_order_tickers)
                            order_book_decode_failures += len(self._last_order_book_unrecognized_tickers)
                            if (
                                not book_updates
                                and not self._last_order_book_payload_valid
                                and not self._last_order_book_snapshot_wait_tickers
                            ):
                                if not order_book_decode_warned:
                                    safe_envelope = self._redact_for_log(envelope)
                                    logger.warning(
                                        "Finam ORDER_BOOK: не удалось разобрать структуру стакана: "
                                        "payload_keys=%s, sample=%s",
                                        list(payload.keys()) if isinstance(payload, dict) else [],
                                        json.dumps(safe_envelope, ensure_ascii=False, default=str)[:1800],
                                    )
                                    order_book_decode_warned = True
                            if book_updates and not first_order_book_logged:
                                sample = next(
                                    (item for item in book_updates if item.get("book_valid", True)),
                                    None,
                                )
                                if sample is not None:
                                    logger.info(
                                        "✅ Finam ORDER_BOOK активен: %s Bid=%s×%s Ask=%s×%s; "
                                        "timestamp_source=%s.",
                                        sample["ticker"], sample["bid"], sample["bid_size"],
                                        sample["ask"], sample["ask_size"],
                                        sample.get("timestamp_source", "UNKNOWN"),
                                    )
                                    first_order_book_logged = True
                            for book_update in book_updates:
                                book_ticker = str(book_update.get("ticker") or "").strip().upper().split("@")[0]
                                symbol_stats = _order_book_symbol_stats(book_ticker) if book_ticker else None
                                if symbol_stats is not None:
                                    symbol_stats["window_parsed"] += 1
                                    symbol_stats["last_parsed_monotonic"] = message_received_monotonic
                                has_bbo_side_update = bool(
                                    book_update.get("bid_present") or book_update.get("ask_present")
                                )
                                if book_update.get("timestamp_source") == "RECEIVE_FALLBACK":
                                    order_book_missing_envelope_timestamp += 1
                                if not book_update.get("book_valid", True):
                                    order_book_invalid_states += 1
                                    invalid_reason = str(book_update.get("book_reason") or "UNKNOWN").upper()
                                    invalid_ticker = str(book_update.get("ticker") or "UNKNOWN").upper()
                                    invalid_ticker_reason = f"{invalid_ticker}/{invalid_reason}"
                                    order_book_invalid_reasons[invalid_reason] = (
                                        order_book_invalid_reasons.get(invalid_reason, 0) + 1
                                    )
                                    order_book_invalid_by_ticker[invalid_ticker_reason] = (
                                        order_book_invalid_by_ticker.get(invalid_ticker_reason, 0) + 1
                                    )
                                    if invalid_reason == "CROSSED_BBO":
                                        if book_update.get("crossed_started"):
                                            order_book_crossed_episodes += 1
                                        now_monotonic = time.monotonic()
                                        last_detail_log = self._crossed_bbo_log_last_at.get(invalid_ticker, 0.0)
                                        if now_monotonic - last_detail_log >= 15.0:
                                            self._crossed_bbo_log_last_at[invalid_ticker] = now_monotonic
                                            bid_price = float(book_update.get("bid") or 0.0)
                                            ask_price = float(book_update.get("ask") or 0.0)
                                            midpoint = (bid_price + ask_price) / 2.0
                                            crossed_bps = (
                                                (bid_price - ask_price) / midpoint * 10000.0
                                                if midpoint > 0.0 else 0.0
                                            )
                                            bid_depth = book_update.get("bid_levels")
                                            ask_depth = book_update.get("ask_levels")
                                            bid_top = bid_depth[:3] if isinstance(bid_depth, list) else []
                                            ask_top = ask_depth[:3] if isinstance(ask_depth, list) else []
                                            logger.warning(
                                                "[ORDER_BOOK CROSSED_BBO] %s: Bid=%.6f x %s, Ask=%.6f x %s; "
                                                "cross=%.2f bps; previous Bid/Ask=%s/%s; "
                                                "side-touched Bid/Ask=%s/%s; best-level-touched=%s/%s; "
                                                "delta-levels Bid=%s Ask=%s; top3 Bid=%s Ask=%s; "
                                                "event_timestamp=%s; timestamp_source=%s; crossed_age=%.3fs.",
                                                invalid_ticker,
                                                bid_price,
                                                book_update.get("bid_size"),
                                                ask_price,
                                                book_update.get("ask_size"),
                                                crossed_bps,
                                                book_update.get("previous_bid"),
                                                book_update.get("previous_ask"),
                                                book_update.get("bid_side_touched"),
                                                book_update.get("ask_side_touched"),
                                                book_update.get("bid_best_level_touched"),
                                                book_update.get("ask_best_level_touched"),
                                                book_update.get("bid_touched_levels"),
                                                book_update.get("ask_touched_levels"),
                                                bid_top,
                                                ask_top,
                                                book_update.get("order_book_timestamp"),
                                                book_update.get("timestamp_source", "UNKNOWN"),
                                                float(book_update.get("crossed_duration_sec") or 0.0),
                                            )
                                        crossed_age_sec = float(book_update.get("crossed_duration_sec") or 0.0)
                                        last_resubscribe = self._crossed_bbo_resubscribe_at.get(invalid_ticker, 0.0)
                                        symbol = f"{invalid_ticker}@MISX"
                                        if (
                                            crossed_age_sec >= crossed_resubscribe_after_sec
                                            and symbol in order_book_subscribed
                                            and now_monotonic - last_resubscribe >= crossed_resubscribe_cooldown_sec
                                        ):
                                            # A long-lived crossed state can indicate a poisoned delta chain.
                                            # Restart the subscription and wait for a full provider snapshot.
                                            self._order_book_snapshot_required.add(invalid_ticker)
                                            self._last_order_book_snapshot_wait_tickers.discard(invalid_ticker)
                                            self._order_book_state.pop(invalid_ticker, None)
                                            self._crossed_bbo_since.pop(invalid_ticker, None)
                                            self._crossed_bbo_resubscribe_at[invalid_ticker] = now_monotonic
                                            try:
                                                await ws.send(json.dumps({
                                                    "action": "UNSUBSCRIBE", "type": "ORDER_BOOK",
                                                    "data": {"symbol": symbol}, "token": self.jwt_token,
                                                }))
                                                await asyncio.sleep(0.05)
                                                await ws.send(json.dumps({
                                                    "action": "SUBSCRIBE", "type": "ORDER_BOOK",
                                                    "data": {"symbol": symbol}, "token": self.jwt_token,
                                                }))
                                                order_book_subscribed_since[symbol] = now_loop
                                                logger.warning(
                                                    "🔄 [ORDER_BOOK RESYNC] %s: persistent CROSSED_BBO %.1fs; "
                                                    "subscription restarted, waiting for full snapshot.",
                                                    invalid_ticker, crossed_age_sec,
                                                )
                                            except Exception as resubscribe_exc:
                                                logger.error(
                                                    "[ORDER_BOOK RESYNC FAILED] %s: %s; BBO remains invalid.",
                                                    invalid_ticker, resubscribe_exc,
                                                )
                                else:
                                    if book_update.get("crossed_recovered"):
                                        logger.warning(
                                            "[ORDER_BOOK CROSSED_RECOVERY] %s: согласованный BBO восстановлен "
                                            "через %.3fs; Bid=%.6f Ask=%.6f.",
                                            book_ticker,
                                            float(book_update.get("crossed_recovered_after_sec") or 0.0),
                                            float(book_update.get("bid") or 0.0),
                                            float(book_update.get("ask") or 0.0),
                                        )
                                if book_update.get("book_valid", True) and not has_bbo_side_update:
                                    order_book_valid_no_bbo_delta += 1
                                try:
                                    queue.put_nowait(book_update)
                                    enqueued_order_book_events += 1
                                    if symbol_stats is not None:
                                        symbol_stats["window_queued"] += 1
                                        symbol_stats["last_queued_monotonic"] = message_received_monotonic
                                    if book_update.get("bid_present"):
                                        enqueued_bbo_side_updates += 1
                                    if book_update.get("ask_present"):
                                        enqueued_bbo_side_updates += 1
                                except asyncio.QueueFull:
                                    if _coalesce_queued_market_event(queue, book_update):
                                        order_book_events_coalesced += 1
                                        if symbol_stats is not None:
                                            symbol_stats["window_coalesced"] = symbol_stats.get("window_coalesced", 0) + 1
                                    else:
                                        order_book_queue_dropped += 1
                                        if symbol_stats is not None:
                                            symbol_stats["window_dropped"] += 1
                                        _record_queue_overflow("ORDER_BOOK", book_update.get("ticker"))
                            continue

                        if subscription_type in {"INSTRUMENT_TRADES", "INSTRUMENT_TRADE"}:
                            instrument_trade_messages += 1
                            trade_events = self._extract_instrument_trades(envelope)
                            instrument_trade_events += len(trade_events)
                            for trade_event in trade_events:
                                try:
                                    queue.put_nowait(trade_event)
                                except asyncio.QueueFull:
                                    # Individual exchange prints are not replaceable snapshots.
                                    # Apply bounded backpressure instead of silently losing
                                    # trade-flow evidence when the consumer briefly falls behind.
                                    instrument_trade_queue_backpressure += 1
                                    await queue.put(trade_event)
                            if trade_events and not first_trade_logged:
                                logger.info(
                                    "📈 Первые сделки Finam: распознано=%d, тикер=%s, side=%s.",
                                    len(trade_events), trade_events[0].get("ticker"), trade_events[0].get("side"),
                                )
                                first_trade_logged = True
                            continue

                        if subscription_type and subscription_type != "QUOTES":
                            continue

                        ticks = self._extract_quotes(envelope)
                        if not ticks:
                            # The documented format is payload.quote[], but the log so far
                            # shows an empty payload. Emit a short, credential-redacted sample
                            # of the actual server envelope so the next diagnosis is based on
                            # wire data, not guessed field names.
                            if data_messages <= 3:
                                candidate = None
                                if isinstance(payload, dict):
                                    candidate = payload.get("quote") or payload.get("quotes")
                                    if isinstance(candidate, list) and candidate:
                                        candidate = candidate[0]
                                safe_envelope = self._redact_for_log(envelope)
                                raw_sample = json.dumps(
                                    safe_envelope, ensure_ascii=False, default=str
                                )[:2500]
                                logger.warning(
                                    "Finam DATA без распознанной котировки: subscription_type=%s, "
                                    "envelope_keys=%s, payload_type=%s, payload_keys=%s, "
                                    "first_quote_keys=%s, sample=%s",
                                    subscription_type or "<не указан>",
                                    list(envelope.keys()),
                                    type(payload).__name__,
                                    list(payload.keys()) if isinstance(payload, dict) else [],
                                    list(candidate.keys()) if isinstance(candidate, dict) else [],
                                    raw_sample,
                                )
                            continue

                        for tick in ticks:
                            try:
                                queue.put_nowait(tick)
                            except asyncio.QueueFull:
                                if _coalesce_queued_market_event(queue, tick):
                                    quote_events_coalesced += 1
                                else:
                                    _record_queue_overflow("QUOTES", tick.get("ticker"))
                                continue

                            enqueued_quotes += 1
                            if enqueued_quotes == 1:
                                logger.info(
                                    "✅ Первая котировка Finam передана в очередь: "
                                    "%s price=%s volume=%s bid=%s ask=%s bid_size=%s ask_size=%s "
                                    "presence=(%s,%s,%s,%s) timestamp=%s",
                                    tick.get("ticker"), tick.get("price"), tick.get("volume"),
                                    tick.get("bid"), tick.get("ask"), tick.get("bid_size"), tick.get("ask_size"),
                                    tick.get("bid_present"), tick.get("ask_present"),
                                    tick.get("bid_size_present"), tick.get("ask_size_present"),
                                    tick.get("timestamp"),
                                )
                            elif enqueued_quotes % 100 == 0:
                                logger.info(
                                    "Поток Finam активен: DATA=%d, котировок в очередь=%d; "
                                    "последний тик %s=%s",
                                    data_messages,
                                    enqueued_quotes,
                                    tick.get("ticker"),
                                    tick.get("price"),
                                )

                    if self.is_ws_running:
                        raise RuntimeError("Finam WebSocket завершил поток без команды остановки.")

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                if not self.is_ws_running:
                    break

                if "maximum number of symbols exceeded" in str(exc).lower():
                    if batch_size > 1:
                        previous_size = batch_size
                        batch_size = max(1, batch_size // 2)
                        batches = [
                            symbols[i:i + batch_size]
                            for i in range(0, len(symbols), batch_size)
                        ]
                        backoff = 1
                        logger.warning(
                            "Finam отклонил размер пакета %d. Уменьшаем до %d и повторяем подписку.",
                            previous_size,
                            batch_size,
                        )
                        refresh_token_before_retry = True
                        await asyncio.sleep(0.5)
                        continue

                    logger.critical(
                        "Finam отклоняет даже пакет из одного символа. Вероятно, достигнут "
                        "лимит подписок аккаунта/тарифа или есть ограничение на соединение. "
                        "Автоповторы остановлены; проверьте лимиты Finam и список инструментов."
                    )
                    self.is_ws_running = False
                    break

                close_code = _websocket_close_code(exc)
                if _websocket_disconnect_log_level(exc) == logging.INFO:
                    logger.info(
                        "Finam WebSocket штатно закрыл соединение (code=%s): %s. "
                        "Переподключение через %d сек.",
                        close_code if close_code is not None else "unknown",
                        exc,
                        backoff,
                    )
                else:
                    logger.error(
                        "Finam WebSocket отключён/не подключён (close_code=%s): %s. "
                        "Повтор через %d сек.",
                        close_code if close_code is not None else "unknown",
                        exc,
                        backoff,
                    )
                refresh_token_before_retry = True
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def stop_market_data_stream(self):
        self.is_ws_running = False
