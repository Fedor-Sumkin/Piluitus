"""Finam order-book snapshot refresh and REST depth-query mixin."""
from __future__ import annotations

from exchange.finam_shared import (
    asyncio, logging, math, datetime, timezone, timedelta, url_quote,
    Callable, Iterable, Any, json, inspect, os, re, time, websockets, httpx,
    logger, JOURNAL_FILE, _MOSCOW_TZ,
    _websocket_close_code, _websocket_disconnect_log_level,
    _coalesce_queued_market_event, _event_timestamp_to_monotonic,
    _should_resync_position_order_book,
)

class FinamOrderBookMixin:
    async def refresh_order_book_snapshot(self, ticker: str, queue: asyncio.Queue) -> bool:
        """Fetch a coherent control snapshot; REST sizes remain informational only."""
        symbol = str(ticker or "").strip().upper().split("@")[0]
        if not symbol:
            return False
        book = await self.get_ticker_order_book(symbol)
        if not isinstance(book, dict):
            return False

        def _levels(raw: object) -> list[dict[str, float]]:
            result: list[dict[str, float]] = []
            if not isinstance(raw, list):
                return result
            for row in raw:
                if not isinstance(row, dict):
                    continue
                price = self._decimal(row.get("price"))
                size = self._decimal(row.get("size", row.get("quantity", row.get("volume"))))
                if price > 0.0 and size > 0.0:
                    result.append({"price": price, "size": size})
            return result

        bids = _levels(book.get("bids"))
        asks = _levels(book.get("asks"))
        if not bids or not asks:
            logger.warning("Finam REST snapshot %s: нет обеих сторон BBO; старое состояние не заменяем.", symbol)
            return False
        best_bid = max(row["price"] for row in bids)
        best_ask = min(row["price"] for row in asks)
        if best_bid <= 0.0 or best_ask < best_bid:
            logger.warning(
                "Finam REST snapshot %s отклонён: некогерентный BBO Bid=%.6f Ask=%.6f.",
                symbol, best_bid, best_ask,
            )
            return False

        # REST does not expose a trustworthy exchange-event timestamp in this
        # adapter. Its receive time must never be promoted to a fresh WS event:
        # doing so would mutate the delta chain and could manufacture an
        # executable BBO. Keep this snapshot strictly diagnostic.
        mid = (best_bid + best_ask) / 2.0
        spread_pct = ((best_ask - best_bid) / mid * 100.0) if mid > 0.0 else math.inf
        state = self._order_book_state.get(symbol, {})
        ws_bids = state.get("bids", {}) if isinstance(state, dict) else {}
        ws_asks = state.get("asks", {}) if isinstance(state, dict) else {}
        ws_bid = max(ws_bids) if isinstance(ws_bids, dict) and ws_bids else 0.0
        ws_ask = min(ws_asks) if isinstance(ws_asks, dict) and ws_asks else 0.0
        if ws_bid > 0.0 and ws_ask >= ws_bid and ws_ask > 0.0:
            bid_delta_pct = (best_bid - ws_bid) / ws_bid * 100.0
            ask_delta_pct = (best_ask - ws_ask) / ws_ask * 100.0
            ws_text = (
                f"WS Bid={ws_bid:.6f} Ask={ws_ask:.6f}; "
                f"REST-vs-WS delta Bid={bid_delta_pct:+.3f}% Ask={ask_delta_pct:+.3f}%"
            )
        else:
            ws_text = "WS BBO=нет"
        logger.warning(
            "📚 [ORDER_BOOK REST CONTROL] %s: REST Bid=%.6f Ask=%.6f spread=%.3f%%; %s; "
            "снимок только диагностический: не помещён в торговую очередь, "
            "не изменяет WS delta-chain, подтверждения BBO или таймеры свежести.",
            symbol, best_bid, best_ask, spread_pct, ws_text,
        )
        return True

    # ------------------------------------------------------------------
    # 3️⃣  GET_TICKER_ORDER_BOOK
    # ------------------------------------------------------------------

    async def get_ticker_order_book(self, ticker: str) -> dict | None:
        """Fetch a diagnostic snapshot with per-symbol backoff; never changes WS BBO."""
        symbol = str(ticker or "").strip().upper().split("@", 1)[0]
        if not symbol:
            return {"bids": [], "asks": []}
        sdk_error: Exception | None = None
        try:
            service = getattr(self.client, "market_data", None) or getattr(self.client, "instruments", None)
            if service and hasattr(service, "get_order_book"):
                call = service.get_order_book(security_board="TQBR", security_code=symbol, order_book_depth=3)
                response = await call if asyncio.iscoroutine(call) else call
                if response:
                    bids = [{"price": self._decimal(getattr(x, "price", x)), "volume": self._decimal(getattr(x, "quantity", 0))} for x in getattr(response, "bids", [])]
                    asks = [{"price": self._decimal(getattr(x, "price", x)), "volume": self._decimal(getattr(x, "quantity", 0))} for x in getattr(response, "asks", [])]
                    bids = [x for x in bids if x["price"] > 0 and x["volume"] > 0]
                    asks = [x for x in asks if x["price"] > 0 and x["volume"] > 0]
                    if bids and asks:
                        self._order_book_http_backoff.pop(symbol, None)
                        return {"bids": bids[:3], "asks": asks[:3]}
        except Exception as exc:
            sdk_error = exc
            logger.debug("Finam SDK orderbook fallback ticker=%s error_type=%s detail=%s", symbol, type(exc).__name__, str(exc)[:240])

        now = time.monotonic()
        next_allowed, failures = self._order_book_http_backoff.get(symbol, (0.0, 0))
        if now < next_allowed:
            logger.debug("Finam REST orderbook backoff ticker=%s retry_in=%.1fs failures=%d", symbol, next_allowed-now, failures)
            return {"bids": [], "asks": []}

        url = f"{self.base_url}/v1/instruments/{url_quote(symbol + '@MISX', safe='@')}/orderbook"
        request_token = self.jwt_token
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(5.0, connect=2.0), http2=True) as client:
                response = await client.get(url, headers=self.headers)
                if response.status_code == 401 and request_token:
                    if await self._refresh_auth_if_unchanged(request_token):
                        response = await client.get(url, headers=self.headers)
                response.raise_for_status()
                data = response.json()
            book = data.get("orderbook", data.get("orderBook", data)) if isinstance(data, dict) else {}
            rows = book.get("rows", []) if isinstance(book, dict) else []
            bids: list[dict[str, float]] = []
            asks: list[dict[str, float]] = []
            for row in rows if isinstance(rows, list) else []:
                if not isinstance(row, dict):
                    continue
                price = self._decimal(row.get("price"))
                buy = self._decimal(row.get("buy_size", row.get("buySize")))
                sell = self._decimal(row.get("sell_size", row.get("sellSize")))
                if price > 0 and buy > 0: bids.append({"price": price, "volume": buy})
                if price > 0 and sell > 0: asks.append({"price": price, "volume": sell})
            if not bids or not asks:
                raise ValueError(f"incomplete orderbook sides: bids={len(bids)} asks={len(asks)}")
            self._order_book_http_backoff.pop(symbol, None)
            return {"bids": bids[:3], "asks": asks[:3]}
        except httpx.HTTPStatusError as exc:
            status = int(exc.response.status_code)
            failures += 1
            delay = min(60.0, 2.0 ** min(failures, 6)) if status == 429 or status >= 500 else 15.0
            self._order_book_http_backoff[symbol] = (time.monotonic() + delay, failures)
            response = exc.response
            try:
                body = json.dumps(self._redact_for_log(response.json()), ensure_ascii=False, default=str)
            except (ValueError, TypeError):
                body = response.text or ""
            body = re.sub(r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+", r"\1<redacted>", body)
            body = re.sub(r"\s+", " ", body).strip()[:300]
            request_id = response.headers.get("x-request-id") or response.headers.get("x-correlation-id") or response.headers.get("request-id") or "unavailable"
            logger.warning("Finam REST orderbook failed ticker=%s status=%d reason=%s request_id=%s retry_in=%.1fs sdk_error=%s response=%s",
                symbol, status, response.reason_phrase, request_id, delay, type(sdk_error).__name__ if sdk_error else "none", body or "<empty>")
        except (httpx.RequestError, ValueError, TypeError, json.JSONDecodeError) as exc:
            failures += 1
            delay = min(60.0, 2.0 ** min(failures, 6))
            self._order_book_http_backoff[symbol] = (time.monotonic() + delay, failures)
            logger.warning("Finam REST orderbook failed ticker=%s error_type=%s detail=%s retry_in=%.1fs sdk_error=%s",
                symbol, type(exc).__name__, str(exc)[:300], delay, type(sdk_error).__name__ if sdk_error else "none")
        except Exception as exc:
            failures += 1
            delay = min(60.0, 2.0 ** min(failures, 6))
            self._order_book_http_backoff[symbol] = (time.monotonic() + delay, failures)
            logger.exception("Unexpected Finam orderbook error ticker=%s retry_in=%.1fs sdk_error=%s",
                symbol, delay, type(sdk_error).__name__ if sdk_error else "none")
        return {"bids": [], "asks": []}

    # ------------------------------------------------------------------
    # 4️⃣  PLACE_ORDER
    # ------------------------------------------------------------------
