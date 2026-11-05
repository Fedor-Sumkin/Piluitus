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


def _crossed_bbo_recovery_due(
    now_monotonic: float,
    crossed_since: float,
    last_recovery_at: float,
    recovery_after_sec: float,
    recovery_cooldown_sec: float,
) -> bool:
    """Check whether a crossed book warrants a rate-limited snapshot resync."""
    return (
        now_monotonic >= crossed_since
        and now_monotonic - crossed_since >= recovery_after_sec
        and now_monotonic >= last_recovery_at
        and now_monotonic - last_recovery_at >= recovery_cooldown_sec
    )
_MOSCOW_TZ = timezone(timedelta(hours=3), name="MSK")


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


class FinamBrokerClient:
    def __init__(self, token: str, is_demo: bool = True):
        self.token = token
        # Единый промышленный хост REST v2 шлюзов Финам
        self.host = "api.finam.ru"
        self.base_url = f"https://{self.host}"
        # Эндпоинт WebSocket‑сервера Финам согласно спецификации API v2
        self.ws_endpoint = f"wss://{self.host}/ws"
        self.jwt_token = ""
        self.headers: dict[str, str] = {}
        # Serializes JWT refreshes triggered by concurrent REST requests.
        self._auth_refresh_lock = asyncio.Lock()
        self.is_demo = is_demo
        self.client: Any = None
        self.is_ws_running = False
        # Local level maps for ORDER_BOOK deltas.
        self._order_book_state: dict[str, dict[str, Any]] = {}
        # Last parser outcome distinguishes a valid delta with no BBO change
        # from a malformed/unrecognized payload.
        self._last_order_book_payload_valid = False
        self._last_order_book_out_of_order = False
        # Optional synchronous callback supplied by main.py. It returns tickers
        # with live positions that must retain BBO even after leaving the screener.
        self.open_positions_provider: Callable[[], Iterable[str]] | None = None
        # Finam publishes per-instrument session intervals; cache them by Moscow date.
        self._asset_schedule_cache: dict[str, tuple[float, dict[str, Any] | None]] = {}
        self._asset_schedule_locks: dict[str, asyncio.Lock] = {}
        # Per-symbol throttle for detailed crossed-BBO diagnostics.
        self._crossed_bbo_log_last_at: dict[str, float] = {}

    # ------------------------------------------------------------------
    # 1️⃣  CONNECT 
    # ------------------------------------------------------------------
    async def connect(self):
        """Получить JWT через официальный REST endpoint Finam и проверить авторизацию."""
        if not self.token or self.token == "default_demo_token":
            raise RuntimeError("FINAM_TOKEN не задан. Укажите API-токен Finam в .env.")

        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.post(
                f"{self.base_url}/v1/sessions",
                json={"secret": self.token},
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            payload = response.json()

        jwt = payload.get("token") or payload.get("access_token")
        if not jwt:
            raise RuntimeError(f"Finam не вернул JWT при авторизации: {payload!r}")

        self.jwt_token = str(jwt)
        self.headers = {
            "Authorization": f"Bearer {self.jwt_token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        }
        logger.info("Авторизация Finam Trade API подтверждена.")

    async def _refresh_auth_if_unchanged(self, rejected_token: str) -> bool:
        """Refresh the REST JWT once when a request proves the current token is rejected.

        Concurrent 401 responses share one refresh: requests that observed the old
        token reuse the newly issued JWT instead of repeatedly authenticating.
        """
        async with self._auth_refresh_lock:
            if self.jwt_token and self.jwt_token != rejected_token:
                return True
            try:
                await self.connect()
            except Exception as exc:
                logger.warning(
                    "Не удалось обновить JWT Finam после HTTP 401 (%s).",
                    type(exc).__name__,
                )
                return False
            return bool(self.jwt_token)

    async def _request_asset_schedule(
        self, url: str, jwt_token: str
    ) -> dict[str, Any]:
        """Fetch one schedule using the supplied JWT."""
        async with httpx.AsyncClient(timeout=5.0) as client:
            response = await client.get(
                url,
                headers={
                    "Authorization": f"Bearer {jwt_token}",
                    "Accept": "application/json",
                },
            )
            response.raise_for_status()
            payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError("Finam schedule response is not a JSON object")
        return payload

    async def get_asset_schedule(self, ticker: str) -> dict[str, Any] | None:
        """Return the authenticated Finam schedule for one MOEX instrument.

        The API returns explicit session intervals (including exceptional sessions).
        Cache only for the current Moscow calendar date; a failed lookup is cached
        briefly to avoid retrying the endpoint for every candidate tick.
        """
        if not self.jwt_token:
            return None

        normalized_ticker = str(ticker or "").strip().upper().split("@", 1)[0]
        if not normalized_ticker:
            return None
        symbol = f"{normalized_ticker}@MISX"
        moscow_day = datetime.now(_MOSCOW_TZ).date().isoformat()
        cache_key = f"{symbol}|{moscow_day}"
        try:
            cache_ttl = float(os.getenv("FINAM_ASSET_SCHEDULE_CACHE_SEC", "300"))
        except (TypeError, ValueError, OverflowError):
            cache_ttl = 300.0
        if not math.isfinite(cache_ttl):
            cache_ttl = 300.0
        cache_ttl = min(1800.0, max(30.0, cache_ttl))

        now = time.monotonic()
        cached = self._asset_schedule_cache.get(cache_key)
        # Negative caching is longer than a few seconds so a provider outage
        # cannot fan out retries across the full 88-symbol universe.
        failure_cache_ttl = 60.0
        if cached is not None and now - cached[0] < (cache_ttl if cached[1] is not None else failure_cache_ttl):
            return cached[1]

        lock = self._asset_schedule_locks.setdefault(cache_key, asyncio.Lock())
        async with lock:
            now = time.monotonic()
            cached = self._asset_schedule_cache.get(cache_key)
            if cached is not None and now - cached[0] < (cache_ttl if cached[1] is not None else failure_cache_ttl):
                return cached[1]
            url = f"{self.base_url}/v1/assets/{url_quote(symbol, safe='@')}/schedule"
            request_jwt = self.jwt_token
            try:
                try:
                    payload = await self._request_asset_schedule(url, request_jwt)
                except httpx.HTTPStatusError as auth_exc:
                    if auth_exc.response.status_code != 401:
                        raise
                    # Finam access JWTs expire while a long-lived WebSocket may
                    # continue streaming. Renew the JWT and retry this REST call once.
                    logger.warning(
                        "Finam отклонил REST JWT для расписания %s (HTTP 401); "
                        "обновляем сессию и повторяем запрос один раз.",
                        normalized_ticker,
                    )
                    if not await self._refresh_auth_if_unchanged(request_jwt):
                        raise
                    payload = await self._request_asset_schedule(url, self.jwt_token)

                if not isinstance(payload, dict) or not isinstance(payload.get("sessions"), list):
                    self._asset_schedule_cache[cache_key] = (time.monotonic(), None)
                    logger.warning(
                        "Finam вернул неизвестный формат расписания для %s; новые входы будут заблокированы.",
                        normalized_ticker,
                    )
                    return None
                self._asset_schedule_cache[cache_key] = (time.monotonic(), payload)
                if len(self._asset_schedule_cache) > 256:
                    self._asset_schedule_cache = {
                        key: value for key, value in self._asset_schedule_cache.items()
                        if key.endswith(f"|{moscow_day}")
                    }
                    # Drop only previous-day locks. All current-day locks remain
                    # stable so concurrent requests cannot bypass per-symbol locking.
                    self._asset_schedule_locks = {
                        key: lock for key, lock in self._asset_schedule_locks.items()
                        if key.endswith(f"|{moscow_day}")
                    }
                logger.info(
                    "📅 Finam schedule загружено: %s, интервалов=%d, кэш=%.0fs.",
                    normalized_ticker, len(payload["sessions"]), cache_ttl,
                )
                return payload
            except httpx.HTTPStatusError as exc:
                # Log the provider status and a redacted response hint so 401/403,
                # 404, 429 and 5xx failures can be distinguished without exposing JWTs.
                response = exc.response
                try:
                    response_payload = response.json()
                    if isinstance(response_payload, (dict, list)):
                        response_hint = json.dumps(
                            self._redact_for_log(response_payload),
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                    else:
                        response_hint = str(response_payload)
                except (ValueError, TypeError, json.JSONDecodeError):
                    response_hint = response.text or ""
                response_hint = re.sub(
                    r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+", r"\1<redacted>", response_hint
                )
                response_hint = re.sub(
                    r"(?i)(\"?(?:access[_-]?token|refresh[_-]?token|token|authorization|password|secret|api[_-]?key)\"?\s*[:=]\s*\"?)[^\"\s&,}]+",
                    r"\1<redacted>", response_hint,
                )
                response_hint = re.sub(r"\s+", " ", response_hint).strip()[:400]
                request_id = (
                    response.headers.get("x-request-id")
                    or response.headers.get("x-correlation-id")
                    or response.headers.get("request-id")
                    or "unavailable"
                )
                self._asset_schedule_cache[cache_key] = (time.monotonic(), None)
                logger.warning(
                    "Не удалось получить расписание Finam для %s: HTTP %s %s; "
                    "request_id=%s; content_type=%s; response=%s; новые входы заблокированы.",
                    normalized_ticker,
                    response.status_code,
                    response.reason_phrase,
                    request_id,
                    response.headers.get("content-type", "unknown"),
                    response_hint or "<empty>",
                )
                return None
            except httpx.RequestError as exc:
                self._asset_schedule_cache[cache_key] = (time.monotonic(), None)
                logger.warning(
                    "Сетевая ошибка при получении расписания Finam для %s (%s); новые входы заблокированы.",
                    normalized_ticker, type(exc).__name__,
                )
                return None
            except Exception as exc:
                self._asset_schedule_cache[cache_key] = (time.monotonic(), None)
                logger.warning(
                    "Неожиданная ошибка при получении расписания Finam для %s (%s); новые входы заблокированы.",
                    normalized_ticker, type(exc).__name__,
                )
                return None

    # ------------------------------------------------------------------
    # 2️⃣  START_MARKET_DATA_STREAM (Адаптирован под асинхронный NumPy-скринер)
    # ------------------------------------------------------------------
    @staticmethod
    def _decimal(value: Any, default: float = 0.0) -> float:
        """Convert Finam decimal fields and reject NaN/Infinity."""
        if isinstance(value, dict):
            value = value.get("value", default)
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return default
        return number if math.isfinite(number) else default

    @staticmethod
    def _redact_for_log(value: Any) -> Any:
        """Recursively redact credentials before logging a server message."""
        secret_words = ("token", "secret", "authorization", "password", "api_key", "access_key")
        if isinstance(value, dict):
            redacted = {}
            for key, item in value.items():
                key_text = str(key).lower()
                redacted[key] = "<redacted>" if any(word in key_text for word in secret_words) else FinamBrokerClient._redact_for_log(item)
            return redacted
        if isinstance(value, list):
            return [FinamBrokerClient._redact_for_log(item) for item in value[:20]]
        return value

    @staticmethod
    def _extract_order_book_symbol_hints(value: Any) -> set[str]:
        """Find instrument symbols in a decoded ORDER_BOOK envelope for diagnostics."""
        found: set[str] = set()
        symbol_keys = {
            "symbol", "symbols", "ticker", "securitycode", "symbolcode",
        }

        def add_symbol(candidate: Any) -> None:
            if isinstance(candidate, str):
                ticker = candidate.strip().upper().split("@", 1)[0]
                if ticker and len(ticker) <= 32 and not any(ch.isspace() for ch in ticker):
                    found.add(ticker)
            elif isinstance(candidate, dict):
                for nested_key, nested_value in candidate.items():
                    normalized = str(nested_key).replace("_", "").lower()
                    if normalized in symbol_keys or normalized == "code":
                        add_symbol(nested_value)
            elif isinstance(candidate, list):
                for item in candidate[:100]:
                    add_symbol(item)

        def visit(node: Any, depth: int = 0) -> None:
            if depth > 7:
                return
            if isinstance(node, dict):
                for key, child in node.items():
                    normalized = str(key).replace("_", "").lower()
                    if normalized in symbol_keys or normalized == "security":
                        add_symbol(child)
                    elif isinstance(child, (dict, list)):
                        visit(child, depth + 1)
            elif isinstance(node, list):
                for item in node[:100]:
                    visit(item, depth + 1)

        visit(value)
        return found

    @staticmethod
    def _decode_payload(payload: Any) -> Any:
        """Decode Finam's payload: it can be a dict or a JSON-encoded string."""
        if isinstance(payload, str):
            payload = payload.strip()
            if not payload:
                return {}
            try:
                return json.loads(payload)
            except json.JSONDecodeError:
                return {}
        return payload

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

    def _extract_order_book_updates(self, envelope: dict) -> list[dict[str, Any]]:
        """Apply Finam ORDER_BOOK level updates and return current top-of-book states.

        Handles the documented payload.order_book[].rows form and common direct
        bids/asks snapshots. Rows are treated as absolute price-level sizes: a
        zero size removes that side at the price. If the stream provides a
        deletion without a side or size, it is ignored rather than guessing.
        """
        self._last_order_book_payload_valid = False
        self._last_order_book_out_of_order = False
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
            if not has_level_payload and not empty_snapshot:
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

                    # Диагностические счётчики: отправка SUBSCRIBE сама по себе не
                    # означает, что сервер уже прислал полезные данные.
                    data_messages = 0
                    enqueued_quotes = 0
                    enqueued_order_book_events = 0
                    enqueued_bbo_side_updates = 0
                    first_data_logged = False
                    first_order_book_logged = False
                    order_book_decode_warned = False
                    order_book_messages = 0
                    order_book_decode_failures = 0
                    order_book_valid_no_bbo_delta = 0
                    order_book_invalid_states = 0
                    order_book_invalid_reasons: dict[str, int] = {}
                    order_book_invalid_by_ticker: dict[str, int] = {}
                    order_book_missing_envelope_timestamp = 0
                    order_book_queue_dropped = 0
                    order_book_out_of_order = 0
                    order_book_subscribed: set[str] = set()
                    order_book_subscribed_since: dict[str, float] = {}
                    order_book_symbol_diag: dict[str, dict[str, Any]] = {}
                    # Persistent crossed books may retain stale depth after a missed
                    # or rejected delta. Request a fresh snapshot for that symbol only.
                    order_book_crossed_since: dict[str, float] = {}
                    order_book_crossed_last_recovery: dict[str, float] = {}
                    order_book_crossed_last_attempt: dict[str, float] = {}
                    try:
                        crossed_recovery_sec = min(
                            30.0,
                            max(1.0, float(os.getenv("FINAM_ORDER_BOOK_CROSSED_RECOVERY_SEC", "5.0"))),
                        )
                    except (TypeError, ValueError, OverflowError):
                        crossed_recovery_sec = 5.0
                    try:
                        crossed_recovery_cooldown_sec = min(
                            300.0,
                            max(15.0, float(os.getenv("FINAM_ORDER_BOOK_CROSSED_RECOVERY_COOLDOWN_SEC", "60.0"))),
                        )
                    except (TypeError, ValueError, OverflowError):
                        crossed_recovery_cooldown_sec = 60.0
                    try:
                        crossed_recovery_retry_sec = min(
                            30.0,
                            max(1.0, float(os.getenv("FINAM_ORDER_BOOK_CROSSED_RECOVERY_RETRY_SEC", "5.0"))),
                        )
                    except (TypeError, ValueError, OverflowError):
                        crossed_recovery_retry_sec = 5.0
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
                    last_no_data_warning_at = 0.0
                    try:
                        order_book_refresh_sec = max(1.0, float(os.getenv("FINAM_ORDER_BOOK_REFRESH_SEC", "1")))
                        order_book_limit = max(1, min(15, int(os.getenv("FINAM_ORDER_BOOK_TOP_N", "15"))))
                    except (TypeError, ValueError):
                        order_book_refresh_sec = 10.0
                        order_book_limit = 15

                    while self.is_ws_running:
                        # Keep ORDER_BOOK subscriptions aligned with the current top-N.
                        # QUOTES for the full universe continue independently.
                        now_loop = asyncio.get_running_loop().time()
                        if screener is not None:
                            try:
                                funnel_status = screener.get_status()
                                selected = (
                                    funnel_status.get("selected_tickers", [])
                                    if funnel_status.get("ready") else []
                                )
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

                                sync_due = now_loop - last_order_book_sync >= order_book_refresh_sec
                                selection_changed = wanted != order_book_subscribed
                                if selection_changed or sync_due:
                                    removed = order_book_subscribed - wanted
                                    added = wanted - order_book_subscribed
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
                                            "позиций_вне_Top=%d, всего=%d, добавлено=%d, снято=%d; "
                                            "активные_позиции=%s; запрошенные_подписки_позиций=%s.",
                                            order_book_limit, len(normalized_selected),
                                            len(position_only_symbols), len(order_book_subscribed),
                                            len(added), len(removed),
                                            position_symbols_text, position_subscriptions_text,
                                        )
                            except Exception as sub_exc:
                                last_order_book_sync = now_loop
                                logger.warning(
                                    "Finam ORDER_BOOK: не удалось синхронизировать подписки, "
                                    "QUOTES продолжается: %s", sub_exc,
                                )

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
                                "(причины=%s; тикеры=%s), "
                                "без_timestamp_envelope=%d, out_of_order=%d, потерь_очереди=%d, "
                                "ошибок_разбора=%d; позиции=[%s]; сообщений_без_тикера=%d.",
                                len(order_book_subscribed), order_book_messages,
                                enqueued_order_book_events, enqueued_bbo_side_updates,
                                order_book_valid_no_bbo_delta, order_book_invalid_states,
                                invalid_reason_summary, invalid_ticker_summary,
                                order_book_missing_envelope_timestamp, order_book_out_of_order,
                                order_book_queue_dropped, order_book_decode_failures,
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

                            order_book_messages = 0
                            order_book_decode_failures = 0
                            order_book_valid_no_bbo_delta = 0
                            order_book_invalid_states = 0
                            order_book_invalid_reasons.clear()
                            order_book_invalid_by_ticker.clear()
                            order_book_missing_envelope_timestamp = 0
                            order_book_out_of_order = 0
                            order_book_queue_dropped = 0
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
                                    "window_parsed", "window_queued", "window_dropped",
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
                            update_tickers = {
                                str(item.get("ticker") or "").strip().upper().split("@")[0]
                                for item in book_updates
                                if isinstance(item, dict) and item.get("ticker")
                            }
                            observed_tickers = symbol_hints | update_tickers
                            parse_missing_tickers = symbol_hints - update_tickers
                            if not observed_tickers:
                                order_book_unattributed_messages += 1
                                if not order_book_unattributed_sample:
                                    safe_payload = self._redact_for_log(payload)
                                    order_book_unattributed_sample = json.dumps(
                                        safe_payload, ensure_ascii=False, default=str
                                    )[:1200]
                            elif parse_missing_tickers and not order_book_parser_sample:
                                safe_payload = self._redact_for_log(payload)
                                order_book_parser_sample = (
                                    f"symbols={','.join(sorted(parse_missing_tickers))}; payload="
                                    + json.dumps(safe_payload, ensure_ascii=False, default=str)[:1000]
                                )
                            for observed_ticker in observed_tickers:
                                symbol_stats = _order_book_symbol_stats(observed_ticker)
                                symbol_stats["window_wire"] += 1
                                symbol_stats["last_wire_monotonic"] = message_received_monotonic
                                if observed_ticker not in update_tickers:
                                    symbol_stats["window_parse_no_update"] += 1
                                    if self._last_order_book_out_of_order:
                                        symbol_stats["window_parse_out_of_order"] += 1
                                    elif not self._last_order_book_payload_valid:
                                        symbol_stats["window_parse_unrecognized"] += 1
                            if not book_updates:
                                if self._last_order_book_out_of_order:
                                    order_book_out_of_order += 1
                                if not self._last_order_book_payload_valid:
                                    order_book_decode_failures += 1
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
                                        now_monotonic = time.monotonic()
                                        crossed_since = order_book_crossed_since.setdefault(
                                            invalid_ticker, now_monotonic
                                        )
                                        symbol = f"{invalid_ticker}@MISX"
                                        last_recovery = order_book_crossed_last_recovery.get(invalid_ticker, 0.0)
                                        last_attempt = order_book_crossed_last_attempt.get(invalid_ticker, 0.0)
                                        recovery_due = _crossed_bbo_recovery_due(
                                            now_monotonic,
                                            crossed_since,
                                            last_recovery,
                                            crossed_recovery_sec,
                                            crossed_recovery_cooldown_sec,
                                        )
                                        retry_due = (
                                            now_monotonic >= last_attempt
                                            and now_monotonic - last_attempt >= crossed_recovery_retry_sec
                                        )
                                        if symbol in order_book_subscribed and recovery_due and retry_due:
                                            # Keep the desired-subscription marker in place while
                                            # resubscribing. If either WS send fails, the next
                                            # retry must still be eligible instead of being
                                            # permanently gated by a discarded subscription.
                                            order_book_crossed_last_attempt[invalid_ticker] = now_monotonic
                                            try:
                                                await ws.send(json.dumps({
                                                    "action": "UNSUBSCRIBE",
                                                    "type": "ORDER_BOOK",
                                                    "data": {"symbol": symbol},
                                                    "token": self.jwt_token,
                                                }))
                                                self._order_book_state.pop(invalid_ticker, None)
                                                await ws.send(json.dumps({
                                                    "action": "SUBSCRIBE",
                                                    "type": "ORDER_BOOK",
                                                    "data": {"symbol": symbol},
                                                    "token": self.jwt_token,
                                                }))
                                                order_book_subscribed_since[symbol] = time.monotonic()
                                                order_book_crossed_last_recovery[invalid_ticker] = time.monotonic()
                                                order_book_crossed_since.pop(invalid_ticker, None)
                                                logger.warning(
                                                    "🔄 [ORDER_BOOK CROSSED_RECOVERY] %s: crossed BBO persisted %.1fs; "
                                                    "fresh ORDER_BOOK snapshot requested. Execution remains blocked "
                                                    "until a valid BBO; cooldown=%.0fs, retry=%.1fs.",
                                                    invalid_ticker,
                                                    now_monotonic - crossed_since,
                                                    crossed_recovery_cooldown_sec,
                                                    crossed_recovery_retry_sec,
                                                )
                                            except Exception as recovery_exc:
                                                logger.error(
                                                    "[ORDER_BOOK CROSSED_RECOVERY] %s: resubscribe failed: %s. "
                                                    "Crossed timer retained; retry in %.1fs; BBO remains invalid.",
                                                    invalid_ticker,
                                                    recovery_exc,
                                                    crossed_recovery_retry_sec,
                                                )
                                    else:
                                        # Missing-side/other invalid states are not a
                                        # persistent crossed book; restart the timer if it
                                        # becomes crossed again later.
                                        order_book_crossed_since.pop(invalid_ticker, None)
                                    if invalid_reason == "CROSSED_BBO":
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
                                                "event_timestamp=%s; timestamp_source=%s.",
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
                                            )
                                else:
                                    # Any non-crossed state ends the persistence timer.
                                    if book_ticker:
                                        order_book_crossed_since.pop(book_ticker, None)
                                    if not has_bbo_side_update:
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
                                    order_book_queue_dropped += 1
                                    if symbol_stats is not None:
                                        symbol_stats["window_dropped"] += 1
                                    logger.warning(
                                        "Очередь рыночных событий заполнена; ORDER_BOOK %s отброшен.",
                                        book_update.get("ticker"),
                                    )
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
                                logger.error(
                                    "Очередь рыночных тиков переполнена; тик %s отброшен.",
                                    tick["ticker"],
                                )
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

                logger.error(
                    "Finam WebSocket отключён/не подключён: %s. Повтор через %d сек.",
                    exc,
                    backoff,
                )
                refresh_token_before_retry = True
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def stop_market_data_stream(self):
        self.is_ws_running = False

    # ------------------------------------------------------------------
    # 3️⃣  GET_TICKER_ORDER_BOOK
    # ------------------------------------------------------------------
    async def get_ticker_order_book(self, ticker: str) -> dict | None:
        """Получение текущего стакана по инструменту через нативный SDK Финама (gRPC/REST)"""
        try:
            md_service = getattr(self.client, 'market_data', None) or getattr(self.client, 'instruments', None)
            if md_service and hasattr(md_service, 'get_order_book'):
                res_call = md_service.get_order_book(security_board="TQBR", security_code=ticker, order_book_depth=3)
                response = await res_call if asyncio.iscoroutine(res_call) else res_call
                
                if response:
                    bids = getattr(response, 'bids', [])
                    asks = getattr(response, 'asks', [])
                    return {
                        "bids": [{"price": float(getattr(b, 'price', b)), "volume": int(getattr(b, 'quantity', b))} for b in bids],
                        "asks": [{"price": float(getattr(a, 'price', a)), "volume": int(getattr(a, 'quantity', a))} for a in asks]
                    }
        except Exception as e:
            logger.debug(f"Резервный переход на HTTP-запрос стакана из-за: {e}")
            
        try:
            url = f"{self.base_url}/v1/instruments/{ticker}@MISX/orderbook"
            params = {}
            async with httpx.AsyncClient(timeout=5.0, http2=True) as client:
                response = await client.get(url, headers=self.headers, params=params)
                if response.status_code == 200:
                    data = response.json()
                    ob = data.get("orderbook", data.get("orderBook", data))
                    rows = ob.get("rows", []) if isinstance(ob, dict) else []
                    bids, asks = [], []
                    for row in rows:
                        price = self._decimal(row.get("price"))
                        buy = self._decimal(row.get("buy_size"))
                        sell = self._decimal(row.get("sell_size"))
                        if price > 0 and buy > 0: bids.append({"price": price, "volume": int(buy)})
                        if price > 0 and sell > 0: asks.append({"price": price, "volume": int(sell)})
                    return {"bids": bids[:3], "asks": asks[:3]}
        except Exception as http_err:
            logger.error(f"Не удалось получить стакан по тикеру {ticker}: {http_err}")
        return {"bids": [], "asks": []}

    # ------------------------------------------------------------------
    # 4️⃣  PLACE_ORDER
    # ------------------------------------------------------------------
    async def place_order(self, ticker: str, lots: int, side: str, price: float, client_id: str):
        """Fail closed until order payload is validated against the account's current REST schema."""
        if self.is_demo is False:
            logger.critical("LIVE-ордер заблокирован: старые методы/поля заявки в проекте не соответствуют текущему REST API Finam. Нужна отдельная проверка схемы заявки и account_id.")
            return None
        logger.warning("Заявка не отправлена: безопасный адаптер не имитирует подтверждение брокера.")
        return None

    # ------------------------------------------------------------------
    # 5️⃣  GET_PORTFOLIO
    # ------------------------------------------------------------------
    async def get_portfolio(self, client_id: str):
        """Получение информации по портфелю аккаунта через оригинальный SDK"""
        try:
            portfolio_service = getattr(self.client, 'portfolio', None) or getattr(self.client, 'accounts', None)
            if portfolio_service and hasattr(portfolio_service, 'get_portfolio'):
                res_call = portfolio_service.get_portfolio(client_code=client_id)
                response = await res_call if asyncio.iscoroutine(res_call) else res_call
                if response:
                    return response
        except Exception as e:
            logger.debug(f"Резервный переход на HTTP-запрос портфеля из-за: {e}")
            
        try:
            url = f"{self.base_url}/v1/accounts/{client_id}"
            params = {}
            async with httpx.AsyncClient(timeout=10.0, http2=True) as client:
                response = await client.get(url, headers=self.headers, params=params)
                if response.status_code == 200:
                    return response.json()
        except Exception as http_err:
            logger.error(f"Ошибка получения нативного портфеля: {http_err}")
        return None

        # ------------------------------------------------------------------
    # 7️⃣  GET_SECURITIES (Скачивание спецификаций лотности Мосбиржи через gRPC/MOEX)
    # ------------------------------------------------------------------
    async def get_securities(self) -> dict | None:
        """
        Запрашивает список доступных инструментов Мосбиржи для извлечения лотности.
        Линия 1: Оригинальный gRPC-сервис InstrumentsService (активируется в пн на боевом).
        Линия 2: Прямой резервный запрос к ISS MOEX на случай выходных или сбоя gRPC.
        """
        # 1. Первая линия обороны: нативный gRPC-клиент SDK (для реального счета)
        try:
            if self.client:
                ins_service = getattr(self.client, 'instruments', None)
                if ins_service and hasattr(ins_service, 'get_securities'):
                    logger.info("🎯 [FINAM gRPC] Запрос списка инструментов доски TQBR через InstrumentsService...")
                    res_call = ins_service.get_securities(board="TQBR")
                    response = await res_call if asyncio.iscoroutine(res_call) else res_call
                    if response and getattr(response, "securities", None):
                        logger.info("✅ [FINAM gRPC] Спецификация инструментов получена от gRPC-шлюза.")
                        return response
        except Exception as grpc_err:
            logger.debug(f"gRPC-шлюз недоступен или отключен брокером: {grpc_err}")

        # 2. Вторая линия обороны: Бронебойный резервный запрос напрямую к инфо-системе Мосбиржи (ISS MOEX)
        # Отдает точные лоты по режиму TQBR 24/7 без авторизации, редиректов и 404 ошибок.
        try:
            moex_url = "https://moex.com"
            params = {"iss.meta": "off", "iss.only": "securities", "securities.columns": "SECID,LOTSIZE"}
            
            logger.info(f"📡 gRPC пуст (выходной режим). Пробиваем резервный шлюз ISS MOEX: {moex_url}...")
            
            async with httpx.AsyncClient(timeout=10.0, http2=True, follow_redirects=True) as http_client:
                response = await http_client.get(moex_url, params=params)
                
                if response.status_code == 200:
                    logger.info("✅ [ISS MOEX] Спецификация лотности акций успешно скачана из ядра Московской биржи.")
                    return response.json()
                else:
                    logger.error(f"❌ Шлюз ISS MOEX отклонил запрос. Статус: {response.status_code}")
                
        except Exception as http_err:
            logger.error(f"Критический сбой всех каналов загрузки инструментов (Finam/MOEX): {http_err}")
            
        return None


    # ------------------------------------------------------------------
    # 6️⃣  MARKET_CLOSE_ALL_POSITIONS (Адаптирован под параллельный вызов)
    # ------------------------------------------------------------------
    async def market_close_all_positions(self, client_id: str):
        """🚨 Полная нативная ликвидация портфеля встречными маркет-ордерами."""
        logger.warning(f"🚨 ИНИЦИИРОВАНА НА ТИВНАЯ ЭКСТРЕННАЯ ЛИКВИДАЦИЯ ПОРТФЕЛЯ {client_id}!")
        portfolio_data = await self.get_portfolio(client_id=client_id)
        if not portfolio_data:
            return

        positions = getattr(portfolio_data, 'positions', []) if not isinstance(portfolio_data, dict) else portfolio_data.get("positions", [])
        
        # Сбор пачки параллельных задач
        tasks = []
        for pos in positions:
            ticker = getattr(pos, 'security_code', '') if not isinstance(pos, dict) else pos.get("securityCode", "")
            balance = int(float(getattr(pos, 'balance', 0) if not isinstance(pos, dict) else pos.get("balance", 0)))
            if balance == 0 or not ticker:
                continue

            side = "SELL" if balance > 0 else "BUY"
            lots_to_close = abs(balance)
            
            tasks.append(self.place_order(ticker=ticker, lots=lots_to_close, side=side, price=0.0, client_id=client_id))
            
        if tasks:
            # 🚨 ИСПРАВЛЕНИЕ: Безопасный перехват результатов. Исключения в отдельных ордерах не роняют общий паник-режим
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for idx, res in enumerate(results):
                if isinstance(res, Exception):
                    logger.error(f"❌ Ошибка при отправке {idx}-го ликвидационного ордера: {res}")
            logger.warning(f"🏁 Все ордера нативной экстренной ликвидации ({len(tasks)} шт) обработаны шлюзом Финама.")
