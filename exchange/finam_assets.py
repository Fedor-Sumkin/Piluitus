"""Finam/MOEX asset metadata, historical bars and session schedule mixin."""
from __future__ import annotations

from exchange.finam_shared import (
    asyncio, logging, math, datetime, timezone, timedelta, url_quote,
    Callable, Iterable, Any, json, inspect, os, re, time, websockets, httpx,
    logger, JOURNAL_FILE, _MOSCOW_TZ,
    _websocket_close_code, _websocket_disconnect_log_level,
    _coalesce_queued_market_event, _event_timestamp_to_monotonic,
    _should_resync_position_order_book,
)

class FinamAssetMixin:
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

    async def get_historical_m1_bars(
        self, ticker: str, lookback_minutes: int = 120
    ) -> list[dict[str, Any]]:
        """Fetch historical M1 OHLC bars; callers must discard the still-open bar."""
        if not self.jwt_token:
            return []
        normalized = str(ticker or "").strip().upper().split("@", 1)[0]
        if not normalized:
            return []
        symbol = f"{normalized}@MISX"
        minutes = min(360, max(30, int(lookback_minutes)))
        end_time = datetime.now(timezone.utc)
        start_time = end_time - timedelta(minutes=minutes)
        params = {
            "timeframe": "TIME_FRAME_M1",
            "interval.start_time": start_time.isoformat(timespec="seconds").replace("+00:00", "Z"),
            "interval.end_time": end_time.isoformat(timespec="seconds").replace("+00:00", "Z"),
        }
        url = f"{self.base_url}/v1/instruments/{url_quote(symbol, safe='@')}/bars"
        request_jwt = self.jwt_token
        for attempt in range(2):
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(8.0, connect=3.0)) as client:
                    response = await client.get(
                        url,
                        headers={"Authorization": f"Bearer {self.jwt_token}", "Accept": "application/json"},
                        params=params,
                    )
                    response.raise_for_status()
                    payload = response.json()
                bars = payload.get("bars", []) if isinstance(payload, dict) else []
                if not isinstance(bars, list):
                    return []
                return [bar for bar in bars if isinstance(bar, dict)]
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 401 and attempt == 0:
                    if await self._refresh_auth_if_unchanged(request_jwt):
                        request_jwt = self.jwt_token
                        continue
                logger.warning(
                    "Finam M1 history %s unavailable: HTTP %s; live-built ATR remains active.",
                    normalized, exc.response.status_code,
                )
                return []
            except (httpx.RequestError, ValueError, TypeError) as exc:
                logger.warning(
                    "Finam M1 history %s request failed (%s); live-built ATR remains active.",
                    normalized, type(exc).__name__,
                )
                return []
        return []

    async def get_assets(self) -> list[dict[str, Any]]:
        """Fetch Finam's tradable asset directory (symbols and instrument identity)."""
        if not self.jwt_token:
            return []
        url = f"{self.base_url}/v1/assets"
        request_jwt = self.jwt_token
        for attempt in range(2):
            try:
                async with httpx.AsyncClient(timeout=10.0) as client:
                    response = await client.get(url, headers={
                        "Authorization": f"Bearer {self.jwt_token}", "Accept": "application/json",
                    })
                    response.raise_for_status()
                    payload = response.json()
                if isinstance(payload, dict):
                    assets = payload.get("assets", [])
                    return [item for item in assets if isinstance(item, dict)] if isinstance(assets, list) else []
                return []
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 401 and attempt == 0:
                    if await self._refresh_auth_if_unchanged(request_jwt):
                        continue
                logger.warning("Finam asset directory unavailable: HTTP %s; static metadata fallback remains active.", exc.response.status_code)
                return []
            except (httpx.RequestError, ValueError, TypeError) as exc:
                logger.warning("Finam asset directory unavailable (%s); static metadata fallback remains active.", type(exc).__name__)
                return []
        return []

    async def get_moex_asset(self, ticker: str) -> dict[str, Any] | None:
        """Fetch public MOEX TQBR security metadata without a Finam brokerage account.

        The ISS security table is the exchange source of truth for LOTSIZE.
        MINSTEP is converted to the scaled integer representation used by the
        existing ASSET_METADATA parser (MINSTEP * 10**DECIMALS).
        """
        normalized = str(ticker or "").strip().upper().split("@", 1)[0]
        if not normalized or not re.fullmatch(r"[A-Z0-9._-]{1,32}", normalized):
            return None

        cached = self._moex_asset_cache.get(normalized)
        now = time.monotonic()
        if cached and now - cached[0] <= self._moex_asset_cache_ttl_sec:
            return dict(cached[1])

        url = (
            "https://iss.moex.com/iss/engines/stock/markets/shares/"
            f"boards/TQBR/securities/{url_quote(normalized, safe='')}.json"
        )
        params = {
            "iss.meta": "off",
            "iss.only": "securities",
            "securities.columns": "SECID,BOARDID,LOTSIZE,MINSTEP,DECIMALS,SHORTNAME,SECNAME",
        }
        try:
            async with httpx.AsyncClient(
                timeout=httpx.Timeout(8.0, connect=3.0),
                headers={"Accept": "application/json", "User-Agent": "PILUITUS-market-metadata/1.0"},
            ) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                payload = response.json()
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "MOEX ISS metadata %s unavailable: HTTP %s; TQBR exchange lot remains unconfirmed.",
                normalized, exc.response.status_code,
            )
            return None
        except (httpx.RequestError, ValueError, TypeError) as exc:
            logger.warning(
                "MOEX ISS metadata %s request failed: %s; TQBR exchange lot remains unconfirmed.",
                normalized, type(exc).__name__,
            )
            return None

        block = payload.get("securities") if isinstance(payload, dict) else None
        if not isinstance(block, dict):
            logger.warning("MOEX ISS metadata %s returned no securities block.", normalized)
            return None
        columns = block.get("columns")
        rows = block.get("data")
        if not isinstance(columns, list) or not isinstance(rows, list):
            logger.warning("MOEX ISS metadata %s returned an invalid securities table.", normalized)
            return None

        for row in rows:
            if isinstance(row, dict):
                record = row
            elif isinstance(row, (list, tuple)) and len(row) == len(columns):
                record = dict(zip(columns, row))
            else:
                continue

            secid = str(record.get("SECID") or "").strip().upper()
            board = str(record.get("BOARDID") or "").strip().upper()
            if secid != normalized or board != "TQBR":
                continue

            try:
                raw_lot = record.get("LOTSIZE")
                lot_float = float(raw_lot)
                lot_size = int(lot_float)
                if not math.isfinite(lot_float) or lot_size <= 0 or lot_float != lot_size:
                    raise ValueError("invalid LOTSIZE")
            except (TypeError, ValueError, OverflowError):
                logger.warning("MOEX ISS metadata %s has invalid LOTSIZE; exchange lot remains unconfirmed.", normalized)
                return None

            decimals = -1
            try:
                raw_decimals = record.get("DECIMALS")
                decimals = int(raw_decimals)
                if decimals < 0 or decimals > 10:
                    decimals = -1
            except (TypeError, ValueError, OverflowError):
                decimals = -1

            min_step_units = 0
            if decimals >= 0:
                try:
                    from decimal import Decimal
                    raw_step = Decimal(str(record.get("MINSTEP")))
                    scaled_step = raw_step * (Decimal(10) ** decimals)
                    if raw_step > 0 and scaled_step == scaled_step.to_integral_value():
                        min_step_units = int(scaled_step)
                except (TypeError, ValueError, ArithmeticError):
                    min_step_units = 0

            result = {
                "symbol": f"{normalized}@MISX",
                "ticker": normalized,
                "mic": "MISX",
                "board": "TQBR",
                "decimals": decimals,
                "min_step": min_step_units,
                "lot_size": {"value": str(lot_size)},
                "quote_currency": "RUB",
                "name": str(record.get("SECNAME") or record.get("SHORTNAME") or ""),
                "metadata_source": "MOEX_ISS_TQBR",
            }
            self._moex_asset_cache[normalized] = (time.monotonic(), dict(result))
            return result

        logger.warning(
            "MOEX ISS metadata %s has no matching TQBR security row; exchange lot remains unconfirmed.",
            normalized,
        )
        return None

    async def get_asset(self, ticker: str, account_id: str | None = None) -> dict[str, Any] | None:
        """Fetch Finam instrument metadata (fixed/tiered price step and reference fields)."""
        if not self.jwt_token:
            logger.warning(
                "Finam GetAsset %s skipped: JWT is empty; price-step metadata unavailable.",
                str(ticker or "").strip().upper()[:32] or "<empty>",
            )
            return None
        normalized = str(ticker or "").strip().upper().split("@", 1)[0]
        if not normalized:
            return None
        symbol = f"{normalized}@MISX"
        # GetAsset accepts an optional account context. Never inherit a
        # discovered/stale account implicitly: PAPER needs instrument specs only;
        # LIVE callers pass their account explicitly.
        account = str(account_id or "").strip()
        params = {"account_id": account} if account and account != "UNKNOWN_ACCOUNT" else None
        url = f"{self.base_url}/v1/assets/{url_quote(symbol, safe='@')}"
        request_jwt = self.jwt_token
        for attempt in range(2):
            response: httpx.Response | None = None
            try:
                async with httpx.AsyncClient(timeout=httpx.Timeout(8.0, connect=3.0)) as client:
                    response = await client.get(
                        url,
                        headers={
                            "Authorization": f"Bearer {self.jwt_token}",
                            "Accept": "application/json",
                        },
                        params=params,
                    )
                    response.raise_for_status()
                    try:
                        payload = response.json()
                    except (ValueError, TypeError) as exc:
                        logger.warning(
                            "Finam GetAsset %s returned invalid JSON: %s; HTTP %s; content_type=%s; body=%s",
                            normalized, type(exc).__name__, response.status_code,
                            response.headers.get("content-type", "<missing>"),
                            self._safe_response_hint(response),
                        )
                        return None
                if isinstance(payload, dict):
                    return payload
                logger.warning(
                    "Finam GetAsset %s returned unexpected payload type=%s; HTTP %s; content_type=%s; body=%s",
                    normalized, type(payload).__name__, response.status_code,
                    response.headers.get("content-type", "<missing>"),
                    self._safe_response_hint(response),
                )
                return None
            except httpx.HTTPStatusError as exc:
                status = int(exc.response.status_code)
                if status == 401 and attempt == 0:
                    if await self._refresh_auth_if_unchanged(request_jwt):
                        request_jwt = self.jwt_token
                        continue
                logger.warning(
                    "Finam GetAsset %s failed: HTTP %s; request_symbol=%s; account_context=%s; response=%s",
                    normalized, status, symbol, bool(params),
                    self._safe_response_hint(exc.response),
                )
                return None
            except httpx.RequestError as exc:
                detail = re.sub(r"\s+", " ", str(exc)).strip()[:180] or "<no detail>"
                logger.warning(
                    "Finam GetAsset %s request failed: error_type=%s; detail=%s; request_symbol=%s",
                    normalized, type(exc).__name__, detail, symbol,
                )
                return None
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "Finam GetAsset %s response processing failed: error_type=%s; detail=%s",
                    normalized, type(exc).__name__, str(exc)[:180],
                )
                return None
        return None

    async def get_asset_params(self, ticker: str, account_id: str | None = None) -> dict[str, Any] | None:
        """Fetch account-aware Finam trading parameters (notably trade_lot_size)."""
        if not self.jwt_token:
            return None
        normalized = str(ticker or "").strip().upper().split("@", 1)[0]
        if not normalized:
            return None
        symbol = f"{normalized}@MISX"
        account = str(account_id or self.account_id or "").strip()
        if not account or account == "UNKNOWN_ACCOUNT":
            logger.error(
                "Finam GetAssetParams %s skipped: no account was resolved from the authenticated session; "
                "new entries remain blocked.",
                normalized,
            )
            return None
        params = {"account_id": account}
        url = f"{self.base_url}/v1/assets/{url_quote(symbol, safe='@')}/params"
        request_jwt = self.jwt_token
        for attempt in range(2):
            try:
                async with httpx.AsyncClient(timeout=3.0) as client:
                    response = await client.get(url, headers={
                        "Authorization": f"Bearer {self.jwt_token}",
                        "Accept": "application/json",
                    }, params=params)
                    response.raise_for_status()
                    payload = response.json()
                return payload if isinstance(payload, dict) else None
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 401 and attempt == 0:
                    if await self._refresh_auth_if_unchanged(request_jwt):
                        continue
                logger.warning(
                    "Finam GetAssetParams %s failed: HTTP %s; response=%s",
                    normalized, exc.response.status_code,
                    self._safe_response_hint(exc.response),
                )
                return None
            except (httpx.RequestError, ValueError, TypeError) as exc:
                logger.warning(
                    "Finam GetAssetParams %s request failed: %s",
                    normalized, type(exc).__name__,
                )
                return None
        return None

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
