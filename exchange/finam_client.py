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
        self._account_discovery_lock = asyncio.Lock()
        self._metadata_account_discovery_lock = asyncio.Lock()
        self.is_demo = bool(is_demo)
        # Execution account identity is used only by LIVE order/account endpoints.
        self.account_id: str = ""
        self.account_id_source: str = "unresolved"
        # PAPER uses a separate read-only account context because Finam GetAsset
        # requires account_id even though the simulator never routes orders to it.
        self.metadata_account_id: str = ""
        self.metadata_account_id_source: str = "unresolved"
        self.account_ids: list[str] = []
        self.session_details: dict[str, Any] = {}
        self.account_details: dict[str, dict[str, Any]] = {}
        self.client: Any = None
        self.is_ws_running = False
        # Local level maps for ORDER_BOOK deltas.
        self._order_book_state: dict[str, dict[str, Any]] = {}
        # Last parser outcome distinguishes a valid delta with no BBO change
        # from a malformed/unrecognized payload.
        self._last_order_book_payload_valid = False
        self._last_order_book_out_of_order = False
        self._last_order_book_out_of_order_tickers: set[str] = set()
        self._last_order_book_unrecognized_tickers: set[str] = set()
        self._last_order_book_out_of_order_details: dict[str, dict[str, Any]] = {}
        # Optional synchronous callback supplied by main.py. It returns tickers
        # with live positions that must retain BBO even after leaving the screener.
        self.open_positions_provider: Callable[[], Iterable[str]] | None = None
        # Finam publishes per-instrument session intervals; cache them by Moscow date.
        self._asset_schedule_cache: dict[str, tuple[float, dict[str, Any] | None]] = {}
        self._asset_schedule_locks: dict[str, asyncio.Lock] = {}
        # Per-symbol throttle for detailed crossed-BBO diagnostics.
        self._crossed_bbo_log_last_at: dict[str, float] = {}
        # Track a crossed-book episode from first detection until the next
        # coherent BBO is published. Individual crossed deltas are not episodes.
        self._crossed_bbo_since: dict[str, float] = {}
        # A forced resubscription invalidates the local delta chain. Ignore all
        # deltas for that symbol until Finam supplies a fresh full snapshot.
        self._order_book_snapshot_required: set[str] = set()
        self._last_order_book_snapshot_wait_tickers: set[str] = set()
        self._crossed_bbo_resubscribe_at: dict[str, float] = {}
        # Diagnostic REST fallback backoff, isolated per instrument.
        self._order_book_http_backoff: dict[str, tuple[float, int]] = {}

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

    async def get_session_details(self) -> dict[str, Any] | None:
        """Return account IDs explicitly granted to the current JWT session."""
        if not self.jwt_token:
            return None
        try:
            async with httpx.AsyncClient(timeout=10.0) as client:
                response = await client.post(
                    f"{self.base_url}/v1/sessions/details",
                    json={"token": self.jwt_token},
                    headers={"Content-Type": "application/json", "Accept": "application/json"},
                )
                response.raise_for_status()
                payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("Finam session details response is not a JSON object")
            return payload
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "Finam session details failed: HTTP %s; response=%s",
                exc.response.status_code,
                self._safe_response_hint(exc.response),
            )
            return None
        except (httpx.RequestError, ValueError, TypeError) as exc:
            logger.warning("Finam session details request failed: %s", type(exc).__name__)
            return None

    async def discover_metadata_account_id(self) -> str | None:
        """Resolve a session-authorized account ID for read-only GetAsset metadata.

        Finam currently requires account_id on GetAsset. PAPER remains an internal
        pseudo-account: this ID is used only as a REST metadata query context and is
        never copied into self.account_id or used for order routing.
        """
        async with self._metadata_account_discovery_lock:
            if self.metadata_account_id:
                return self.metadata_account_id

            details = await self.get_session_details()
            session_data = details if isinstance(details, dict) else {}
            nested = session_data.get("session") or session_data.get("data")
            if isinstance(nested, dict):
                session_data = nested
            raw_ids = session_data.get("account_ids")
            account_ids = list(dict.fromkeys(
                str(value).strip()
                for value in (raw_ids if isinstance(raw_ids, list) else [])
                if isinstance(value, (str, int)) and str(value).strip()
            ))
            self.account_ids = account_ids

            explicit_id = str(os.getenv("FINAM_ACCOUNT_ID", "") or "").strip()
            legacy_id = str(os.getenv("FINAM_CLIENT_ID", "") or "").strip()
            selected = ""
            source = ""

            if account_ids:
                if explicit_id and explicit_id in account_ids:
                    selected, source = explicit_id, "session_details_FINAM_ACCOUNT_ID"
                elif legacy_id and legacy_id in account_ids:
                    selected, source = legacy_id, "session_details_FINAM_CLIENT_ID"
                else:
                    # Every ID here is explicitly authorized by this JWT session.
                    # GetAsset lot_size is exchange metadata, not an order request.
                    selected, source = account_ids[0], "session_details_first_authorized_account"
            else:
                # Older token configurations may omit account_ids. Verify only
                # explicitly configured candidates; never guess an account number.
                candidates = []
                if explicit_id:
                    candidates.append((explicit_id, "scoped_FINAM_ACCOUNT_ID"))
                if legacy_id and legacy_id != explicit_id:
                    candidates.append((legacy_id, "scoped_FINAM_CLIENT_ID"))
                for candidate_id, candidate_source in candidates:
                    payload = await self.get_account(candidate_id)
                    if not isinstance(payload, dict):
                        continue
                    account_data = payload.get("account")
                    if isinstance(account_data, dict):
                        payload = account_data
                    returned_id = str(
                        payload.get("account_id") or payload.get("id") or ""
                    ).strip()
                    if not returned_id or returned_id == candidate_id:
                        selected, source = candidate_id, candidate_source
                        break

            if not selected:
                logger.warning(
                    "[FINAM METADATA CONTEXT] read-only account context could not be resolved; "
                    "GetAsset refresh is skipped. PAPER pseudo-account and order routing are unchanged."
                )
                return None

            self.metadata_account_id = selected
            self.metadata_account_id_source = source
            logger.info(
                "[FINAM METADATA CONTEXT] read-only context resolved; source=%s; "
                "authorized_accounts=%d; selected=%s; purpose=GetAsset metadata only; "
                "PAPER order routing remains internal.",
                source,
                len(account_ids),
                self._mask_account_id(selected),
            )
            return self.metadata_account_id

    async def get_account(self, account_id: str) -> dict[str, Any] | None:
        """Fetch account metadata for an explicit or session-authorized account ID."""
        account = str(account_id or "").strip()
        if not self.jwt_token or not account:
            return None
        url = f"{self.base_url}/v1/accounts/{url_quote(account, safe='')}"
        request_jwt = self.jwt_token
        for attempt in range(2):
            try:
                async with httpx.AsyncClient(timeout=8.0) as client:
                    response = await client.get(
                        url,
                        headers={"Authorization": f"Bearer {self.jwt_token}", "Accept": "application/json"},
                    )
                    response.raise_for_status()
                    payload = response.json()
                return payload if isinstance(payload, dict) else None
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code == 401 and attempt == 0:
                    if await self._refresh_auth_if_unchanged(request_jwt):
                        continue
                logger.warning(
                    "Finam account details unavailable for %s: HTTP %s; response=%s",
                    self._mask_account_id(account),
                    exc.response.status_code,
                    self._safe_response_hint(exc.response),
                )
                return None
            except (httpx.RequestError, ValueError, TypeError) as exc:
                logger.warning(
                    "Finam account details request failed for %s: %s",
                    self._mask_account_id(account),
                    type(exc).__name__,
                )
                return None
        return None

    @staticmethod
    def _mask_account_id(account_id: str) -> str:
        value = str(account_id or "")
        return f"***{value[-4:]}" if len(value) > 4 else "***"

    @staticmethod
    def _account_type_text(account: dict[str, Any] | None) -> str:
        if not isinstance(account, dict):
            return ""
        return str(account.get("type") or account.get("account_type") or "").strip().upper()

    @classmethod
    def _is_demo_account(cls, account: dict[str, Any] | None) -> bool:
        if not isinstance(account, dict):
            return False
        if account.get("is_demo") is True:
            return True
        return any(marker in cls._account_type_text(account) for marker in ("DEMO", "SIMULATION", "PAPER"))

    @classmethod
    def _is_live_account(cls, account: dict[str, Any] | None) -> bool:
        if not isinstance(account, dict):
            return False
        if account.get("is_demo") is False:
            return True
        return any(marker in cls._account_type_text(account) for marker in ("REAL", "LIVE"))

    @classmethod
    def _choose_account_id(
        cls,
        account_ids: list[str],
        explicit_account_id: str,
        legacy_client_id: str,
        account_details: dict[str, dict[str, Any]],
        is_demo: bool,
    ) -> tuple[str, str]:
        """Choose only an account authorized by the current JWT session."""
        ids = list(dict.fromkeys(str(value).strip() for value in account_ids if str(value).strip()))

        def mode_matches(account_id: str) -> bool:
            details = account_details.get(account_id)
            if is_demo and cls._is_live_account(details):
                return False
            if not is_demo and cls._is_demo_account(details):
                return False
            return True

        # FINAM_ACCOUNT_ID is preferred, but a stale explicit setting must not
        # prevent use of FINAM_CLIENT_ID when the latter is independently present
        # in the authenticated session's account_ids list.
        if explicit_account_id and explicit_account_id in ids and mode_matches(explicit_account_id):
            return explicit_account_id, "FINAM_ACCOUNT_ID"

        if legacy_client_id and legacy_client_id in ids and mode_matches(legacy_client_id):
            source = "FINAM_CLIENT_ID_matched_account_ids"
            if explicit_account_id and explicit_account_id != legacy_client_id:
                source += "_after_explicit_reject"
            return legacy_client_id, source

        # An explicit account ID is never silently replaced by an arbitrary
        # single account from the session. It may only fall back to the configured
        # legacy value when that value is itself session-authorized.
        if explicit_account_id:
            return "", "FINAM_ACCOUNT_ID_not_authorized_or_mode_mismatch"
        if legacy_client_id and legacy_client_id in ids:
            return "", "FINAM_CLIENT_ID_mode_mismatch"

        if len(ids) == 1:
            only_id = ids[0]
            if not mode_matches(only_id):
                return "", "single_account_mode_mismatch"
            return only_id, "session_details_single_account"

        if is_demo:
            candidates = [value for value in ids if cls._is_demo_account(account_details.get(value))]
            source = "session_details_unique_demo_account"
        else:
            candidates = [value for value in ids if cls._is_live_account(account_details.get(value))]
            source = "session_details_unique_live_account"
        if len(candidates) == 1:
            return candidates[0], source
        return "", "ambiguous_account_selection"

    async def discover_accounts(self) -> str | None:
        """Resolve one account from the authenticated session or a scoped account GET.

        Finam may return an empty account_ids array for some established token
        configurations. In that case each explicitly configured candidate is
        checked independently through GET /v1/accounts/{id}; a failed preferred
        FINAM_ACCOUNT_ID must not suppress verification of FINAM_CLIENT_ID.
        """
        async with self._account_discovery_lock:
            if self.account_id:
                return self.account_id

            details = await self.get_session_details()
            details_available = isinstance(details, dict)
            if not details_available:
                logger.warning(
                    "[FINAM ACCOUNT DISCOVERY] session details недоступны; "
                    "будут проверены только явно заданные FINAM_ACCOUNT_ID/FINAM_CLIENT_ID "
                    "через scoped GET /v1/accounts/{id}."
                )
                details = {}

            raw_ids = details.get("account_ids")
            raw_type = type(raw_ids).__name__
            ids_source = raw_ids if isinstance(raw_ids, list) else []
            account_ids = list(dict.fromkeys(
                str(value).strip() for value in ids_source
                if isinstance(value, (str, int)) and str(value).strip()
            ))
            self.session_details = details
            logger.info(
                "[FINAM ACCOUNT DISCOVERY] response_keys=%s; account_ids_type=%s; "
                "account_ids_count=%d; readonly=%s; session_details_available=%s.",
                ",".join(sorted(str(key) for key in details.keys()))[:300] or "-",
                raw_type,
                len(account_ids),
                bool(details.get("readonly", False)),
                details_available,
            )

            explicit_id = str(os.getenv("FINAM_ACCOUNT_ID", "") or "").strip()
            legacy_id = str(os.getenv("FINAM_CLIENT_ID", "") or "").strip()
            account_details: dict[str, dict[str, Any]] = {}
            selected = ""
            source = ""

            if account_ids:
                self.account_ids = account_ids
                for candidate_id in account_ids:
                    account_payload = await self.get_account(candidate_id)
                    if not isinstance(account_payload, dict):
                        continue
                    nested = account_payload.get("account")
                    if isinstance(nested, dict):
                        account_payload = nested
                    account_details[candidate_id] = account_payload

                selected, source = self._choose_account_id(
                    account_ids, explicit_id, legacy_id, account_details, self.is_demo
                )
                if not selected:
                    if explicit_id:
                        reason = (
                            "FINAM_ACCOUNT_ID не авторизован/не соответствует режиму, "
                            "а FINAM_CLIENT_ID не подтверждён этим списком счетов"
                        )
                    elif len(account_ids) > 1:
                        reason = "несколько счетов без единственного однозначного типа DEMO/LIVE"
                    else:
                        reason = "тип единственного счёта не соответствует FINAM_DEMO"
                    logger.error(
                        "⛔ [FINAM ACCOUNT DISCOVERY] выбор счёта не выполнен: %s; доступно=%d; "
                        "новые входы остаются заблокированы.",
                        reason, len(account_ids),
                    )
                    self.account_details = account_details
                    return None
            else:
                # GET /accounts/{id} is scoped by the requested account path and
                # returns 404 when the JWT cannot access that account. Try both
                # configured names in priority order, without logging their values.
                candidates: list[tuple[str, str]] = []
                if explicit_id:
                    candidates.append((explicit_id, "FINAM_ACCOUNT_ID"))
                if legacy_id and legacy_id != explicit_id:
                    candidates.append((legacy_id, "FINAM_CLIENT_ID"))

                for candidate_id, candidate_source in candidates:
                    account_payload = await self.get_account(candidate_id)
                    if isinstance(account_payload, dict):
                        nested = account_payload.get("account")
                        if isinstance(nested, dict):
                            account_payload = nested
                    returned_id = ""
                    if isinstance(account_payload, dict):
                        returned_id = str(
                            account_payload.get("account_id") or account_payload.get("id") or ""
                        ).strip()
                    identity_ok = not returned_id or returned_id == candidate_id
                    mode_ok = isinstance(account_payload, dict) and not (
                        (self.is_demo and self._is_live_account(account_payload))
                        or (not self.is_demo and self._is_demo_account(account_payload))
                    )
                    payload_ok = isinstance(account_payload, dict) and bool(account_payload)
                    if payload_ok and identity_ok and mode_ok:
                        selected = candidate_id
                        # Keep source labels stable: callers should not need to
                        # parse implementation details of how the ID was verified.
                        source = candidate_source
                        account_ids = [candidate_id]
                        account_details[candidate_id] = account_payload
                        logger.info(
                            "[FINAM ACCOUNT DISCOVERY] account_ids пуст/отсутствует; "
                            "%s подтверждён GET /v1/accounts/{id}; response_id_present=%s; mode_ok=%s.",
                            candidate_source, bool(returned_id), mode_ok,
                        )
                        break

                    logger.warning(
                        "[FINAM ACCOUNT DISCOVERY] %s не подтверждён GET /v1/accounts/{id}: "
                        "payload_ok=%s; identity_match=%s; mode_ok=%s; проверяется следующий "
                        "явно заданный кандидат, если он есть.",
                        candidate_source, payload_ok, identity_ok, mode_ok,
                    )

                if not selected:
                    self.account_ids = []
                    self.account_details = account_details
                    if not candidates:
                        reason = "account_ids пуст/отсутствует, FINAM_ACCOUNT_ID и FINAM_CLIENT_ID не заданы"
                    else:
                        reason = "ни один явно заданный идентификатор не подтверждён GET /v1/accounts/{id}"
                    logger.error(
                        "⛔ [FINAM ACCOUNT DISCOVERY] %s; новые входы заблокированы.",
                        reason,
                    )
                    return None

            self.account_ids = account_ids
            self.account_details = account_details
            self.account_id = selected
            self.account_id_source = source
            selected_details = account_details.get(selected, {})
            logger.info(
                "✅ [FINAM ACCOUNT DISCOVERY] доступно=%d; выбран=%s; source=%s; type=%s; readonly=%s.",
                len(account_ids), self._mask_account_id(selected), source,
                self._account_type_text(selected_details) or "unknown",
                bool(details.get("readonly", False)),
            )
            return self.account_id

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

    async def get_assets(self) -> list[dict[str, Any]]:
        """Fetch Finam's tradable asset directory once for tick-size metadata."""
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

    async def get_asset(self, ticker: str, account_id: str | None = None) -> dict[str, Any] | None:
        """Fetch detailed instrument metadata (decimals, min_step, exchange lot_size)."""
        if not self.jwt_token:
            logger.warning(
                "Finam GetAsset %s skipped: JWT is empty; public exchange lot metadata unavailable.",
                str(ticker or "").strip().upper()[:32] or "<empty>",
            )
            return None
        normalized = str(ticker or "").strip().upper().split("@", 1)[0]
        if not normalized:
            return None
        symbol = f"{normalized}@MISX"
        # Finam validates account_id on GetAsset. PAPER supplies a separately
        # discovered read-only metadata context; it never populates the LIVE
        # execution account field and is never used for order placement.
        account = str(account_id or self.metadata_account_id or "").strip()
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
    def _safe_response_hint(response: httpx.Response, max_chars: int = 400) -> str:
        """Return a bounded provider error body with credentials redacted."""
        try:
            payload = response.json()
        except (ValueError, TypeError):
            payload = response.text or ""
        if isinstance(payload, (dict, list)):
            rendered = json.dumps(
                FinamBrokerClient._redact_for_log(payload),
                ensure_ascii=False,
                separators=(",", ":"),
            )
        else:
            rendered = str(payload)
        rendered = re.sub(
            r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+",
            r"\1<redacted>",
            rendered,
        )
        rendered = re.sub(
            r'(?i)((?:access[_-]?token|refresh[_-]?token|token|authorization|password|secret|api[_-]?key)\s*[:=]\s*)("[^"]*"|[^,\s}]+)',
            r"\1<redacted>",
            rendered,
        )
        rendered = re.sub(r"\s+", " ", rendered).strip()
        return rendered[:max(0, int(max_chars))] or "<empty>"

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
                    first_order_book_logged = False
                    first_trade_logged = False
                    instrument_trade_messages = 0
                    instrument_trade_events = 0
                    instrument_trade_queue_dropped = 0
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
                        position_rest_refresh_interval_sec = max(3.0, float(os.getenv("FINAM_POSITION_REST_BBO_REFRESH_SEC", "5")))
                        position_rest_refresh_trigger_age_sec = max(0.5, float(os.getenv("FINAM_POSITION_REST_BBO_TRIGGER_AGE_SEC", "1.5")))
                    except (TypeError, ValueError):
                        crossed_resubscribe_after_sec = 5.0
                        crossed_resubscribe_cooldown_sec = 30.0
                        position_rest_refresh_interval_sec = 5.0
                        position_rest_refresh_trigger_age_sec = 1.5

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
                                    if max(bid_age, ask_age) < position_rest_refresh_trigger_age_sec:
                                        continue

                                    # REST is diagnostic-only, so stale position BBO must
                                    # recover through a fresh provider snapshot on the WS
                                    # subscription. Restart the delta chain with a cooldown;
                                    # never manufacture freshness from REST receive time.
                                    last_resubscribe = self._crossed_bbo_resubscribe_at.get(
                                        position_ticker, 0.0
                                    )
                                    if (
                                        position_symbol in order_book_subscribed
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
                                "📈 Finam INSTRUMENT_TRADES 60s: messages=%d, trades=%d, queue_dropped=%d.",
                                instrument_trade_messages, instrument_trade_events, instrument_trade_queue_dropped,
                            )
                            instrument_trade_messages = 0
                            instrument_trade_events = 0
                            instrument_trade_queue_dropped = 0
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
                                "ошибок_разбора=%d; позиции=[%s]; сообщений_без_тикера=%d.",
                                len(order_book_subscribed), order_book_messages,
                                enqueued_order_book_events, enqueued_bbo_side_updates,
                                order_book_valid_no_bbo_delta, order_book_invalid_states,
                                invalid_reason_summary, invalid_ticker_summary,
                                order_book_crossed_episodes, order_book_missing_envelope_timestamp,
                                order_book_out_of_order, order_book_queue_dropped,
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
                                    logger.warning("[ORDER_BOOK OUT_OF_ORDER RESYNC] %s: delta chain invalidated; waiting for full snapshot.", stale_ticker)
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
                                    order_book_queue_dropped += 1
                                    if symbol_stats is not None:
                                        symbol_stats["window_dropped"] += 1
                                    logger.warning(
                                        "Очередь рыночных событий заполнена; ORDER_BOOK %s отброшен.",
                                        book_update.get("ticker"),
                                    )
                            continue

                        if subscription_type in {"INSTRUMENT_TRADES", "INSTRUMENT_TRADE"}:
                            instrument_trade_messages += 1
                            trade_events = self._extract_instrument_trades(envelope)
                            instrument_trade_events += len(trade_events)
                            for trade_event in trade_events:
                                try:
                                    queue.put_nowait(trade_event)
                                except asyncio.QueueFull:
                                    instrument_trade_queue_dropped += 1
                                    if screener is not None:
                                        screener.reset_trade_history(trade_event.get("ticker"))
                                    logger.error(
                                        "Очередь рыночных событий переполнена; INSTRUMENT_TRADE %s id=%s отброшен.",
                                        trade_event.get("ticker"), trade_event.get("trade_id"),
                                    )
                                    continue
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
