"""Finam authentication, account discovery and account metadata mixin."""
from __future__ import annotations

from exchange.finam_shared import (
    asyncio, logging, math, datetime, timezone, timedelta, url_quote,
    Callable, Iterable, Any, json, inspect, os, re, time, websockets, httpx,
    logger, JOURNAL_FILE, _MOSCOW_TZ,
    _websocket_close_code, _websocket_disconnect_log_level,
    _coalesce_queued_market_event, _event_timestamp_to_monotonic,
    _should_resync_position_order_book,
)

class FinamAccountMixin:
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
