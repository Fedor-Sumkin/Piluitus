"""Stable FinamBrokerClient facade composed from focused gateway mixins."""
from __future__ import annotations

from exchange.finam_shared import (
    asyncio, logging, math, datetime, timezone, timedelta, url_quote,
    Callable, Iterable, Any, json, inspect, os, re, time, websockets, httpx,
    logger, JOURNAL_FILE, _MOSCOW_TZ,
    _websocket_close_code, _websocket_disconnect_log_level,
    _coalesce_queued_market_event, _event_timestamp_to_monotonic,
    _should_resync_position_order_book,
)

from exchange.finam_helpers import FinamHelpersMixin
from exchange.finam_accounts import FinamAccountMixin
from exchange.finam_assets import FinamAssetMixin
from exchange.finam_payload_parsers import FinamPayloadParserMixin
from exchange.finam_market_data import FinamMarketDataMixin
from exchange.finam_order_book import FinamOrderBookMixin
from exchange.finam_orders import FinamOrdersMixin


class FinamBrokerClient(
    FinamHelpersMixin,
    FinamAccountMixin,
    FinamAssetMixin,
    FinamPayloadParserMixin,
    FinamMarketDataMixin,
    FinamOrderBookMixin,
    FinamOrdersMixin,
):
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
        # Public MOEX ISS instrument specifications are static during a session.
        # Cache verified TQBR metadata to avoid re-querying all 88 symbols every 15 minutes.
        self._moex_asset_cache: dict[str, tuple[float, dict[str, Any]]] = {}
        self._moex_asset_cache_ttl_sec = 6.0 * 60.0 * 60.0
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
