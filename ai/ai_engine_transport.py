"""Transport, retry and lifecycle mixin for the public AIEngine facade."""
from __future__ import annotations

from ai.ai_engine_shared import (
    AsyncClient,
    HTTPStatusError,
    Settings,
    httpx,
    asyncio,
    hashlib,
    json,
    logging,
    math,
    os,
    re,
    time,
    normalise_rag_context,
    select_prompt_rules,
    classify_direction_evidence,
    classify_m1_evidence_state,
    log,
    thought_log,
    VLLMCircuitOpenError,
    MODEL_REASON_CODES,
)

class AIEngineTransportMixin:
    @staticmethod
    def _effective_max_tokens() -> int:
        """Keep the short APPROVE/HOLD response within a bounded generation budget."""
        try:
            return max(384, min(768, int(getattr(Settings, "VLLM_MAX_TOKENS", 768))))
        except (TypeError, ValueError, OverflowError):
            return 768

    @staticmethod
    def _request_timeout_seconds() -> float:
        """Return a finite, positive per-attempt timeout for vLLM requests."""
        try:
            configured = float(getattr(Settings, "VLLM_TIMEOUT", 30))
        except (TypeError, ValueError, OverflowError):
            configured = 30.0
        if not math.isfinite(configured):
            return 30.0
        return max(1.0, configured)

    def __init__(self):
        endpoint = Settings.VLLM_ENDPOINT.rstrip("/")
        scheme_end = endpoint.find("://") + 3
        first_slash = endpoint.find("/", scheme_end)

        if first_slash == -1:
            self.base_url = endpoint
        else:
            self.base_url = endpoint[:first_slash]

        self.api_path = "/v1/chat/completions"
        self.model_name = Settings.VLLM_MODEL
        # A connection outage must not trigger a fresh pair of failed HTTP calls
        # for every queued candidate. The first request after cooldown is the probe.
        self._transport_failure_count = 0
        self._transport_open_until = 0.0
        self._transport_state_lock = asyncio.Lock()
        
        limits = httpx.Limits(max_connections=100, max_keepalive_connections=20, keepalive_expiry=30.0)
        timeout = httpx.Timeout(connect=5.0, read=45.0, write=10.0, pool=10.0)

        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            limits=limits,
            timeout=timeout,
            http2=True,
        )
        
        self.clean_cyrillic_pattern = re.compile(r"[^a-zA-Zа-яА-ЯёЁ0-9_\s\.,;:\-\(\)%₽?!+=/\'\"<>]")
        self.spaces_pattern = re.compile(r"\s+")
        model_label = str(self.model_name).replace("\\", "/").rstrip("/").rsplit("/", 1)[-1]
        log.info(
            "🧠 AIEngine v34 загружен: файл=ai_engine.py | API=%s | модель=%s | max_tokens=%s",
            self.api_path, model_label,
            self._effective_max_tokens(),
        )

    async def _transport_circuit_retry_after(self) -> float:
        """Return remaining cooldown; do not send traffic while the circuit is open."""
        lock = getattr(self, "_transport_state_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._transport_state_lock = lock
        async with lock:
            remaining = float(getattr(self, "_transport_open_until", 0.0) or 0.0) - time.monotonic()
            return max(0.0, remaining)

    async def _record_transport_failure(self) -> float:
        """Open an exponentially backed-off circuit after a failed connection cycle."""
        lock = getattr(self, "_transport_state_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._transport_state_lock = lock
        try:
            base = max(1.0, float(getattr(Settings, "VLLM_CONNECT_BACKOFF_BASE_SEC", 5.0)))
        except (TypeError, ValueError, OverflowError):
            base = 5.0
        try:
            maximum = max(base, float(getattr(Settings, "VLLM_CONNECT_BACKOFF_MAX_SEC", 60.0)))
        except (TypeError, ValueError, OverflowError):
            maximum = max(base, 60.0)
        async with lock:
            failures = int(getattr(self, "_transport_failure_count", 0) or 0) + 1
            self._transport_failure_count = failures
            delay = min(maximum, base * (2 ** min(failures - 1, 16)))
            self._transport_open_until = time.monotonic() + delay
            return delay

    async def _record_transport_success(self) -> None:
        """A successful HTTP response closes the connection circuit."""
        lock = getattr(self, "_transport_state_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._transport_state_lock = lock
        async with lock:
            self._transport_failure_count = 0
            self._transport_open_until = 0.0

    async def _post_with_retry(
        self,
        path: str,
        *,
        json: dict,
        retries: int = 1,
        retry_timeouts: bool = True,
    ) -> httpx.Response:
        """Retry one transient failure; back off after a confirmed connection outage."""
        retry_after = await self._transport_circuit_retry_after()
        if retry_after > 0.0:
            raise VLLMCircuitOpenError(retry_after)

        timeout_seconds = self._request_timeout_seconds()
        retry_limit = max(0, min(1, int(retries)))
        for attempt in range(retry_limit + 1):
            try:
                async with asyncio.timeout(timeout_seconds):
                    response = await self.client.post(
                        path,
                        json=json,
                        timeout=timeout_seconds,
                    )
                await self._record_transport_success()
                return response
            except (httpx.TimeoutException, TimeoutError) as exc:
                if attempt >= retry_limit or not retry_timeouts:
                    log.error(
                        "[vLLM TIMEOUT] Запрос не завершился за %.1fs; attempts=%d/%d; retry_timeouts=%s",
                        timeout_seconds, attempt + 1, retry_limit + 1, retry_timeouts,
                    )
                    raise
                log.warning(
                    "[vLLM RETRY] Тайм-аут запроса (%s, %.1fs); повтор 1/1 через 0.4s",
                    type(exc).__name__, timeout_seconds,
                )
                await asyncio.sleep(0.4)
            except httpx.TransportError as exc:
                if attempt >= retry_limit:
                    if isinstance(exc, httpx.ConnectError):
                        backoff = await self._record_transport_failure()
                        log.error(
                            "[vLLM CONNECT CIRCUIT] соединение недоступно после %d попыток; "
                            "backoff=%.1fs; дальнейшие запросы временно подавляются.",
                            attempt + 1, backoff,
                        )
                    else:
                        log.error(
                            "[vLLM REQUEST FAILED] Транспортная ошибка после %d попыток: %s",
                            attempt + 1, type(exc).__name__,
                        )
                    raise
                log.warning(
                    "[vLLM RETRY] Временный сетевой сбой (%s); повтор запроса 1/1 через 0.4s",
                    type(exc).__name__,
                )
                await asyncio.sleep(0.4)
        raise RuntimeError("vLLM retry loop exited unexpectedly")

    async def close(self):
        await self.client.aclose()
        log.info("[AIEngine] HTTP‑клиент закрыт")
