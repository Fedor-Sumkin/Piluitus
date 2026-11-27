import re
import json
import hashlib
import logging
import math
import asyncio
import time
import os
from uuid import uuid4
from httpx import AsyncClient, HTTPStatusError
from config import Settings
import httpx
from rag_prompt_utils import normalise_rag_context, select_prompt_rules
from signal_guard import classify_direction_evidence

log = logging.getLogger("Piluitus.AIEngine")
thought_log = logging.getLogger("Piluitus.EvolutionThoughts")


class VLLMCircuitOpenError(httpx.ConnectError):
    """Raised while vLLM is in a bounded connection-failure backoff window."""

    def __init__(self, retry_after_sec: float):
        self.retry_after_sec = max(0.0, float(retry_after_sec))
        super().__init__(
            f"vLLM connection circuit is open; retry after {self.retry_after_sec:.1f}s"
        )
MODEL_REASON_CODES = {
    "APPROVED",
    "HOLD_WEAK_DIRECTION",
    "HOLD_M1_CONTRADICTION",
    "HOLD_STALE_BBO",
    "HOLD_WEAK_VOLUME",
    "HOLD_WIDE_SPREAD",
    "HOLD_INSUFFICIENT_SUPPORT",
    "HOLD_RISK_REWARD",
    "HOLD_ENTRY_COST",
    "HOLD_UNCLEAR",
    "NO_LOCAL_CANDIDATE",
}

class AIEngine:
    @staticmethod
    def _effective_max_tokens() -> int:
        """Keep the short APPROVE/HOLD response within a bounded generation budget."""
        try:
            return max(384, min(768, int(getattr(Settings, "VLLM_MAX_TOKENS", 768))))
        except (TypeError, ValueError, OverflowError):
            return 768

    @staticmethod
    def _effective_max_concurrency() -> int:
        """Bound parallel vLLM calls so inference bursts cannot swamp the local server."""
        try:
            configured = int(getattr(Settings, "VLLM_MAX_CONCURRENCY", 4))
        except (TypeError, ValueError, OverflowError):
            configured = 4
        return max(1, min(16, configured))

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
        # Observe real request concurrency before introducing any artificial cap.
        self._transport_inflight_count = 0
        self._transport_peak_inflight = 0
        self._transport_inflight_lock = asyncio.Lock()
        self._transport_semaphore = asyncio.Semaphore(self._effective_max_concurrency())

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
            "🧠 AIEngine v34 загружен: файл=ai_engine.py | API=%s | модель=%s | max_tokens=%s | max_concurrency=%s",
            self.api_path, model_label,
            self._effective_max_tokens(), self._effective_max_concurrency(),
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

    async def _transport_request_started(self) -> int:
        """Track active HTTP calls for diagnostics; this does not throttle requests."""
        lock = getattr(self, "_transport_inflight_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._transport_inflight_lock = lock
        async with lock:
            current = int(getattr(self, "_transport_inflight_count", 0) or 0) + 1
            self._transport_inflight_count = current
            self._transport_peak_inflight = max(
                int(getattr(self, "_transport_peak_inflight", 0) or 0), current
            )
            return current

    async def _transport_request_finished(self) -> int:
        """Decrement active HTTP calls and return the remaining count."""
        lock = getattr(self, "_transport_inflight_lock", None)
        if lock is None:
            lock = asyncio.Lock()
            self._transport_inflight_lock = lock
        async with lock:
            current = max(0, int(getattr(self, "_transport_inflight_count", 0) or 0) - 1)
            self._transport_inflight_count = current
            return current

    async def _post_with_retry(
        self,
        path: str,
        *,
        json: dict,
        retries: int = 1,
        retry_timeouts: bool = True,
        request_context: dict | None = None,
    ) -> httpx.Response:
        """Retry one transient failure; attach metrics to this candidate's response."""
        retry_after = await self._transport_circuit_retry_after()
        if retry_after > 0.0:
            raise VLLMCircuitOpenError(retry_after)

        timeout_seconds = self._request_timeout_seconds()
        retry_limit = max(0, min(1, int(retries)))
        semaphore = getattr(self, "_transport_semaphore", None)
        if semaphore is None:
            semaphore = asyncio.Semaphore(self._effective_max_concurrency())
            self._transport_semaphore = semaphore

        for attempt in range(retry_limit + 1):
            queue_started = time.perf_counter()
            await semaphore.acquire()
            semaphore_wait_ms = (time.perf_counter() - queue_started) * 1000.0
            tracked = False
            attempt_started = time.perf_counter()
            response = None
            error_type = None
            last_exception = None
            retry_delay = False
            try:
                await self._transport_request_started()
                tracked = True
                async with asyncio.timeout(timeout_seconds):
                    response = await self.client.post(
                        path,
                        json=json,
                        timeout=timeout_seconds,
                    )
                await self._record_transport_success()
                return response
            except (httpx.TimeoutException, TimeoutError) as exc:
                last_exception = exc
                error_type = type(exc).__name__
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
                retry_delay = True
            except httpx.TransportError as exc:
                last_exception = exc
                error_type = type(exc).__name__
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
                retry_delay = True
            finally:
                elapsed_ms = (time.perf_counter() - attempt_started) * 1000.0
                try:
                    remaining = (
                        await self._transport_request_finished()
                        if tracked else int(getattr(self, "_transport_inflight_count", 0) or 0)
                    )
                finally:
                    semaphore.release()
                context = request_context if isinstance(request_context, dict) else {}
                transport_metrics = {
                    "candidate_id": str(context.get("candidate_id") or "UNATTRIBUTED"),
                    "ticker": str(context.get("ticker") or "UNKNOWN"),
                    "request_id": str(context.get("request_id") or "UNATTRIBUTED"),
                    "stage": str(context.get("stage") or "PRIMARY"),
                    "status_code": response.status_code if response is not None else None,
                    "attempt": attempt + 1,
                    "attempts_allowed": retry_limit + 1,
                    "queue_wait_ms": round(semaphore_wait_ms, 3),
                    "elapsed_ms": round(elapsed_ms, 3),
                    "inflight_after": remaining,
                    "peak_inflight": int(getattr(self, "_transport_peak_inflight", 0) or 0),
                    "concurrency_limit": self._effective_max_concurrency(),
                    "error_type": error_type,
                }
                if response is not None:
                    response.extensions["piluitus_transport_metrics"] = transport_metrics
                self._transport_last_metrics = dict(transport_metrics)
                if response is None and last_exception is not None:
                    try:
                        setattr(last_exception, "piluitus_transport_metrics", transport_metrics)
                    except Exception:
                        pass
                log.info(
                    "[vLLM TRANSPORT] candidate_id=%s request_id=%s ticker=%s stage=%s "
                    "status=%s attempt=%d/%d queue_wait_ms=%.1f elapsed_ms=%.1f "
                    "inflight=%d peak_inflight=%d limit=%d error=%s",
                    transport_metrics["candidate_id"], transport_metrics["request_id"],
                    transport_metrics["ticker"], transport_metrics["stage"],
                    response.status_code if response is not None else "-",
                    attempt + 1, retry_limit + 1, semaphore_wait_ms, elapsed_ms, remaining,
                    int(getattr(self, "_transport_peak_inflight", 0) or 0),
                    self._effective_max_concurrency(), error_type or "-",
                )
            if retry_delay:
                await asyncio.sleep(0.4)
        raise RuntimeError("vLLM retry loop exited unexpectedly")

    def _clean_text(self, text: str) -> str:
        """Нормализует текст, сохраняя английские названия индикаторов (VWAP, SMA, Bid/Ask)."""
        if not text:
            return "Нет данных."
        text = re.sub(r"<think>.*?(?:</think>|$)", " ", str(text), flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r"```(?:json)?|```", " ", text, flags=re.IGNORECASE)
        text = text.replace("—", "-").replace("–", "-").replace("…", "...")
        cleaned = re.sub(r"[^a-zA-Zа-яА-ЯёЁ0-9_\s\.,;:\-\(\)%₽?!+=/\'\"<>]", " ", text)
        cleaned = re.sub(r"\s+", " ", cleaned).strip(" \t\r\n,;:-")
        return cleaned or "Нет краткого вывода."

    def _compact_opinion(self, value: object, max_chars: int = 160) -> str:
        """Возвращает короткое завершённое предложение без обрезания посреди мысли."""
        fallback = "Недостаточно подтверждённых данных для краткого вывода."
        if value is None:
            return fallback
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False)

        text = self._clean_text(str(value))
        text = re.sub(r"^(?:Тактика|Критика|Вывод|Риск)\s*:\s*", "", text, flags=re.IGNORECASE)
        text = text.strip(" \t\r\n,;:-…")
        if not text or text.endswith("..."):
            return fallback

        # Убираем распространённые следы повреждённой генерации до показа в UI.
        broken = re.compile(
            r"\bregex\b|vendorsky|\bneither\b|\btrapping\b|^коротко\.?$|"
            r"куп(?:ку|ки|он(?:у)?)\s*(?:или|/)\s*продаж|"
            r"\b(?:metriки|decide|shortselling)\b|на\s*-\s*[a-zа-яё]+",
            re.IGNORECASE,
        )
        if broken.search(text):
            return fallback

        # Берём только первое полное предложение, если оно есть.
        sentence_match = re.search(r"^(.+?[.!?])(?:\s|$)", text)
        if sentence_match:
            sentence = sentence_match.group(1).strip()
            if len(sentence) <= max_chars:
                return sentence
            # Если первое предложение превышает лимит, не отрезаем его на полуслове.
            return fallback

        # Короткий текст без знака конца предложения можно безопасно завершить точкой.
        if len(text) < max_chars:
            return text.rstrip(" ,;:-") + "."

        # Нельзя надёжно сжать длинное незавершённое предложение без изменения смысла.
        return fallback

    @staticmethod
    def _build_local_opinions(
        model_decision: str,
        candidate_direction: str,
        metrics: object,
    ) -> tuple[str, str]:
        """Build UI/evolution explanations from structured verdict and measured metrics only."""
        side = candidate_direction if candidate_direction in {"BUY", "SELL"} else "NONE"
        decision = str(model_decision or "HOLD").upper()
        if side == "NONE":
            tactic = "Направленный локальный кандидат отсутствует."
        elif decision == "APPROVE":
            tactic = f"Модель одобрила локальный кандидат {side}."
        else:
            tactic = f"Модель отклонила локальный кандидат {side}."

        values: list[str] = []
        if isinstance(metrics, dict):
            specifications = (
                ("price_change_pct", "изменение цены", "%", True),
                ("price_vs_vwap_pct", "отклонение от VWAP", "%", True),
                ("volume_spike", "всплеск размера сделки", "x", False),
                ("spread_pct", "спред", "%", False),
            )
            for key, label, suffix, signed in specifications:
                raw = metrics.get(key)
                if isinstance(raw, bool):
                    continue
                try:
                    number = float(raw)
                except (TypeError, ValueError, OverflowError):
                    continue
                if not math.isfinite(number):
                    continue
                formatted = f"{number:+.3f}" if signed else f"{number:.3f}"
                values.append(f"{label} {formatted}{suffix}")

        if values:
            critic = "Измерения: " + "; ".join(values) + "."
        elif side == "NONE":
            critic = "Вход не запрашивался: отсутствует направленный кандидат."
        else:
            critic = "Свободное пояснение модели не используется; решение принято по JSON-вердикту."
        if decision == "APPROVE" and side in {"BUY", "SELL"}:
            critic += " ATR и портфельный риск проверяются кодом."
        elif decision == "HOLD" and side in {"BUY", "SELL"}:
            critic += " Вердикт модели — HOLD."
        return tactic, critic

    def _extract_message_text(self, choice: dict) -> str:
        """Извлекает текст финального ответа из OpenAI-compatible ответа vLLM."""
        message = choice.get("message")
        if isinstance(message, dict):
            content = message.get("content")
            if isinstance(content, str):
                return content.strip()
            if isinstance(content, list):
                parts = []
                for item in content:
                    if isinstance(item, str):
                        parts.append(item)
                    elif isinstance(item, dict) and isinstance(item.get("text"), str):
                        parts.append(item["text"])
                return "\n".join(parts).strip()
        # Совместимость с некоторыми OpenAI-compatible серверами/старыми форматами.
        fallback = choice.get("text", "")
        return fallback.strip() if isinstance(fallback, str) else str(fallback).strip()

    @staticmethod
    def _extract_json_object(text: str) -> dict:
        """Find a top-level JSON object; prefer one with the complete decision schema."""
        if not isinstance(text, str) or not text.strip():
            raise ValueError("vLLM вернул пустой текст вместо JSON")

        cleaned = text.strip().lstrip("\ufeff")
        cleaned = re.sub(r"^\s*```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```\s*$", "", cleaned)
        required = {"final_decision", "confidence_score"}
        complete_object = None
        decision_object = None
        first_object = None

        depth = 0
        root_start = None
        in_string = False
        escaped = False
        for index, current in enumerate(cleaned):
            if root_start is None:
                if current != "{":
                    continue
                root_start = index
                depth = 1
                in_string = False
                escaped = False
                continue

            if in_string:
                if escaped:
                    escaped = False
                elif current == "\\":
                    escaped = True
                elif current == '"':
                    in_string = False
                continue

            if current == '"':
                in_string = True
            elif current == "{":
                depth += 1
            elif current == "}":
                depth -= 1
                if depth == 0 and root_start is not None:
                    candidate = cleaned[root_start:index + 1]
                    try:
                        parsed = json.loads(candidate)
                    except json.JSONDecodeError:
                        parsed = None
                    if isinstance(parsed, dict):
                        if required.issubset(parsed):
                            complete_object = parsed
                            break
                        if "final_decision" in parsed and decision_object is None:
                            decision_object = parsed
                        if first_object is None:
                            first_object = parsed
                    root_start = None

        if complete_object is not None:
            return complete_object
        if decision_object is not None:
            return decision_object
        if first_object is not None:
            return first_object
        preview = re.sub(r"\s+", " ", cleaned)[:240] or "<пустой ответ>"
        raise ValueError(f"В ответе модели нет корректного JSON-объекта; начало ответа: {preview!r}")

    @staticmethod
    def _parse_confidence_score(value: object) -> float | None:
        """Parse a normalized score (0..1), a percent (0..100), or a percent string."""
        if value is None or isinstance(value, bool):
            return None
        is_percent = False
        if isinstance(value, str):
            text = value.strip().replace(',', '.')
            if text.endswith('%'):
                is_percent = True
                text = text[:-1].strip()
            if not text:
                return None
            try:
                number = float(text)
            except (TypeError, ValueError, OverflowError):
                return None
        elif isinstance(value, (int, float)):
            number = float(value)
        else:
            return None
        if not (number == number and abs(number) != float('inf')):
            return None
        if is_percent or number > 1.0:
            number /= 100.0
        if not 0.0 <= number <= 1.0:
            return None
        return number

    def _cpu_intensive_processing(self, raw_response_data: dict) -> dict:
        """Validate required APPROVE/HOLD fields and an optional bounded diagnostic reason code."""
        try:
            choices = raw_response_data.get("choices") if isinstance(raw_response_data, dict) else None
            if not isinstance(choices, list) or not choices or not isinstance(choices[0], dict):
                raise ValueError("Ответ API не содержит choices[0]")

            choice = choices[0]
            generated_text = self._extract_message_text(choice)
            if not generated_text:
                message = choice.get("message")
                message_keys = list(message.keys()) if isinstance(message, dict) else []
                finish_reason = choice.get("finish_reason", "не указан")
                raise ValueError(
                    "Пустой choices[0].message.content; "
                    f"finish_reason={finish_reason}; поля message={message_keys}"
                )

            parsed = self._extract_json_object(generated_text)
            required = {"final_decision", "confidence_score"}
            optional = {"reason_code"}
            actual = set(parsed)
            missing = sorted(required - actual)
            extra = sorted(actual - required - optional)
            if missing or extra:
                raise ValueError(
                    f"Нарушена схема JSON: missing={missing or '-'}; extra={extra or '-'}"
                )

            raw_decision_value = parsed.get("final_decision")
            if not isinstance(raw_decision_value, str):
                raise ValueError("Поле final_decision должно быть строкой APPROVE или HOLD")
            raw_decision = raw_decision_value.strip().upper()
            if raw_decision not in {"APPROVE", "HOLD"}:
                raise ValueError("Недопустимое final_decision: разрешены только APPROVE/HOLD")

            raw_confidence = parsed.get("confidence_score")
            if isinstance(raw_confidence, bool) or not isinstance(raw_confidence, (int, float)):
                raise ValueError("Поле confidence_score должно быть JSON-числом от 0 до 1")
            confidence = float(raw_confidence)
            if not math.isfinite(confidence) or not 0.0 <= confidence <= 1.0:
                raise ValueError("Поле confidence_score должно находиться в диапазоне 0..1")

            raw_reason_code = parsed.get("reason_code")
            reason_code = (
                str(raw_reason_code).strip().upper()
                if isinstance(raw_reason_code, str) else "NOT_PROVIDED"
            )
            if reason_code not in MODEL_REASON_CODES:
                # Older prompt/model combinations sometimes emit HOLD_{ENUM} or
                # append a short explanation to the enum. Recover only an exact
                # known reason; never infer a reason from arbitrary metric text.
                legacy_match = re.fullmatch(r"HOLD_\{\s*([A-Z0-9_]+)\s*\}", reason_code)
                legacy_reason = legacy_match.group(1) if legacy_match else ""
                if legacy_reason in MODEL_REASON_CODES:
                    reason_code = legacy_reason
                elif f"HOLD_{legacy_reason}" in MODEL_REASON_CODES:
                    reason_code = f"HOLD_{legacy_reason}"
                elif "M1_NEUTRAL" in reason_code or "M1_UNAVAILABLE" in reason_code:
                    reason_code = "HOLD_INSUFFICIENT_SUPPORT"
                elif reason_code == "HOLD":
                    reason_code = "HOLD_UNCLEAR"
                else:
                    # reason_code is advisory; an invalid value must not turn a
                    # valid APPROVE/HOLD JSON contract into a warning storm.
                    log.debug("[vLLM REASON CODE] Unrecognized optional value %r; using NOT_PROVIDED.", raw_reason_code)
                    reason_code = "NOT_PROVIDED"
            usage = raw_response_data.get("usage", {})
            return {
                "error_type": None,
                "model_reason_code": reason_code,
                "final_decision": raw_decision,
                "model_decision": raw_decision,
                "model_raw_decision": raw_decision,
                "model_contract_violation": False,
                "contract_error_type": None,
                "confidence_score": confidence,
                "confidence_valid": True,
                "confidence_raw": raw_confidence,
                "finish_reason": choice.get("finish_reason", "не указан"),
                "usage": usage if isinstance(usage, dict) else {},
            }
        except Exception as exc:
            try:
                choice = raw_response_data.get("choices", [{}])[0]
                raw_preview = self._extract_message_text(choice)[:300] if isinstance(choice, dict) else ""
                finish_reason = choice.get("finish_reason", "не указан") if isinstance(choice, dict) else "не указан"
            except Exception:
                raw_preview = ""
                finish_reason = "не указан"
            usage = raw_response_data.get("usage", {}) if isinstance(raw_response_data, dict) else {}
            error_text = str(exc)
            lowered = error_text.lower()
            if finish_reason == "length":
                error_type = "truncated"
            elif "схем" in lowered or "missing=" in lowered or "extra=" in lowered:
                error_type = "schema"
            elif "final_decision" in lowered:
                error_type = "invalid_decision"
            elif "confidence_score" in lowered:
                error_type = "invalid_confidence"
            elif "json" in lowered:
                error_type = "invalid_json"
            else:
                error_type = "invalid_response"
            return {
                "error_type": "text_parsing",
                "exc": error_text,
                "raw_preview": raw_preview,
                "finish_reason": finish_reason,
                "usage": usage if isinstance(usage, dict) else {},
                "final_decision": "HOLD",
                "model_decision": "HOLD",
                "model_raw_decision": "INVALID_RESPONSE",
                "model_contract_violation": True,
                "confidence_score": 0.0,
                "confidence_valid": False,
                "confidence_raw": None,
                "model_reason_code": "NOT_PROVIDED",
                "contract_error_type": error_type,
            }

    @staticmethod
    def _diagnostic_preview(text: object, limit: int = 180) -> str:
        """Короткий диагностический фрагмент без длинных блоков reasoning."""
        if not isinstance(text, str):
            return "<нет текста>"
        safe = re.sub(r"<think>.*?(?:</think>|$)", "<think>…</think>", text, flags=re.DOTALL | re.IGNORECASE)
        safe = re.sub(r"\s+", " ", safe).strip()
        return safe[:limit]

    @staticmethod
    def _http_error_detail(response: httpx.Response, limit: int = 1400) -> str:
        """Извлекает описание ошибки vLLM из JSON или обычного тела ответа."""
        detail = ""
        try:
            body = response.json()
        except ValueError:
            body = None
        if isinstance(body, dict):
            error = body.get("error", body)
            if isinstance(error, dict):
                detail = str(error.get("message") or error.get("detail") or error)
            else:
                detail = str(error)
        if not detail:
            detail = response.text or "Пустое тело ответа сервера."
        detail = re.sub(r"\s+", " ", detail).strip()
        return detail[:limit]

    @staticmethod
    def _is_response_format_error(detail: str) -> bool:
        lower = detail.lower()
        return any(marker in lower for marker in (
            "response_format", "json_object", "structured output",
            "structured_outputs", "guided_json", "guided decoding",
        ))

    @staticmethod
    def _is_context_length_error(detail: str) -> bool:
        lower = detail.lower()
        return any(marker in lower for marker in (
            "maximum context length", "max_model_len", "context length",
            "context window", "maximum sequence length", "max_seq_len",
            "prompt is too long", "too many tokens", "input token",
            "number of tokens", "exceeds the model length",
        ))

    @staticmethod
    def _is_sampling_parameter_error(detail: str) -> bool:
        lower = detail.lower()
        return "temperature" in lower and any(
            marker in lower for marker in ("unsupported", "not supported", "invalid", "unknown")
        )

    @staticmethod
    def _prompt_value(value: object, max_len: int = 28) -> str:
        """Сжимает значение метрики, не превращая отсутствие данных в ноль."""
        if value is None:
            return "NA"
        if isinstance(value, bool):
            return "1" if value else "0"
        if isinstance(value, (int, float)):
            try:
                numeric = float(value)
                if not (numeric == numeric and abs(numeric) != float("inf")):
                    return "NA"
                return f"{numeric:.4g}"
            except (TypeError, ValueError, OverflowError):
                return "NA"
        text = re.sub(r"\s+", " ", str(value)).strip()
        return text[:max_len]

    @staticmethod
    def _metric_number(metrics: object, key: str, default: float = math.nan) -> float:
        if not isinstance(metrics, dict):
            return default
        value = metrics.get(key)
        if isinstance(value, bool):
            return float(value)
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return default
        return number if math.isfinite(number) else default

    @classmethod
    def _m1_evidence_state(cls, metrics: object, candidate_direction: str) -> str:
        """Classify M1 as aligned/opposite/neutral/unavailable using fresh closed bars only."""
        direction = str(candidate_direction or "").strip().upper()
        if direction not in {"BUY", "SELL"} or not isinstance(metrics, dict):
            return "UNAVAILABLE"
        bar_count = cls._metric_number(metrics, "bar_count", 0.0)
        atr_periods = cls._metric_number(metrics, "bar_atr_periods", 0.0)
        atr_age = cls._metric_number(metrics, "bar_atr_age_sec", math.nan)
        if not math.isfinite(atr_age) or atr_age < 0.0 or atr_age > 180.0 or atr_periods < 5.0:
            return "UNAVAILABLE"
        pattern_confirmed = cls._metric_number(metrics, "bar_pattern_confirmed", 0.0) >= 0.5
        pattern_direction = cls._metric_number(metrics, "bar_pattern_direction_code", 0.0)
        if pattern_confirmed and pattern_direction in {-1.0, 1.0}:
            matching = (direction == "BUY" and pattern_direction > 0.0) or (
                direction == "SELL" and pattern_direction < 0.0
            )
            return "ALIGNED" if matching else "OPPOSITE"
        # EMA9/EMA21 are not meaningful until 21 contiguous closed M1 bars exist.
        if bar_count < 21.0:
            return "UNAVAILABLE"
        trend_up = cls._metric_number(metrics, "bar_trend_up", 0.0) >= 0.5
        trend_down = cls._metric_number(metrics, "bar_trend_down", 0.0) >= 0.5
        if trend_up and trend_down:
            return "UNAVAILABLE"
        if direction == "BUY":
            if trend_down:
                return "OPPOSITE"
            if trend_up:
                return "ALIGNED"
        else:
            if trend_up:
                return "OPPOSITE"
            if trend_down:
                return "ALIGNED"
        return "NEUTRAL"

    @staticmethod
    def _max_spread_to_atr_ratio() -> float:
        try:
            value = float(getattr(Settings, "MAX_SPREAD_TO_ATR_RATIO", 0.50))
        except (TypeError, ValueError, OverflowError):
            value = 0.50
        if not math.isfinite(value):
            return 0.50
        return min(2.0, max(0.05, value))

    @staticmethod
    def _direction_momentum_neutral_band_pct() -> float:
        try:
            value = float(os.getenv("PILUITUS_DIRECTION_MOMENTUM_NEUTRAL_PCT", "0.01"))
        except (TypeError, ValueError, OverflowError):
            value = 0.01
        return min(1.0, max(0.0, value)) if math.isfinite(value) else 0.01

    @staticmethod
    def _direction_vwap_neutral_band_pct() -> float:
        try:
            value = float(os.getenv("PILUITUS_DIRECTION_VWAP_NEUTRAL_PCT", "0.03"))
        except (TypeError, ValueError, OverflowError):
            value = 0.03
        return min(1.0, max(0.0, value)) if math.isfinite(value) else 0.03

    @classmethod
    def _prepare_prompt_metrics(cls, metrics: object, candidate_direction: str) -> dict:
        prepared = dict(metrics) if isinstance(metrics, dict) else {}
        spread = cls._metric_number(prepared, "spread_pct", math.nan)
        atr_pct = cls._metric_number(prepared, "bar_atr_pct", math.nan)
        spread_available = cls._metric_number(
            prepared, "spread_available", 1.0 if math.isfinite(spread) and spread >= 0.0 else 0.0
        )
        if spread_available >= 0.5 and math.isfinite(spread) and spread >= 0.0 and math.isfinite(atr_pct) and atr_pct > 0.0:
            prepared["spread_to_atr_ratio"] = spread / atr_pct
        else:
            prepared["spread_to_atr_ratio"] = None
        prepared["m1_evidence_state"] = cls._m1_evidence_state(prepared, candidate_direction)
        prepared["direction_evidence_state"] = classify_direction_evidence(candidate_direction, prepared)
        prepared["spread_to_atr_limit"] = cls._max_spread_to_atr_ratio()
        return prepared

    @classmethod
    def _infer_hold_reason_code(
        cls,
        candidate_direction: str,
        metrics: object,
    ) -> str:
        """Infer a diagnostic HOLD reason from measured evidence; never changes the verdict."""
        direction = str(candidate_direction or "").strip().upper()
        prepared = cls._prepare_prompt_metrics(metrics, direction)

        if direction not in {"BUY", "SELL"}:
            return "NO_LOCAL_CANDIDATE"

        if prepared.get("m1_evidence_state") == "OPPOSITE":
            return "HOLD_M1_CONTRADICTION"

        direction_state = str(prepared.get("direction_evidence_state") or "UNAVAILABLE")
        if direction_state == "OPPOSITE":
            return "HOLD_WEAK_DIRECTION"

        trade_ready = cls._metric_number(prepared, "trade_data_ready", 0.0) >= 0.5
        trade_age = cls._metric_number(prepared, "trade_metrics_age_sec", math.inf)
        if not trade_ready or not math.isfinite(trade_age) or not 0.0 <= trade_age <= 5.0:
            return "HOLD_INSUFFICIENT_SUPPORT"

        bid = cls._metric_number(prepared, "bid", math.nan)
        ask = cls._metric_number(prepared, "ask", math.nan)
        bid_age = cls._metric_number(prepared, "bid_quote_age_sec", math.inf)
        ask_age = cls._metric_number(prepared, "ask_quote_age_sec", math.inf)
        bbo_available = (
            cls._metric_number(prepared, "bid_ask_available", 0.0) >= 0.5
            and cls._metric_number(prepared, "spread_available", 0.0) >= 0.5
        )
        if (
            not bbo_available or not math.isfinite(bid) or not math.isfinite(ask)
            or bid <= 0.0 or ask < bid or not 0.0 <= bid_age <= 2.0
            or not 0.0 <= ask_age <= 2.0
        ):
            return "HOLD_STALE_BBO"

        spread = cls._metric_number(prepared, "spread_pct", math.nan)
        spread_ratio = cls._metric_number(prepared, "spread_to_atr_ratio", math.nan)
        if (
            (math.isfinite(spread) and spread > 0.5)
            or (math.isfinite(spread_ratio) and spread_ratio > cls._max_spread_to_atr_ratio())
        ):
            return "HOLD_WIDE_SPREAD"

        volume_spike = cls._metric_number(prepared, "volume_spike", math.nan)
        if not math.isfinite(volume_spike) or volume_spike < 1.5:
            return "HOLD_WEAK_VOLUME"

        price_change = cls._metric_number(prepared, "price_change_pct", math.nan)
        momentum_band = cls._direction_momentum_neutral_band_pct()
        momentum_aligned = (
            (direction == "BUY" and math.isfinite(price_change) and price_change > momentum_band)
            or (direction == "SELL" and math.isfinite(price_change) and price_change < -momentum_band)
        )
        if not momentum_aligned and prepared.get("m1_evidence_state") != "ALIGNED":
            return "HOLD_WEAK_DIRECTION"

        return "HOLD_INSUFFICIENT_SUPPORT"

    @classmethod
    def _validate_model_reason_code(
        cls,
        model_decision: str,
        reason_code: str,
        candidate_direction: str,
        metrics: object,
    ) -> tuple[str, str]:
        """Reconcile advisory LLM reason codes with measured evidence; never changes the verdict."""
        decision = str(model_decision or "HOLD").strip().upper()
        reason = str(reason_code or "NOT_PROVIDED").strip().upper()
        prepared = cls._prepare_prompt_metrics(metrics, candidate_direction)
        replacement = reason
        if decision == "APPROVE":
            replacement = "APPROVED"
        elif decision == "HOLD":
            if reason in {"NOT_PROVIDED", "HOLD_UNCLEAR"}:
                inferred = cls._infer_hold_reason_code(candidate_direction, prepared)
                if inferred != "HOLD_INSUFFICIENT_SUPPORT" or reason == "NOT_PROVIDED":
                    replacement = inferred
            if reason == "APPROVED":
                replacement = "HOLD_UNCLEAR"
            elif reason == "HOLD_M1_CONTRADICTION" and prepared.get("m1_evidence_state") != "OPPOSITE":
                # The model's reason is advisory. If its claimed M1 contradiction
                # disagrees with the measured closed-bar state, reclassify the
                # HOLD using the other independent evidence instead of masking a
                # measurable spread/BBO/volume problem as generic insufficiency.
                replacement = cls._infer_hold_reason_code(candidate_direction, prepared)
                if replacement == "HOLD_M1_CONTRADICTION":
                    replacement = "HOLD_INSUFFICIENT_SUPPORT"
            elif reason == "HOLD_WEAK_DIRECTION":
                direction = str(candidate_direction or "").strip().upper()
                price_change = cls._metric_number(prepared, "price_change_pct", math.nan)
                trade_ready = cls._metric_number(prepared, "trade_data_ready", 0.0) >= 0.5
                metrics_age = cls._metric_number(prepared, "trade_metrics_age_sec", math.inf)
                direction_state = classify_direction_evidence(direction, prepared)
                try:
                    momentum_band = max(
                        0.0, float(os.getenv("PILUITUS_DIRECTION_MOMENTUM_NEUTRAL_PCT", "0.01"))
                    )
                except (TypeError, ValueError, OverflowError):
                    momentum_band = 0.01
                momentum_aligned = (
                    ((direction == "BUY" and price_change > momentum_band)
                     or (direction == "SELL" and price_change < -momentum_band))
                    and direction_state in {"ALIGNED", "NEUTRAL"}
                )
                m1_aligned = (
                    prepared.get("m1_evidence_state") == "ALIGNED"
                    and direction_state in {"ALIGNED", "NEUTRAL"}
                )
                if trade_ready and math.isfinite(metrics_age) and 0.0 <= metrics_age <= 5.0 and (
                    momentum_aligned or m1_aligned
                ):
                    replacement = "HOLD_INSUFFICIENT_SUPPORT"
            elif reason == "HOLD_WIDE_SPREAD":
                spread = cls._metric_number(prepared, "spread_pct", math.nan)
                ratio = cls._metric_number(prepared, "spread_to_atr_ratio", math.nan)
                ratio_limit = cls._max_spread_to_atr_ratio()
                if not (math.isfinite(spread) and spread > 0.5) and not (
                    math.isfinite(ratio) and ratio > ratio_limit
                ):
                    replacement = "HOLD_INSUFFICIENT_SUPPORT"
            elif reason == "HOLD_WEAK_VOLUME":
                volume_spike = cls._metric_number(prepared, "volume_spike", math.nan)
                if math.isfinite(volume_spike) and volume_spike >= 1.5:
                    replacement = "HOLD_INSUFFICIENT_SUPPORT"
            elif reason == "HOLD_STALE_BBO":
                max_age = cls._metric_number(prepared, "bbo_max_age_sec", 2.0)
                bid_age = cls._metric_number(prepared, "bid_quote_age_sec", math.nan)
                ask_age = cls._metric_number(prepared, "ask_quote_age_sec", math.nan)
                bid = cls._metric_number(prepared, "bid", math.nan)
                ask = cls._metric_number(prepared, "ask", math.nan)
                flags_ok = (
                    cls._metric_number(prepared, "bid_ask_available", 0.0) >= 0.5
                    and cls._metric_number(prepared, "spread_available", 0.0) >= 0.5
                )
                if (
                    flags_ok and math.isfinite(bid) and math.isfinite(ask) and bid > 0.0
                    and ask >= bid and math.isfinite(bid_age) and math.isfinite(ask_age)
                    and 0.0 <= bid_age <= max_age and 0.0 <= ask_age <= max_age
                ):
                    replacement = "HOLD_INSUFFICIENT_SUPPORT"
        if decision == "HOLD" and reason in {"NOT_PROVIDED", "HOLD_UNCLEAR"} and replacement != reason:
            return replacement, "INFERRED"
        return replacement, "CONSISTENT" if replacement == reason else "RECLASSIFIED"

    @classmethod
    def _build_live_prompt(
        cls,
        ticker: object,
        price: object,
        volume: object,
        candidate_direction: str,
        metrics: object,
        rag_context: str,
        *,
        max_chars: int = 2100,
        include_rule_text: bool = True,
    ) -> str:
        """Короткий prompt с запасом под лимит vLLM 2048 токенов."""
        direction = candidate_direction if candidate_direction in {"BUY", "SELL"} else "NONE"
        # Keep the most useful directional and trade-quality measurements first:
        # prompt overflow may trim later context, never the core 30s flow metrics.
        common_keys = (
            # The prompt is character-bounded. Keep decision-critical trade flow
            # and M1 state first so optional diagnostics cannot crowd them out.
            "trade_data_ready", "trade_metrics_age_sec", "trade_history_coverage_sec",
            "trade_volume_5s", "trade_volume_baseline_5s", "trade_count_60s",
            "trade_flow_imbalance_30s", "trade_price_change_30s_pct",
            "trade_realized_volatility_60s_pct",
            "order_book_valid", "order_book_age_sec",
            "queue_imbalance_5s", "queue_imbalance_5s_available",
            "queue_imbalance_5s_coverage_sec", "queue_imbalance_5s_age_sec",
            "ofi_5s", "ofi_5s_available", "ofi_5s_coverage_sec", "ofi_5s_age_sec",
            "entry_cost_to_atr_ratio", "entry_cost_to_atr_limit", "entry_cost_evaluation_limit",
            "entry_minimum_favorable_move_pct", "entry_cost_model_status",
            "entry_risk_reward_available", "entry_planned_reward_to_risk",
            "entry_net_reward_to_risk", "entry_min_net_reward_to_risk",
            "entry_stop_distance_pct", "entry_target_move_pct", "entry_net_reward_pct",
            "bar_count", "bar_atr_periods", "bar_atr_age_sec", "m1_evidence_state",
            "bar_trend_up", "bar_trend_down",
            "price_change_pct", "price_vs_vwap_pct", "volume_spike", "volume_spike_raw",
            "spread_pct", "spread_to_atr_ratio", "spread_to_atr_limit",
            "bid_ask_available", "spread_available", "bid_quote_age_sec",
            "ask_quote_age_sec", "bbo_max_age_sec",
            "trade_count_5s", "trade_baseline_bucket_count",
            "trade_baseline_trade_count", "trade_baseline_min_bucket_count",
            "trade_buy_volume_30s", "trade_sell_volume_30s", "trade_vwap_60s",
            "bid", "ask", "queue_imbalance", "queue_imbalance_available",
            "bar_atr_pct", "bar_body_ratio", "bar_close_position",
        )
        if direction == "SELL":
            directional_keys = (
                "bar_pattern_confirmed", "bar_pattern_direction_code", "bar_breakout_down",
                "bar_pullback_sell", "bid_size_drop_pct", "top_bid_history_samples",
                "bid_size_drop_age_sec",
            )
        else:
            directional_keys = (
                "bar_pattern_confirmed", "bar_pattern_direction_code", "bar_breakout_up",
                "bar_pullback_buy", "ask_size_drop_pct", "top_ask_history_samples",
                "ask_size_drop_age_sec",
            )
        metric_keys = common_keys + directional_keys + ("bar_pattern_name", "bar_signal_gap_atr")
        prompt_metrics = cls._prepare_prompt_metrics(metrics, direction)
        metric_parts: list[str] = []
        if prompt_metrics:
            for key in metric_keys:
                value = prompt_metrics.get(key)
                if value is None:
                    continue
                metric_parts.append(f"{key}={cls._prompt_value(value, 18)}")
        metrics_text = ";".join(metric_parts)

        momentum_neutral_band_pct = cls._direction_momentum_neutral_band_pct()
        vwap_neutral_band_pct = cls._direction_vwap_neutral_band_pct()
        direction_rules = {
            "BUY": (
                f"BUY requires positive 15s trade momentum beyond +{momentum_neutral_band_pct:.3f}%. "
                "A fresh bullish M1 pattern may support BUY if VWAP is not materially opposite; "
                "positive 30s flow supports BUY."
            ),
            "SELL": (
                f"SELL requires negative 15s trade momentum beyond -{momentum_neutral_band_pct:.3f}%. "
                "A fresh bearish M1 pattern may support SELL if VWAP is not materially opposite; "
                "negative 30s flow supports SELL."
            ),
            "NONE": "No local direction: MUST HOLD.",
        }[direction]

        quote_depletion_rule = (
            "Ask depletion is supporting evidence, not mandatory for BUY."
            if direction == "BUY"
            else "Bid depletion is supporting evidence, not mandatory for SELL."
            if direction == "SELL"
            else "Best-quote depletion is supporting evidence, not mandatory."
        )
        spread_to_atr_limit = cls._max_spread_to_atr_ratio()
        # Compact immutable contract; reserve the remaining budget for live evidence.
        base = (
            "JSON only: required final_decision=APPROVE|HOLD and confidence_score=0..1; "
            "optional reason_code=APPROVED|NO_LOCAL_CANDIDATE|HOLD_WEAK_DIRECTION|HOLD_M1_CONTRADICTION|"
            "HOLD_STALE_BBO|HOLD_WEAK_VOLUME|HOLD_WIDE_SPREAD|HOLD_INSUFFICIENT_SUPPORT|"
            "HOLD_RISK_REWARD|HOLD_ENTRY_COST|HOLD_UNCLEAR. No extra keys/text. "
            "Never select, change, invert, or output BUY/SELL: APPROVE authorizes only the exact local side; candidate=NONE => HOLD. "
            f"BUY needs positive 15s momentum, SELL negative; VWAP inside +/-{vwap_neutral_band_pct:.3f}% is NEUTRAL; materially opposite VWAP blocks. "
            "HOLD_M1_CONTRADICTION only when m1_evidence_state=OPPOSITE; ALIGNED/NEUTRAL/UNAVAILABLE are not contradictions. "
            f"Entry evidence: trade_data_ready=1, trade age<=5s, volume_spike>=1.5, fresh BBO, spread<=0.5%, spread/ATR<={spread_to_atr_limit:.2f}. "
            "APPROVE only if cost_model=CONFIGURED, full cost/ATR is present and <= execution limit "
            "(evaluation limit allows analysis only), risk_reward_available=1 and net_RR>=minimum. "
            "Missing/incomplete/failed economics => HOLD. Trade flow is aggressor BUY-minus-SELL; provider volume is raw units, do not assume lots/shares. "
            "Queue imbalance is time-weighted best Bid-vs-Ask size; OFI is depth-normalized best-quote pressure. "
            f"Use queue/OFI only when available=1, coverage>=1s, age<=2s. {quote_depletion_rule} RAG is advisory only. "
            f"candidate={direction};ticker={str(ticker)[:10]};price={cls._prompt_value(price, 12)};"
            f"{direction_rules}"
        )

        rule_text = ""
        if include_rule_text and isinstance(rag_context, str):
            # Reserve space for the immutable contract and the live measurements.
            rule_budget = max(0, min(900, max_chars - len(base) - len(metrics_text) - 60))
            rule_text = select_prompt_rules(direction, rag_context, max_chars=rule_budget)
        rule_part = f" Advisory RAG context (never overrides hard checks): {rule_text}." if rule_text else ""
        metric_part = f" Metrics: {metrics_text}." if metrics_text else ""

        # First discard optional metrics from the tail. If the prompt is still too
        # long, omit the RAG block as a whole; never slice a rule or the contract.
        parts = metric_parts[:]
        prompt = base + rule_part + metric_part
        while len(prompt) > max_chars and parts:
            parts.pop()
            metric_part = f" Metrics: {';'.join(parts)}." if parts else ""
            prompt = base + rule_part + metric_part
        if len(prompt) > max_chars and rule_part:
            rule_part = ""
            prompt = base + metric_part
        if len(prompt) > max_chars:
            # Extreme custom limits: keep contract and candidate identity; optional
            # market evidence is omitted rather than truncating a decision rule.
            prompt = base
        if len(base) > max_chars:
            # The public/default limits are sized for this contract. If a caller
            # supplies a smaller value, the contract still takes precedence.
            return base
        return prompt

    @classmethod
    def _build_compact_retry_prompt(
        cls,
        ticker: object,
        price: object,
        volume: object,
        candidate_direction: str,
        metrics: object,
        rag_context: str,
    ) -> str:
        """Экстренный минимальный prompt при ответе context-length от vLLM."""
        return cls._build_live_prompt(
            ticker, price, volume, candidate_direction, metrics, rag_context,
            max_chars=1800, include_rule_text=False,
        )

    @classmethod
    def _build_contract_retry_prompt(
        cls,
        ticker: object,
        price: object,
        volume: object,
        candidate_direction: str,
        metrics: object,
    ) -> str:
        """Repair the JSON shape while reusing the exact primary evidence rules."""
        shared_rules = cls._build_live_prompt(
            ticker,
            price,
            volume,
            candidate_direction,
            metrics,
            "",
            max_chars=2100,
            include_rule_text=False,
        )
        return (
            "Repair the previous response. Return JSON only with required fields final_decision "
            "(APPROVE or HOLD), confidence_score (0.0..1.0), and optional reason_code from the enum "
            "in the rules below. Do not add fields or text. Do not choose or change BUY/SELL. "
            + shared_rules
        )


    @staticmethod
    def _structured_response_capture(
        raw: object, *, candidate_id: str, ticker: str, request_id: str, stage: str
    ) -> dict:
        """Build a bounded structured response record; never retain model reasoning tags."""
        source = raw if isinstance(raw, dict) else {}
        choices = source.get("choices") if isinstance(source.get("choices"), list) else []
        choice = choices[0] if choices and isinstance(choices[0], dict) else {}
        message = choice.get("message") if isinstance(choice.get("message"), dict) else {}
        content = message.get("content", choice.get("text", ""))
        if isinstance(content, list):
            content = "\n".join(
                str(item.get("text", "")) if isinstance(item, dict) else str(item)
                for item in content
            )
        content = re.sub(r"<think>.*?(?:</think>|$)", "[REDACTED_REASONING]", str(content or ""), flags=re.DOTALL | re.IGNORECASE)[:16384]
        usage = source.get("usage", {})
        capture = {
            "event": "vllm_structured_response", "candidate_id": str(candidate_id),
            "ticker": str(ticker), "request_id": str(request_id), "stage": str(stage),
            "response_id": source.get("id"), "model": source.get("model"),
            "created": source.get("created"), "finish_reason": choice.get("finish_reason"),
            "usage": usage if isinstance(usage, dict) else {}, "content": content,
        }
        log.info("[vLLM STRUCTURED RESPONSE] %s", json.dumps(capture, ensure_ascii=False, separators=(",", ":"), default=str))
        return capture

    @staticmethod
    def _approval_economics_gate(candidate_direction: str, metrics: object) -> tuple[bool, str, dict]:
        """Hard code-side approval gate; the model cannot waive incomplete economics."""
        values = metrics if isinstance(metrics, dict) else {}
        ratio = AIEngine._metric_number(values, "entry_cost_to_atr_ratio", math.nan)
        limit = AIEngine._metric_number(values, "entry_cost_to_atr_limit", math.nan)
        net_rr = AIEngine._metric_number(values, "entry_net_reward_to_risk", math.nan)
        min_rr = AIEngine._metric_number(values, "entry_min_net_reward_to_risk", math.nan)
        complete = str(values.get("entry_cost_model_status") or "").strip().upper() == "CONFIGURED"
        rr_available = AIEngine._metric_number(values, "entry_risk_reward_available", 0.0) >= 0.5
        details = {
            "cost_ratio": ratio if math.isfinite(ratio) else None,
            "execution_limit": limit if math.isfinite(limit) else None,
            "cost_model_status": str(values.get("entry_cost_model_status") or "UNKNOWN"),
            "net_reward_to_risk": net_rr if math.isfinite(net_rr) else None,
            "minimum_net_reward_to_risk": min_rr if math.isfinite(min_rr) else None,
            "risk_reward_available": rr_available,
        }
        if candidate_direction not in {"BUY", "SELL"}:
            return False, "NO_LOCAL_CANDIDATE", details
        if not complete or not math.isfinite(ratio) or not math.isfinite(limit) or ratio > limit:
            return False, "HOLD_ENTRY_COST", details
        if not rr_available or not math.isfinite(net_rr) or not math.isfinite(min_rr) or net_rr < min_rr:
            return False, "HOLD_RISK_REWARD", details
        return True, "APPROVED", details

    @staticmethod
    def _normalise_rag_context(value: object) -> str:
        """Return raw RAG rule lines without decorative bullets that break matching."""
        return normalise_rag_context(value)

    async def analyze_market_anomaly(self, payload: dict) -> dict:
        if not isinstance(payload, dict):
            payload = {"raw_data": str(payload)}
            
        ticker = str(payload.get("ticker", "UNKNOWN"))
        price = payload.get("price", 0.0)
        volume = payload.get("volume", 0)
        candidate_id = str(payload.get("shadow_candidate_id") or payload.get("candidate_id") or f"UNLINKED-{uuid4().hex}")
        request_id = uuid4().hex
        transport_samples: list[dict] = []
        structured_capture: dict | None = None

        async def _post_candidate(body: dict, *, retries: int = 1, retry_timeouts: bool = True, stage: str = "PRIMARY") -> httpx.Response:
            context = {"candidate_id": candidate_id, "ticker": ticker, "request_id": request_id, "stage": stage}
            try:
                options = {"request_context": context}
                if retries != 1:
                    options["retries"] = retries
                if retry_timeouts is not True:
                    options["retry_timeouts"] = retry_timeouts
                response = await self._post_with_retry(self.api_path, json=body, **options)
            except Exception as exc:
                sample = getattr(exc, "piluitus_transport_metrics", None)
                if isinstance(sample, dict):
                    transport_samples.append(dict(sample))
                raise
            sample = response.extensions.get("piluitus_transport_metrics")
            if isinstance(sample, dict):
                transport_samples.append(dict(sample))
            return response

        def _transport_summary() -> dict:
            return {
                "candidate_id": candidate_id, "request_id": request_id,
                "attempt_count": len(transport_samples), "samples": list(transport_samples),
            }
        candidate_direction = str(payload.get("candidate_direction", "NONE")).upper()
        if candidate_direction not in {"BUY", "SELL"}:
            candidate_direction = "NONE"

        raw_metrics = payload.get("metrics", {})
        metrics_dict = raw_metrics if isinstance(raw_metrics, dict) else {}

        rag_str = self._normalise_rag_context(payload.get("rag_context", []))

        if not rag_str.strip():
            rag_str = "Нет активного правила для текущих данных. Направленный сигнал не подтверждён; выбрать HOLD."

        # Контекст сервера пользователя ограничен 2048 токенами. Не отправляем
        # исходный длинный prompt с полным словарём метрик и несколькими RAG-правилами.
        user_prompt = self._build_live_prompt(
            ticker, price, volume, candidate_direction, metrics_dict, rag_str,
            max_chars=2100, include_rule_text=True,
        )
        prompt_fingerprint = hashlib.sha256(user_prompt.encode("utf-8")).hexdigest()[:16]
        log.debug(
            "[vLLM PROMPT] ticker=%s direction=%s chars=%d metrics=%d fingerprint=%s",
            ticker, candidate_direction, len(user_prompt),
            len(metrics_dict) if isinstance(metrics_dict, dict) else 0,
            prompt_fingerprint,
        )

        start_inference_time = time.time()
        request_timeout = self._request_timeout_seconds()

        try:
            # Two request attempts plus the retry delay fit inside this bounded phase.
            async with asyncio.timeout(request_timeout * 2.0 + 0.5):
                configured_max_tokens = self._effective_max_tokens()
                request_body = {
                    "model": self.model_name,
                    "messages": [{"role": "user", "content": user_prompt}],
                    "response_format": {"type": "json_object"},
                    "temperature": 0.1,
                    "max_tokens": configured_max_tokens,
                }
                active_body = dict(request_body)
                resp = await _post_candidate(active_body)

                # Сначала обрабатываем только известные причины отказа API.
                if resp.status_code in (400, 422):
                    error_detail = self._http_error_detail(resp)
                    if self._is_response_format_error(error_detail) and "response_format" in request_body:
                        log.warning(
                            "[vLLM COMPAT] Сервер отклонил response_format; повторяем без JSON-mode. Причина: %s",
                            error_detail,
                        )
                        active_body.pop("response_format", None)
                        resp = await _post_candidate(active_body, retries=0)

                if resp.status_code in (400, 422):
                    error_detail = self._http_error_detail(resp)
                    if self._is_context_length_error(error_detail):
                        compact_body = dict(active_body)
                        compact_body["messages"] = [{
                            "role": "user",
                            "content": self._build_compact_retry_prompt(
                                ticker, price, volume, candidate_direction, metrics_dict, rag_str
                            ),
                        }]
                        compact_body["max_tokens"] = min(configured_max_tokens, 320)
                        active_body = compact_body
                        user_prompt = compact_body["messages"][0]["content"]
                        prompt_fingerprint = hashlib.sha256(user_prompt.encode("utf-8")).hexdigest()[:16]
                        active_prompt_fingerprint = prompt_fingerprint
                        active_prompt_chars = len(user_prompt)
                        log.warning(
                            "[vLLM COMPAT] Переполнение контекста; повторяем компактный запрос, max_tokens=%s. Причина: %s",
                            compact_body["max_tokens"], error_detail,
                        )
                        resp = await _post_candidate(compact_body, retries=0)

                if resp.status_code in (400, 422):
                    error_detail = self._http_error_detail(resp)
                    if self._is_sampling_parameter_error(error_detail) and "temperature" in active_body:
                        sampling_body = dict(active_body)
                        sampling_body.pop("temperature", None)
                        active_body = sampling_body
                        log.warning(
                            "[vLLM COMPAT] Сервер отклонил temperature; повторяем без параметра. Причина: %s",
                            error_detail,
                        )
                        resp = await _post_candidate(sampling_body, retries=0)

                if resp.status_code >= 400:
                    error_detail = self._http_error_detail(resp)
                    request_id = resp.headers.get("x-request-id") or resp.headers.get("x-vllm-request-id") or "не указан"
                    log.error(
                        "[vLLM HTTP ERROR] status=%s | request_id=%s | endpoint=%s | max_tokens=%s | prompt_chars=%s | detail=%s",
                        resp.status_code, request_id, self.api_path,
                        active_body.get("max_tokens"),
                        len(active_body.get("messages", [{}])[0].get("content", "")), error_detail,
                    )
                    resp.raise_for_status()
                raw = resp.json()
                structured_capture = self._structured_response_capture(
                    raw, candidate_id=candidate_id, ticker=ticker, request_id=request_id, stage="PRIMARY"
                )
        except (httpx.TimeoutException, TimeoutError) as exc:
            log.exception(
                "[AIEngine v34] Тайм-аут запроса к vLLM (%s) после ограниченного повтора: %r",
                self.api_path, exc,
            )
            return {
                "final_decision": "HOLD",
                "model_decision": "HOLD",
                "model_raw_decision": "INVALID_RESPONSE",
                "model_contract_violation": False,
                "contract_error_type": None,
                "error_type": "timeout",
                "request_error": True,
                "request_error_type": "timeout",
                "confidence_score": 0.0,
                "confidence_valid": False,
                "tactic_opinion": "vLLM не ответил в отведённое время; вход заблокирован.",
                "critic_opinion": self._compact_opinion(str(exc), max_chars=165),
                "latency": round(time.time() - start_inference_time, 3),
                "vllm_transport_metrics": _transport_summary(),
                "candidate_id": candidate_id, "request_id": request_id,
            }
        except HTTPStatusError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            detail = self._http_error_detail(exc.response) if exc.response is not None else str(exc)
            log.error(
                "[AIEngine v34] vLLM отклонил запрос: HTTP %s | endpoint=%s | detail=%s",
                status, self.api_path, detail,
            )
            return {
                "final_decision": "HOLD",
                "model_decision": "HOLD",
                "model_raw_decision": "INVALID_RESPONSE",
                "model_contract_violation": False,
                "contract_error_type": None,
                "error_type": "http_error",
                "request_error": True,
                "request_error_type": "http_error",
                "confidence_score": 0.0,
                "confidence_valid": False,
                "tactic_opinion": "vLLM отклонил запрос; торговый вход заблокирован.",
                "critic_opinion": self._compact_opinion(f"HTTP {status}: {detail}", max_chars=165),
                "latency": round(time.time() - start_inference_time, 3),
            }
        except httpx.TransportError as exc:
            if isinstance(exc, VLLMCircuitOpenError):
                log.debug(
                    "[vLLM CIRCUIT OPEN] запрос пропущен; следующий пробный вызов через %.1fs.",
                    exc.retry_after_sec,
                )
            else:
                # Repeated connection failures are already summarized by the bounded
                # retry/circuit logs above; avoid dumping a traceback per candidate.
                log.error(
                    "[AIEngine v34] Сетевая ошибка запроса к vLLM (%s): %s: %s",
                    self.api_path, type(exc).__name__, exc,
                )
            return {
                "final_decision": "HOLD",
                "model_decision": "HOLD",
                "model_raw_decision": "INVALID_RESPONSE",
                "model_contract_violation": False,
                "contract_error_type": None,
                "error_type": "transport_error",
                "request_error": True,
                "request_error_type": "transport_error",
                "confidence_score": 0.0,
                "confidence_valid": False,
                "tactic_opinion": "Сбой сетевого соединения с vLLM; вход заблокирован.",
                "critic_opinion": self._compact_opinion(str(exc), max_chars=165),
                "latency": round(time.time() - start_inference_time, 3),
            }
        except Exception as exc:
            log.exception("[AIEngine v34] Ошибка запроса/ответа vLLM (%s): %r", self.api_path, exc)
            return {
                "final_decision": "HOLD",
                "model_decision": "HOLD",
                "model_raw_decision": "INVALID_RESPONSE",
                "model_contract_violation": False,
                "contract_error_type": None,
                "error_type": "request_error",
                "request_error": True,
                "request_error_type": "request_error",
                "confidence_score": 0.0,
                "confidence_valid": False,
                "tactic_opinion": "Ошибка запроса к vLLM; вход заблокирован.",
                "critic_opinion": self._compact_opinion(str(exc), max_chars=165),
                "latency": round(time.time() - start_inference_time, 3),
            }
        
        inference_latency = time.time() - start_inference_time
        parse_started = time.perf_counter()
        processed = await asyncio.to_thread(self._cpu_intensive_processing, raw)
        parse_latency_ms = (time.perf_counter() - parse_started) * 1000.0

        # One bounded repair attempt for malformed or contract-invalid model output.
        retry_attempted = False
        retry_recovered = False
        retry_failed = False
        retry_error_detail = ""
        retry_latency = 0.0
        active_prompt_fingerprint = prompt_fingerprint
        active_prompt_chars = len(user_prompt)
        if processed.get("error_type") == "text_parsing" or processed.get("model_contract_violation"):
            retry_attempted = True
            first_error_type = str(processed.get("contract_error_type") or "invalid_response")
            first_error = str(processed.get("exc") or "Нарушен контракт ответа")
            retry_prompt = self._build_contract_retry_prompt(
                ticker, price, volume, candidate_direction, metrics_dict
            )
            retry_prompt_fingerprint = hashlib.sha256(retry_prompt.encode("utf-8")).hexdigest()[:16]
            active_prompt_fingerprint = retry_prompt_fingerprint
            active_prompt_chars = len(retry_prompt)
            retry_body = {
                "model": self.model_name,
                "messages": [{"role": "user", "content": retry_prompt}],
                "response_format": {"type": "json_object"},
                "temperature": 0.0,
                "max_tokens": min(configured_max_tokens, 384),
            }
            log.warning(
                "[LLM CONTRACT RETRY] ticker=%s candidate=%s error_type=%s finish_reason=%s "
                "completion_tokens=%s max_tokens=%s prompt_chars=%d fingerprint=%s",
                ticker, candidate_direction, first_error_type,
                processed.get("finish_reason", "не указан"),
                (processed.get("usage") or {}).get("completion_tokens")
                if isinstance(processed.get("usage"), dict) else None,
                retry_body["max_tokens"], len(retry_prompt), retry_prompt_fingerprint,
            )
            retry_started = time.perf_counter()
            try:
                async with asyncio.timeout(request_timeout + 0.5):
                    retry_resp = await _post_candidate(
                        retry_body, retries=0, retry_timeouts=False, stage="CONTRACT_RETRY"
                    )
                    if retry_resp.status_code in (400, 422):
                        retry_detail = self._http_error_detail(retry_resp)
                        if self._is_response_format_error(retry_detail):
                            retry_body.pop("response_format", None)
                            retry_resp = await _post_candidate(
                                retry_body, retries=0, retry_timeouts=False, stage="CONTRACT_RETRY"
                            )
                    if retry_resp.status_code in (400, 422):
                        retry_detail = self._http_error_detail(retry_resp)
                        if self._is_sampling_parameter_error(retry_detail) and "temperature" in retry_body:
                            retry_body.pop("temperature", None)
                            retry_resp = await self._post_with_retry(
                                self.api_path, json=retry_body, retries=0, retry_timeouts=False
                            )
                    if retry_resp.status_code >= 400:
                        raise RuntimeError(
                            f"contract-retry HTTP {retry_resp.status_code}: "
                            f"{self._http_error_detail(retry_resp)}"
                        )
                    retry_raw = retry_resp.json()
                    structured_capture = self._structured_response_capture(
                        retry_raw, candidate_id=candidate_id, ticker=ticker, request_id=request_id, stage="CONTRACT_RETRY"
                    )
                retry_processed = await asyncio.to_thread(self._cpu_intensive_processing, retry_raw)
                retry_latency = time.perf_counter() - retry_started
                retry_valid = (
                    retry_processed.get("error_type") != "text_parsing"
                    and not retry_processed.get("model_contract_violation", False)
                )
                if retry_valid:
                    processed = retry_processed
                    retry_recovered = True
                    active_prompt_fingerprint = retry_prompt_fingerprint
                    active_prompt_chars = len(retry_prompt)
                else:
                    processed = retry_processed
                    retry_failed = True
                    retry_error_detail = str(processed.get("exc") or "Повторный ответ также нарушил контракт")
                log.warning(
                    "[LLM CONTRACT RESULT] ticker=%s status=%s first_error=%s retry_error=%s "
                    "retry_finish_reason=%s retry_completion_tokens=%s retry_latency=%.3fs",
                    ticker, "RECOVERED" if retry_recovered else "FAILED", first_error_type,
                    processed.get("contract_error_type") if retry_failed else "-",
                    retry_processed.get("finish_reason", "не указан"),
                    (retry_processed.get("usage") or {}).get("completion_tokens")
                    if isinstance(retry_processed.get("usage"), dict) else None,
                    retry_latency,
                )
            except Exception as retry_exc:
                retry_failed = True
                retry_error_detail = str(retry_exc)
                log.error(
                    "[LLM CONTRACT RESULT] ticker=%s status=FAILED first_error=%s retry_error=%s",
                    ticker, first_error_type, retry_error_detail,
                )
                processed = dict(processed)
                processed["first_contract_error_type"] = first_error_type
                processed["first_contract_error"] = first_error
            processed["contract_retry_attempted"] = True
            processed["contract_retry_recovered"] = retry_recovered
            processed["contract_retry_failed"] = retry_failed
            if retry_failed:
                processed["contract_retry_error"] = retry_error_detail
                processed["first_contract_error_type"] = first_error_type
                processed["first_contract_error"] = first_error

        inference_latency = time.time() - start_inference_time
        usage = processed.get("usage") if isinstance(processed.get("usage"), dict) else {}
        log.info(
            "[vLLM RAW LATENCY] ticker=%s total=%.3fs parse_ms=%.1f contract_retry_ms=%.1f "
            "prompt_chars=%d prompt_tokens=%s max_tokens=%d retry=%s finish_reason=%s "
            "completion_tokens=%s peak_inflight=%d",
            ticker, inference_latency, parse_latency_ms, retry_latency * 1000.0,
            active_prompt_chars, usage.get("prompt_tokens"), configured_max_tokens,
            retry_attempted, processed.get("finish_reason", "stop"),
            usage.get("completion_tokens"),
            int(getattr(self, "_transport_peak_inflight", 0) or 0),
        )

        if processed.get("error_type") == "text_parsing":
            finish_reason = str(processed.get("finish_reason", "не указан"))
            usage = processed.get("usage", {})
            completion_tokens = usage.get("completion_tokens") if isinstance(usage, dict) else None
            log.warning(
                "[AIEngine v34] Контракт ответа не пройден: %s | error_type=%s | finish_reason=%s | "
                "completion_tokens=%s | retry_attempted=%s | retry_failed=%s | preview=%r",
                processed.get("exc", "неизвестная ошибка"),
                processed.get("contract_error_type", "unknown"),
                finish_reason,
                completion_tokens,
                processed.get("contract_retry_attempted", False),
                processed.get("contract_retry_failed", False),
                self._diagnostic_preview(processed.get("raw_preview", "")),
            )
            specific_error_type = str(
                processed.get("contract_error_type")
                or processed.get("first_contract_error_type")
                or "invalid_response"
            )
            return {
                "error_type": "text_parsing",
                "prompt_chars": active_prompt_chars,
                "prompt_fingerprint": active_prompt_fingerprint,
                "candidate_direction": candidate_direction,
                "model_reason_code_raw": "NOT_PROVIDED",
                "final_decision": "HOLD",
                "model_decision": "HOLD",
                "model_raw_decision": "INVALID_RESPONSE",
                "model_contract_violation": True,
                "contract_error_type": specific_error_type,
                "confidence_score": 0.0,
                "confidence_valid": False,
                "confidence_raw": None,
                "tactic_opinion": "Ответ модели не соответствует JSON-контракту; вход заблокирован.",
                "critic_opinion": (
                    "Повторная проверка не восстановила корректный ответ. "
                    "Подробности сохранены в диагностическом журнале."
                ),
                "latency": round(inference_latency, 3),
                "contract_retry_attempted": retry_attempted,
                "contract_retry_recovered": retry_recovered,
                "contract_retry_failed": retry_failed,
                "first_contract_error_type": processed.get("first_contract_error_type"),
                "first_contract_error": processed.get("first_contract_error"),
                "contract_retry_error": processed.get("contract_retry_error"),
                "candidate_id": candidate_id, "request_id": request_id,
                "vllm_transport_metrics": _transport_summary(),
                "vllm_response_capture": structured_capture,
            }
        elif processed.get("error_type") == "emergency_fallback":
            res = processed["fallback_data"]
            res["latency"] = round(inference_latency, 3)
            return res
        
        model_decision = str(processed.get("model_decision", processed.get("final_decision", "HOLD"))).upper()
        processed["model_decision_before_economic_gate"] = model_decision
        processed["model_decision"] = model_decision
        if model_decision == "APPROVE":
            economics_ok, economics_reason, economics_details = self._approval_economics_gate(
                candidate_direction, metrics_dict
            )
            processed["model_economic_gate"] = {
                "passed": economics_ok, "reason": economics_reason, **economics_details
            }
            if not economics_ok:
                log.warning(
                    "[LLM APPROVAL OVERRIDE] candidate_id=%s ticker=%s reason=%s details=%s",
                    candidate_id, ticker, economics_reason,
                    json.dumps(economics_details, ensure_ascii=False, default=str),
                )
                processed["model_decision"] = "HOLD"
                processed["final_decision"] = "HOLD"
                processed["model_reason_code"] = economics_reason
                processed["model_reason_code_raw"] = economics_reason
                processed["confidence_score_before_economic_gate"] = processed.get("confidence_score")
                processed["confidence_score"] = 0.0
                processed["confidence_valid"] = True
                processed["confidence_raw"] = 0.0
        else:
            processed["model_economic_gate"] = {"passed": False, "reason": "MODEL_HOLD"}
        model_decision = str(processed.get("model_decision", "HOLD")).upper()
        raw_reason_code = str(processed.get("model_reason_code") or "NOT_PROVIDED").upper()
        validated_reason_code, reason_validation = self._validate_model_reason_code(
            model_decision, raw_reason_code, candidate_direction, metrics_dict
        )
        processed["model_reason_code_raw"] = raw_reason_code
        processed["model_reason_code"] = validated_reason_code
        processed["model_reason_validation"] = reason_validation
        if model_decision == "HOLD":
            log.info(
                "[LLM HOLD DIAGNOSTIC] ticker=%s candidate=%s raw_reason=%s reason=%s validation=%s "
                "direction=%s m1=%s trade_ready=%s trade_age=%s volume_spike=%s spread=%s",
                ticker, candidate_direction, raw_reason_code, validated_reason_code, reason_validation,
                self._prepare_prompt_metrics(metrics_dict, candidate_direction).get("direction_evidence_state"),
                self._m1_evidence_state(metrics_dict, candidate_direction),
                self._prompt_value(metrics_dict.get("trade_data_ready"), 10),
                self._prompt_value(metrics_dict.get("trade_metrics_age_sec"), 10),
                self._prompt_value(metrics_dict.get("volume_spike"), 10),
                self._prompt_value(metrics_dict.get("spread_pct"), 10),
            )
        processed["prompt_chars"] = active_prompt_chars
        processed["prompt_fingerprint"] = active_prompt_fingerprint
        prepared_metrics = self._prepare_prompt_metrics(metrics_dict, candidate_direction)
        processed["m1_evidence_state"] = prepared_metrics.get("m1_evidence_state", "UNAVAILABLE")
        processed["spread_to_atr_ratio"] = prepared_metrics.get("spread_to_atr_ratio")
        if reason_validation == "RECLASSIFIED":
            log.warning(
                "[LLM REASON MISMATCH] ticker=%s candidate=%s decision=%s reported=%s normalized=%s "
                "m1_state=%s spread_pct=%s spread_to_atr_ratio=%s volume_spike=%s",
                ticker, candidate_direction, model_decision, raw_reason_code, validated_reason_code,
                prepared_metrics.get("m1_evidence_state"),
                self._prompt_value(prepared_metrics.get("spread_pct"), 12),
                self._prompt_value(prepared_metrics.get("spread_to_atr_ratio"), 12),
                self._prompt_value(prepared_metrics.get("volume_spike"), 12),
            )
        processed["candidate_direction"] = candidate_direction
        processed["tactic_opinion"], processed["critic_opinion"] = self._build_local_opinions(
            model_decision, candidate_direction, metrics_dict
        )
        processed["latency"] = round(inference_latency, 3)
        processed["vllm_transport_metrics"] = _transport_summary()
        processed["candidate_id"] = candidate_id
        processed["request_id"] = request_id
        processed["vllm_response_capture"] = structured_capture
        processed.setdefault("contract_retry_attempted", retry_attempted)
        processed.setdefault("contract_retry_recovered", retry_recovered)
        processed.setdefault("contract_retry_failed", retry_failed)
        log.debug(
            "[vLLM SCORE] ticker=%s model_decision=%s candidate=%s contract_violation=%s score=%s valid=%s raw=%r",
            ticker, model_decision, candidate_direction,
            processed.get("model_contract_violation", False),
            processed.get("confidence_score"), processed.get("confidence_valid"), processed.get("confidence_raw"),
        )
        return processed

    async def analyze_market_state(self, *args, **kwargs) -> dict:
        payload = {}
        if kwargs:
            payload = kwargs
        elif args:
            first_arg = args[0]
            if isinstance(first_arg, dict):
                payload = first_arg
            elif isinstance(first_arg, (list, tuple)) and len(first_arg) > 0:
                inner_item = first_arg[0]
                if isinstance(inner_item, dict):
                    payload = inner_item
                else:
                    payload = {"ticker": "SBER", "price": 0.0, "volume": 0, "metrics": {}}
            else:
                payload = {"ticker": "SBER", "price": 0.0, "volume": 0, "metrics": {}}
        else:
            payload = {"ticker": "SBER", "price": 0.0, "volume": 0, "metrics": {}}
            
        return await self.analyze_market_anomaly(payload)

    async def close(self):
        await self.client.aclose()
        log.info("[AIEngine] HTTP‑клиент закрыт")
