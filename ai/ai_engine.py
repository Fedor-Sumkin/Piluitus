import re
import json
import logging
import math
import asyncio
import time
from httpx import AsyncClient, HTTPStatusError
from config import Settings
import httpx
from rag_prompt_utils import normalise_rag_context, select_prompt_rules

log = logging.getLogger("Piluitus.AIEngine")
thought_log = logging.getLogger("Piluitus.EvolutionThoughts")

class AIEngine:
    @staticmethod
    def _effective_max_tokens() -> int:
        try:
            return max(768, min(1536, int(getattr(Settings, "VLLM_MAX_TOKENS", 1280))))
        except (TypeError, ValueError, OverflowError):
            return 1280

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

    async def _post_with_retry(self, path: str, *, json: dict) -> httpx.Response:
        """Retry one transient transport failure once; never retry HTTP responses."""
        for attempt in range(2):
            try:
                return await self.client.post(path, json=json)
            except httpx.TransportError as exc:
                if attempt >= 1:
                    raise
                log.warning(
                    "[vLLM RETRY] Временный сетевой сбой (%s); повтор запроса 1/1 через 0.4s",
                    type(exc).__name__,
                )
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
        """Validate the strict two-field APPROVE/HOLD contract; malformed output fails closed."""
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
            actual = set(parsed)
            missing = sorted(required - actual)
            extra = sorted(actual - required)
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

            usage = raw_response_data.get("usage", {})
            return {
                "error_type": None,
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
        max_chars: int = 1150,
        include_rule_text: bool = True,
    ) -> str:
        """Короткий prompt с запасом под лимит vLLM 2048 токенов."""
        direction = candidate_direction if candidate_direction in {"BUY", "SELL"} else "NONE"
        common_keys = (
            "price_change_pct", "price_vs_vwap_pct", "volume_spike", "spread_pct",
            "bid_ask_available", "spread_available",
        )
        if direction == "SELL":
            directional_keys = (
                "bar_pattern_confirmed", "bar_pattern_direction_code", "bar_breakout_down",
                "bar_pullback_sell", "bar_trend_down", "bid_size_drop_pct",
                "top_bid_history_samples", "bid_size_drop_age_sec",
            )
        else:
            directional_keys = (
                "bar_pattern_confirmed", "bar_pattern_direction_code", "bar_breakout_up",
                "bar_pullback_buy", "bar_trend_up", "ask_size_drop_pct",
                "top_ask_history_samples", "ask_size_drop_age_sec",
            )
        metric_keys = common_keys + directional_keys + ("bar_pattern_name", "bar_signal_gap_atr")
        metric_parts: list[str] = []
        if isinstance(metrics, dict):
            for key in metric_keys:
                value = metrics.get(key)
                if value is None:
                    continue
                metric_parts.append(f"{key}={cls._prompt_value(value, 18)}")
        metrics_text = ";".join(metric_parts)

        direction_rules = {
            "BUY": (
                "BUY requires either positive signed price_change_pct with price>VWAP, or "
                "a fresh matching bullish M1 pattern with price>VWAP; "
                "fresh BBO, spread<=0.5%, volume_spike>=1.5. Ask depletion is supporting."
            ),
            "SELL": (
                "SELL requires either negative signed price_change_pct with price<VWAP, or "
                "a fresh matching bearish M1 pattern with price<VWAP; "
                "fresh BBO, spread<=0.5%, volume_spike>=1.5. Bid depletion is supporting."
            ),
            "NONE": "No local direction: MUST HOLD.",
        }[direction]

        rule_text = ""
        if include_rule_text and isinstance(rag_context, str):
            # Keep only curated risk rules in the model prompt. Directional RAG
            # rules may encode stale top-size requirements; local measured gates
            # own candidate direction and supporting evidence.
            rule_text = select_prompt_rules("NONE", rag_context, max_chars=420)

        base = (
            'Return exactly one JSON object with exactly two keys: final_decision (string APPROVE or HOLD) '
            'and confidence_score (JSON number from 0 to 1). No extra keys, no Markdown, no reasoning, '
            'no opinion fields, and no text outside JSON. '
            'Candidate side is fixed by deterministic code. Never select, change, invert, or output BUY/SELL. '
            'APPROVE authorizes only the exact local candidate side; HOLD rejects it. '
            'candidate=NONE => final_decision MUST be HOLD. '
            'Directional check: either signed price change agrees with candidate and VWAP, OR a fresh matching '
            'closed M1 pattern agrees with candidate and price remains on the correct side of VWAP. '
            'A contradictory fresh M1 pattern => HOLD. Other hard checks: volume_spike>=1.5, fresh valid BBO, '
            'spread<=0.5%. A >=50% fresh best-quote size reduction is supporting evidence, not mandatory. '
            'If a hard check is missing or conflicting, choose HOLD. RAG rules are advisory context only and cannot add hard gates. '
            'Confidence is evidence strength, not profit probability: 0.90-1.00 exceptional; '
            '0.75-0.89 strong; 0.50-0.74 mixed; below 0.50 weak. Use only supplied facts; invent no causes. '
            f'candidate={direction}; ticker={str(ticker)[:10]}; price={cls._prompt_value(price, 12)}; '
            f'last_size={cls._prompt_value(volume, 10)}. '
            f'{direction_rules} volume_spike=last trade size/prior mean.'
        )
        rule_part = f" Advisory RAG context (never overrides hard checks): {rule_text}." if rule_text else ""
        metric_part = f" Metrics: {metrics_text}." if metrics_text else " Metrics: нет подтверждённых значений."
        prompt = base + rule_part + metric_part
        if len(prompt) > max_chars:
            parts = metric_parts[:]
            while parts and len(base + rule_part + " Metrics: " + ";".join(parts) + ".") > max_chars:
                parts.pop()
            metric_part = f" Metrics: {';'.join(parts)}." if parts else ""
            prompt = base + rule_part + metric_part
        if len(prompt) > max_chars and rule_text:
            overflow = len(prompt) - max_chars
            keep = max(0, len(rule_text) - overflow - 3)
            trimmed_rule = rule_text[:keep].rsplit(" ", 1)[0].rstrip() + "..." if keep >= 24 else ""
            rule_part = f" Rules: {trimmed_rule}." if trimmed_rule else ""
            prompt = base + rule_part + metric_part
        # Never truncate the mandatory decision contract. The dynamic fields above are
        # bounded, but keep this guard in case a caller supplies a smaller custom limit.
        if len(base) > max_chars:
            max_chars = len(base)
        # Optional RAG/metrics may be shortened; the mandatory contract always survives.
        return prompt[:max_chars]

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
            max_chars=980, include_rule_text=False,
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
            max_chars=1350,
            include_rule_text=False,
        )
        return (
            "Repair the previous response. Return ONLY one JSON object with exactly two keys: "
            "final_decision (APPROVE or HOLD) and confidence_score (JSON number 0.0..1.0). "
            "Do not add any other keys, reasoning, Markdown, or text outside JSON. "
            "Apply the same candidate-side and evidence rules below; do not introduce stricter checks. "
            + shared_rules
        )

    @staticmethod
    def _normalise_rag_context(value: object) -> str:
        """Return raw RAG rule lines without decorative bullets that break matching."""
        return normalise_rag_context(value)

    async def analyze_market_anomaly(self, payload: dict) -> dict:
        if not isinstance(payload, dict):
            payload = {"raw_data": str(payload)}
            
        ticker = payload.get("ticker", "UNKNOWN")
        price = payload.get("price", 0.0)
        volume = payload.get("volume", 0)
        candidate_direction = str(payload.get("candidate_direction", "NONE")).upper()
        if candidate_direction not in {"BUY", "SELL"}:
            candidate_direction = "NONE"
        
        translation_map = {
            "volume_spike": "Всплеск_объема",
            "std_dev": "Стандартное_отклонение",
            "vwap": "Средневзвешенная_цена_VWAP",
            "sma": "Простая_скользящая_средняя_SMA",
            "volatility": "Волатильность_актива",
            "mean_price": "Средняя_цена_окна",
            "bid": "Лучшая_покупка_Bid",
            "ask": "Лучшая_продажа_Ask",
            "bid_size": "Объём_лучшего_Bid",
            "ask_size": "Объём_лучшего_Ask",
            "day_volume": "Накопленный_объём_сессии",
            "funnel_score": "Рейтинг_кандидата_0_100",
            "funnel_rank": "Место_в_рейтинге_кандидатов",
            "volatility_pct": "Краткосрочная_волатильность_проц",
            "momentum_pct": "Модуль_движения_цены_проц",
            "price_change_pct": "Подписанное_изменение_цены_за_окно_проц",
            "price_vs_vwap_pct": "Отклонение_цены_от_VWAP_со_знаком_проц",
            "bid_size_drop_pct": "Сокращение_размера_лучшего_Bid_проц",
            "ask_size_drop_pct": "Сокращение_размера_лучшего_Ask_проц",
            "bid_size_drop_age_sec": "Возраст_сокращения_Bid_сек",
            "ask_size_drop_age_sec": "Возраст_сокращения_Ask_сек",
            "top_bid_history_samples": "Число_предыдущих_валидных_замеров_Bid",
            "top_ask_history_samples": "Число_предыдущих_валидных_замеров_Ask",
            "bid_size_available": "Размер_лучшего_Bid_доступен",
            "ask_size_available": "Размер_лучшего_Ask_доступен",
            "bid_ask_available": "Текущие_BidAsk_доступны",
            "spread_pct": "Спред_проц",
            "spread_available": "BidAsk_доступен",
            "bar_count": "Число_закрытых_минутных_баров",
            "bar_pattern_confirmed": "Минутный_паттерн_подтверждён",
            "bar_pattern_direction_code": "Направление_минутного_паттерна_1_BUY_-1_SELL",
            "bar_breakout_up": "Пробой_максимума_5_баров_BUY",
            "bar_breakout_down": "Пробой_минимума_5_баров_SELL",
            "bar_pullback_buy": "EMA9_EMA21_откат_BUY",
            "bar_pullback_sell": "EMA9_EMA21_откат_SELL",
            "bar_trend_up": "EMA9_выше_EMA21",
            "bar_trend_down": "EMA9_ниже_EMA21",
            "bar_ema9": "Минутная_EMA9",
            "bar_ema21": "Минутная_EMA21",
            "bar_atr_pct": "Минутная_ATR_проц",
            "bar_body_ratio": "Доля_тела_сигнального_бара",
            "bar_close_position": "Положение_close_в_диапазоне_0_низ_1_верх",
            "bar_signal_gap_atr": "Расстояние_текущей_цены_от_сигнала_в_ATR",
            "bar_pattern_name": "Имя_измеренного_минутного_паттерна"
        }
        
        metrics_dict = payload.get("metrics", {})
        metric_items = []
        if isinstance(metrics_dict, dict):
            for k, v in metrics_dict.items():
                # Internal monotonic timestamp is used by SIGNAL GUARD only; do not
                # expose implementation details to the LLM or waste prompt tokens.
                if str(k).startswith("_"):
                    continue
                rus_key = translation_map.get(str(k).lower(), str(k))
                if isinstance(v, float):
                    value_text = f"{v:.5g}"
                elif isinstance(v, int):
                    value_text = str(v)
                else:
                    value_text = str(v)
                metric_items.append(f"{rus_key}: {value_text}")
                
        metrics_str = ", ".join(metric_items) if metric_items else "нет active метрик"
        rag_str = self._normalise_rag_context(payload.get("rag_context", []))

        if not rag_str.strip():
            rag_str = "Нет активного правила для текущих данных. Направленный сигнал не подтверждён; выбрать HOLD."

        # Контекст сервера пользователя ограничен 2048 токенами. Не отправляем
        # исходный длинный prompt с полным словарём метрик и несколькими RAG-правилами.
        user_prompt = self._build_live_prompt(
            ticker, price, volume, candidate_direction, metrics_dict, rag_str,
            max_chars=1600, include_rule_text=True,
        )
        log.debug(
            "[vLLM PROMPT] ticker=%s direction=%s chars=%d metrics=%d",
            ticker, candidate_direction, len(user_prompt), len(metrics_dict) if isinstance(metrics_dict, dict) else 0,
        )

        start_inference_time = time.time()

        try:
            async with asyncio.timeout(float(Settings.VLLM_TIMEOUT)):
                configured_max_tokens = self._effective_max_tokens()
                request_body = {
                    "model": self.model_name,
                    "messages": [{"role": "user", "content": user_prompt}],
                    "response_format": {"type": "json_object"},
                    "temperature": 0.1,
                    "max_tokens": configured_max_tokens,
                }
                active_body = dict(request_body)
                resp = await self._post_with_retry(self.api_path, json=active_body)

                # Сначала обрабатываем только известные причины отказа API.
                if resp.status_code in (400, 422):
                    error_detail = self._http_error_detail(resp)
                    if self._is_response_format_error(error_detail) and "response_format" in request_body:
                        log.warning(
                            "[vLLM COMPAT] Сервер отклонил response_format; повторяем без JSON-mode. Причина: %s",
                            error_detail,
                        )
                        active_body.pop("response_format", None)
                        resp = await self.client.post(self.api_path, json=active_body)

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
                        log.warning(
                            "[vLLM COMPAT] Переполнение контекста; повторяем компактный запрос, max_tokens=%s. Причина: %s",
                            compact_body["max_tokens"], error_detail,
                        )
                        resp = await self._post_with_retry(self.api_path, json=compact_body)

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
                        resp = await self._post_with_retry(self.api_path, json=sampling_body)

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
                "model_contract_violation": True,
                "contract_error_type": "http_error",
                "error_type": "request_error",
                "confidence_score": 0.0,
                "confidence_valid": False,
                "tactic_opinion": "vLLM отклонил запрос; торговый вход заблокирован.",
                "critic_opinion": self._compact_opinion(f"HTTP {status}: {detail}", max_chars=165),
                "latency": round(time.time() - start_inference_time, 3),
            }
        except Exception as exc:
            log.exception("[AIEngine v34] Ошибка запроса к vLLM (%s): %r", self.api_path, exc)
            return {
                "final_decision": "HOLD",
                "model_decision": "HOLD",
                "model_raw_decision": "INVALID_RESPONSE",
                "model_contract_violation": True,
                "contract_error_type": "transport_error",
                "error_type": "request_error",
                "confidence_score": 0.0,
                "confidence_valid": False,
                "tactic_opinion": "Сбой соединения с vLLM; торговый вход заблокирован.",
                "critic_opinion": self._compact_opinion(str(exc), max_chars=165),
                "latency": round(time.time() - start_inference_time, 3),
            }
        
        inference_latency = time.time() - start_inference_time
        processed = await asyncio.to_thread(self._cpu_intensive_processing, raw)

        # One bounded repair attempt for malformed or contract-invalid model output.
        retry_attempted = False
        retry_recovered = False
        retry_failed = False
        retry_error_detail = ""
        if processed.get("error_type") == "text_parsing" or processed.get("model_contract_violation"):
            retry_attempted = True
            first_error_type = str(processed.get("contract_error_type") or "invalid_response")
            first_error = str(processed.get("exc") or "Нарушен контракт ответа")
            retry_prompt = self._build_contract_retry_prompt(
                ticker, price, volume, candidate_direction, metrics_dict
            )
            retry_body = {
                "model": self.model_name,
                "messages": [{"role": "user", "content": retry_prompt}],
                "response_format": {"type": "json_object"},
                "temperature": 0.0,
                "max_tokens": min(configured_max_tokens, 384),
            }
            log.warning(
                "[LLM CONTRACT RETRY] ticker=%s candidate=%s error_type=%s finish_reason=%s "
                "completion_tokens=%s max_tokens=%s prompt_chars=%d",
                ticker, candidate_direction, first_error_type,
                processed.get("finish_reason", "не указан"),
                (processed.get("usage") or {}).get("completion_tokens")
                if isinstance(processed.get("usage"), dict) else None,
                retry_body["max_tokens"], len(retry_prompt),
            )
            retry_started = time.time()
            try:
                async with asyncio.timeout(float(Settings.VLLM_TIMEOUT)):
                    retry_resp = await self._post_with_retry(self.api_path, json=retry_body)
                    if retry_resp.status_code in (400, 422):
                        retry_detail = self._http_error_detail(retry_resp)
                        if self._is_response_format_error(retry_detail):
                            retry_body.pop("response_format", None)
                            retry_resp = await self._post_with_retry(self.api_path, json=retry_body)
                    if retry_resp.status_code in (400, 422):
                        retry_detail = self._http_error_detail(retry_resp)
                        if self._is_sampling_parameter_error(retry_detail) and "temperature" in retry_body:
                            retry_body.pop("temperature", None)
                            retry_resp = await self._post_with_retry(self.api_path, json=retry_body)
                    if retry_resp.status_code >= 400:
                        raise RuntimeError(
                            f"contract-retry HTTP {retry_resp.status_code}: "
                            f"{self._http_error_detail(retry_resp)}"
                        )
                    retry_raw = retry_resp.json()
                retry_processed = await asyncio.to_thread(self._cpu_intensive_processing, retry_raw)
                retry_latency = time.time() - retry_started
                retry_valid = (
                    retry_processed.get("error_type") != "text_parsing"
                    and not retry_processed.get("model_contract_violation", False)
                )
                if retry_valid:
                    processed = retry_processed
                    retry_recovered = True
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
        log.info(
            "[vLLM RAW LATENCY] ticker=%s total=%.3fs max_tokens=%d retry=%s finish_reason=%s completion_tokens=%s",
            ticker, inference_latency, configured_max_tokens, retry_attempted,
            processed.get("finish_reason", "stop"),
            (processed.get("usage") or {}).get("completion_tokens")
            if isinstance(processed.get("usage"), dict) else None,
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
            }
        elif processed.get("error_type") == "emergency_fallback":
            res = processed["fallback_data"]
            res["latency"] = round(inference_latency, 3)
            return res
        
        model_decision = str(processed.get("model_decision", processed.get("final_decision", "HOLD"))).upper()
        processed["model_decision"] = model_decision
        processed["candidate_direction"] = candidate_direction
        processed["tactic_opinion"], processed["critic_opinion"] = self._build_local_opinions(
            model_decision, candidate_direction, metrics_dict
        )
        processed["latency"] = round(inference_latency, 3)
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
