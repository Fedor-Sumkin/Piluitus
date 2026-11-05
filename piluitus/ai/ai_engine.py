import re
import json
import logging
import asyncio
import time
from httpx import AsyncClient, HTTPStatusError
from config import Settings
import httpx
from rag_prompt_utils import normalise_rag_context, select_prompt_rules

log = logging.getLogger("Piluitus.AIEngine")
thought_log = logging.getLogger("Piluitus.EvolutionThoughts")

class AIEngine:
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
            min(1024, max(768, int(getattr(Settings, "VLLM_MAX_TOKENS", 768)))) ,
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
        cleaned = self.clean_cyrillic_pattern.sub(" ", text)
        cleaned = self.spaces_pattern.sub(" ", cleaned).strip(" \t\r\n,;:-")
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
        """Находит первый корректный JSON-объект даже внутри Markdown/пояснений."""
        if not isinstance(text, str) or not text.strip():
            raise ValueError("vLLM вернул пустой текст вместо JSON")

        cleaned = text.strip().lstrip("\ufeff")
        # Убираем распространённые Markdown-ограждения, но не полагаемся только на них.
        cleaned = re.sub(r"^\s*```(?:json)?\s*", "", cleaned, flags=re.IGNORECASE)
        cleaned = re.sub(r"\s*```\s*$", "", cleaned)

        # Сканируем баланс фигурных скобок с учётом кавычек и escape-последовательностей.
        # Это не ломается, если в tactic_opinion/critic_opinion есть символы { или }.
        for start_idx, char in enumerate(cleaned):
            if char != "{":
                continue
            depth = 0
            in_string = False
            escaped = False
            for idx in range(start_idx, len(cleaned)):
                current = cleaned[idx]
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
                    if depth == 0:
                        candidate = cleaned[start_idx:idx + 1]
                        try:
                            parsed = json.loads(candidate)
                        except json.JSONDecodeError:
                            break
                        if isinstance(parsed, dict):
                            return parsed
                        break
        preview = re.sub(r"\s+", " ", cleaned)[:240]
        if not preview:
            preview = "<пустой ответ>"
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
            if "final_decision" not in parsed:
                raise ValueError("В JSON отсутствует обязательное поле final_decision")

            decision = str(parsed.get("final_decision", "HOLD")).strip().upper()
            if decision not in {"BUY", "SELL", "HOLD"}:
                # Не угадываем BUY/SELL по тексту — неизвестное действие блокируется.
                decision = "HOLD"

            raw_confidence = parsed.get("confidence_score")
            confidence = self._parse_confidence_score(raw_confidence)
            confidence_valid = confidence is not None
            if confidence is None:
                confidence = 0.0

            # У HOLD может быть уверенность в решении не открывать позицию.
            # Не обнуляем её только из-за класса решения.

            tactic = parsed.get("tactic_opinion", "")
            critic = parsed.get("critic_opinion", "")
            if isinstance(tactic, (dict, list)):
                tactic = json.dumps(tactic, ensure_ascii=False)
            if isinstance(critic, (dict, list)):
                critic = json.dumps(critic, ensure_ascii=False)

            return {
                "error_type": None,
                "final_decision": decision,
                "confidence_score": confidence,
                "confidence_valid": confidence_valid,
                "confidence_raw": raw_confidence,
                "tactic_opinion": self._compact_opinion(tactic, max_chars=150),
                "critic_opinion": self._compact_opinion(critic, max_chars=165),
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
            return {
                "error_type": "text_parsing",
                "exc": str(exc),
                "raw_preview": raw_preview,
                "finish_reason": finish_reason,
                "usage": usage if isinstance(usage, dict) else {},
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
                "BUY requires price_change_pct>0 and price_vs_vwap_pct>0; "
                "ask_size_drop_pct>=50 with top_ask_history_samples>=3 and age<=2s; "
                "or confirmed bullish bar_pattern with breakout_up/pullback_buy."
            ),
            "SELL": (
                "SELL requires price_change_pct<0 and price_vs_vwap_pct<0; "
                "bid_size_drop_pct>=50 with top_bid_history_samples>=3 and age<=2s; "
                "or confirmed bearish bar_pattern with breakout_down/pullback_sell."
            ),
            "NONE": "No locally confirmed direction: HOLD.",
        }[direction]

        rule_text = ""
        if include_rule_text and isinstance(rag_context, str):
            # Preserve risk constraints and the active directional rule.
            # RAG is context, not proof that a setup fired.
            rule_text = select_prompt_rules(direction, rag_context, max_chars=420)

        base = (
            'Return ONLY one compact JSON object; no Markdown, no chain-of-thought, no extra keys. '
            'Required keys: final_decision (BUY/SELL/HOLD), confidence_score (number 0..1), '
            'tactic_opinion (Russian, <=80 chars), critic_opinion (Russian, <=80 chars). '
            'Each opinion must be one short complete sentence. Never describe causes, news, macro events, '
            'Bitcoin, or other external facts unless explicitly present in Metrics or Rule. '
            'Do not invent explanations, order-book events, or price drivers. State only what supplied metrics prove. '
            'Confidence is evidence strength, NOT probability of profit: 0.90-1.00 exceptionally clear; '
            '0.75-0.89 all required entry checks confirmed; 0.50-0.74 mixed; below 0.50 weak. '
            f'candidate={direction}; ticker={str(ticker)[:10]}; price={cls._prompt_value(price, 12)}; '
            f'last_size={cls._prompt_value(volume, 10)}. '
            'Required common checks: volume_spike>=1.5, valid BBO, spread<=0.5%. '
            f'{direction_rules} '
            'volume_spike=last trade size/prior mean. Missing or conflicting evidence means HOLD. '
            'Use short factual wording; do not repeat the prompt.'
        )
        rule_part = f" Rules: {rule_text}." if rule_text else ""
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
        # Last-resort bound for unusually long ticker/metric values.
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
                configured_max_tokens = max(768, min(1024, int(getattr(Settings, "VLLM_MAX_TOKENS", 768))))
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
                "confidence_score": 0.0,
                "confidence_valid": False,
                "tactic_opinion": "Сбой соединения с vLLM; торговый вход заблокирован.",
                "critic_opinion": self._compact_opinion(str(exc), max_chars=165),
                "latency": round(time.time() - start_inference_time, 3),
            }
        
        inference_latency = time.time() - start_inference_time
        log.warning(f"⏱️ [vLLM RAW LATENCY] Время обработки тензоров: {inference_latency:.3f} сек.")

        # 🔥 ВЫНОС ТЯЖЕЛОЙ ОБРАБОТКИ СТРОК И РЕГУЛЯРОК В ПОТОК ОС
        processed = await asyncio.to_thread(self._cpu_intensive_processing, raw)

        if processed.get("error_type") == "text_parsing":
            finish_reason = str(processed.get("finish_reason", "не указан"))
            usage = processed.get("usage", {})
            completion_tokens = usage.get("completion_tokens") if isinstance(usage, dict) else None
            log.warning(
                "[AIEngine v34] JSON-разбор не удался: %s | finish_reason=%s | "
                "completion_tokens=%s | preview=%r",
                processed.get("exc", "неизвестная ошибка"),
                finish_reason,
                completion_tokens,
                self._diagnostic_preview(processed.get("raw_preview", "")),
            )
            if finish_reason == "length":
                critic = (
                    "Ответ был обрезан лимитом токенов до появления JSON "
                    f"(finish_reason=length, completion_tokens={completion_tokens}). "
                    "Ответ не помещается в лимит генерации; проверьте фактический VLLM_MAX_TOKENS и длину reasoning."
                )
            elif not processed.get("raw_preview", "").strip():
                critic = "vLLM вернул пустое поле content; проверьте ответ сервера и reasoning parser."
            else:
                critic = str(processed["exc"])
            return {
                "final_decision": "HOLD",
                "confidence_score": 0.0,
                "confidence_valid": False,
                "tactic_opinion": "Ответ модели не прошёл проверку формата; вход заблокирован.",
                "critic_opinion": self._compact_opinion(critic, max_chars=165),
                "latency": round(inference_latency, 3)
            }
        elif processed.get("error_type") == "emergency_fallback":
            res = processed["fallback_data"]
            res["latency"] = round(inference_latency, 3)
            return res
        
        processed["latency"] = round(inference_latency, 3)
        log.debug(
            "[vLLM SCORE] ticker=%s decision=%s score=%s valid=%s raw=%r",
            ticker, processed.get("final_decision"), processed.get("confidence_score"),
            processed.get("confidence_valid"), processed.get("confidence_raw"),
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
