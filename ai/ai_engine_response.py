"""Response parsing, evidence normalization and verdict validation mixin."""
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

class AIEngineResponseMixin:
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
        """Use the same fresh-pattern/closed-trend classification as the PAPER gate."""
        return classify_m1_evidence_state(metrics, candidate_direction)

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
            # reason_code is optional in the response contract. A missing code
            # is a defaulted diagnostic value, not a model/evidence mismatch.
            if reason == "NOT_PROVIDED":
                return replacement, "INFERRED"
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
