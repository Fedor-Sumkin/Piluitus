"""Stable public AIEngine facade; implementation is split by responsibility."""
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

from ai.ai_engine_transport import AIEngineTransportMixin
from ai.ai_engine_response import AIEngineResponseMixin
from ai.ai_engine_prompt import AIEnginePromptMixin


class AIEngine(AIEngineTransportMixin, AIEngineResponseMixin, AIEnginePromptMixin):
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
                        user_prompt = compact_body["messages"][0]["content"]
                        prompt_fingerprint = hashlib.sha256(user_prompt.encode("utf-8")).hexdigest()[:16]
                        active_prompt_fingerprint = prompt_fingerprint
                        active_prompt_chars = len(user_prompt)
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
        processed = await asyncio.to_thread(self._cpu_intensive_processing, raw)

        # One bounded repair attempt for malformed or contract-invalid model output.
        retry_attempted = False
        retry_recovered = False
        retry_failed = False
        retry_error_detail = ""
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
            retry_started = time.time()
            try:
                async with asyncio.timeout(request_timeout + 0.5):
                    retry_resp = await self._post_with_retry(
                        self.api_path,
                        json=retry_body,
                        retry_timeouts=False,
                    )
                    if retry_resp.status_code in (400, 422):
                        retry_detail = self._http_error_detail(retry_resp)
                        if self._is_response_format_error(retry_detail):
                            retry_body.pop("response_format", None)
                            retry_resp = await self._post_with_retry(self.api_path, json=retry_body, retry_timeouts=False)
                    if retry_resp.status_code in (400, 422):
                        retry_detail = self._http_error_detail(retry_resp)
                        if self._is_sampling_parameter_error(retry_detail) and "temperature" in retry_body:
                            retry_body.pop("temperature", None)
                            retry_resp = await self._post_with_retry(self.api_path, json=retry_body, retry_timeouts=False)
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
            }
        elif processed.get("error_type") == "emergency_fallback":
            res = processed["fallback_data"]
            res["latency"] = round(inference_latency, 3)
            return res
        
        model_decision = str(processed.get("model_decision", processed.get("final_decision", "HOLD"))).upper()
        processed["model_decision"] = model_decision
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
