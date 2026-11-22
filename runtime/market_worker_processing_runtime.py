
"""Queue-driven LLM candidate processing and verdict orchestration."""
from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime
from typing import Any, Callable
from uuid import uuid4


class MarketWorkerProcessingRuntime:
    """Process queued market candidates without owning application-level globals."""

    def __init__(
        self,
        *,
        db: Any,
        ai: Any,
        anomaly_queue: Any,
        execution_engine: Any,
        risk_manager: Any,
        session_lock: Any,
        session_generation_provider: Callable[[], int],
        candidate_is_expired: Callable[..., bool],
        candidate_ttl_sec: float,
        trade_flow_increment: Callable[[str], None],
        logger: Any,
        settings: Any,
        validate_daily_loss_limit: Callable[[Any], float],
        parse_finite_float: Callable[[Any], float | None],
        background_operations: Any,
        log_trading_event: Callable[..., Any],
        attach_llm_rule_context: Callable[..., Any],
        build_llm_feature_snapshot: Callable[[Any], dict[str, Any]],
        build_llm_error_response: Callable[..., dict[str, Any]],
        process_llm_response: Callable[..., tuple[dict[str, Any], dict[str, Any]]],
        validate_post_llm_entry: Callable[..., dict[str, Any]],
        screener: Any,
        queue_flow_conflict_reason: Callable[..., str],
        prepare_entry_execution_plan: Callable[..., dict[str, Any]],
        lot_sizes: dict[str, int],
        trade_flow_guard_block: Callable[[str], None],
        finite_float: Callable[[Any], float | None],
        nonnegative_int: Callable[[Any], int | None],
        build_default_signal_reason: Callable[..., str],
        build_verdict_explanation: Callable[..., tuple[str, str]],
        classify_local_evidence_diagnostic: Callable[..., str],
        build_pre_llm_risk_block_verdict: Callable[..., dict[str, Any]],
        build_llm_verdict_record: Callable[..., dict[str, Any]],
        link_shadow_candidate_verdict: Callable[..., bool],
        shadow_tracker: Any,
        enrich_ai_analysis_with_verdict: Callable[..., None],
        verdict_cache: list[dict[str, Any]],
        max_cache_size: int,
        manager: Any,
        balance_snapshot: dict[str, Any],
        publish_market_verdict: Callable[..., Any],
        execute_and_publish: Callable[..., Any],
        release_ai_ticker: Callable[[str], None],
        paper_session_generation_lock: Any,
        getenv: Callable[..., str] = os.getenv,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Any] = asyncio.sleep,
        now_local: Callable[[], datetime] | None = None,
        uuid_factory: Callable[[], Any] = uuid4,
    ) -> None:
        self.db = db
        self.ai = ai
        self.anomaly_queue = anomaly_queue
        self.execution_engine = execution_engine
        self.risk_manager = risk_manager
        self.session_lock = session_lock
        self.session_generation_provider = session_generation_provider
        self.candidate_is_expired = candidate_is_expired
        self.candidate_ttl_sec = float(candidate_ttl_sec)
        self.trade_flow_increment = trade_flow_increment
        self.logger = logger
        self.settings = settings
        self.validate_daily_loss_limit = validate_daily_loss_limit
        self.parse_finite_float = parse_finite_float
        self.background_operations = background_operations
        self.log_trading_event = log_trading_event
        self.attach_llm_rule_context = attach_llm_rule_context
        self.build_llm_feature_snapshot = build_llm_feature_snapshot
        self.build_llm_error_response = build_llm_error_response
        self.process_llm_response = process_llm_response
        self.validate_post_llm_entry = validate_post_llm_entry
        self.screener = screener
        self.queue_flow_conflict_reason = queue_flow_conflict_reason
        self.prepare_entry_execution_plan = prepare_entry_execution_plan
        self.lot_sizes = lot_sizes
        self.trade_flow_guard_block = trade_flow_guard_block
        self.finite_float = finite_float
        self.nonnegative_int = nonnegative_int
        self.build_default_signal_reason = build_default_signal_reason
        self.build_verdict_explanation = build_verdict_explanation
        self.classify_local_evidence_diagnostic = classify_local_evidence_diagnostic
        self.build_pre_llm_risk_block_verdict = build_pre_llm_risk_block_verdict
        self.build_llm_verdict_record = build_llm_verdict_record
        self.link_shadow_candidate_verdict = link_shadow_candidate_verdict
        self.shadow_tracker = shadow_tracker
        self.enrich_ai_analysis_with_verdict = enrich_ai_analysis_with_verdict
        self.verdict_cache = verdict_cache
        self.max_cache_size = int(max_cache_size)
        self.manager = manager
        self.balance_snapshot = balance_snapshot
        self.publish_market_verdict = publish_market_verdict
        self.execute_and_publish = execute_and_publish
        self.release_ai_ticker = release_ai_ticker
        self.paper_session_generation_lock = paper_session_generation_lock
        self.getenv = getenv
        self.monotonic = monotonic
        self.sleep = sleep
        self.now_local = now_local or (lambda: datetime.now().astimezone())
        self.uuid_factory = uuid_factory
        self._last_risk_block_verdict_at = 0.0

    async def start_processing(self) -> None:
        """Run the single queue consumer used by the application lifespan."""
        self.logger.info("🤖 ИИ‑конвейер воркеров Piluitus v35.2 успешно запущен.")
        while True:
            await self.process_next_item()

    async def process_next_item(self) -> None:
        """Process exactly one queue item and always ACK/release it in finally."""
        anomaly_queue = self.anomaly_queue
        execution_engine = self.execution_engine
        risk_manager = self.risk_manager
        _paper_session_generation = self.session_generation_provider
        candidate_is_expired = self.candidate_is_expired
        _AI_CANDIDATE_TTL_SEC = self.candidate_ttl_sec
        _trade_flow_inc = self.trade_flow_increment
        logger = self.logger
        Settings = self.settings
        validate_daily_loss_limit = self.validate_daily_loss_limit
        _parse_finite_float = self.parse_finite_float
        _background_operations = self.background_operations
        log_trading_event_to_file = self.log_trading_event
        attach_llm_rule_context = self.attach_llm_rule_context
        build_llm_feature_snapshot = self.build_llm_feature_snapshot
        build_llm_error_response = self.build_llm_error_response
        process_llm_response = self.process_llm_response
        validate_post_llm_entry = self.validate_post_llm_entry
        screener = self.screener
        _paper_queue_flow_conflict_reason = self.queue_flow_conflict_reason
        prepare_entry_execution_plan = self.prepare_entry_execution_plan
        FINAM_TRADE_LOT_SIZES = self.lot_sizes
        _trade_flow_guard_block = self.trade_flow_guard_block
        finite_float = self.finite_float
        nonnegative_int = self.nonnegative_int
        build_default_signal_reason = self.build_default_signal_reason
        build_verdict_explanation = self.build_verdict_explanation
        classify_local_evidence_diagnostic = self.classify_local_evidence_diagnostic
        build_pre_llm_risk_block_verdict = self.build_pre_llm_risk_block_verdict
        build_llm_verdict_record = self.build_llm_verdict_record
        link_shadow_candidate_verdict = self.link_shadow_candidate_verdict
        scalping_shadow_tracker = self.shadow_tracker
        enrich_ai_analysis_with_verdict = self.enrich_ai_analysis_with_verdict
        last_ai_verdicts_cache = self.verdict_cache
        MAX_CACHE_SIZE = self.max_cache_size
        manager = self.manager
        _dashboard_balance_snapshot = self.balance_snapshot
        publish_market_verdict = self.publish_market_verdict
        _execute_and_publish = self.execute_and_publish
        _release_ai_ticker = self.release_ai_ticker
        _paper_session_lock = self.paper_session_generation_lock

        queue_item_acquired = False
        pending_ticker = ""
        try:
            priority, _sequence_id, _timestamp_id, event_type, event_payload = await anomaly_queue.get()
            queue_item_acquired = True
            if event_type == "MARKET_TICK" and isinstance(event_payload, dict):
                pending_ticker = str(event_payload.get("ticker", "")).strip().upper()
                if (
                    getattr(execution_engine, "_maintenance_active", False)
                    or event_payload.get("paper_session_generation") != _paper_session_generation()
                ):
                    return
                created_at = event_payload.get("candidate_created_monotonic")
                if created_at is not None and candidate_is_expired(
                    created_at, ttl_sec=_AI_CANDIDATE_TTL_SEC
                ):
                    _trade_flow_inc("candidate_expired")
                    logger.info(
                        "[CANDIDATE TTL] %s side=%s expired before LLM; evidence age exceeded %.1fs.",
                        pending_ticker,
                        event_payload.get("candidate_direction", "NONE"),
                        _AI_CANDIDATE_TTL_SEC,
                    )
                    return

            if priority == 0:
                logger.warning(f"🚨 [ПРИОРИТЕТНЫЙ ПЕРЕХВАТ] Выполнение экстренной команды: {event_type}")
                if event_type == "CMD_CLOSE_ALL":
                    await execution_engine.close_all_positions(db=self.db) 
                elif event_type == "CMD_SET_RISK_LIMIT":
                    try:
                        risk_manager.daily_loss_limit = validate_daily_loss_limit(
                            event_payload.get("value", Settings.DAILY_LOSS_LIMIT_PCT)
                        )
                    except ValueError as exc:
                        logger.error("[RISK LIMIT] Некорректное значение в очереди: %s", exc)
                return

            ticker   = event_payload.get("ticker") or event_payload.get("security_code") or "UNKNOWN"
            price    = event_payload.get("price") or 0.0
            metrics  = event_payload.get("metrics", {})
            if not isinstance(metrics, dict):
                metrics = {}
            candidate_direction_for_diag = str(event_payload.get("candidate_direction") or "").upper()
            llm_input_features = build_llm_feature_snapshot(metrics)
            volume_spike = _parse_finite_float(metrics.get("volume_spike"))
            volume_5s = _parse_finite_float(metrics.get("trade_volume_5s"))
            volume_baseline_5s = _parse_finite_float(metrics.get("trade_volume_baseline_5s"))
            trade_count_60s = _parse_finite_float(metrics.get("trade_count_60s"))
            trade_data_ready = (_parse_finite_float(metrics.get("trade_data_ready")) or 0.0) >= 0.5
            volume_spike_text = f"{volume_spike:.2f}×" if volume_spike is not None else "Н/Д"
            if trade_data_ready and volume_5s is not None and volume_baseline_5s is not None:
                baseline_buckets = int(_parse_finite_float(metrics.get("trade_baseline_bucket_count")) or 0)
                trades_5s = int(_parse_finite_float(metrics.get("trade_count_5s")) or 0)
                volume_text = (
                    f"Всплеск объёма: {volume_spike_text}; "
                    f"5с={volume_5s:.3f} ед. провайдера, медианная база={volume_baseline_5s:.3f}; "
                    f"сделок 5с={trades_5s}, сделок 60с={int(trade_count_60s or 0)}, "
                    f"активных 5с-интервалов базы={baseline_buckets}."
                )
            else:
                volume_text = f"Объём сделок: Н/Д — история потока не готова; spike={volume_spike_text}."

            trade_realized_volatility = _parse_finite_float(
                metrics.get("trade_realized_volatility_60s_pct")
            )
            m1_atr_pct = _parse_finite_float(metrics.get("bar_atr_pct"))
            m1_atr_periods = _parse_finite_float(metrics.get("bar_atr_periods")) or 0.0
            if trade_data_ready and trade_realized_volatility is not None:
                volatility_text = (
                    f"Реализованная волатильность по сделкам за 60с: "
                    f"{trade_realized_volatility:.5f}%."
                )
            elif m1_atr_pct is not None and m1_atr_pct > 0.0 and m1_atr_periods >= 5.0:
                volatility_text = f"M1 ATR ({int(m1_atr_periods)} закрытых баров): {m1_atr_pct:.5f}%."
            else:
                volatility_text = "Волатильность: Н/Д — нет подтверждённой истории."

            _background_operations.spawn(
                log_trading_event_to_file(
                    ticker=ticker,
                    price=price,
                    decision="В ОЧЕРЕДИ (ПРОВЕРКА РИСКОВ)",
                    confidence=0.0,
                    tactic=volume_text,
                    critic=volatility_text,
                    candidate_id=str(event_payload.get("shadow_candidate_id") or "") or None,
                ),
                name=f"trading-log-queue:{ticker}",
            )

            trading_allowed = await risk_manager.is_trading_allowed(self.db)
            if (
                getattr(execution_engine, "_maintenance_active", False)
                or event_payload.get("paper_session_generation") != _paper_session_generation()
            ):
                return
            if not trading_allowed:
                _trade_flow_inc("risk_blocked")
                _background_operations.spawn(
                    log_trading_event_to_file(
                        ticker,
                        price,
                        "RISK_BLOCK",
                        0.0,
                        "Торговля заблокирована.",
                        "RiskManager запретил торговлю до запроса к ИИ.",
                        candidate_id=str(event_payload.get("shadow_candidate_id") or "") or None,
                    ),
                    name=f"trading-log-risk-block:{ticker}",
                )
                await self.publish_pre_llm_risk_block(
                    event_payload, int(event_payload.get("paper_session_generation", -1))
                )
                return

            await attach_llm_rule_context(
                event_payload,
                db=self.db,
                ticker=ticker,
                candidate_direction=candidate_direction_for_diag,
                logger=logger,
            )

            logger.info(
                "🧭 [LLM INPUT] ticker=%s candidate=%s features=%s",
                ticker, candidate_direction_for_diag,
                json.dumps(llm_input_features, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            )
            _trade_flow_inc("llm_calls")
            try:
                ai_response = await asyncio.wait_for(
                    self.ai.analyze_market_state(event_payload),
                    timeout=float(Settings.VLLM_TIMEOUT) * 2.0 + 1.0,
                )
            except asyncio.TimeoutError as exc:
                ai_response = build_llm_error_response(
                    exc, error_type="worker_timeout"
                )
            except Exception as exc:
                ai_response = build_llm_error_response(
                    exc, error_type="worker_error"
                )

            if (
                getattr(execution_engine, "_maintenance_active", False)
                or event_payload.get("paper_session_generation") != _paper_session_generation()
            ):
                return

            ai_analysis, llm_state = process_llm_response(
                ai_response,
                candidate_direction=event_payload.get("candidate_direction", ""),
                ticker=ticker,
                increment_counter=_trade_flow_inc,
                logger=logger,
            )
            request_error = llm_state["request_error"]
            request_error_type = llm_state["request_error_type"]
            model_decision = llm_state["model_decision"]
            candidate_direction = llm_state["candidate_direction"]
            model_reason_code = llm_state["model_reason_code"]
            candidate_decision = llm_state["candidate_decision"]
            model_contract_violation = llm_state["model_contract_violation"]
            decision = llm_state["decision"]
            confidence_valid = llm_state["confidence_valid"]
            confidence = llm_state["confidence"]
            candidate_confidence = llm_state["candidate_confidence"]

            # Rebuild executable BBO and trade-flow evidence after LLM latency.
            # Pre-LLM metrics remain available separately for attribution, but
            # stale quote sides must never authorize a new entry.
            post_llm_state = validate_post_llm_entry(
                screener=screener,
                ticker=ticker,
                metrics=metrics,
                decision=decision,
                candidate_direction=candidate_direction,
                paper_mode=bool(getattr(execution_engine, "paper_mode", False)),
                queue_flow_conflict_reason=_paper_queue_flow_conflict_reason,
                parse_finite_float=_parse_finite_float,
            )
            evidence = post_llm_state["evidence"]
            post_llm_refresh = post_llm_state["post_llm_refresh"]
            refreshed_price = post_llm_state["refreshed_price"]
            if refreshed_price is not None:
                price = refreshed_price
            valid_evidence = post_llm_state["valid_evidence"]
            block_reason = post_llm_state["block_reason"]
            post_llm_features = post_llm_state["post_llm_features"]
            post_llm_evidence_validation = post_llm_state["post_llm_evidence_validation"]
            post_llm_evidence_reason = post_llm_state["post_llm_evidence_reason"]

            verdict_id = uuid4().hex
            entry_plan = prepare_entry_execution_plan(
                ticker=ticker,
                price=price,
                decision=decision,
                confidence=confidence,
                confidence_valid=confidence_valid,
                valid_evidence=valid_evidence,
                block_reason=block_reason,
                signal_reason=(
                    f"Ошибка запроса к vLLM ({request_error_type}); вход запрещён."
                    if request_error else
                    "LLM нарушила контракт APPROVE/HOLD; вход запрещён."
                    if model_contract_violation else ""
                ),
                metrics=metrics,
                ai_analysis=ai_analysis,
                lot_multiplier=FINAM_TRADE_LOT_SIZES.get(ticker, 0),
                execution_engine=execution_engine,
                risk_manager=risk_manager,
                paper_trading=bool(Settings.PAPER_TRADING),
                increment_counter=_trade_flow_inc,
                guard_block=_trade_flow_guard_block,
                finite_float=finite_float,
                nonnegative_int=nonnegative_int,
                logger=logger,
                min_atr_periods_raw=os.getenv("PILUITUS_ATR_MIN_PERIODS", "5"),
                atr_multiplier_raw=os.getenv("ATR_MULTIPLIER", "1.5"),
            )
            decision = entry_plan["decision"]
            confidence = entry_plan["confidence"]
            signal_reason = entry_plan["signal_reason"]
            execution_status = entry_plan["execution_status"]
            execution_message = entry_plan["execution_message"]
            requested_lots = entry_plan["requested_lots"]
            requested_notional = entry_plan["requested_notional"]
            lot_multiplier = entry_plan["lot_multiplier"]

            # Do not expose free-form LLM reasoning as evidence. User-facing text
            # is generated only from locally measured values and guard outcomes.
            signal_reason = build_default_signal_reason(
                decision=decision,
                candidate_decision=candidate_decision,
                confidence_valid=confidence_valid,
                signal_reason=signal_reason,
                model_decision=model_decision,
            )
            tactic_text, critic_text = build_verdict_explanation(
                decision, candidate_decision, evidence, signal_reason
            )
            local_evidence_diagnostic = classify_local_evidence_diagnostic(
                block_reason=block_reason,
                candidate_direction=candidate_direction,
                request_error=request_error,
                request_error_type=request_error_type,
                model_contract_violation=model_contract_violation,
                model_decision=model_decision,
                model_reason_code=model_reason_code,
                confidence_valid=confidence_valid,
                decision=decision,
                confidence=confidence,
            )
            logger.info(
                "[LLM DECISION] %s candidate=%s candidate_id=%s verdict_id=%s model=%s final=%s "
                "model_confidence=%s final_confidence=%s confidence_valid=%s "
                "contract_violation=%s model_reason_code=%s model_reason_raw=%s "
                "reason_validation=%s m1=%s direction_gate=%s prompt_chars=%s prompt_fingerprint=%s "
                "reason=%s local_evidence=%s pre_llm_features=%s post_llm_features=%s "
                "post_llm_evidence_validation=%s post_llm_evidence_reason=%s post_llm_refresh=%s",
                ticker,
                candidate_direction or "NONE",
                str(event_payload.get("shadow_candidate_id") or "NONE"),
                verdict_id,
                model_decision,
                decision,
                f"{candidate_confidence:.1f}%" if candidate_confidence is not None else "invalid",
                f"{confidence:.1f}%" if confidence_valid else "invalid",
                confidence_valid,
                model_contract_violation,
                model_reason_code,
                ai_analysis.get("model_reason_code_raw", "NOT_PROVIDED"),
                ai_analysis.get("model_reason_validation", "UNKNOWN"),
                ai_analysis.get("m1_evidence_state", "UNAVAILABLE"),
                metrics.get("direction_evidence_state", "UNAVAILABLE"),
                ai_analysis.get("prompt_chars", "N/A"),
                ai_analysis.get("prompt_fingerprint", "N/A"),
                signal_reason or ("APPROVED" if decision in {"BUY", "SELL"} else "MODEL_HOLD"),
                local_evidence_diagnostic,
                json.dumps(llm_input_features, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                json.dumps(post_llm_features, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                post_llm_evidence_validation,
                post_llm_evidence_reason,
                json.dumps(post_llm_refresh, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            )

            verdict_data = build_llm_verdict_record(
                verdict_id=verdict_id,
                event_payload=event_payload,
                now_local=datetime.now().astimezone(),
                ticker=ticker,
                price=price,
                decision=decision,
                candidate_decision=candidate_decision,
                model_decision=model_decision,
                candidate_confidence=candidate_confidence,
                confidence=confidence,
                confidence_valid=confidence_valid,
                signal_reason=signal_reason,
                model_reason_code=model_reason_code,
                execution_status=execution_status,
                execution_message=execution_message,
                requested_lots=requested_lots,
                requested_notional=requested_notional,
                tactic_text=tactic_text,
                critic_text=critic_text,
                local_evidence_diagnostic=local_evidence_diagnostic,
                llm_input_features=llm_input_features,
                post_llm_features=post_llm_features,
                post_llm_evidence_validation=post_llm_evidence_validation,
                post_llm_evidence_reason=post_llm_evidence_reason,
                post_llm_refresh=post_llm_refresh,
                ai_analysis=ai_analysis,
            )
            signal_status = verdict_data["signal_status"]
            shadow_candidate_id = str(event_payload.get("shadow_candidate_id") or "")
            link_shadow_candidate_verdict(
                verdict_data,
                shadow_candidate_id=shadow_candidate_id,
                shadow_tracker=scalping_shadow_tracker,
                logger=logger,
                verdict_id=verdict_id,
                model_decision=model_decision,
                final_decision=decision,
                signal_status=signal_status,
                confidence=candidate_confidence,
                reason_code=model_reason_code,
                local_guard=local_evidence_diagnostic,
                execution_status=execution_status,
                ticker=ticker,
            )
            enrich_ai_analysis_with_verdict(
                ai_analysis,
                verdict_data=verdict_data,
                shadow_candidate_id=shadow_candidate_id,
                verdict_id=verdict_id,
                decision=decision,
                confidence=confidence,
                confidence_valid=confidence_valid,
                candidate_confidence=candidate_confidence,
                signal_status=signal_status,
                model_decision=model_decision,
                model_reason_code=model_reason_code,
                local_evidence_diagnostic=local_evidence_diagnostic,
                candidate_decision=candidate_decision,
                llm_input_features=llm_input_features,
                post_llm_features=post_llm_features,
                post_llm_evidence_validation=post_llm_evidence_validation,
                post_llm_evidence_reason=post_llm_evidence_reason,
                post_llm_refresh=post_llm_refresh,
                metrics=metrics,
                tactic_text=tactic_text,
                critic_text=critic_text,
            )

            _background_operations.spawn(
                log_trading_event_to_file(
                    ticker=ticker,
                    price=price,
                    decision=f"ВЕРДИКТ ИИ: {decision} (кандидат={candidate_decision}, исполнение={execution_status})",
                    confidence=round(candidate_confidence, 1) if candidate_confidence is not None else 0.0,
                    tactic=tactic_text,
                    critic=critic_text,
                    candidate_id=shadow_candidate_id or None,
                    verdict_id=verdict_id,
                    model_confidence=candidate_confidence,
                    final_confidence=round(confidence, 1) if confidence_valid else None,
                ),
                name=f"trading-log-verdict:{ticker}:{verdict_id}",
            )

            async with _paper_session_lock:
                if (
                    getattr(execution_engine, "_maintenance_active", False)
                    or event_payload.get("paper_session_generation") != _paper_session_generation()
                ):
                    return
                await publish_market_verdict(
                    verdict_data,
                    db=self.db,
                    verdict_cache=last_ai_verdicts_cache,
                    max_cache_size=MAX_CACHE_SIZE,
                    manager=manager,
                    balance_snapshot=_dashboard_balance_snapshot,
                    logger=logger,
                    candidate_id=str(event_payload.get("shadow_candidate_id") or "NONE"),
                )

                if execution_status == "PENDING":
                    _background_operations.spawn(
                        _execute_and_publish(
                            verdict_id=verdict_id,
                            ticker=ticker,
                            lots=requested_lots,
                            side=decision,
                            price=price,
                            ai_analysis=dict(ai_analysis),
                            session_generation=int(event_payload.get("paper_session_generation", -1)),
                            candidate_created_monotonic=event_payload.get("candidate_created_monotonic"),
                            shadow_candidate_id=shadow_candidate_id or None,
                        ),
                        name=f"paper-execution:{ticker}:{verdict_id}",
                    )

        except Exception as outer_exc:
            logger.error("[MarketWorker] КРИТИЧЕСКИЙ СБОЙ КОНВЕЙЕРА: %r", outer_exc, exc_info=True)

        finally:
            # Подтверждаем только элемент, который worker действительно извлёк.
            # ACK выполняется ровно один раз на всех путях, включая досрочный выход.
            if queue_item_acquired:
                anomaly_queue.task_done()
            if pending_ticker:
                _release_ai_ticker(pending_ticker)
            await asyncio.sleep(0.001)

    async def publish_pre_llm_risk_block(
        self, event_payload: dict[str, Any], session_generation: int
    ) -> None:
        """Publish a local pre-LLM risk rejection only while its session is current."""
        async with self.session_lock:
            if (
                getattr(self.execution_engine, "_maintenance_active", False)
                or session_generation != self.session_generation_provider()
            ):
                return
            await self.publish_pre_llm_risk_block_locked(event_payload)

    async def publish_pre_llm_risk_block_locked(
        self, event_payload: dict[str, Any]
    ) -> None:
        """Persist and broadcast a rate-limited RiskManager verdict."""
        now_mono = self.monotonic()
        if now_mono - self._last_risk_block_verdict_at < 10.0:
            return
        self._last_risk_block_verdict_at = now_mono
        verdict = self.build_pre_llm_risk_block_verdict(
            event_payload,
            now_local=self.now_local(),
            verdict_id=self.uuid_factory().hex,
            explanation_builder=self.build_verdict_explanation,
        )
        ticker = verdict["ticker"]
        self.verdict_cache.append(verdict)
        if len(self.verdict_cache) > self.max_cache_size:
            self.verdict_cache.pop(0)
        try:
            redis_inst = getattr(self.db, "redis", getattr(self.db, "_redis", None))
            if redis_inst is not None:
                await redis_inst.lpush(
                    "piluitus:verdicts:history",
                    json.dumps(verdict, ensure_ascii=False),
                )
                await redis_inst.ltrim(
                    "piluitus:verdicts:history", 0, self.max_cache_size - 1
                )
        except Exception as persist_exc:
            self.logger.warning(
                "Не удалось сохранить RiskManager-вердикт %s: %s",
                ticker,
                persist_exc,
            )
        try:
            payload = {
                "type": "MARKET_UPDATE",
                **verdict,
                "balance": dict(self.balance_snapshot),
            }
            await self.manager.broadcast(json.dumps(payload, ensure_ascii=False))
        except Exception as broadcast_exc:
            self.logger.error(
                "Не удалось отправить RiskManager-вердикт в веб-интерфейс: %s",
                broadcast_exc,
            )
