"""Local admission pipeline for selected market candidates."""
from __future__ import annotations
import asyncio
import time
from collections.abc import Callable, Mapping
from typing import Any


class MarketCandidatePipeline:
    """Preserve the pre-LLM gate order while isolating candidate admission."""

    def __init__(self, *, execution_engine: Any, anomaly_queue: Any,
                 queue_item_factory: Any, trade_flow_increment: Callable[[str], None],
                 infer_direction: Callable[[Mapping[str, Any]], str | None],
                 classify_direction: Callable[[str, Mapping[str, Any]], str],
                 log_shadow_candidate: Callable[[str, str, dict[str, Any]], dict[str, Any]],
                 queue_flow_conflict_reason: Callable[[str, dict[str, Any]], str],
                 validate_pre_llm_entry_evidence: Callable[..., tuple[bool, str]],
                 paper_mode_provider: Callable[[], bool],
                 guard_reason_bucket: Callable[[str], str],
                 store_pending_bbo_candidate: Callable[[str, dict[str, Any], object | None], None],
                 mark_shadow_llm_not_called: Callable[[object, str], None],
                 trade_flow_guard_block: Callable[[str], None],
                 log_prefilter_blocked: Callable[..., None],
                 validate_atr_readiness: Callable[[Mapping[str, Any]], tuple[bool, str]],
                 reserve_ai_ticker: Callable[[str, str, dict[str, Any]], bool],
                 build_candidate_payload: Callable[..., dict[str, Any]],
                 release_ai_ticker: Callable[[str], None],
                 update_numpy_impulse_status: Callable[[str], None],
                 logger: Any, monotonic: Callable[[], float] = time.monotonic) -> None:
        self.execution_engine = execution_engine
        self.anomaly_queue = anomaly_queue
        self.queue_item_factory = queue_item_factory
        self.trade_flow_increment = trade_flow_increment
        self.infer_direction = infer_direction
        self.classify_direction = classify_direction
        self.log_shadow_candidate = log_shadow_candidate
        self.queue_flow_conflict_reason = queue_flow_conflict_reason
        self.validate_pre_llm_entry_evidence = validate_pre_llm_entry_evidence
        self.paper_mode_provider = paper_mode_provider
        self.guard_reason_bucket = guard_reason_bucket
        self.store_pending_bbo_candidate = store_pending_bbo_candidate
        self.mark_shadow_llm_not_called = mark_shadow_llm_not_called
        self.trade_flow_guard_block = trade_flow_guard_block
        self.log_prefilter_blocked = log_prefilter_blocked
        self.validate_atr_readiness = validate_atr_readiness
        self.reserve_ai_ticker = reserve_ai_ticker
        self.build_candidate_payload = build_candidate_payload
        self.release_ai_ticker = release_ai_ticker
        self.update_numpy_impulse_status = update_numpy_impulse_status
        self.logger = logger
        self.monotonic = monotonic

    def process_candidate(self, *, ticker: str, price: float, volume: float,
                          bid: float, ask: float, bid_size: float, ask_size: float,
                          day_volume: float, analysis: Mapping[str, Any],
                          supplied_metrics: Any, session_generation: int,
                          received_monotonic: object | None) -> bool:
        """Process one selected anomaly; False means it was not a candidate."""
        if not (analysis.get("is_anomaly") and analysis.get("selection_ready")
                and analysis.get("selected")):
            return False
        self.trade_flow_increment("selected_anomalies")
        metrics = analysis.get("metrics", supplied_metrics if isinstance(supplied_metrics, dict) else {})
        if not isinstance(metrics, dict):
            metrics = {}
        active = self.execution_engine.get_open_position_tickers()
        opening = ticker in getattr(self.execution_engine, "_opening_tickers", set())
        if ticker in active or opening:
            self.trade_flow_increment("active_position_skipped")
            return True

        direction = self.infer_direction(metrics)
        if direction is None:
            self.trade_flow_increment("direction_prefilter_skipped")
            self.log_prefilter_blocked(
                ticker,
                "Локальное направление не подтверждено: нет импульса цены либо VWAP показывает сильное встречное движение.",
            )
            return True

        state = self.classify_direction(direction, metrics)
        metrics["direction_evidence_state"] = state
        self.trade_flow_increment("local_candidate_buy" if direction == "BUY" else "local_candidate_sell")
        self.logger.debug(
            "[DIRECTION GATE] %s side=%s state=%s price_change=%.5f%% vs_vwap=%.5f%%",
            ticker, direction, state,
            float(metrics.get("price_change_pct", 0.0) or 0.0),
            float(metrics.get("price_vs_vwap_pct", 0.0) or 0.0),
        )
        shadow = self.log_shadow_candidate(ticker, direction, metrics)
        shadow_id = shadow.get("candidate_id")
        # Let the LLM evaluate strategic conditions for a locally selected instrument.
        # Queue/flow conflict, M1, volume, spread/ATR and cost gates are rechecked
        # after the model response before any execution plan can be prepared.
        evidence_ok, evidence_reason = self.validate_pre_llm_entry_evidence(
            direction, metrics, paper_mode=self.paper_mode_provider()
        )

        if not evidence_ok:
            if self.guard_reason_bucket(evidence_reason) == "quotes":
                pending = self.build_candidate_payload(
                    ticker=ticker, candidate_created_monotonic=self.monotonic(),
                    price=price, volume=volume, bid=bid, ask=ask,
                    bid_size=bid_size, ask_size=ask_size, day_volume=day_volume,
                    candidate_direction=direction, shadow_candidate_id=shadow_id,
                    paper_session_generation=session_generation, metrics=metrics,
                )
                self.store_pending_bbo_candidate(ticker, pending, received_monotonic)
            else:
                self.mark_shadow_llm_not_called(shadow_id, "SIGNAL_EVIDENCE_BLOCKED")
                self.trade_flow_increment("prefilter_blocked")
                self.trade_flow_guard_block(evidence_reason)
                self.log_prefilter_blocked(ticker, evidence_reason)
            return True

        atr_ready, atr_reason = self.validate_atr_readiness(metrics)
        if not atr_ready:
            self.mark_shadow_llm_not_called(shadow_id, "ATR_NOT_READY")
            self.trade_flow_increment("atr_unready_candidates")
            self.log_prefilter_blocked(ticker, atr_reason, interval_sec=60.0)
            return True
        if not self.reserve_ai_ticker(ticker, direction, metrics):
            self.mark_shadow_llm_not_called(shadow_id, "AI_DEDUPLICATED")
            self.trade_flow_increment("candidate_deduplicated")
            return True

        payload = self.build_candidate_payload(
            ticker=ticker, candidate_created_monotonic=self.monotonic(),
            price=price, volume=volume, bid=bid, ask=ask,
            bid_size=bid_size, ask_size=ask_size, day_volume=day_volume,
            candidate_direction=direction, shadow_candidate_id=shadow_id,
            paper_session_generation=session_generation, metrics=metrics,
        )
        try:
            self.anomaly_queue.put_nowait(
                self.queue_item_factory.create(1, "MARKET_TICK", payload)
            )
            self.trade_flow_increment("anomalies_queued")
            self.trade_flow_increment("prefilter_pass_buy" if direction == "BUY" else "prefilter_pass_sell")
            self.update_numpy_impulse_status(ticker)
        except asyncio.QueueFull:
            self.mark_shadow_llm_not_called(shadow_id, "AI_QUEUE_FULL")
            self.release_ai_ticker(ticker)
            self.trade_flow_increment("queue_dropped")
            self.logger.error("Очередь AI-сигналов заполнена; аномалия %s отброшена.", ticker)
        return True
