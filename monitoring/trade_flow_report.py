"""Formatting and emission of the periodic TRADE FLOW diagnostics report.

This module is observational only. It formats counters already collected by
the application and never changes candidate filtering or order execution.
"""
from __future__ import annotations

import logging
from collections.abc import Mapping


class TradeFlowReportLogger:
    """Emit the established 60-second TRADE FLOW summary."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def log(self, counts: Mapping[str, int], *, pending_bbo: int = 0) -> None:
        names = (
            "quotes_received", "selected_quotes", "selected_anomalies", "anomalies_queued",
            "queue_dropped", "direction_prefilter_skipped", "prefilter_blocked", "candidate_deduplicated",
            "bbo_deferred", "bbo_resumed", "bbo_expired", "bbo_superseded", "bbo_coalesced", "bbo_cancelled",
            "bbo_resume_attempted", "bbo_resume_from_book", "bbo_resume_from_quotes", "bbo_resume_expired",
            "bbo_resume_session_cancelled", "bbo_resume_invalid_payload", "bbo_resume_funnel_not_ready",
            "bbo_resume_not_selected", "bbo_resume_active_position", "bbo_resume_quote_missing",
            "bbo_resume_book_unconfirmed", "bbo_resume_invalid_bbo", "bbo_resume_book_stale",
            "bbo_resume_confirmation_stale", "bbo_resume_other_book_failure",
            "bbo_resume_direction_changed", "bbo_resume_evidence_quotes", "bbo_resume_evidence_blocked",
            "bbo_resume_atr_blocked", "bbo_resume_payload_changed", "bbo_resume_deduplicated",
            "bbo_resume_queue_full",
            "local_candidate_buy", "local_candidate_sell",
            "prefilter_pass_buy", "prefilter_pass_sell",
            "llm_calls", "llm_model_approve", "llm_model_hold",
            "llm_hold_buy_m1", "llm_hold_buy_support", "llm_hold_buy_risk_reward",
            "llm_hold_buy_direction", "llm_hold_buy_volume", "llm_hold_buy_spread",
            "llm_hold_buy_stale", "llm_hold_buy_other",
            "llm_hold_sell_m1", "llm_hold_sell_support", "llm_hold_sell_risk_reward",
            "llm_hold_sell_direction", "llm_hold_sell_volume", "llm_hold_sell_spread",
            "llm_hold_sell_stale", "llm_hold_sell_other",
            "llm_request_error", "llm_timeout",
            "post_llm_evidence_blocked", "post_llm_confidence_blocked",
            "llm_approve_buy", "llm_approve_sell",
            "llm_hold", "llm_hold_buy", "llm_hold_sell", "llm_contract_violation",
            "llm_contract_retry", "llm_contract_retry_recovered", "llm_contract_retry_failed",
            "orders_opened_buy", "orders_opened_sell", "guard_blocked",
            "guard_quotes", "guard_spread", "guard_volume", "guard_direction", "guard_size",
            "guard_text", "guard_other", "confidence_blocked", "risk_blocked", "zero_lot",
            "strategy_shadow_evaluated", "strategy_shadow_pass", "strategy_shadow_reject",
            "shadow_block_bbo_unavailable", "shadow_block_bbo_stale",
            "shadow_block_order_book_not_confirmed", "shadow_block_atr_not_ready",
            "shadow_block_spread", "shadow_block_other",
            "orders_scheduled", "orderbook_updates", "orderbook_unusable",
            "orderbook_side_touches", "orderbook_heartbeats", "orderbook_invalid",
            "orderbook_out_of_order", "active_position_skipped", "atr_unready_candidates",
            "execution_bbo_blocked", "execution_spread_atr_blocked", "atr_history_blocked", "portfolio_cap_blocked",
            "portfolio_size_reduced", "entry_session_blocked", "execution_other_blocked",
            "execution_stale_signal_blocked", "risk_final_gate_blocked",
            "candidate_expired", "finam_lot_unavailable",
        )
        values = {key: counts.get(key, 0) for key in names}
        values["bbo_pending"] = pending_bbo
        local_total = counts.get("local_candidate_buy", 0) + counts.get("local_candidate_sell", 0)
        pass_total = counts.get("prefilter_pass_buy", 0) + counts.get("prefilter_pass_sell", 0)
        values["prefilter_pass_pct"] = round(100.0 * pass_total / local_total, 1) if local_total else 0.0
        self._logger.info(
            "📊 [TRADE FLOW 60s] котировки=%(quotes_received)d, котировки_top15=%(selected_quotes)d, "
            "аномалии_top15=%(selected_anomalies)d, в_очереди=%(anomalies_queued)d, очередь_потери=%(queue_dropped)d; "
            "направление_не_совпало=%(direction_prefilter_skipped)d, prefilter_block=%(prefilter_blocked)d, "
            "duplicate=%(candidate_deduplicated)d; локально(BUY=%(local_candidate_buy)d,SELL=%(local_candidate_sell)d), "
            "до_LLM(BUY=%(prefilter_pass_buy)d,SELL=%(prefilter_pass_sell)d;доля=%(prefilter_pass_pct).1f%%); "
            "LLM=%(llm_calls)d, model(APPROVE=%(llm_model_approve)d,HOLD=%(llm_model_hold)d), "
            "HOLD_reason BUY(M1=%(llm_hold_buy_m1)d,support=%(llm_hold_buy_support)d,RR=%(llm_hold_buy_risk_reward)d,"
            "direction=%(llm_hold_buy_direction)d,volume=%(llm_hold_buy_volume)d,spread=%(llm_hold_buy_spread)d,"
            "stale=%(llm_hold_buy_stale)d,other=%(llm_hold_buy_other)d); "
            "SELL(M1=%(llm_hold_sell_m1)d,support=%(llm_hold_sell_support)d,RR=%(llm_hold_sell_risk_reward)d,"
            "direction=%(llm_hold_sell_direction)d,volume=%(llm_hold_sell_volume)d,spread=%(llm_hold_sell_spread)d,"
            "stale=%(llm_hold_sell_stale)d,other=%(llm_hold_sell_other)d); "
            "после_LLM(guard_block=%(post_llm_evidence_blocked)d,confidence_block=%(post_llm_confidence_blocked)d), "
            "итог(APPROVE_BUY=%(llm_approve_buy)d,APPROVE_SELL=%(llm_approve_sell)d,"
            "HOLD_BUY-кандидат=%(llm_hold_buy)d,HOLD_SELL-кандидат=%(llm_hold_sell)d), "
            "request_error=%(llm_request_error)d (timeout=%(llm_timeout)d); "
            "contract_error=%(llm_contract_violation)d (retry=%(llm_contract_retry)d,"
            "recovered=%(llm_contract_retry_recovered)d,failed=%(llm_contract_retry_failed)d); "
            "открыто(BUY=%(orders_opened_buy)d,SELL=%(orders_opened_sell)d); "
            "Guard=%(guard_blocked)d (BBO=%(guard_quotes)d, спред=%(guard_spread)d, объём=%(guard_volume)d, "
            "направление=%(guard_direction)d, размер=%(guard_size)d, текст=%(guard_text)d, прочее=%(guard_other)d); "
            "ORDER_BOOK: принятых_состояний=%(orderbook_updates)d, затронуто_сторон_BBO=%(orderbook_side_touches)d, "
            "heartbeat_без_движения_BBO=%(orderbook_heartbeats)d, некорректных_состояний=%(orderbook_invalid)d, "
            "событий_не_по_порядку=%(orderbook_out_of_order)d, ошибок_обработки=%(orderbook_unusable)d; "
            "повторные кандидаты по открытым позициям=%(active_position_skipped)d; "
            "ATR ожидание до LLM (кандидаты отложены)=%(atr_unready_candidates)d; "
            "блок ATR после APPROVE=%(atr_history_blocked)d; "
            "Spread/ATR execution block=%(execution_spread_atr_blocked)d; "
            "BBO wait=%(bbo_deferred)d, resumed=%(bbo_resumed)d, expired=%(bbo_expired)d, "
            "superseded=%(bbo_superseded)d, coalesced=%(bbo_coalesced)d, cancelled=%(bbo_cancelled)d, pending_now=%(bbo_pending)d, candidate_expired_pre_LLM=%(candidate_expired)d; "
            "BBO resume: attempts=%(bbo_resume_attempted)d (ORDER_BOOK=%(bbo_resume_from_book)d, QUOTES=%(bbo_resume_from_quotes)d), "
            "expired=%(bbo_resume_expired)d, session_cancel=%(bbo_resume_session_cancelled)d, "
            "funnel_not_ready=%(bbo_resume_funnel_not_ready)d, not_selected=%(bbo_resume_not_selected)d, "
            "active_position=%(bbo_resume_active_position)d, invalid_payload=%(bbo_resume_invalid_payload)d, "
            "book_wait(missing=%(bbo_resume_quote_missing)d, unconfirmed=%(bbo_resume_book_unconfirmed)d, "
            "invalid=%(bbo_resume_invalid_bbo)d, stale=%(bbo_resume_book_stale)d, "
            "confirmation_stale=%(bbo_resume_confirmation_stale)d, other=%(bbo_resume_other_book_failure)d), "
            "direction_changed=%(bbo_resume_direction_changed)d, evidence(quotes=%(bbo_resume_evidence_quotes)d, "
            "blocked=%(bbo_resume_evidence_blocked)d), ATR=%(bbo_resume_atr_blocked)d, "
            "payload_changed=%(bbo_resume_payload_changed)d, deduplicated=%(bbo_resume_deduplicated)d, "
            "queue_full=%(bbo_resume_queue_full)d; "
            "после_LLM_исполнение: portfolio_cap_block=%(portfolio_cap_blocked)d, "
            "portfolio_size_reduced=%(portfolio_size_reduced)d, entry_session_block=%(entry_session_blocked)d, "
            "other_block=%(execution_other_blocked)d, stale_signal_block=%(execution_stale_signal_blocked)d, "
            "risk_final_gate_block=%(risk_final_gate_blocked)d, finam_lot_unavailable=%(finam_lot_unavailable)d; "
            "strategy_shadow=%(strategy_shadow_pass)d/%(strategy_shadow_evaluated)d pass "
            "(reject=%(strategy_shadow_reject)d; hard_blocks: BBO_missing=%(shadow_block_bbo_unavailable)d, "
            "BBO_stale=%(shadow_block_bbo_stale)d, book_unconfirmed=%(shadow_block_order_book_not_confirmed)d, "
            "ATR=%(shadow_block_atr_not_ready)d, spread=%(shadow_block_spread)d, other=%(shadow_block_other)d); "
            "confidence=%(confidence_blocked)d, RiskManager_pre_LLM=%(risk_blocked)d, "
            "минимальный_лот_по_риску=%(zero_lot)d, BBO_entry_block=%(execution_bbo_blocked)d, "
            "ордеров_принято/открыто=%(orders_scheduled)d",
            values,
        )
    
