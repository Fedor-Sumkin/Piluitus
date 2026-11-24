"""Local post-LLM entry gates and execution-readiness planning.

This module preserves the worker's existing guard order and never submits an
order. It only decides whether a validated candidate is ready to be handed to
the execution layer and, when it is, calculates the requested size.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any


def _apply_post_llm_evidence_gate(
    *,
    decision: str,
    confidence: float,
    valid_evidence: bool,
    block_reason: str,
    signal_reason: str,
    ticker: str,
    increment_counter: Callable[[str], Any],
    guard_block: Callable[[str], Any],
    logger: Any,
    ai_analysis: dict[str, Any],
) -> tuple[str, float, str]:
    """Apply the measured-evidence gate before the confidence gate."""
    if decision not in {"BUY", "SELL"} or valid_evidence:
        return decision, confidence, signal_reason

    requested_decision = decision
    reason = block_reason or "Измеряемые условия входа не пройдены."
    increment_counter("post_llm_evidence_blocked")
    guard_block(reason)
    logger.warning(
        "🛑 [SIGNAL GUARD] Заблокирован кандидат %s по %s: %s",
        requested_decision,
        ticker,
        reason,
    )
    ai_analysis["final_decision"] = "HOLD"
    ai_analysis["confidence_score"] = 0.0
    return "HOLD", 0.0, reason


def _apply_confidence_gate(
    *,
    decision: str,
    confidence: float,
    confidence_valid: bool,
    signal_reason: str,
    increment_counter: Callable[[str], Any],
    ai_analysis: dict[str, Any],
) -> tuple[str, float, str]:
    """Apply the unchanged 75% confidence threshold after evidence validation."""
    if decision not in {"BUY", "SELL"} or (confidence_valid and confidence >= 75.0):
        return decision, confidence, signal_reason

    increment_counter("confidence_blocked")
    increment_counter("post_llm_confidence_blocked")
    if not confidence_valid:
        reason = "Модель не вернула корректную числовую confidence_score; вход заблокирован."
    else:
        reason = (
            f"Оценка модели {confidence:.0f}% ниже порога входа 75%; "
            "новая позиция не открыта."
        )
    ai_analysis["final_decision"] = "HOLD"
    # Keep the normalized model score visible for diagnostic attribution.
    ai_analysis["confidence_score"] = confidence / 100.0
    return "HOLD", confidence, reason


def _minimum_atr_periods(raw_value: object) -> int:
    try:
        return min(14, max(2, int(raw_value)))
    except (TypeError, ValueError):
        return 5


def _prepare_atr_protected_size(
    *,
    ticker: str,
    price: float,
    decision: str,
    metrics: Mapping[str, Any],
    ai_analysis: dict[str, Any],
    lot_multiplier: int,
    execution_engine: Any,
    risk_manager: Any,
    paper_trading: bool,
    increment_counter: Callable[[str], Any],
    finite_float: Callable[[Any], float | None],
    nonnegative_int: Callable[[Any], int | None],
    logger: Any,
    min_atr_periods_raw: object,
    atr_multiplier_raw: object,
) -> tuple[str, str, int, float]:
    """Build unchanged ATR stops and risk-sized quantity; never submits an order."""
    atr_value = finite_float(metrics.get("bar_atr")) or 0.0
    atr_periods = nonnegative_int(metrics.get("bar_atr_periods")) or 0
    entry_bbo_available = (finite_float(metrics.get("spread_available")) or 0.0) >= 0.5
    entry_bid = (finite_float(metrics.get("bid")) or 0.0) if entry_bbo_available else 0.0
    entry_ask = (finite_float(metrics.get("ask")) or 0.0) if entry_bbo_available else 0.0
    entry_spread_pct = (finite_float(metrics.get("spread_pct")) or 0.0) if entry_bbo_available else 0.0
    entry_bbo_age_sec = (finite_float(metrics.get("bbo_age_sec")) or 0.0) if entry_bbo_available else 0.0

    diagnostic_stop_builder = getattr(
        execution_engine, "calculate_atr_stop_with_diagnostics", None
    )
    if callable(diagnostic_stop_builder):
        stop_loss, take_profit, protection_reason = diagnostic_stop_builder(
            ticker,
            price,
            decision,
            atr_value=atr_value,
            atr_periods=atr_periods,
            bid=entry_bid,
            ask=entry_ask,
        )
    else:
        stop_loss, take_profit = execution_engine.calculate_atr_stop(
            ticker,
            price,
            decision,
            atr_value=atr_value,
            atr_periods=atr_periods,
            bid=entry_bid,
            ask=entry_ask,
        )
        min_atr_periods = _minimum_atr_periods(min_atr_periods_raw)
        protection_reason = (
            "ATR_HISTORY_UNAVAILABLE"
            if atr_value <= 0.0 or atr_periods < min_atr_periods
            else "STOP_TARGET_CALCULATION_FAILED"
        )
    if stop_loss <= 0.0 or take_profit <= 0.0:
        min_atr_periods = _minimum_atr_periods(min_atr_periods_raw)
        ai_analysis["atr_entry_ready"] = False
        ai_analysis["stop_protection_status"] = "BLOCKED"
        ai_analysis["stop_protection_reason"] = protection_reason
        if protection_reason == "ATR_HISTORY_UNAVAILABLE":
            message = (
                f"Недостаточно закрытых M1-диапазонов для ATR: "
                f"{atr_periods}/{min_atr_periods}, ATR={atr_value:.8f}; "
                "новая позиция не открыта."
            )
            increment_counter("atr_history_blocked")
        elif protection_reason == "PRICE_TICK_METADATA_UNAVAILABLE":
            message = (
                f"Нет подтверждённого Finam MINSTEP/price_step для {ticker}; "
                "SL/TP не сформированы, новая позиция не открыта."
            )
            increment_counter("price_tick_metadata_blocked")
        else:
            message = (
                f"Не удалось сформировать допустимые SL/TP для {ticker}: "
                f"{protection_reason}; новая позиция не открыта."
            )
            increment_counter("stop_protection_blocked")
        logger.warning(
            "Сигнал %s по %s заблокирован после LLM: %s",
            decision, ticker, message,
        )
        return "BLOCKED", message, 0, 0.0

    ai_analysis["atr_entry_ready"] = True
    ai_analysis["stop_protection_status"] = "READY"
    ai_analysis["stop_protection_reason"] = "OK"
    ai_analysis["stop_loss"] = stop_loss
    ai_analysis["take_profit"] = take_profit
    ai_analysis["atr_at_entry"] = atr_value
    ai_analysis["atr_periods_at_entry"] = atr_periods
    ai_analysis["atr_age_sec_at_entry"] = (
        finite_float(metrics.get("bar_atr_age_sec"))
        if isinstance(metrics, dict) else None
    )
    ai_analysis["atr_multiplier"] = finite_float(atr_multiplier_raw) or 1.5
    ai_analysis["entry_bid"] = entry_bid
    ai_analysis["entry_ask"] = entry_ask
    ai_analysis["entry_spread_pct"] = entry_spread_pct
    ai_analysis["entry_bbo_age_sec"] = entry_bbo_age_sec

    requested_lots = risk_manager.calculate_lot_size(ticker, price, stop_loss)
    if requested_lots <= 0:
        increment_counter("zero_lot")
        if lot_multiplier <= 0:
            increment_counter("finam_lot_unavailable")
            venue_lot_name = (
                "биржевой lot_size" if paper_trading
                else "Finam account trade_lot_size"
            )
            message = (
                f"{venue_lot_name} для {ticker} не подтверждён; "
                "новая позиция не открыта."
            )
            logger.warning(
                "Сигнал %s по %s заблокирован: lot_size не подтверждён.",
                decision,
                ticker,
            )
        else:
            message = "Риск на минимальный лот превышает установленный лимит."
            logger.warning(
                "Сигнал %s по %s пропущен: риск на минимальный лот превышает лимит.",
                decision,
                ticker,
            )
        return "BLOCKED", message, requested_lots, 0.0

    requested_notional = requested_lots * lot_multiplier * price
    logger.info(
        "🧮 [POSITION SIZE] %s %s: lots=%d shares=%d price=%.4f notional=%.2f ₽ planned_risk=%.2f ₽ ATR=%.6f (%d M1 ranges) stop=%.4f%%",
        decision,
        ticker,
        requested_lots,
        requested_lots * lot_multiplier,
        price,
        requested_notional,
        abs(price - stop_loss) * requested_lots * lot_multiplier,
        atr_value,
        atr_periods,
        abs(price - stop_loss) / price * 100.0,
    )
    return "PENDING", "Заявка передана в контур исполнения; ожидается результат.", requested_lots, requested_notional


def prepare_entry_execution_plan(
    *,
    ticker: str,
    price: float,
    decision: str,
    confidence: float,
    confidence_valid: bool,
    valid_evidence: bool,
    block_reason: str,
    signal_reason: str,
    metrics: Mapping[str, Any],
    ai_analysis: dict[str, Any],
    lot_multiplier: int,
    execution_engine: Any,
    risk_manager: Any,
    paper_trading: bool,
    increment_counter: Callable[[str], Any],
    guard_block: Callable[[str], Any],
    finite_float: Callable[[Any], float | None],
    nonnegative_int: Callable[[Any], int | None],
    logger: Any,
    min_atr_periods_raw: object = "5",
    atr_multiplier_raw: object = "1.5",
) -> dict[str, Any]:
    """Apply evidence, confidence, ATR and sizing gates in their original order."""
    decision, confidence, signal_reason = _apply_post_llm_evidence_gate(
        decision=decision,
        confidence=confidence,
        valid_evidence=valid_evidence,
        block_reason=block_reason,
        signal_reason=signal_reason,
        ticker=ticker,
        increment_counter=increment_counter,
        guard_block=guard_block,
        logger=logger,
        ai_analysis=ai_analysis,
    )
    decision, confidence, signal_reason = _apply_confidence_gate(
        decision=decision,
        confidence=confidence,
        confidence_valid=confidence_valid,
        signal_reason=signal_reason,
        increment_counter=increment_counter,
        ai_analysis=ai_analysis,
    )

    execution_status = "NOT_REQUESTED"
    execution_message = "Новая позиция не запрашивалась."
    requested_lots = 0
    requested_notional = 0.0
    normalized_lot_multiplier = max(int(lot_multiplier or 0), 0)

    if decision in {"BUY", "SELL"} and confidence >= 75.0:
        logger.warning(
            "🎯 СИГНАЛ УТВЕРЖДЁН ИИ: %s → %s (%.1f%%)",
            ticker,
            decision,
            confidence,
        )
        (
            execution_status,
            execution_message,
            requested_lots,
            requested_notional,
        ) = _prepare_atr_protected_size(
            ticker=ticker,
            price=price,
            decision=decision,
            metrics=metrics,
            ai_analysis=ai_analysis,
            lot_multiplier=normalized_lot_multiplier,
            execution_engine=execution_engine,
            risk_manager=risk_manager,
            paper_trading=paper_trading,
            increment_counter=increment_counter,
            finite_float=finite_float,
            nonnegative_int=nonnegative_int,
            logger=logger,
            min_atr_periods_raw=min_atr_periods_raw,
            atr_multiplier_raw=atr_multiplier_raw,
        )

    return {
        "decision": decision,
        "confidence": confidence,
        "signal_reason": signal_reason,
        "execution_status": execution_status,
        "execution_message": execution_message,
        "requested_lots": requested_lots,
        "requested_notional": requested_notional,
        "lot_multiplier": normalized_lot_multiplier,
    }
