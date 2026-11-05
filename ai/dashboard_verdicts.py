"""Deterministic dashboard verdict text and execution-state normalisation.

LLM-provided free-form explanations are deliberately excluded from this module.
Only locally measured market metrics and explicit guard reasons are rendered.
"""
from __future__ import annotations

import math
import os
import time
from typing import Any


def finite_float(value: object) -> float | None:
    """Convert a JSON-like value to a finite float without accepting None/bools."""
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        number = float(value)
    elif isinstance(value, str):
        normalized = value.strip().replace(",", ".")
        if not normalized:
            return None
        try:
            number = float(normalized)
        except ValueError:
            return None
    else:
        return None
    return number if math.isfinite(number) else None


def nonnegative_int(value: object) -> int | None:
    """Convert a JSON-like value to a non-negative whole number."""
    number = finite_float(value)
    if number is None or number < 0 or not number.is_integer():
        return None
    return int(number)


def _metric_number(metrics: dict[str, Any], key: str) -> float | None:
    return finite_float(metrics.get(key))


def build_verdict_explanation(
    final_decision: str,
    candidate_decision: str,
    metrics: dict[str, Any],
    reason: str = "",
) -> tuple[str, str]:
    """Build concise Russian explanations only from measured local metrics."""
    final_decision = str(final_decision or "HOLD").upper()
    candidate_decision = str(candidate_decision or "HOLD").upper()
    if not isinstance(metrics, dict):
        metrics = {}

    change = _metric_number(metrics, "price_change_pct")
    vs_vwap = _metric_number(metrics, "price_vs_vwap_pct")
    spike = _metric_number(metrics, "volume_spike")
    spread = _metric_number(metrics, "spread_pct")

    facts: list[str] = []
    pattern = str(metrics.get("bar_pattern_name", "") or "").strip().upper()
    pattern_confirmed = _metric_number(metrics, "bar_pattern_confirmed") or 0.0
    pattern_signal_at = _metric_number(metrics, "_bar_pattern_signal_at_monotonic") or -1.0
    try:
        pattern_max_age = max(2.0, float(os.getenv("PILUITUS_PATTERN_SIGNAL_MAX_AGE_SEC", "15")))
    except (TypeError, ValueError):
        pattern_max_age = 15.0
    pattern_age = time.monotonic() - pattern_signal_at if pattern_signal_at > 0 else float("inf")
    supported_patterns = {
        "M1_RANGE_BREAKOUT_BUY": ("bar_breakout_up", "BUY", 0.5),
        "M1_RANGE_BREAKOUT_SELL": ("bar_breakout_down", "SELL", -0.5),
        "M1_EMA_PULLBACK_BUY": ("bar_pullback_buy", "BUY", 0.5),
        "M1_EMA_PULLBACK_SELL": ("bar_pullback_sell", "SELL", -0.5),
    }
    if pattern in supported_patterns and pattern_confirmed >= 0.5 and -0.25 <= pattern_age <= pattern_max_age:
        flag_key, pattern_side, direction_threshold = supported_patterns[pattern]
        pattern_code = _metric_number(metrics, "bar_pattern_direction_code")
        flag = _metric_number(metrics, flag_key) or 0.0
        direction_matches = (
            pattern_code is not None
            and (pattern_code >= direction_threshold if pattern_side == "BUY" else pattern_code <= direction_threshold)
        )
        if flag >= 0.5 and direction_matches:
            facts.append(f"минутный паттерн {pattern}")
    if change is not None:
        facts.append(f"изменение цены {change:+.4f}%")
    if vs_vwap is not None:
        facts.append(f"отклонение от VWAP {vs_vwap:+.4f}%")
    if spike is not None:
        facts.append(f"размер последней сделки {spike:.2f}× к среднему")
    if spread is not None:
        facts.append(f"спред {spread:.3f}%")

    if final_decision in {"BUY", "SELL"}:
        side_ru = "покупку" if final_decision == "BUY" else "продажу"
        detail = "; ".join(facts) if facts else "измеряемые параметры получены"
        tactic = f"Локальные проверки допускают {side_ru}: {detail}."
        critic = "Измеряемые условия входа и порог уверенности пройдены; риск и размер заявки проверяются отдельно."
        return tactic, critic

    if candidate_decision in {"BUY", "SELL"}:
        tactic = f"Кандидат ИИ {candidate_decision} не допущен к открытию позиции."
    elif change is not None and vs_vwap is not None and change * vs_vwap > 0:
        direction = "растёт" if change > 0 else "снижается"
        tactic = f"Цена {direction}; движение согласовано с отклонением от VWAP. Обязательные условия входа не подтверждены."
    else:
        tactic = "Направленный импульс не подтверждён измеряемыми условиями входа."
    critic = str(reason or "Модель не предложила направленный вход.")
    return tactic, critic


def normalise_execution_status(raw_status: str, mode: str) -> str:
    """Do not label broker acceptance as a filled LIVE position."""
    status = str(raw_status or "FAILED").strip().upper()
    trade_mode = str(mode or "PAPER").strip().upper()
    if status == "SUCCESS":
        return "OPENED" if trade_mode == "PAPER" else "ACCEPTED"
    if status == "SKIPPED":
        return "BLOCKED"
    if status == "FAILED":
        return "FAILED"
    return "FAILED"
