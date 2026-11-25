"""Stable reason buckets for signal-gate and pre-LLM diagnostics."""
from __future__ import annotations


def classify_guard_reason(reason: object) -> str:
    """Map human or machine reasons to bounded, stable diagnostic categories.

    Cost and queue/flow failures used to fall through to the generic "other"
    bucket because their messages did not contain older generic keywords.
    Existing "quotes" semantics are preserved for the BBO retry path.
    """
    value = str(reason or "").strip().lower()
    if not value:
        return "other"
    if (
        "queue_trade_flow_conflict" in value
        or "queue/flow" in value
        or "дисбаланс очереди" in value
        or ("queue" in value and "flow" in value and ("conflict" in value or "противореч" in value))
    ):
        return "queue_flow"
    if (
        "cost/atr" in value
        or "cost_to_atr" in value
        or "round-trip" in value
        or "издержк" in value
        or "стоимость входа" in value
    ):
        return "cost"
    if "спред" in value or "spread" in value:
        return "spread"
    if "atr" in value or "m1-диапазон" in value or "m1-истори" in value or "m1 " in value:
        return "atr"
    if (
        "bid/ask" in value or "котировк" in value or "актуальн" in value
        or "bbo" in value or "order_book" in value or "order book" in value
        or "стакан" in value
    ):
        return "quotes"
    if "всплеск" in value or "объём" in value or "объем" in value or "volume" in value:
        return "volume"
    if (
        "импульс" in value or "vwap" in value or "направлен" in value
        or "direction" in value or "momentum" in value
    ):
        return "direction"
    if (
        "размер лучшей заявки" in value or "сокращение" in value
        or "истории размера" in value or "size" in value
    ):
        return "size"
    if "risk/reward" in value or "risk_reward" in value or "соотношени" in value or "риск/прибыл" in value:
        return "risk_reward"
    if "объяснение" in value or "текст" in value or "поврежд" in value or "контракт" in value:
        return "text"
    return "other"
