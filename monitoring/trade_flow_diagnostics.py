"""Pure reason bucketing for trade-flow diagnostics.

These helpers only normalize diagnostic labels. They do not approve, reject,
size, schedule, or execute trading candidates.
"""
from __future__ import annotations


def guard_reason_bucket(reason: object) -> str:
    """Map a pre-LLM guard explanation to a bounded diagnostic category."""
    lowered = str(reason or "").lower()
    if "bid/ask" in lowered or "котировк" in lowered or "актуальн" in lowered:
        return "quotes"
    if "спред" in lowered:
        return "spread"
    if "atr" in lowered or "m1-диапазон" in lowered or "m1-истори" in lowered:
        return "atr"
    if "размер лучшей заявки" in lowered or "сокращение" in lowered or "истории размера" in lowered:
        return "size"
    if "всплеск" in lowered or "объём" in lowered or "объем" in lowered:
        return "volume"
    if "импульс" in lowered or "vwap" in lowered or "цена не" in lowered:
        return "direction"
    return "other"


def bbo_resume_reason_bucket(reason: object) -> str:
    """Map a human-readable BBO recovery failure to a stable diagnostic counter."""
    lowered = str(reason or "").strip().lower()
    if "состояние котировок" in lowered or "состояния стакана" in lowered:
        return "quote_missing"
    if "не подтверждён" in lowered or "не подтвержден" in lowered or "целостный" in lowered:
        return "book_unconfirmed"
    if "непротиворечив" in lowered or "двухсторон" in lowered:
        return "invalid_bbo"
    if "order_book устарело" in lowered:
        return "book_stale"
    if "свежего подтверждения" in lowered:
        return "confirmation_stale"
    return "other_book_failure"
