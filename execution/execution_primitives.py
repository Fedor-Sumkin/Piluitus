"""Shared types and narrow input primitives for execution components."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any, TypedDict, cast


class BBOExitQuote(TypedDict):
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    bid_levels: list[list[float]]
    ask_levels: list[list[float]]
    bid_age_sec: float
    ask_age_sec: float
    age_sec: float
    executable_price: float
    executable_side: str
    spread_pct: float
    max_age_sec: float
    order_book_valid: bool
    order_book_age_sec: float
TradeRecord = dict[str, Any]


def _finite_or_default(value: object, default: float = 0.0) -> float:
    """Convert only known scalar input types; reject arbitrary objects safely."""
    if not isinstance(value, (int, float, str)):
        return default
    try:
        number = float(value)
        return number if math.isfinite(number) else default
    except (TypeError, ValueError, OverflowError):
        return default


def _as_string_mapping(value: object) -> Mapping[str, object] | None:
    """Narrow untrusted dynamic data before any mapping operation."""
    if not isinstance(value, Mapping):
        return None
    return cast(Mapping[str, object], value)
