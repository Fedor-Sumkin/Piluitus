"""Normalize execution fill details before publishing verdict outcomes."""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from ai.dashboard_verdicts import finite_float, nonnegative_int


def normalize_execution_fill(
    result: Mapping[str, Any],
    *,
    requested_lots: int,
    ticker: str,
    price: float,
    lot_sizes: Mapping[str, int],
    allow_fallback: bool = False,
) -> dict[str, Any]:
    """Return stable order/fill fields, preserving executor values when supplied."""
    order_id = str(result.get("orderId") or result.get("order_id") or "")
    raw_lots = result.get("lots")
    raw_shares = result.get("shares")
    raw_notional = result.get("notional_entry_rub")

    actual_lots = nonnegative_int(raw_lots)
    actual_shares = nonnegative_int(raw_shares)
    actual_notional = finite_float(raw_notional)
    executor_requested_lots = nonnegative_int(result.get("requested_lots"))

    if allow_fallback and raw_lots is None:
        actual_lots = max(0, int(requested_lots))

    if allow_fallback and raw_shares is None and actual_lots is not None:
        lot_multiplier = max(int(lot_sizes.get(ticker, 0) or 0), 0)
        actual_shares = actual_lots * lot_multiplier

    if allow_fallback and raw_notional is None and actual_shares is not None:
        actual_notional = round(actual_shares * float(price), 2)

    return {
        "order_id": order_id,
        "actual_lots": actual_lots,
        "actual_shares": actual_shares,
        "actual_notional": actual_notional,
        "requested_lots": executor_requested_lots,
    }
