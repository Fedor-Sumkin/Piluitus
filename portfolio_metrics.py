"""Book-cost helpers for the PILUITUS dashboard.

"Assets used" is the acquisition cost of currently open positions, not their live
mark-to-market value and not the broker's regulatory margin. Market quotes must not
change this figure. The module is pure and can be tested without external services.
"""
from __future__ import annotations

import math
from typing import Any, Iterable, Mapping


def _finite_positive(value: Any) -> float | None:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(number) or number <= 0:
        return None
    return number


def calculate_assets_used(
    positions: Iterable[Mapping[str, Any]],
    latest_prices: Mapping[str, float] | None = None,
    lot_sizes: Mapping[str, int] | None = None,
) -> float:
    """Sum fixed acquisition cost of open positions.

    The ``latest_prices`` argument remains for backwards compatibility but is
    intentionally ignored. Book cost = entry/acquisition price * current quantity.
    PILUITUS records ``volume`` as shares; if it is absent, lots are converted via
    saved/configured lot size. A persisted ``notional_entry_rub`` is the final
    fallback for legacy records with incomplete price/quantity fields. Long and
    short positions both count as capital committed to that active trade.
    """
    _ = latest_prices  # Never mark book cost to the live market price.
    lot_sizes = lot_sizes or {}
    total = 0.0

    for position in positions:
        if not isinstance(position, Mapping):
            continue
        status = str(position.get("status", "OPEN")).strip().upper()
        if status and status not in {"OPEN", "STALE_REVIEW"}:
            continue
        if status == "STALE_REVIEW" and str(position.get("reconciliation_status", "")).upper() != "REVIEW_REQUIRED":
            continue

        ticker = str(
            position.get("ticker")
            or position.get("security_code")
            or position.get("securityCode")
            or ""
        ).strip().upper().split("@")[0]
        if not ticker:
            continue

        quantity: float | None = None
        for key in ("volume", "quantity", "qty", "shares"):
            raw = position.get(key)
            if raw is not None:
                try:
                    candidate = float(raw)
                except (TypeError, ValueError, OverflowError):
                    continue
                if math.isfinite(candidate) and candidate > 0:
                    quantity = candidate
                    break

        if quantity is None:
            try:
                lots = float(position.get("lots", 0))
            except (TypeError, ValueError, OverflowError):
                lots = 0.0
            if math.isfinite(lots) and lots > 0:
                try:
                    lot_size = float(position.get("lot_size") or lot_sizes.get(ticker, 1))
                except (TypeError, ValueError, OverflowError):
                    lot_size = 1.0
                if not math.isfinite(lot_size) or lot_size <= 0:
                    lot_size = 1.0
                quantity = lots * lot_size

        # entry_price is the immutable acquisition price. current_price and
        # last_price must never be used for this dashboard field.
        entry_price = None
        for key in ("entry_price", "purchase_price", "average_entry_price", "average_price"):
            entry_price = _finite_positive(position.get(key))
            if entry_price is not None:
                break

        if quantity is not None and entry_price is not None:
            total += abs(entry_price * quantity)
            continue

        # Backwards-compatible fallback for historical records without enough
        # fields to reconstruct entry_price * quantity.
        notional = _finite_positive(position.get("notional_entry_rub"))
        if notional is not None:
            total += notional

    return round(total, 2)


def calculate_free_balance(initial_deposit: float, assets_cost: float) -> float:
    """Return configured deposit less the acquisition cost of open assets.

    Do not clamp at zero: a negative value exposes an accounting/position-limit
    violation instead of hiding it in the dashboard.
    """
    try:
        deposit = float(initial_deposit)
        assets = float(assets_cost)
    except (TypeError, ValueError, OverflowError):
        return 0.0
    if not math.isfinite(deposit) or not math.isfinite(assets):
        return 0.0
    return round(deposit - assets, 2)
