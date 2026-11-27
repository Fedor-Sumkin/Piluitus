"""Strict consistency checks for restored PAPER position journal rows."""
from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any


def _number(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return result if math.isfinite(result) else None


def audit_open_paper_position(
    trade: Mapping[str, Any],
    *,
    expected_order_id: str | None = None,
) -> dict[str, Any]:
    """Validate identifiers, volume/lot arithmetic and partial-fill accounting.

    Legacy rows without partial-fill fields remain readable. Once a partial-fill
    ledger is present, its quantities must reconcile exactly with the residual
    OPEN position. Truncated ledgers are reported as unverifiable, not silently
    treated as complete.
    """
    reasons: list[str] = []
    order_id = str(trade.get("id") or "").strip()
    ticker = str(trade.get("ticker") or "").strip().upper().split("@")[0]
    side = str(trade.get("side") or "").strip().upper()
    if not order_id:
        reasons.append("MISSING_ORDER_ID")
    if expected_order_id and order_id and order_id != str(expected_order_id):
        reasons.append("ORDER_ID_KEY_MISMATCH")
    if not ticker:
        reasons.append("MISSING_TICKER")
    if side not in {"BUY", "SELL"}:
        reasons.append("INVALID_SIDE")
    if str(trade.get("status") or "").upper() != "OPEN":
        reasons.append("NOT_OPEN")

    remaining = _number(trade.get("volume", trade.get("original_volume")))
    original = _number(trade.get("original_volume", trade.get("volume")))
    lot_size = _number(trade.get("lot_size", 1))
    lots = _number(trade.get("lots"))
    if remaining is None or original is None or remaining <= 0.0 or original < remaining:
        reasons.append("INVALID_REMAINING_OR_ORIGINAL_VOLUME")
    if lot_size is None or lot_size <= 0.0 or int(lot_size) != lot_size:
        reasons.append("INVALID_LOT_SIZE")
        lot_size = None
    if remaining is not None and lot_size and int(remaining) % int(lot_size):
        reasons.append("REMAINING_VOLUME_NOT_LOT_ALIGNED")
    if lots is not None and remaining is not None and lot_size and int(lots) * int(lot_size) != int(remaining):
        reasons.append("LOTS_SHARES_MISMATCH")

    fills_raw = trade.get("exit_partial_fills")
    fill_count = _number(trade.get("exit_partial_fill_count", 0))
    ledger_truncated = bool(
        fill_count is not None and fill_count > 100.0
        and isinstance(fills_raw, list) and len(fills_raw) >= 100
    )
    if ledger_truncated:
        reasons.append("PARTIAL_FILL_LEDGER_TRUNCATED_REQUIRES_REVIEW")
    if fills_raw is not None and not isinstance(fills_raw, list):
        reasons.append("PARTIAL_FILL_LEDGER_INVALID")
    elif isinstance(fills_raw, list) and not ledger_truncated:
        filled = 0
        partial_net = 0.0
        for index, row in enumerate(fills_raw):
            if not isinstance(row, Mapping):
                reasons.append(f"PARTIAL_FILL_{index}_INVALID")
                continue
            qty = _number(row.get("quantity_shares"))
            price = _number(row.get("fill_price_after_slippage", row.get("raw_average_price")))
            net = _number(row.get("net_pnl_rub"))
            if qty is None or qty <= 0.0 or int(qty) != qty:
                reasons.append(f"PARTIAL_FILL_{index}_INVALID_QUANTITY")
                continue
            if price is None or price <= 0.0 or net is None:
                reasons.append(f"PARTIAL_FILL_{index}_INVALID_EXECUTION")
                continue
            if lot_size and int(qty) % int(lot_size):
                reasons.append(f"PARTIAL_FILL_{index}_NOT_LOT_ALIGNED")
            filled += int(qty)
            partial_net += net
        if original is not None and remaining is not None and filled != int(original - remaining):
            reasons.append("PARTIAL_FILL_QUANTITY_DOES_NOT_RECONCILE")
        recorded_realized = _number(trade.get("realized_net_pnl_rub"))
        if recorded_realized is not None and fills_raw and abs(recorded_realized - partial_net) > 0.02:
            reasons.append("PARTIAL_FILL_NET_PNL_DOES_NOT_RECONCILE")
        if fill_count is not None and int(fill_count) != len(fills_raw):
            reasons.append("PARTIAL_FILL_COUNT_MISMATCH")
    elif fills_raw is None:
        if fill_count is not None and fill_count > 0.0:
            reasons.append("PARTIAL_FILL_LEDGER_MISSING")
        if (
            original is not None and remaining is not None
            and int(original) != int(remaining)
        ):
            reasons.append("PARTIAL_FILL_LEDGER_MISSING_FOR_REDUCED_POSITION")

    entry = _number(trade.get("entry_price"))
    stop = _number(trade.get("stop_loss"))
    initial_stop = _number(trade.get("initial_stop_loss", trade.get("stop_loss")))
    target = _number(trade.get("take_profit"))
    geometry_valid = bool(
        entry is not None and stop is not None and initial_stop is not None and target is not None
        and entry > 0.0 and stop > 0.0 and initial_stop > 0.0 and target > 0.0
        and (
            (side == "BUY" and initial_stop < entry < target and initial_stop <= stop < target)
            or (side == "SELL" and target < entry < initial_stop and target < stop <= initial_stop)
        )
    )
    if not geometry_valid:
        reasons.append("INVALID_SL_TP_GEOMETRY")

    return {
        "valid": not reasons,
        "status": "VERIFIED" if not reasons else "REVIEW_REQUIRED",
        "order_id": order_id or None,
        "ticker": ticker or None,
        "remaining_shares": int(remaining) if remaining is not None and remaining > 0 else None,
        "original_shares": int(original) if original is not None and original > 0 else None,
        "partial_fills_verified": isinstance(fills_raw, list) and not ledger_truncated,
        "partial_ledger_truncated": ledger_truncated,
        "reasons": reasons,
    }
