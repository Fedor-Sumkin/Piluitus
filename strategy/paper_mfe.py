"""Executable-BBO MFE versus realized PAPER net-PnL attribution.

The BBO MFE is an opportunity benchmark, not a promise of a fill. It excludes
queue/depth impact; configured exit slippage and round-trip commissions are
estimated explicitly so it can be compared on a net basis with the closed trade.
"""
from __future__ import annotations

import math
from typing import Any, Mapping


def _finite(value: object) -> float | None:
    if isinstance(value, bool):
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def build_paper_mfe_reconciliation(
    trade: Mapping[str, Any],
    realized_net_pnl_rub: object,
) -> dict[str, Any]:
    """Estimate full-position BBO MFE net of known round-trip costs.

    MFE percent is price-based and therefore scaled to original position size,
    including positions that had partial exits. Actual realized PnL already
    includes partial exits, commissions, and modeled slippage.
    """
    unavailable = {
        "mfe_reconciliation_status": "MISSING_REQUIRED_MFE_OR_ENTRY_DATA",
        "max_favorable_bbo_mfe_gross_rub": None,
        "max_favorable_bbo_mfe_net_estimate_rub": None,
        "mfe_estimated_entry_commission_rub": None,
        "mfe_estimated_exit_commission_rub": None,
        "mfe_estimated_exit_slippage_rub": None,
        "realized_net_pnl_rub_for_mfe": _finite(realized_net_pnl_rub),
        "realized_capture_of_net_mfe_pct": None,
        "net_mfe_minus_realized_pnl_rub": None,
        "mfe_comparison_basis": "EXECUTABLE_BBO_ONLY_NO_DEPTH_OR_QUEUE_IMPACT",
    }
    entry = _finite(trade.get("entry_price"))
    original_qty = _finite(trade.get("original_volume", trade.get("volume")))
    favorable_pct = _finite(trade.get("max_favorable_bbo_excursion_pct"))
    realized = _finite(realized_net_pnl_rub)
    if (
        entry is None or entry <= 0.0 or original_qty is None or original_qty <= 0.0
        or favorable_pct is None or favorable_pct < 0.0 or realized is None
    ):
        return unavailable

    side = str(trade.get("side") or "").strip().upper()
    if side not in {"BUY", "SELL"}:
        return unavailable
    exit_mfe_price = entry * (1.0 + favorable_pct / 100.0 if side == "BUY" else 1.0 - favorable_pct / 100.0)
    if not math.isfinite(exit_mfe_price) or exit_mfe_price <= 0.0:
        return unavailable

    gross_mfe = entry * original_qty * favorable_pct / 100.0
    fee_rate = _finite(trade.get("commission_pct_per_side"))
    if fee_rate is None or fee_rate < 0.0:
        return unavailable

    # The close path folds previously allocated partial-entry fees back into
    # entry_commission_rub. On an open position, add realized and remaining.
    recorded_entry_fee = _finite(trade.get("entry_commission_rub"))
    realized_entry_fee = _finite(trade.get("realized_entry_commission_rub", 0.0))
    if recorded_entry_fee is None or realized_entry_fee is None:
        return unavailable
    if str(trade.get("status") or "").upper() == "CLOSED":
        entry_fee = max(0.0, recorded_entry_fee)
    else:
        entry_fee = max(0.0, recorded_entry_fee) + max(0.0, realized_entry_fee)
    if entry_fee <= 0.0 and fee_rate > 0.0:
        entry_fee = entry * original_qty * fee_rate / 100.0

    exit_fee = exit_mfe_price * original_qty * fee_rate / 100.0
    slip_bps = _finite(trade.get("paper_slippage_bps"))
    if slip_bps is None or slip_bps < 0.0:
        return unavailable
    exit_slippage = exit_mfe_price * original_qty * slip_bps / 10_000.0
    net_mfe = gross_mfe - entry_fee - exit_fee - exit_slippage
    capture_pct = realized / net_mfe * 100.0 if net_mfe > 0.0 else None
    return {
        "mfe_reconciliation_status": "ESTIMATED_NET_MFE_AVAILABLE",
        "max_favorable_bbo_mfe_gross_rub": round(gross_mfe, 4),
        "max_favorable_bbo_mfe_net_estimate_rub": round(net_mfe, 4),
        "mfe_estimated_entry_commission_rub": round(entry_fee, 4),
        "mfe_estimated_exit_commission_rub": round(exit_fee, 4),
        "mfe_estimated_exit_slippage_rub": round(exit_slippage, 4),
        "realized_net_pnl_rub_for_mfe": round(realized, 4),
        "realized_capture_of_net_mfe_pct": round(capture_pct, 4) if capture_pct is not None else None,
        "net_mfe_minus_realized_pnl_rub": round(net_mfe - realized, 4),
        "mfe_comparison_basis": "EXECUTABLE_BBO_ONLY_NO_DEPTH_OR_QUEUE_IMPACT",
    }
