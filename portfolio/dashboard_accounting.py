"""Pure dashboard balance calculations for the portfolio layer."""
from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from typing import Any

from portfolio.portfolio_metrics import calculate_assets_used, calculate_free_balance


def _finite_number(value: object, default: float) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return number if math.isfinite(number) else default


def _bounded_percentage(value: object, default: float, ceiling: float) -> float:
    return min(max(0.0, _finite_number(value, default)), ceiling)


def build_dashboard_balance_snapshot(
    *,
    positions: Iterable[object],
    unresolved_positions: Iterable[object] = (),
    initial_deposit: object,
    total_pnl: object = 0.0,
    realized_pnl: object = 0.0,
    open_pnl: object = 0.0,
    max_gross_exposure_pct: object = 0.15,
    max_single_position_pct: object = 0.05,
    lot_sizes: Mapping[str, int] | None = None,
) -> dict[str, Any]:
    """Build the dashboard snapshot using fixed acquisition cost, never live prices."""
    active_positions = list(positions)
    unresolved = list(unresolved_positions)
    capital_positions = list(active_positions)
    seen_ids = {
        str(position.get("id", ""))
        for position in capital_positions
        if isinstance(position, dict)
    }
    for position in unresolved:
        if isinstance(position, dict) and str(position.get("id", "")) not in seen_ids:
            capital_positions.append(position)

    deposit = max(0.0, _finite_number(initial_deposit, 0.0))
    pnl = _finite_number(total_pnl, 0.0)
    realized = _finite_number(realized_pnl, 0.0)
    floating = _finite_number(open_pnl, pnl - realized)
    gross_pct = _bounded_percentage(max_gross_exposure_pct, 0.15, 0.15)
    single_pct = _bounded_percentage(max_single_position_pct, 0.05, 0.05)

    assets_used = calculate_assets_used(
        positions=capital_positions, latest_prices=None, lot_sizes=lot_sizes,
    )
    unresolved_assets_cost = calculate_assets_used(positions=unresolved, lot_sizes=lot_sizes)
    free_balance = calculate_free_balance(deposit + realized, assets_used)
    exposure_cap = round(deposit * gross_pct, 2)
    single_position_cap = round(deposit * single_pct, 2)
    over_cap = assets_used > exposure_cap

    snapshot = {
        "equity": round(deposit + pnl, 2),
        "initial_deposit": round(deposit, 2),
        "free_margin": free_balance,
        "margin_used": round(assets_used, 2),
        "exposure_cap": exposure_cap,
        "exposure_cap_pct": gross_pct,
        "single_position_cap": single_position_cap,
        "unreconciled_positions_count": len(unresolved),
        "pnl": round(pnl, 2),
        "realized_pnl": round(realized, 2),
        "open_pnl": round(floating, 2),
        "active_positions_count": len(active_positions),
    }
    return {
        "snapshot": snapshot,
        "assets_used": assets_used,
        "unresolved_assets_cost": unresolved_assets_cost,
        "exposure_cap": exposure_cap,
        "max_gross_pct": gross_pct,
        "over_cap": over_cap,
        "guard_signature": (len(unresolved), over_cap),
    }
