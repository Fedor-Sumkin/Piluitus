"""Shared-capital and fresh-BBO mark-to-market replay simulation."""
from __future__ import annotations

import math
from typing import Any, Iterable, Mapping

from .bbo_replay_simulator_primitives import (
    _fresh_side,
    _number,
    _observation_epoch,
)

def _portfolio_capacity_simulation(
    trade_results: list[Mapping[str, Any]],
    scenario_name: str,
    initial_capital_rub: float,
    max_allocation_pct: float,
    *,
    events: Iterable[Mapping[str, Any]] = (),
    max_bbo_age_sec: float = 2.0,
) -> dict[str, Any]:
    """Apply shared-capital limits and mark portfolio equity from fresh BBOs."""
    capital = _number(initial_capital_rub)
    allocation_pct = _number(max_allocation_pct)
    max_age = _number(max_bbo_age_sec)
    if not math.isfinite(capital) or capital <= 0.0:
        raise ValueError("initial_capital_rub must be finite and positive")
    if not math.isfinite(allocation_pct) or not 0.0 < allocation_pct <= 100.0:
        raise ValueError("max_allocation_pct must be in (0, 100]")
    if not math.isfinite(max_age) or max_age <= 0.0:
        raise ValueError("max_bbo_age_sec must be finite and positive")

    allocation_limit = capital * allocation_pct / 100.0
    # MARK before EXIT before ENTRY at equal timestamps: a quote received before
    # a new entry cannot mark that new position, while exits use their close BBO.
    timeline: list[tuple[float, int, str, Mapping[str, Any], float]] = []
    rejected_invalid = 0
    for trade in trade_results:
        scenarios = trade.get("scenarios", [])
        selected = next(
            (row for row in scenarios
             if isinstance(row, Mapping) and str(row.get("name") or "") == scenario_name),
            None,
        )
        if not isinstance(selected, Mapping):
            continue
        entry_epoch = _number(trade.get("entry_time_epoch"))
        notional = _number(trade.get("entry_price")) * _number(trade.get("quantity"))
        if not math.isfinite(entry_epoch) or not math.isfinite(notional) or notional <= 0.0:
            rejected_invalid += 1
            continue
        exit_row = selected.get("exit") if isinstance(selected.get("exit"), Mapping) else None
        exit_epoch = _number(exit_row.get("event_time_epoch")) if exit_row else math.nan
        if exit_row is not None and (not math.isfinite(exit_epoch) or exit_epoch < entry_epoch):
            exit_row = None
        timeline.append((entry_epoch, 2, "ENTRY", trade, notional))
        if exit_row is not None:
            # Release prior positions before same-time new entries, but an
            # immediate exit cannot precede its own entry in the event order.
            exit_priority = 1 if exit_epoch > entry_epoch else 3
            timeline.append((exit_epoch, exit_priority, "EXIT", trade, notional))

    market_events_seen = 0
    for event in events:
        if not isinstance(event, Mapping) or str(event.get("event_kind") or "").upper() == "EXIT_TRIGGER":
            continue
        stamp = _observation_epoch(event)
        session = str(event.get("session_id") or "").strip()
        ticker = str(event.get("ticker") or "").strip().upper().split("@")[0]
        if not math.isfinite(stamp) or not session or not ticker:
            continue
        market_events_seen += 1
        timeline.append((stamp, 0, "MARK", event, 0.0))

    timeline.sort(key=lambda item: (
        item[0], item[1],
        str(item[3].get("ticker") or item[3].get("position_id") or ""),
        int(_number(item[3].get("sequence"), 0.0)),
    ))
    cash = capital
    reserved = 0.0
    active: dict[str, dict[str, Any]] = {}
    marks: dict[str, dict[str, float]] = {}
    accepted: set[str] = set()
    rejected_capacity = 0
    peak_reserved = 0.0
    realized_pnls: list[float] = []
    closed = 0
    peak_open_positions = 0
    mark_updates = mark_stale_events = mark_invalid_events = 0
    mark_out_of_order_events = 0
    complete_snapshots = incomplete_snapshots = 0
    max_missing_marks = max_stale_marks = 0
    running_equity = capital
    peak_realized_equity = capital
    realized_max_drawdown = 0.0
    peak_marked_equity = capital
    peak_marked_epoch = None
    marked_max_drawdown = marked_max_drawdown_pct = 0.0
    marked_drawdown_peak_epoch = marked_drawdown_trough_epoch = None
    latest_complete_equity = latest_complete_unrealized = None
    latest_complete_epoch = latest_complete_reserved = latest_complete_open_count = None
    last_timeline_epoch = None
    last_snapshot_complete = False

    def _position_key(trade: Mapping[str, Any]) -> str:
        return str(trade.get("position_id") or trade.get("trade_index") or trade.get("id") or "")

    def _snapshot(stamp: float) -> None:
        nonlocal complete_snapshots, incomplete_snapshots
        nonlocal max_missing_marks, max_stale_marks, peak_marked_equity, peak_marked_epoch
        nonlocal marked_max_drawdown, marked_max_drawdown_pct
        nonlocal marked_drawdown_peak_epoch, marked_drawdown_trough_epoch
        nonlocal latest_complete_equity, latest_complete_unrealized
        nonlocal latest_complete_epoch, latest_complete_reserved, latest_complete_open_count
        nonlocal last_snapshot_complete
        unrealized = 0.0
        missing = stale = 0
        for position_id, active_row in active.items():
            mark = marks.get(position_id)
            if mark is None:
                missing += 1
                continue
            mark_age = stamp - mark["timestamp"]
            if mark_age < 0.0 or mark_age > max_age:
                stale += 1
                continue
            trade = active_row["trade"]
            side = str(trade.get("side") or "").upper()
            entry = _number(trade.get("entry_price"))
            quantity = _number(trade.get("quantity"))
            raw_price = mark["price"]
            costs = trade.get("cost_assumptions")
            costs = costs if isinstance(costs, Mapping) else {}
            commission = _number(costs.get("commission_pct_per_side"), 0.0)
            slippage = _number(costs.get("slippage_bps"), 0.0)
            entry_fee = _number(costs.get("entry_commission_rub"), 0.0)
            if (
                side not in {"BUY", "SELL"} or not math.isfinite(entry)
                or not math.isfinite(quantity) or quantity <= 0.0
                or not math.isfinite(commission) or commission < 0.0
                or not math.isfinite(slippage) or slippage < 0.0
                or not math.isfinite(entry_fee) or entry_fee < 0.0
            ):
                missing += 1
                continue
            slip_rate = slippage / 10_000.0
            estimated_exit_fill = raw_price * (
                1.0 - slip_rate if side == "BUY" else 1.0 + slip_rate
            )
            sign = 1.0 if side == "BUY" else -1.0
            gross = (estimated_exit_fill - entry) * quantity * sign
            estimated_exit_fee = estimated_exit_fill * quantity * commission / 100.0
            unrealized += gross - entry_fee - estimated_exit_fee

        max_missing_marks = max(max_missing_marks, missing)
        max_stale_marks = max(max_stale_marks, stale)
        if missing or stale:
            incomplete_snapshots += 1
            last_snapshot_complete = False
            return
        complete_snapshots += 1
        last_snapshot_complete = True
        equity = cash + reserved + unrealized
        if equity > peak_marked_equity:
            peak_marked_equity = equity
            peak_marked_epoch = stamp
        drawdown = max(0.0, peak_marked_equity - equity)
        if drawdown > marked_max_drawdown:
            marked_max_drawdown = drawdown
            marked_drawdown_peak_epoch = peak_marked_epoch
            marked_drawdown_trough_epoch = stamp
            marked_max_drawdown_pct = (
                drawdown / peak_marked_equity * 100.0 if peak_marked_equity > 0.0 else 0.0
            )
        latest_complete_equity = equity
        latest_complete_unrealized = unrealized
        latest_complete_epoch = stamp
        latest_complete_reserved = reserved
        latest_complete_open_count = len(active)

    for stamp, _priority, event_kind, payload, notional in timeline:
        last_timeline_epoch = stamp
        if event_kind == "MARK":
            event = payload
            session = str(event.get("session_id") or "").strip()
            ticker = str(event.get("ticker") or "").strip().upper().split("@")[0]
            for position_id, active_row in active.items():
                trade = active_row["trade"]
                if (
                    str(trade.get("session_id") or "").strip() != session
                    or str(trade.get("ticker") or "").strip().upper().split("@")[0] != ticker
                ):
                    continue
                side = str(trade.get("side") or "").upper()
                price, quote_age, freshness = _fresh_side(event, side, max_age)
                if freshness == "OK":
                    # Preserve captured age and reject a delayed quote that
                    # would move the executable mark backwards in time.
                    effective_stamp = stamp - quote_age
                    previous_mark = marks.get(position_id)
                    if previous_mark is not None and effective_stamp < previous_mark["timestamp"]:
                        mark_out_of_order_events += 1
                        continue
                    marks[position_id] = {
                        "price": price, "timestamp": effective_stamp
                    }
                    mark_updates += 1
                elif freshness == "STALE_EXECUTABLE_SIDE":
                    mark_stale_events += 1
                else:
                    mark_invalid_events += 1
            _snapshot(stamp)
            continue

        trade = payload
        position_id = _position_key(trade)
        if event_kind == "ENTRY":
            if position_id in active or notional > allocation_limit + 1e-9:
                rejected_capacity += 1
                _snapshot(stamp)
                continue
            if reserved + notional > allocation_limit + 1e-9 or cash + 1e-9 < notional:
                rejected_capacity += 1
                _snapshot(stamp)
                continue
            active[position_id] = {"notional": notional, "trade": trade}
            accepted.add(position_id)
            reserved += notional
            cash -= notional
            peak_reserved = max(peak_reserved, reserved)
            peak_open_positions = max(peak_open_positions, len(active))
            marks.pop(position_id, None)
        else:
            active_row = active.pop(position_id, None)
            marks.pop(position_id, None)
            if active_row is not None:
                reserved_amount = active_row["notional"]
                exit_row = next(
                    (row.get("exit") for row in trade.get("scenarios", [])
                     if isinstance(row, Mapping)
                     and str(row.get("name") or "") == scenario_name
                     and isinstance(row.get("exit"), Mapping)),
                    None,
                )
                pnl = _number(exit_row.get("net_pnl_rub")) if isinstance(exit_row, Mapping) else math.nan
                if not math.isfinite(pnl):
                    pnl = 0.0
                cash += reserved_amount + pnl
                reserved -= reserved_amount
                realized_pnls.append(pnl)
                closed += 1
                running_equity += pnl
                peak_realized_equity = max(peak_realized_equity, running_equity)
                realized_max_drawdown = max(
                    realized_max_drawdown, peak_realized_equity - running_equity
                )
        _snapshot(stamp)

    equity_at_cost = cash + reserved
    current_equity = latest_complete_equity if (
        last_snapshot_complete and latest_complete_epoch == last_timeline_epoch
    ) else None
    return {
        "scenario": scenario_name,
        "initial_capital_rub": round(capital, 2),
        "max_allocation_pct": round(allocation_pct, 3),
        "max_allocated_capital_rub": round(allocation_limit, 2),
        "accepted_positions": len(accepted),
        "rejected_for_capacity": rejected_capacity,
        "rejected_invalid_positions": rejected_invalid,
        "closed_positions": closed,
        "open_positions_at_end": len(active),
        "peak_open_positions": peak_open_positions,
        "peak_reserved_capital_rub": round(peak_reserved, 2),
        "peak_allocation_utilization_pct": round(
            peak_reserved / allocation_limit * 100.0, 8
        ) if allocation_limit > 0.0 else None,
        "peak_total_capital_utilization_pct": round(
            peak_reserved / capital * 100.0, 8
        ),
        "ending_reserved_capital_rub": round(reserved, 2),
        "ending_free_cash_rub": round(cash, 4),
        "realized_net_pnl_rub": round(sum(realized_pnls), 4),
        "expectancy_per_closed_position_rub": round(sum(realized_pnls) / closed, 4) if closed else None,
        "portfolio_max_drawdown_rub": round(realized_max_drawdown, 4),
        "ending_equity_at_cost_rub": round(equity_at_cost, 4),
        "mark_to_market": {
            "available": complete_snapshots > 0,
            "ending_equity_rub": round(current_equity, 8) if current_equity is not None else None,
            "ending_unrealized_net_pnl_rub": (
                round(latest_complete_unrealized, 8)
                if current_equity is not None and latest_complete_unrealized is not None else None
            ),
            "ending_reserved_capital_rub": (
                round(latest_complete_reserved, 2)
                if current_equity is not None and latest_complete_reserved is not None else None
            ),
            "ending_marked_open_positions": (
                latest_complete_open_count if current_equity is not None else None
            ),
            "max_drawdown_rub": round(marked_max_drawdown, 4),
            "max_drawdown_pct": round(marked_max_drawdown_pct, 4),
            "drawdown_peak_epoch": marked_drawdown_peak_epoch,
            "drawdown_trough_epoch": marked_drawdown_trough_epoch,
            "complete_equity_snapshots": complete_snapshots,
            "incomplete_equity_snapshots": incomplete_snapshots,
            "mark_coverage_pct": round(
                complete_snapshots / (complete_snapshots + incomplete_snapshots) * 100.0,
                3,
            ) if complete_snapshots + incomplete_snapshots else None,
            "market_events_seen": market_events_seen,
            "valid_bbo_mark_updates": mark_updates,
            "stale_bbo_mark_events": mark_stale_events,
            "invalid_bbo_mark_events": mark_invalid_events,
            "out_of_order_bbo_mark_events": mark_out_of_order_events,
            "max_positions_without_fresh_mark": max_missing_marks,
            "max_positions_with_stale_mark": max_stale_marks,
            "last_complete_snapshot_epoch": latest_complete_epoch,
            "last_timeline_epoch": last_timeline_epoch,
            "ending_snapshot_complete": current_equity is not None,
            "method": "CASH_FUNDED_FULL_NOTIONAL_BBO_MARK_AFTER_ESTIMATED_EXIT_COSTS",
            "limitations": [
                "Only fresh executable-side BBO marks are used: BUY positions at Bid and SELL positions at Ask.",
                "Estimated unrealized PnL deducts entry commission, estimated exit commission and configured adverse exit slippage.",
                "If any open position lacks a fresh mark, that equity snapshot is unavailable; stale prices are never carried forward.",
                "The bounded journal stores top-of-book only; this is not full-depth liquidation value or broker account equity.",
                "Drawdown is calculated only over complete observed snapshots and cannot detect losses during missing market-data intervals.",
            ],
        },
        "capital_model": "FULL_ENTRY_NOTIONAL_RESERVED_FOR_BUY_AND_SELL",
        "limitations": [
            "Full entry notional is reserved for both BUY and SELL; broker margin, leverage and settlement rules are not modeled.",
            "Open positions are valued at entry cost in ending_equity_at_cost_rub; see mark_to_market for fresh-BBO valuation.",
            "Only positions whose entry and replayed exit fit the available event timeline are realized; this is a research capacity model, not an execution guarantee.",
        ],
    }
