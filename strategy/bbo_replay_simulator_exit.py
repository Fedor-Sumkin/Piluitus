"""Single-position deterministic PAPER exit scenario replay."""
from __future__ import annotations

import math
from typing import Any, Iterable, Mapping

from .bbo_replay_simulator_primitives import (
    DEFAULT_SCENARIOS,
    _adverse_exit_fill,
    _break_even_trigger,
    _fresh_side,
    _net_pnl,
    _number,
    _observation_epoch,
    _prepare_events,
    _epoch,
)

def replay_exit_scenarios(
    events: Iterable[Mapping[str, Any]],
    position: Mapping[str, Any],
    *,
    max_bbo_age_sec: float = 2.0,
    scenarios: Iterable[Mapping[str, Any]] | None = None,
    _session_metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Replay production baseline and BBO-based alternatives in session sequence.

    Accepts the persisted PAPER trade shape: bbo_replay_session_id (preferred),
    ticker, side, entry_price, entry_timestamp, volume, stop_loss, take_profit,
    commission_pct_per_side and paper_slippage_bps. The normalized aliases
    session_id, entry_time_epoch, quantity and slippage_bps are also accepted.
    entry_commission_rub is optional and is derived from entry notional when absent.
    """
    session_id = str(
        position.get("bbo_replay_session_id") or position.get("session_id") or ""
    ).strip()
    ticker = str(position.get("ticker") or "").strip().upper().split("@")[0]
    side = str(position.get("side") or "").strip().upper()
    entry = _number(position.get("entry_price"))
    entry_epoch = _epoch(position.get("entry_time_epoch") or position.get("entry_timestamp"))
    quantity = _number(position.get("quantity", position.get("volume")))
    stop = _number(position.get("stop_loss"))
    target = _number(position.get("take_profit"))
    commission = _number(position.get("commission_pct_per_side"))
    slippage_raw = position.get("slippage_bps")
    if slippage_raw is None:
        slippage_raw = position.get("paper_slippage_bps")
    slippage = _number(slippage_raw)
    max_age = _number(max_bbo_age_sec)

    if not session_id or not ticker or side not in {"BUY", "SELL"}:
        raise ValueError("position requires session_id, ticker and side BUY/SELL")
    if not all(math.isfinite(value) for value in (
        entry, entry_epoch, quantity, stop, target, commission, slippage, max_age
    )):
        raise ValueError("position is missing finite entry, risk, cost or freshness values")
    if entry <= 0.0 or quantity <= 0.0 or commission < 0.0 or slippage < 0.0 or max_age <= 0.0:
        raise ValueError("entry, quantity and BBO TTL must be positive; costs cannot be negative")
    if side == "BUY" and not stop < entry < target:
        raise ValueError("BUY position must satisfy stop_loss < entry_price < take_profit")
    if side == "SELL" and not target < entry < stop:
        raise ValueError("SELL position must satisfy take_profit < entry_price < stop_loss")

    entry_fee = _number(position.get("entry_commission_rub"))
    if not math.isfinite(entry_fee):
        entry_fee = entry * quantity * commission / 100.0
    risk_per_unit = abs(entry - stop)
    position_id = str(position.get("id") or position.get("position_id") or "").strip()
    rows, coverage = _prepare_events(
        events, session_id=session_id, ticker=ticker, entry_epoch=entry_epoch,
        session_metadata=_session_metadata,
    )
    all_trigger_rows = [
        row for row in rows
        if str(row.get("event_kind") or "").upper() == "EXIT_TRIGGER"
    ]
    trigger_rows = [
        row for row in all_trigger_rows
        if position_id and str(row.get("trade_id") or "").strip() == position_id
    ]
    coverage["unmatched_exit_trigger_events"] = len(all_trigger_rows) - len(trigger_rows)
    coverage["ticker_exit_trigger_events"] = len(trigger_rows)
    rows = [
        row for row in rows
        if str(row.get("event_kind") or "").upper() != "EXIT_TRIGGER"
    ] + trigger_rows
    rows.sort(key=lambda item: int(_number(item.get("sequence"), 0)))
    # Coverage describes BBO observations once, not once per strategy.
    bbo_rows = [
        row for row in rows
        if str(row.get("event_kind") or "").upper() != "EXIT_TRIGGER"
    ]
    trigger_rows = [
        row for row in rows
        if str(row.get("event_kind") or "").upper() == "EXIT_TRIGGER"
    ]
    for row in bbo_rows:
        _price, _age, freshness = _fresh_side(row, side, max_age)
        if freshness == "OK":
            coverage["fresh_executable_observations"] += 1
        elif freshness == "STALE_EXECUTABLE_SIDE":
            coverage["stale_events"] += 1
        elif freshness in {"INVALID_BBO", "CROSSED_BBO"}:
            coverage["invalid_events"] += 1
        else:
            coverage["missing_side_events"] += 1

    strategy_specs = list(scenarios) if scenarios is not None else list(DEFAULT_SCENARIOS)
    outputs: list[dict[str, Any]] = []

    def _make_exit(
        reason: str, trigger_price: float, raw_price: float,
        row: Mapping[str, Any], age: float, elapsed: float,
    ) -> dict[str, Any]:
        fill = _adverse_exit_fill(raw_price, side, slippage)
        net, gross, fees = _net_pnl(
            entry, fill, quantity, side, entry_fee, commission
        )
        return {
            "reason": reason,
            "trigger_price": round(float(trigger_price), 8),
            "raw_executable_price": round(raw_price, 8),
            "fill_price_after_slippage": round(fill, 8),
            "event_sequence": int(_number(row.get("sequence"), 0)),
            "trigger_event_sequence": int(_number(
                row.get("trigger_event_sequence", row.get("sequence")), 0
            )),
            "trigger_source": row.get("trigger_source"),
            "trigger_market_time_epoch": _number(
                row.get("trigger_market_time_epoch"), math.nan
            ) if math.isfinite(_number(row.get("trigger_market_time_epoch"))) else None,
            "event_time_epoch": _observation_epoch(row),
            "market_event_time_epoch": _epoch(row.get("event_time_epoch")),
            "elapsed_sec": round(elapsed, 3),
            "bbo_age_sec": round(age, 6),
            "gross_pnl_rub": round(gross, 4),
            "total_commission_rub": round(fees, 4),
            "net_pnl_rub": round(net, 4),
            "net_return_pct_on_entry_notional": round(
                net / (entry * quantity) * 100.0, 6
            ),
        }

    for spec in strategy_specs:
        name = str(spec.get("name") or spec.get("kind") or "UNNAMED")
        kind = str(spec.get("kind") or "baseline").lower()
        state = {
            "armed": False,
            "extreme": entry,
            "dynamic_stop": stop,
            "exit": None,
            "pending_trigger": None,
            "exit_trigger_mode": None,
            "last_fresh_price": None,
            "last_elapsed_sec": 0.0,
            "fresh_observations": 0,
            "stale_observations": 0,
            "invalid_observations": 0,
        }
        for row in rows:
            event_kind = str(row.get("event_kind") or "").upper()
            if event_kind == "EXIT_TRIGGER":
                if kind == "baseline" and state["exit"] is None:
                    state["pending_trigger"] = state["pending_trigger"] or row
                    pending = state["pending_trigger"]
                    immediate_price = _number(pending.get("trigger_bbo_price"))
                    immediate_age = _number(pending.get("trigger_bbo_age_sec"))
                    immediate_ttl = min(
                        max_age,
                        _number(pending.get("trigger_bbo_max_age_sec"), max_age),
                    )
                    if (
                        bool(pending.get("trigger_bbo_valid"))
                        and math.isfinite(immediate_price) and immediate_price > 0.0
                        and math.isfinite(immediate_age) and 0.0 <= immediate_age <= immediate_ttl
                    ):
                        stamp = _observation_epoch(pending)
                        elapsed = max(state["last_elapsed_sec"], stamp - entry_epoch, 0.0)
                        state["last_elapsed_sec"] = elapsed
                        state["exit"] = _make_exit(
                            str(pending.get("trigger_reason") or "STOP_LOSS").upper(),
                            _number(pending.get("trigger_price"), stop),
                            immediate_price, pending, immediate_age, elapsed,
                        )
                        state["exit_trigger_mode"] = "ACTUAL_LATCHED_TRIGGER"
                continue

            price, age, freshness = _fresh_side(row, side, max_age)
            if freshness != "OK":
                if freshness == "STALE_EXECUTABLE_SIDE":
                    state["stale_observations"] += 1
                else:
                    state["invalid_observations"] += 1

                continue
            received_epoch = _observation_epoch(row)
            if not math.isfinite(received_epoch):
                continue
            elapsed = max(state["last_elapsed_sec"], received_epoch - entry_epoch, 0.0)
            state["last_elapsed_sec"] = elapsed
            state["fresh_observations"] += 1
            state["last_fresh_price"] = price

            if kind == "baseline" and state["pending_trigger"] is not None:
                pending = state["pending_trigger"]
                fill_context = dict(row)
                fill_context["trigger_event_sequence"] = pending.get("sequence")
                fill_context["trigger_source"] = pending.get("trigger_source")
                fill_context["trigger_market_time_epoch"] = pending.get("trigger_market_time_epoch")
                state["exit"] = _make_exit(
                    str(pending.get("trigger_reason") or "STOP_LOSS").upper(),
                    _number(pending.get("trigger_price"), stop),
                    price, fill_context, age, elapsed,
                )
                state["exit_trigger_mode"] = "ACTUAL_LATCHED_TRIGGER"
                break

            sign = 1.0 if side == "BUY" else -1.0
            favorable_r = ((price - entry) * sign) / risk_per_unit
            active_stop = stop
            if kind == "break_even":
                arm_r = max(0.1, _number(spec.get("arm_r"), 1.0))
                if not state["armed"] and favorable_r >= arm_r:
                    state["armed"] = True
                if state["armed"]:
                    active_stop = max(stop, _break_even_trigger(
                        entry, side, quantity, entry_fee, commission, slippage
                    )) if side == "BUY" else min(stop, _break_even_trigger(
                        entry, side, quantity, entry_fee, commission, slippage
                    ))
            elif kind == "trailing":
                arm_r = max(0.1, _number(spec.get("arm_r"), 1.0))
                gap_r = max(0.1, _number(spec.get("gap_r"), 0.75))
                if side == "BUY":
                    state["extreme"] = max(state["extreme"], price)
                else:
                    state["extreme"] = min(state["extreme"], price)
                if not state["armed"] and favorable_r >= arm_r:
                    state["armed"] = True
                if state["armed"]:
                    candidate_stop = (
                        state["extreme"] - gap_r * risk_per_unit
                        if side == "BUY"
                        else state["extreme"] + gap_r * risk_per_unit
                    )
                    active_stop = max(stop, candidate_stop) if side == "BUY" else min(stop, candidate_stop)

            reason = ""
            trigger = None
            if side == "BUY":
                if price <= active_stop:
                    reason, trigger = ("TRAILING_STOP" if state["armed"] and kind == "trailing" else
                                       "BREAK_EVEN" if state["armed"] and kind == "break_even" and active_stop > stop else
                                       "STOP_LOSS"), active_stop
                elif kind != "trailing" and price >= target:
                    reason, trigger = "TAKE_PROFIT", target
            else:
                if price >= active_stop:
                    reason, trigger = ("TRAILING_STOP" if state["armed"] and kind == "trailing" else
                                       "BREAK_EVEN" if state["armed"] and kind == "break_even" and active_stop < stop else
                                       "STOP_LOSS"), active_stop
                elif kind != "trailing" and price <= target:
                    reason, trigger = "TAKE_PROFIT", target

            if not reason and kind == "time_stop":
                timeout = max(1.0, _number(spec.get("time_stop_sec"), 180.0))
                if elapsed >= timeout:
                    reason, trigger = "TIME_STOP", price
            if reason:
                state["exit"] = _make_exit(
                    reason, float(trigger), price, row, age, elapsed
                )
                state["exit_trigger_mode"] = "BBO_THRESHOLD_PROXY"
                break

        exit_row = state["exit"]
        outputs.append({
            "name": name,
            "kind": kind,
            "status": "CLOSED" if exit_row else "OPEN_AT_END",
            "trigger_mode": (
                state["exit_trigger_mode"]
                or (
                    "ACTUAL_TRIGGER_PENDING" if kind == "baseline" and state["pending_trigger"] is not None
                    else "BBO_THRESHOLD_PROXY" if kind == "baseline"
                    else "BBO_THRESHOLD_RESEARCH"
                )
            ),
            "pending_trigger": (
                {
                    "reason": state["pending_trigger"].get("trigger_reason"),
                    "trigger_price": state["pending_trigger"].get("trigger_price"),
                    "trigger_sequence": state["pending_trigger"].get("sequence"),
                }
                if state["pending_trigger"] is not None and exit_row is None else None
            ),
            "exit": exit_row,
            "armed": bool(state["armed"]),
            "last_fresh_price": state["last_fresh_price"],
            "last_elapsed_sec": round(state["last_elapsed_sec"], 3),
            "fresh_observations": state["fresh_observations"],
            "stale_observations": state["stale_observations"],
            "invalid_observations": state["invalid_observations"],
        })

    return {
        "schema_version": 1,
        "engine": "PAPER_EXIT_REPLAY_V1",
        "session_id": session_id,
        "position_id": str(position.get("position_id") or position_id),
        "ticker": ticker,
        "side": side,
        "entry_price": entry,
        "entry_time_epoch": entry_epoch,
        "quantity": quantity,
        "risk_per_unit": risk_per_unit,
        "max_bbo_age_sec": max_age,
        "cost_assumptions": {
            "commission_pct_per_side": commission,
            "slippage_bps": slippage,
            "entry_commission_rub": round(entry_fee, 4),
            "assumptions_explicit": True,
        },
        "coverage": coverage,
        "limitations": [
            "BBO_ONLY_PROXY applies to alternative BBO-threshold scenarios. BASELINE uses journaled production EXIT_TRIGGER events when available; legacy sessions without them fall back to BBO threshold proxy.",
            "Time-stop exits occur on the first fresh executable BBO observation at or after the timeout.",
            "No queue position, passive fills, full-depth impact or exchange latency is simulated.",
            "Events use the recorded event-time clock; missing or unreliable exchange timestamps require separate quality review.",
        ],
        "scenarios": outputs,
    }
