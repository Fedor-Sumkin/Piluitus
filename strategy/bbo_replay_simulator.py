"""Deterministic, research-only exit replay over persisted executable BBO events.

This is a BBO-trigger proxy, not a full reproduction of the production monitor:
the production monitor can latch SL/TP from INSTRUMENT_TRADES before a fresh
executable BBO becomes available. The BBO journal does not contain those
trade-trigger events, so results are explicitly labelled as proxy simulations.
"""
from __future__ import annotations

import math
from datetime import datetime
from typing import Any, Iterable, Mapping


DEFAULT_SCENARIOS: tuple[dict[str, Any], ...] = (
    {"name": "BASELINE_SL_TP", "kind": "baseline"},
    {"name": "TIME_STOP_180S", "kind": "time_stop", "time_stop_sec": 180.0},
    {"name": "TIME_STOP_300S", "kind": "time_stop", "time_stop_sec": 300.0},
    {"name": "BREAK_EVEN_1R", "kind": "break_even", "arm_r": 1.0},
    {
        "name": "TRAILING_1R_ARM_0_75R_GAP",
        "kind": "trailing",
        "arm_r": 1.0,
        "gap_r": 0.75,
    },
)


def _number(value: object, default: float = math.nan) -> float:
    if isinstance(value, bool):
        return default
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return result if math.isfinite(result) else default


def _epoch(value: object) -> float:
    number = _number(value)
    if math.isfinite(number):
        return number
    if isinstance(value, str) and value.strip():
        try:
            parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                return math.nan
            return parsed.timestamp()
        except (TypeError, ValueError, OverflowError):
            return math.nan
    return math.nan


def _fresh_side(event: Mapping[str, Any], side: str, max_age_sec: float) -> tuple[float, float, str]:
    if not bool(event.get("bbo_valid", False)):
        return 0.0, math.inf, "INVALID_BBO"
    bid = _number(event.get("bid"), 0.0)
    ask = _number(event.get("ask"), 0.0)
    if bid > 0.0 and ask > 0.0 and ask < bid:
        return 0.0, math.inf, "CROSSED_BBO"
    if side == "BUY":
        price = bid
        age = _number(event.get("bid_age_sec_at_capture"))
        confirmed = _number(event.get("bid_confirmed_monotonic"))
    else:
        price = ask
        age = _number(event.get("ask_age_sec_at_capture"))
        confirmed = _number(event.get("ask_confirmed_monotonic"))
    if not math.isfinite(age):
        received = _number(event.get("received_monotonic"))
        age = received - confirmed if math.isfinite(received) and math.isfinite(confirmed) else math.inf
    if not math.isfinite(price) or price <= 0.0:
        return 0.0, age, "MISSING_EXECUTABLE_SIDE"
    if not math.isfinite(age) or age < 0.0:
        return 0.0, age, "INVALID_SIDE_AGE"
    if age > max_age_sec:
        return 0.0, age, "STALE_EXECUTABLE_SIDE"
    return price, age, "OK"


def _adverse_exit_fill(raw_price: float, position_side: str, slippage_bps: float) -> float:
    slip = max(0.0, slippage_bps) / 10_000.0
    return raw_price * (1.0 - slip if position_side == "BUY" else 1.0 + slip)


def _net_pnl(
    entry_price: float,
    exit_fill: float,
    quantity: float,
    position_side: str,
    entry_fee: float,
    commission_pct_per_side: float,
) -> tuple[float, float, float]:
    sign = 1.0 if position_side == "BUY" else -1.0
    gross = (exit_fill - entry_price) * quantity * sign
    exit_fee = exit_fill * quantity * commission_pct_per_side / 100.0
    return gross - entry_fee - exit_fee, gross, entry_fee + exit_fee


def _break_even_trigger(
    entry_price: float,
    position_side: str,
    quantity: float,
    entry_fee: float,
    commission_pct_per_side: float,
    slippage_bps: float,
) -> float:
    fee_rate = commission_pct_per_side / 100.0
    slip_rate = slippage_bps / 10_000.0
    entry_fee_per_share = entry_fee / quantity
    if position_side == "BUY":
        if fee_rate >= 1.0 or slip_rate >= 1.0:
            return math.nan
        return ((entry_price + entry_fee_per_share) / (1.0 - fee_rate)) / (1.0 - slip_rate)
    raw = ((entry_price - entry_fee_per_share) / (1.0 + fee_rate)) / (1.0 + slip_rate)
    return raw if raw > 0.0 else math.nan


def _prepare_events(
    events: Iterable[Mapping[str, Any]],
    *,
    session_id: str,
    ticker: str,
) -> tuple[list[Mapping[str, Any]], dict[str, int]]:
    rows: list[Mapping[str, Any]] = []
    seen: set[int] = set()
    stats = {
        "session_events": 0,
        "duplicate_sequences": 0,
        "sequence_gaps": 0,
        "ticker_events": 0,
        "invalid_events": 0,
        "stale_events": 0,
        "missing_side_events": 0,
        "fresh_executable_observations": 0,
    }
    for event in events:
        if not isinstance(event, Mapping) or str(event.get("session_id") or "") != session_id:
            continue
        stats["session_events"] += 1
        seq_raw = _number(event.get("sequence"), math.nan)
        if not math.isfinite(seq_raw) or seq_raw <= 0 or int(seq_raw) != seq_raw:
            continue
        seq = int(seq_raw)
        if seq in seen:
            stats["duplicate_sequences"] += 1
            continue
        seen.add(seq)
        rows.append(event)
    rows.sort(key=lambda item: int(_number(item.get("sequence"), 0)))
    seqs = [int(_number(row.get("sequence"), 0)) for row in rows]
    if len(seqs) > 1:
        stats["sequence_gaps"] = sum(
            max(0, right - left - 1) for left, right in zip(seqs, seqs[1:])
        )
    selected = [
        row for row in rows
        if str(row.get("ticker") or "").strip().upper().split("@")[0] == ticker
    ]
    stats["ticker_events"] = len(selected)
    return selected, stats


def replay_exit_scenarios(
    events: Iterable[Mapping[str, Any]],
    position: Mapping[str, Any],
    *,
    max_bbo_age_sec: float = 2.0,
    scenarios: Iterable[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Replay BBO-only exit hypotheses; input order is normalized by session sequence.

    Required position fields:
      session_id, ticker, side, entry_price, entry_time_epoch, quantity,
      stop_loss, take_profit, commission_pct_per_side, slippage_bps.
    entry_commission_rub is optional and is derived from entry notional when absent.
    """
    session_id = str(position.get("session_id") or "").strip()
    ticker = str(position.get("ticker") or "").strip().upper().split("@")[0]
    side = str(position.get("side") or "").strip().upper()
    entry = _number(position.get("entry_price"))
    entry_epoch = _epoch(position.get("entry_time_epoch"))
    quantity = _number(position.get("quantity"))
    stop = _number(position.get("stop_loss"))
    target = _number(position.get("take_profit"))
    commission = _number(position.get("commission_pct_per_side"))
    slippage = _number(position.get("slippage_bps"))
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
    rows, coverage = _prepare_events(events, session_id=session_id, ticker=ticker)
    # Coverage describes the input stream once, not once per strategy.
    for row in rows:
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

    for spec in strategy_specs:
        name = str(spec.get("name") or spec.get("kind") or "UNNAMED")
        kind = str(spec.get("kind") or "baseline").lower()
        state = {
            "armed": False,
            "extreme": entry,
            "dynamic_stop": stop,
            "exit": None,
            "last_fresh_price": None,
            "last_elapsed_sec": 0.0,
            "fresh_observations": 0,
            "stale_observations": 0,
            "invalid_observations": 0,
        }
        for row in rows:
            price, age, freshness = _fresh_side(row, side, max_age)
            if freshness != "OK":
                if freshness == "STALE_EXECUTABLE_SIDE":
                    state["stale_observations"] += 1
                else:
                    state["invalid_observations"] += 1

                continue
            received_epoch = _epoch(row.get("event_time_epoch"))
            if not math.isfinite(received_epoch):
                continue
            elapsed = max(state["last_elapsed_sec"], received_epoch - entry_epoch, 0.0)
            state["last_elapsed_sec"] = elapsed
            state["fresh_observations"] += 1
            state["last_fresh_price"] = price

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
                elif price >= target:
                    reason, trigger = "TAKE_PROFIT", target
            else:
                if price >= active_stop:
                    reason, trigger = ("TRAILING_STOP" if state["armed"] and kind == "trailing" else
                                       "BREAK_EVEN" if state["armed"] and kind == "break_even" and active_stop < stop else
                                       "STOP_LOSS"), active_stop
                elif price <= target:
                    reason, trigger = "TAKE_PROFIT", target

            if not reason and kind == "time_stop":
                timeout = max(1.0, _number(spec.get("time_stop_sec"), 180.0))
                if elapsed >= timeout:
                    reason, trigger = "TIME_STOP", price
            if reason:
                fill = _adverse_exit_fill(price, side, slippage)
                net, gross, fees = _net_pnl(
                    entry, fill, quantity, side, entry_fee, commission
                )
                state["exit"] = {
                    "reason": reason,
                    "trigger_price": round(float(trigger), 8),
                    "raw_executable_price": round(price, 8),
                    "fill_price_after_slippage": round(fill, 8),
                    "event_sequence": int(_number(row.get("sequence"), 0)),
                    "event_time_epoch": received_epoch,
                    "elapsed_sec": round(elapsed, 3),
                    "bbo_age_sec": round(age, 6),
                    "gross_pnl_rub": round(gross, 4),
                    "total_commission_rub": round(fees, 4),
                    "net_pnl_rub": round(net, 4),
                    "net_return_pct_on_entry_notional": round(
                        net / (entry * quantity) * 100.0, 6
                    ),
                }
                break

        exit_row = state["exit"]
        outputs.append({
            "name": name,
            "kind": kind,
            "status": "CLOSED" if exit_row else "OPEN_AT_END",
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
        "engine": "BBO_ONLY_EXIT_PROXY",
        "session_id": session_id,
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
            "BBO_ONLY_PROXY: production SL/TP can first trigger on INSTRUMENT_TRADES; trade-trigger events are absent from this journal.",
            "Time-stop exits occur on the first fresh executable BBO observation at or after the timeout.",
            "No queue position, passive fills, full-depth impact or exchange latency is simulated.",
            "Events use the recorded event-time clock; missing or unreliable exchange timestamps require separate quality review.",
        ],
        "scenarios": outputs,
    }
