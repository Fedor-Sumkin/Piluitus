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
            # Legacy PAPER entry_timestamp values were serialized from naive
            # datetime.now(); datetime.timestamp() preserves the host-local zone.
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
        "missing_event_timestamps": 0,
        "event_time_regressions": 0,
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
    previous_epoch_by_ticker: dict[str, float] = {}
    for row in rows:
        stamp = _epoch(row.get("event_time_epoch"))
        if not math.isfinite(stamp):
            stats["missing_event_timestamps"] += 1
            continue
        symbol = str(row.get("ticker") or "").strip().upper().split("@")[0]
        previous_epoch = previous_epoch_by_ticker.get(symbol, math.nan)
        if math.isfinite(previous_epoch) and stamp < previous_epoch:
            stats["event_time_regressions"] += 1
        previous_epoch_by_ticker[symbol] = stamp
    selected = [
        row for row in rows
        if str(row.get("ticker") or "").strip().upper().split("@")[0] == ticker
    ]
    stats["ticker_events"] = len(selected)
    stats["ticker_bbo_events"] = sum(
        1 for row in selected if str(row.get("event_kind") or "").upper() != "EXIT_TRIGGER"
    )
    stats["ticker_exit_trigger_events"] = sum(
        1 for row in selected if str(row.get("event_kind") or "").upper() == "EXIT_TRIGGER"
    )
    return selected, stats


def replay_exit_scenarios(
    events: Iterable[Mapping[str, Any]],
    position: Mapping[str, Any],
    *,
    max_bbo_age_sec: float = 2.0,
    scenarios: Iterable[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Replay BBO-only exit hypotheses; input order is normalized by session sequence.

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
    rows, coverage = _prepare_events(events, session_id=session_id, ticker=ticker)
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
            "event_time_epoch": _epoch(row.get("event_time_epoch")),
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
                        stamp = _epoch(pending.get("event_time_epoch"))
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
            received_epoch = _epoch(row.get("event_time_epoch"))
            if not math.isfinite(received_epoch):
                continue
            elapsed = max(state["last_elapsed_sec"], received_epoch - entry_epoch, 0.0)
            state["last_elapsed_sec"] = elapsed
            state["fresh_observations"] += 1
            state["last_fresh_price"] = price

            if kind == "baseline" and state["pending_trigger"] is not None:
                pending = state["pending_trigger"]
                state["exit"] = _make_exit(
                    str(pending.get("trigger_reason") or "STOP_LOSS").upper(),
                    _number(pending.get("trigger_price"), stop),
                    price, row, age, elapsed,
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
        "position_id": str(position.get("position_id") or position.get("id") or ""),
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


def _sample_quality(sample_size: int) -> str:
    if sample_size < 30:
        return "INSUFFICIENT_LT_30"
    if sample_size < 100:
        return "PRELIMINARY_30_99"
    if sample_size < 300:
        return "DEVELOPING_100_299"
    return "LARGE_300_PLUS"


def _summarize_replay_trades(rows: list[Mapping[str, Any]]) -> dict[str, Any]:
    closed = [row for row in rows if isinstance(row.get("exit"), Mapping)]
    closed.sort(key=lambda row: (
        _number(row["exit"].get("event_time_epoch"), 0.0),
        str(row.get("position_id") or ""),
    ))
    pnls = [_number(row["exit"].get("net_pnl_rub"), 0.0) for row in closed]
    wins = [value for value in pnls if value > 0.0]
    losses = [value for value in pnls if value < 0.0]
    risk_values = [
        _number(row.get("risk_rub"), math.nan) for row in closed
    ]
    r_values = [
        pnl / risk for pnl, risk in zip(pnls, risk_values)
        if math.isfinite(risk) and risk > 0.0
    ]
    cumulative = 0.0
    peak = 0.0
    max_drawdown = 0.0
    for pnl in pnls:
        cumulative += pnl
        peak = max(peak, cumulative)
        max_drawdown = max(max_drawdown, peak - cumulative)
    return {
        "trades": len(rows),
        "closed": len(closed),
        "open_at_end": len(rows) - len(closed),
        "sample_quality": _sample_quality(len(closed)),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate_pct": round(len(wins) / len(closed) * 100.0, 3) if closed else None,
        "net_pnl_rub": round(sum(pnls), 4) if closed else None,
        "expectancy_rub": round(sum(pnls) / len(pnls), 4) if pnls else None,
        "average_win_rub": round(sum(wins) / len(wins), 4) if wins else None,
        "average_loss_rub": round(sum(losses) / len(losses), 4) if losses else None,
        "profit_factor": (
            round(sum(wins) / abs(sum(losses)), 4)
            if losses else None
        ),
        "expectancy_r": round(sum(r_values) / len(r_values), 4) if r_values else None,
        "max_drawdown_rub": round(max_drawdown, 4) if closed else None,
        "drawdown_basis": "SEQUENTIAL_CLOSED_TRADE_PNL_NOT_PORTFOLIO_DRAWDOWN",
        "average_holding_sec": round(sum(
            _number(row["exit"].get("elapsed_sec"), 0.0) for row in closed
        ) / len(closed), 3) if closed else None,
    }


def replay_exit_batch(
    events: Iterable[Mapping[str, Any]],
    positions: Iterable[Mapping[str, Any]],
    *,
    max_bbo_age_sec: float = 2.0,
    scenarios: Iterable[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    """Replay many PAPER positions and summarize each exit hypothesis.

    This is an offline research helper. It does not read Memurai or alter positions.
    """
    event_rows = list(events)
    position_rows = list(positions)
    if not position_rows:
        return {
            "schema_version": 1,
            "engine": "PAPER_EXIT_REPLAY_V1",
            "positions": 0,
            "trades": [],
            "summary_by_scenario": {},
        }

    scenario_specs = list(scenarios) if scenarios is not None else None
    trade_results: list[dict[str, Any]] = []
    grouped: dict[str, list[dict[str, Any]]] = {}
    for index, position in enumerate(position_rows):
        if not isinstance(position, Mapping):
            continue
        result = replay_exit_scenarios(
            event_rows,
            position,
            max_bbo_age_sec=max_bbo_age_sec,
            scenarios=scenario_specs,
        )
        risk_rub = result["risk_per_unit"] * result["quantity"]
        result["risk_rub"] = round(risk_rub, 4)
        result["trade_index"] = index
        trade_results.append(result)
        for scenario in result["scenarios"]:
            grouped.setdefault(str(scenario["name"]), []).append({
                "position_id": result["position_id"],
                "ticker": result["ticker"],
                "side": result["side"],
                "session_id": result["session_id"],
                "risk_rub": result["risk_rub"],
                "exit": scenario["exit"],
            })

    summaries: dict[str, Any] = {}
    for name, rows in grouped.items():
        summaries[name] = {
            "overall": _summarize_replay_trades(rows),
            "by_ticker": {},
            "by_side": {},
            "by_session": {},
        }
        for field, target in (
            ("ticker", "by_ticker"),
            ("side", "by_side"),
            ("session_id", "by_session"),
        ):
            buckets: dict[str, list[Mapping[str, Any]]] = {}
            for row in rows:
                key = str(row.get(field) or "UNKNOWN")
                buckets.setdefault(key, []).append(row)
            summaries[name][target] = {
                key: _summarize_replay_trades(bucket)
                for key, bucket in sorted(buckets.items())
            }

    return {
        "schema_version": 1,
        "engine": "PAPER_EXIT_REPLAY_V1",
        "positions": len(trade_results),
        "trades": trade_results,
        "summary_by_scenario": summaries,
        "limitations": [
            "BASELINE uses actual latched EXIT_TRIGGER events when the journal contains them; legacy sessions without trigger events use a BBO-threshold proxy.",
            "Open positions at the end of the replay are excluded from realized scenario PnL.",
            "Portfolio-level overlap, capital contention and simultaneous execution are not modeled.",
        ],
    }
