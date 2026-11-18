"""Deterministic, research-only exit replay over persisted PAPER market events.

The baseline consumes actual latched EXIT_TRIGGER events when the journal has
them. Alternative stop-management scenarios use executable BBO thresholds and
are explicitly marked as research hypotheses, not production behavior.
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


def _observation_epoch(event: Mapping[str, Any]) -> float:
    """Prefer local receive time for holding-time calculations and entry filtering."""
    received = _epoch(event.get("received_at_utc"))
    return received if math.isfinite(received) else _epoch(event.get("event_time_epoch"))


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
    entry_epoch: float,
    session_metadata: Mapping[str, Any] | None = None,
) -> tuple[list[Mapping[str, Any]], dict[str, int]]:
    rows: list[Mapping[str, Any]] = []
    seen: set[int] = set()
    stats = {
        "session_events": 0,
        "duplicate_sequences": 0,
        "sequence_gaps": 0,
        "missing_sequence_prefix": 0,
        "first_sequence": 0,
        "last_sequence": 0,
        "ticker_events": 0,
        "invalid_events": 0,
        "stale_events": 0,
        "missing_side_events": 0,
        "fresh_executable_observations": 0,
        "missing_event_timestamps": 0,
        "event_time_regressions": 0,
        "pre_entry_events": 0,
    }
    if session_metadata is None:
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
        if seqs:
            stats["first_sequence"] = seqs[0]
            stats["last_sequence"] = seqs[-1]
            stats["missing_sequence_prefix"] = max(0, seqs[0] - 1)
            stats["sequence_gaps"] = stats["missing_sequence_prefix"]
        if len(seqs) > 1:
            stats["sequence_gaps"] += sum(
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
    else:
        for key in (
            "session_events", "duplicate_sequences", "sequence_gaps",
            "missing_sequence_prefix", "first_sequence", "last_sequence",
            "missing_event_timestamps", "event_time_regressions",
        ):
            stats[key] = int(_number(session_metadata.get(key), 0.0))
        for event in events:
            if not isinstance(event, Mapping):
                continue
            seq_raw = _number(event.get("sequence"), math.nan)
            if not math.isfinite(seq_raw) or seq_raw <= 0 or int(seq_raw) != seq_raw:
                continue
            seq = int(seq_raw)
            if seq in seen:
                continue
            seen.add(seq)
            rows.append(event)
        rows.sort(key=lambda item: int(_number(item.get("sequence"), 0)))
    ticker_rows = [
        row for row in rows
        if str(row.get("ticker") or "").strip().upper().split("@")[0] == ticker
    ]
    selected: list[Mapping[str, Any]] = []
    for row in ticker_rows:
        observed_at = _observation_epoch(row)
        if not math.isfinite(observed_at):
            continue
        if observed_at < entry_epoch:
            stats["pre_entry_events"] += 1
            continue
        selected.append(row)
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



def _paired_scenario_comparison(
    trade_results: list[Mapping[str, Any]],
) -> dict[str, Any]:
    """Compare exit hypotheses only on identical position cohorts."""
    scenario_names: list[str] = []
    baseline_name: str | None = None
    by_scenario: dict[str, dict[int, Mapping[str, Any]]] = {}

    for trade in trade_results:
        trade_index = int(_number(trade.get("trade_index"), -1))
        for scenario in trade.get("scenarios", []):
            if not isinstance(scenario, Mapping):
                continue
            name = str(scenario.get("name") or "")
            if not name:
                continue
            if name not in scenario_names:
                scenario_names.append(name)
            by_scenario.setdefault(name, {})[trade_index] = {
                "position_id": str(trade.get("position_id") or ""),
                "ticker": str(trade.get("ticker") or ""),
                "side": str(trade.get("side") or ""),
                "session_id": str(trade.get("session_id") or ""),
                "risk_rub": _number(trade.get("risk_rub"), 0.0),
                "exit": scenario.get("exit") if isinstance(scenario.get("exit"), Mapping) else None,
            }
            if baseline_name is None and str(scenario.get("kind") or "").lower() == "baseline":
                baseline_name = name

    if baseline_name is None:
        return {
            "available": False,
            "reason": "NO_BASELINE_SCENARIO",
            "comparisons_vs_baseline": {},
            "all_scenarios_common_cohort": None,
        }

    baseline_rows = by_scenario.get(baseline_name, {})
    comparisons: dict[str, Any] = {}
    for alternative_name in scenario_names:
        if alternative_name == baseline_name:
            continue
        alternative_rows = by_scenario.get(alternative_name, {})
        shared_positions = sorted(set(baseline_rows) & set(alternative_rows))
        baseline_closed = {
            index: row for index, row in baseline_rows.items() if row.get("exit") is not None
        }
        alternative_closed = {
            index: row for index, row in alternative_rows.items() if row.get("exit") is not None
        }
        paired = sorted(set(baseline_closed) & set(alternative_closed))
        baseline_only = sorted(set(baseline_closed) - set(alternative_closed))
        alternative_only = sorted(set(alternative_closed) - set(baseline_closed))

        baseline_metrics_rows: list[Mapping[str, Any]] = []
        alternative_metrics_rows: list[Mapping[str, Any]] = []
        deltas: list[float] = []
        better = equal = worse = 0
        baseline_wins = alternative_wins = 0
        details: list[dict[str, Any]] = []
        for index in paired:
            base = baseline_closed[index]
            alt = alternative_closed[index]
            base_exit = base["exit"]
            alt_exit = alt["exit"]
            base_pnl = _number(base_exit.get("net_pnl_rub"))
            alt_pnl = _number(alt_exit.get("net_pnl_rub"))
            if not math.isfinite(base_pnl) or not math.isfinite(alt_pnl):
                continue
            delta = alt_pnl - base_pnl
            deltas.append(delta)
            better += int(delta > 1e-9)
            worse += int(delta < -1e-9)
            equal += int(abs(delta) <= 1e-9)
            baseline_wins += int(base_pnl > 0.0)
            alternative_wins += int(alt_pnl > 0.0)
            common = {
                "position_id": base["position_id"],
                "ticker": base["ticker"],
                "side": base["side"],
                "session_id": base["session_id"],
                "risk_rub": base["risk_rub"],
            }
            baseline_metrics_rows.append({**common, "exit": base_exit})
            alternative_metrics_rows.append({**common, "exit": alt_exit})
            if len(details) < 200:
                details.append({
                    **common,
                    "baseline_net_pnl_rub": round(base_pnl, 4),
                    "alternative_net_pnl_rub": round(alt_pnl, 4),
                    "delta_net_pnl_rub": round(delta, 4),
                    "baseline_exit_reason": base_exit.get("reason"),
                    "alternative_exit_reason": alt_exit.get("reason"),
                })

        count = len(deltas)
        comparisons[alternative_name] = {
            "baseline_scenario": baseline_name,
            "alternative_scenario": alternative_name,
            "cohort": {
                "replayed_positions": len(shared_positions),
                "baseline_closed": len(baseline_closed),
                "alternative_closed": len(alternative_closed),
                "paired_closed": count,
                "baseline_closed_alternative_open": len(baseline_only),
                "alternative_closed_baseline_open": len(alternative_only),
                "sample_quality": _sample_quality(count),
            },
            "baseline_on_paired_cohort": _summarize_replay_trades(baseline_metrics_rows),
            "alternative_on_paired_cohort": _summarize_replay_trades(alternative_metrics_rows),
            "paired_difference": {
                "alternative_minus_baseline_net_pnl_rub": round(sum(deltas), 4) if deltas else None,
                "mean_delta_net_pnl_rub": round(sum(deltas) / count, 4) if count else None,
                "median_delta_net_pnl_rub": round(sorted(deltas)[count // 2], 4) if count and count % 2 else (
                    round((sorted(deltas)[count // 2 - 1] + sorted(deltas)[count // 2]) / 2.0, 4)
                    if count else None
                ),
                "alternative_better": better,
                "equal_within_1e_9_rub": equal,
                "alternative_worse": worse,
                "baseline_wins_on_paired_cohort": baseline_wins,
                "alternative_wins_on_paired_cohort": alternative_wins,
                "win_rate_delta_percentage_points": round(
                    (alternative_wins - baseline_wins) / count * 100.0, 3
                ) if count else None,
            },
            "paired_trades": details,
            "note": (
                "Both scenarios are evaluated on exactly the same positions that closed in both. "
                "Scenario-only closures are reported separately and are not assigned zero PnL. "
                "This is trade-level paired replay, not a portfolio simulation."
            ),
        }

    common_indexes: set[int] | None = None
    for name in scenario_names:
        closed_indexes = {
            index for index, row in by_scenario.get(name, {}).items()
            if row.get("exit") is not None
        }
        common_indexes = closed_indexes if common_indexes is None else common_indexes & closed_indexes
    common_indexes = common_indexes or set()
    all_common: dict[str, Any] = {
        "positions_closed_in_every_scenario": len(common_indexes),
        "sample_quality": _sample_quality(len(common_indexes)),
        "summary_by_scenario": {},
    }
    for name in scenario_names:
        rows = []
        for index in sorted(common_indexes):
            source = by_scenario[name][index]
            rows.append({
                "position_id": source["position_id"],
                "ticker": source["ticker"],
                "side": source["side"],
                "session_id": source["session_id"],
                "risk_rub": source["risk_rub"],
                "exit": source["exit"],
            })
        all_common["summary_by_scenario"][name] = _summarize_replay_trades(rows)

    return {
        "available": True,
        "baseline_scenario": baseline_name,
        "comparisons_vs_baseline": comparisons,
        "all_scenarios_common_cohort": all_common,
        "note": (
            "Use paired comparisons as the primary scenario comparison. "
            "The all-scenarios cohort is stricter and may be small; open-at-end outcomes "
            "are not converted to zero-return trades."
        ),
    }


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

def replay_exit_batch(
    events: Iterable[Mapping[str, Any]],
    positions: Iterable[Mapping[str, Any]],
    *,
    max_bbo_age_sec: float = 2.0,
    scenarios: Iterable[Mapping[str, Any]] | None = None,
    portfolio_capital_rub: float | None = None,
    max_allocation_pct: float = 15.0,
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
            "rejected_positions": [],
            "trades": [],
            "summary_by_scenario": {},
            "matched_cohort_comparison": {
                "available": False,
                "reason": "NO_REPLAYED_POSITIONS",
                "comparisons_vs_baseline": {},
                "all_scenarios_common_cohort": None,
            },
        }

    scenario_specs = list(scenarios) if scenarios is not None else None
    trade_results: list[dict[str, Any]] = []
    rejected_positions: list[dict[str, str]] = []
    grouped: dict[str, list[dict[str, Any]]] = {}

    # Index once by session and ticker; precompute session-wide sequence diagnostics.
    events_by_session_ticker: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    session_rows: dict[str, list[Mapping[str, Any]]] = {}
    for event in event_rows:
        if not isinstance(event, Mapping):
            continue
        event_session = str(event.get("session_id") or "").strip()
        if not event_session:
            continue
        session_rows.setdefault(event_session, []).append(event)

    session_metadata: dict[str, dict[str, int]] = {}
    for event_session, rows in session_rows.items():
        sequence_counts: dict[int, int] = {}
        valid_rows: list[Mapping[str, Any]] = []
        for event in rows:
            sequence = _number(event.get("sequence"), math.nan)
            if not math.isfinite(sequence) or sequence <= 0 or int(sequence) != sequence:
                continue
            sequence_int = int(sequence)
            sequence_counts[sequence_int] = sequence_counts.get(sequence_int, 0) + 1
            if sequence_counts[sequence_int] == 1:
                valid_rows.append(event)
        valid_rows.sort(key=lambda item: int(_number(item.get("sequence"), 0)))
        for row in valid_rows:
            event_ticker = str(row.get("ticker") or "").strip().upper().split("@")[0]
            if event_ticker:
                events_by_session_ticker.setdefault(
                    (event_session, event_ticker), []
                ).append(row)
        sequences = [int(_number(row.get("sequence"), 0)) for row in valid_rows]
        gaps = max(0, sequences[0] - 1) if sequences else 0
        if len(sequences) > 1:
            gaps += sum(
                max(0, right - left - 1)
                for left, right in zip(sequences, sequences[1:])
            )
        previous_epoch_by_ticker: dict[str, float] = {}
        missing_timestamps = regressions = 0
        for row in valid_rows:
            stamp = _epoch(row.get("event_time_epoch"))
            if not math.isfinite(stamp):
                missing_timestamps += 1
                continue
            symbol = str(row.get("ticker") or "").strip().upper().split("@")[0]
            previous_epoch = previous_epoch_by_ticker.get(symbol, math.nan)
            if math.isfinite(previous_epoch) and stamp < previous_epoch:
                regressions += 1
            previous_epoch_by_ticker[symbol] = stamp
        session_metadata[event_session] = {
            "session_events": len(rows),
            "duplicate_sequences": sum(count - 1 for count in sequence_counts.values()),
            "sequence_gaps": gaps,
            "missing_sequence_prefix": max(0, sequences[0] - 1) if sequences else 0,
            "first_sequence": sequences[0] if sequences else 0,
            "last_sequence": sequences[-1] if sequences else 0,
            "missing_event_timestamps": missing_timestamps,
            "event_time_regressions": regressions,
        }

    for index, position in enumerate(position_rows):
        if not isinstance(position, Mapping):
            continue
        position_session = str(
            position.get("bbo_replay_session_id") or position.get("session_id") or ""
        ).strip()
        position_ticker = str(position.get("ticker") or "").strip().upper().split("@")[0]
        relevant_events = events_by_session_ticker.get(
            (position_session, position_ticker), []
        )
        try:
            result = replay_exit_scenarios(
                relevant_events,
                position,
                max_bbo_age_sec=max_bbo_age_sec,
                scenarios=scenario_specs,
                _session_metadata=session_metadata.get(position_session),
            )
        except ValueError as exc:
            rejected_positions.append({
                "position_id": str(position.get("id") or position.get("position_id") or index),
                "reason": str(exc),
            })
            continue
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

    result = {
        "schema_version": 1,
        "engine": "PAPER_EXIT_REPLAY_V1",
        "positions": len(trade_results),
        "rejected_positions": rejected_positions,
        "trades": trade_results,
        "summary_by_scenario": summaries,
        "matched_cohort_comparison": _paired_scenario_comparison(trade_results),
        "limitations": [
            "BASELINE uses actual latched EXIT_TRIGGER events when the journal contains them; legacy sessions without trigger events use a BBO-threshold proxy.",
            "Open positions at the end of the replay are excluded from realized scenario PnL.",
            "Portfolio capacity is modeled only when portfolio_capital_rub is explicitly supplied.",
        ],
    }
    if portfolio_capital_rub is not None:
        result["portfolio_capacity"] = {
            name: _portfolio_capacity_simulation(
                trade_results, name, portfolio_capital_rub, max_allocation_pct,
                events=event_rows, max_bbo_age_sec=max_bbo_age_sec,
            )
            for name in summaries
        }
    return result
