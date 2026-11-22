"""Shared numeric, timestamp, BBO and event-preparation primitives for replay."""
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
