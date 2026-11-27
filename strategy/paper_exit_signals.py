"""Evidence-based PAPER position exits from fresh aggressor-flow observations.

The module is deliberately pure: it never places orders, reads Redis, or uses
stale data as a substitute for missing trade-flow evidence.
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


def evaluate_paper_exit_signal(
    *,
    position_side: object,
    metrics: Mapping[str, Any],
    sample_id: object,
    favorable_r: object,
    state: dict[str, Any],
    min_momentum_pct: object = 0.02,
    min_flow_imbalance: object = 0.15,
    min_queue_imbalance: object = 0.10,
    confirmations: object = 2,
    max_metrics_age_sec: object = 5.0,
    partial_profit_arm_r: object = 1.0,
    partial_profit_enabled: bool = True,
) -> dict[str, Any]:
    """Return NONE, PARTIAL_PROFIT, or SIGNAL_REVERSAL on distinct fresh prints.

    A hard exit requires both adverse short-term price momentum and adverse
    known aggressor flow. A weakening setup can request one partial profit
    scale-out after the configured R threshold. Fresh aligned queue pressure
    suppresses both actions; missing queue data is not fabricated.
    """
    side = str(position_side or "").strip().upper()
    if side not in {"BUY", "SELL"} or not isinstance(metrics, Mapping):
        return {"action": "NONE", "reason": "INVALID_INPUT"}

    sample = str(sample_id).strip() if sample_id is not None else ""
    if not sample:
        state["paper_exit_signal_hard_confirmations"] = 0
        state["paper_exit_signal_partial_confirmations"] = 0
        return {"action": "NONE", "reason": "MISSING_TRADE_SAMPLE_ID"}

    age = _finite(metrics.get("trade_metrics_age_sec"))
    flow_ready = _finite(metrics.get("trade_flow_ready"))
    known_count = _finite(metrics.get("trade_flow_known_trade_count_30s"))
    known_share = _finite(metrics.get("trade_flow_known_volume_share_30s"))
    max_age = _finite(max_metrics_age_sec)
    if max_age is None or max_age <= 0.0:
        max_age = 5.0
    if (
        age is None or age < 0.0 or age > max_age
        or flow_ready is None or flow_ready < 0.5
        or known_count is None or known_count < 3.0
        or known_share is None or known_share < 0.50
    ):
        state["paper_exit_signal_hard_confirmations"] = 0
        state["paper_exit_signal_partial_confirmations"] = 0
        return {
            "action": "NONE",
            "reason": "STALE_OR_INSUFFICIENT_AGGRESSOR_FLOW",
            "metrics_age_sec": age,
        }

    sign = 1.0 if side == "BUY" else -1.0
    momentum_15 = _finite(metrics.get("price_change_pct"))
    momentum_30 = _finite(metrics.get("trade_price_change_30s_pct"))
    flow = _finite(metrics.get("trade_flow_imbalance_30s"))
    if momentum_15 is None or flow is None:
        state["paper_exit_signal_hard_confirmations"] = 0
        state["paper_exit_signal_partial_confirmations"] = 0
        return {"action": "NONE", "reason": "MISSING_DIRECTIONAL_METRICS"}

    try:
        momentum_threshold = max(0.001, min(1.0, float(min_momentum_pct)))
    except (TypeError, ValueError, OverflowError):
        momentum_threshold = 0.02
    try:
        flow_threshold = max(0.05, min(0.90, float(min_flow_imbalance)))
    except (TypeError, ValueError, OverflowError):
        flow_threshold = 0.15
    try:
        queue_threshold = max(0.05, min(0.90, float(min_queue_imbalance)))
    except (TypeError, ValueError, OverflowError):
        queue_threshold = 0.10

    signed_momentum_15 = momentum_15 * sign
    signed_momentum_30 = momentum_30 * sign if momentum_30 is not None else None
    signed_flow = flow * sign
    queue_flag = _finite(metrics.get("queue_imbalance_5s_available"))
    queue_available = queue_flag is not None and queue_flag >= 0.5
    queue_age = _finite(metrics.get("queue_imbalance_5s_age_sec"))
    queue_coverage = _finite(metrics.get("queue_imbalance_5s_coverage_sec"))
    queue_value = _finite(metrics.get("queue_imbalance_5s"))
    queue_fresh = bool(
        queue_available
        and queue_age is not None and 0.0 <= queue_age <= min(2.0, max_age)
        and queue_coverage is not None and queue_coverage >= 1.0
        and queue_value is not None
    )
    signed_queue = queue_value * sign if queue_fresh and queue_value is not None else None
    queue_aligned = signed_queue is not None and signed_queue >= queue_threshold
    queue_opposing = signed_queue is not None and signed_queue <= -queue_threshold

    momentum_reversed = (
        signed_momentum_15 <= -momentum_threshold
        and signed_momentum_30 is not None
        and signed_momentum_30 <= 0.0
    )
    flow_opposing = signed_flow <= -flow_threshold
    hard_reversal = momentum_reversed and flow_opposing and not queue_aligned
    favorable = _finite(favorable_r)
    try:
        partial_arm = max(0.5, min(5.0, float(partial_profit_arm_r)))
    except (TypeError, ValueError, OverflowError):
        partial_arm = 1.0
    momentum_fading = (
        signed_momentum_15 <= momentum_threshold
        and (signed_momentum_30 is None or signed_momentum_30 <= momentum_threshold)
    )
    partial_weakness = bool(
        partial_profit_enabled
        and not state.get("paper_profit_partial_taken", False)
        and favorable is not None and favorable >= partial_arm
        and flow_opposing and momentum_fading and not queue_aligned
        and not hard_reversal
    )

    last_sample = str(state.get("paper_exit_signal_last_sample_id") or "")
    is_new_sample = sample != last_sample
    if is_new_sample:
        hard_count = int(_finite(state.get("paper_exit_signal_hard_confirmations")) or 0)
        partial_count = int(_finite(state.get("paper_exit_signal_partial_confirmations")) or 0)
        state["paper_exit_signal_hard_confirmations"] = hard_count + 1 if hard_reversal else 0
        state["paper_exit_signal_partial_confirmations"] = partial_count + 1 if partial_weakness else 0
        state["paper_exit_signal_last_sample_id"] = sample
    hard_count = int(_finite(state.get("paper_exit_signal_hard_confirmations")) or 0)
    partial_count = int(_finite(state.get("paper_exit_signal_partial_confirmations")) or 0)
    try:
        needed = min(5, max(2, int(confirmations)))
    except (TypeError, ValueError, OverflowError):
        needed = 2

    action = "NONE"
    reason = "NO_CONFIRMED_EXIT_SIGNAL"
    if hard_count >= needed:
        action = "SIGNAL_REVERSAL"
        reason = "CONFIRMED_OPPOSITE_MOMENTUM_AND_AGGRESSOR_FLOW"
    elif partial_count >= needed:
        action = "PARTIAL_PROFIT"
        reason = "PROFITABLE_POSITION_WITH_PERSISTENT_FLOW_WEAKNESS"

    return {
        "action": action,
        "reason": reason,
        "sample_id": sample,
        "new_sample": is_new_sample,
        "confirmations": needed,
        "hard_reversal_confirmations": hard_count,
        "partial_profit_confirmations": partial_count,
        "momentum_15s_signed_pct": round(signed_momentum_15, 6),
        "momentum_30s_signed_pct": round(signed_momentum_30, 6) if signed_momentum_30 is not None else None,
        "aggressor_flow_signed_imbalance": round(signed_flow, 6),
        "queue_signed_imbalance": round(signed_queue, 6) if signed_queue is not None else None,
        "queue_opposing": queue_opposing,
        "metrics_age_sec": round(age, 4),
        "favorable_r": round(favorable, 4) if favorable is not None else None,
    }
