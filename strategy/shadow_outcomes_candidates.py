"""Candidate lifecycle and LLM/execution attribution for Shadow outcomes."""
from __future__ import annotations

import math
import time
from collections import deque
from collections.abc import Mapping
from datetime import datetime, timezone
from statistics import median
from typing import Any
from uuid import uuid4

from strategy.market_context import classify_market_regime, estimate_entry_economics

from .shadow_outcomes_primitives import (
    _MOSCOW_TZ,
    _apply_adverse_slippage,
    _cost_settings,
    _number,
    logger,
)


class ShadowCandidateMixin:
    """Register candidates and attach their asynchronous decision lifecycle."""

    def record_candidate(
        self,
        ticker: str,
        direction: str,
        metrics: Mapping[str, Any],
        shadow_result: Mapping[str, Any],
    ) -> str | None:
        """Start a forward label only when the candidate had a fresh coherent BBO."""
        symbol = str(ticker or "").strip().upper().split("@")[0]
        side = str(direction or "").strip().upper()
        if not symbol or side not in {"BUY", "SELL"} or not isinstance(metrics, Mapping):
            return None
        fresh, quote = self._fresh_bbo(metrics)
        if not fresh:
            return None

        entry_bid = quote["bid"]
        entry_ask = quote["ask"]
        entry_price = entry_ask if side == "BUY" else entry_bid
        entry_mid = (entry_bid + entry_ask) / 2.0
        commission_pct, slippage_bps = _cost_settings()
        entry_fill = _apply_adverse_slippage(
            entry_price, "BUY" if side == "BUY" else "SELL", slippage_bps
        )
        atr_pct = _number(metrics, "bar_atr_pct", math.nan)
        atr_history = self._atr_history.setdefault(symbol, deque(maxlen=300))
        prior_atr = list(atr_history)
        atr_reference = median(prior_atr) if len(prior_atr) >= 20 else None
        market_context = classify_market_regime(
            metrics,
            atr_reference_pct=atr_reference,
            atr_reference_samples=len(prior_atr),
        )
        entry_economics = estimate_entry_economics(
            side=side, bid=entry_bid, ask=entry_ask,
            commission_pct_per_side=commission_pct,
            slippage_bps_per_side=slippage_bps, atr_pct=atr_pct,
        )
        if math.isfinite(atr_pct) and atr_pct > 0.0:
            atr_history.append(atr_pct)
        now_mono = time.monotonic()
        created_at_utc = datetime.now(timezone.utc)
        created_at_msk = created_at_utc.astimezone(_MOSCOW_TZ)
        candidate_id = uuid4().hex[:16]
        row = {
            "candidate_id": candidate_id,
            "ticker": symbol,
            "side": side,
            "created_monotonic": now_mono,
            "created_at_utc": created_at_utc.isoformat(timespec="milliseconds"),
            "created_at_msk": created_at_msk.isoformat(timespec="milliseconds"),
            "entry_hour_msk": created_at_msk.strftime("%H"),
            "entry_bid": entry_bid,
            "entry_ask": entry_ask,
            "entry_bid_age_sec": quote["bid_age_sec"],
            "entry_ask_age_sec": quote["ask_age_sec"],
            "entry_bbo_max_age_sec": quote["max_age_sec"],
            "entry_mid": entry_mid,
            "entry_executable_price": entry_price,
            "entry_fill_price": entry_fill,
            "commission_pct_per_side": commission_pct,
            "slippage_bps": slippage_bps,
            "score": _number(shadow_result, "score"),
            "shadow_eligible": bool(shadow_result.get("eligible", False)),
            "regime": str(shadow_result.get("regime") or "UNKNOWN"),
            **market_context,
            "entry_economics": entry_economics,
            "minimum_favorable_move_pct": (
                entry_economics.get("minimum_favorable_move_pct")
                if entry_economics.get("available") else None
            ),
            "cost_to_atr_ratio": (
                entry_economics.get("cost_to_atr_ratio")
                if entry_economics.get("available") else None
            ),
            "hard_blocks": [
                str(item) for item in shadow_result.get("hard_blocks", [])
                if str(item).strip()
            ][:12] if isinstance(shadow_result.get("hard_blocks", []), (list, tuple, set)) else [],
            "reasons": [
                str(item) for item in shadow_result.get("reasons", [])
                if str(item).strip()
            ][:12] if isinstance(shadow_result.get("reasons", []), (list, tuple, set)) else [],
            "queue_imbalance": _number(metrics, "queue_imbalance") if _number(metrics, "queue_imbalance_available") >= 0.5 else None,
            "queue_imbalance_5s": _number(metrics, "queue_imbalance_5s") if _number(metrics, "queue_imbalance_5s_available") >= 0.5 else None,
            "queue_imbalance_5s_coverage_sec": _number(metrics, "queue_imbalance_5s_coverage_sec"),
            "queue_imbalance_5s_samples": int(max(0.0, _number(metrics, "queue_imbalance_5s_samples"))),
            "queue_imbalance_5s_positive_time_pct": _number(metrics, "queue_imbalance_5s_positive_time_pct", math.nan),
            "ofi_5s": _number(metrics, "ofi_5s") if _number(metrics, "ofi_5s_available") >= 0.5 else math.nan,
            "ofi_5s_available": bool(_number(metrics, "ofi_5s_available") >= 0.5),
            "ofi_5s_samples": int(max(0.0, _number(metrics, "ofi_5s_samples"))),
            "ofi_5s_coverage_sec": _number(metrics, "ofi_5s_coverage_sec"),
            "spread_pct": _number(metrics, "spread_pct"),
            "atr_pct": atr_pct,
            "spread_atr_ratio": _number(shadow_result, "spread_atr_ratio", math.nan),
            "volume_spike": _number(metrics, "volume_spike"),
            "trade_count_60s": int(max(0.0, _number(metrics, "trade_count_60s"))),
            "trade_flow_imbalance_30s": _number(metrics, "trade_flow_imbalance_30s", math.nan),
            "trade_history_coverage_sec": _number(metrics, "trade_history_coverage_sec", math.nan),
            "trade_realized_volatility_60s_pct": _number(metrics, "trade_realized_volatility_60s_pct", math.nan),
            "trade_volume_5s": _number(metrics, "trade_volume_5s", math.nan),
            "trade_volume_baseline_5s": _number(metrics, "trade_volume_baseline_5s", math.nan),
            "price_change_pct": _number(metrics, "price_change_pct"),
            "price_vs_vwap_pct": _number(metrics, "price_vs_vwap_pct"),
            "m1_pattern_confirmed": _number(metrics, "bar_pattern_confirmed"),
            "m1_pattern": str(metrics.get("bar_pattern_name") or ""),
            "m1_signal_gap_atr": _number(metrics, "bar_signal_gap_atr", math.nan),
            "m1_breakout_extension_atr": _number(metrics, "bar_breakout_extension_atr", math.nan),
            "remaining_horizons": list(self.horizons_seconds),
            # Excursions are net of configured two-sided costs; gross values are
            # retained separately. Zero is the entry-time baseline when no
            # favorable/adverse movement has yet been observed.
            "mfe_executable_pct": 0.0,
            "mae_executable_pct": 0.0,
            "mfe_gross_executable_pct": 0.0,
            "mae_gross_executable_pct": 0.0,
            "time_to_mfe_sec": 0.0,
            "time_to_mae_sec": 0.0,
            "mfe_at_utc": created_at_utc.isoformat(timespec="milliseconds"),
            "mae_at_utc": created_at_utc.isoformat(timespec="milliseconds"),
            "valid_bbo_observations": 0,
            "first_valid_bbo_elapsed_sec": None,
            "last_valid_bbo_elapsed_sec": None,
            "bbo_observation_span_sec": 0.0,
            "no_fresh_bbo_observations": 0,
            "last_bbo_unavailable_reason": None,
            "llm_linked": False,
            "llm_attribution_state": "PENDING",
            "llm_not_called_reason": None,
            "llm_verdict_id": None,
            "llm_model_decision": "NOT_LINKED",
            "llm_final_decision": "NOT_LINKED",
            "llm_signal_status": "NOT_LINKED",
            "llm_confidence": None,
            "llm_reason_code": "NOT_LINKED",
            "llm_local_guard": None,
            "llm_execution_status": "NOT_LINKED",
        }
        pending = self._pending.setdefault(symbol, [])
        pending.append(row)
        if len(pending) >= self.max_pending_per_ticker and symbol not in self._pending_backlog_warned:
            self._pending_backlog_warned.add(symbol)
            logger.warning(
                "[STRATEGY SHADOW BACKLOG] ticker=%s pending=%d warning_threshold=%d; "
                "no candidates were discarded; all forward horizons remain queued.",
                symbol, len(pending), self.max_pending_per_ticker,
            )
        logger.info(
            "[STRATEGY SHADOW SAMPLE] id=%s ticker=%s side=%s score=%.1f eligible=%s "
            "entry=%s entry_fill=%.6f commission=%.4f%%/side slippage=%.2fbps "
            "trades60=%d flow30=%s spread_ATR=%s market_regime=%s "
            "min_favorable_move=%s cost_to_ATR=%s",
            candidate_id, symbol, side, row["score"], row["shadow_eligible"],
            "ASK" if side == "BUY" else "BID", entry_fill, commission_pct, slippage_bps,
            row["trade_count_60s"],
            f'{row["trade_flow_imbalance_30s"]:+.3f}' if math.isfinite(row["trade_flow_imbalance_30s"]) else "NA",
            f'{row["spread_atr_ratio"]:.3f}' if math.isfinite(row["spread_atr_ratio"]) else "NA",
            row.get("market_regime", "UNKNOWN"),
            f'{row["minimum_favorable_move_pct"]:.4f}%' if row.get("minimum_favorable_move_pct") is not None else "NA",
            f'{row["cost_to_atr_ratio"]:.3f}' if row.get("cost_to_atr_ratio") is not None else "NA",
        )
        return candidate_id

    def attach_llm_verdict(
        self,
        candidate_id: str,
        *,
        verdict_id: str,
        model_decision: str,
        final_decision: str,
        signal_status: str,
        confidence: float | None,
        reason_code: str,
        local_guard: str,
        execution_status: str,
    ) -> bool:
        """Attach the final LLM/system verdict to its still-pending shadow candidate."""
        identifier = str(candidate_id or "").strip()
        if not identifier:
            return False
        model = str(model_decision or "UNKNOWN").strip().upper()
        final = str(final_decision or "HOLD").strip().upper()
        status = str(signal_status or "UNKNOWN").strip().upper()
        try:
            confidence_value = float(confidence) if confidence is not None else math.nan
        except (TypeError, ValueError, OverflowError):
            confidence_value = math.nan
        if not math.isfinite(confidence_value):
            confidence_value = None
        else:
            confidence_value = min(100.0, max(0.0, confidence_value))
        for rows in self._pending.values():
            for row in rows:
                if row.get("candidate_id") != identifier:
                    continue
                if row.get("llm_attribution_state") == "NOT_CALLED":
                    return False
                row.update({
                    "llm_linked": True,
                    "llm_attribution_state": "LINKED",
                    "llm_not_called_reason": None,
                    "llm_verdict_id": str(verdict_id or "") or None,
                    "llm_model_decision": model,
                    "llm_final_decision": final,
                    "llm_signal_status": status,
                    "llm_confidence": confidence_value,
                    "llm_reason_code": str(reason_code or "NOT_PROVIDED")[:80],
                    "llm_local_guard": str(local_guard or "")[:160],
                    "llm_execution_status": str(execution_status or "UNKNOWN").upper(),
                })
                return True
        return False

    def mark_llm_not_called(self, candidate_id: str, *, reason_code: str) -> bool:
        """Record a known pre-LLM rejection without confusing it with a lost link."""
        identifier = str(candidate_id or "").strip()
        reason = str(reason_code or "PREFILTER_BLOCKED").strip().upper()[:80]
        if not identifier:
            return False
        for rows in self._pending.values():
            for row in rows:
                if row.get("candidate_id") != identifier:
                    continue
                if row.get("llm_attribution_state") == "LINKED":
                    return False
                row.update({
                    "llm_linked": False,
                    "llm_attribution_state": "NOT_CALLED",
                    "llm_not_called_reason": reason,
                    "llm_model_decision": "NOT_CALLED",
                    "llm_final_decision": "NOT_CALLED",
                    "llm_signal_status": "NOT_CALLED",
                    "llm_reason_code": reason,
                    "llm_local_guard": "PRE_LLM_FILTER",
                    "llm_execution_status": "NOT_REQUESTED",
                })
                return True
        return False

    def update_execution_result(
        self,
        candidate_id: str,
        *,
        execution_status: str,
    ) -> bool:
        """Update the linked candidate with the final asynchronous execution result."""
        identifier = str(candidate_id or "").strip()
        if not identifier:
            return False
        status = str(execution_status or "UNKNOWN").strip().upper()
        for rows in self._pending.values():
            for row in rows:
                if row.get("candidate_id") == identifier and row.get("llm_linked"):
                    row["llm_execution_status"] = status
                    return True
        return False
