"""Forward outcome labelling for research-only PILUITUS strategy candidates.

All observations are hypothetical. Entries and exits use the executable side
of a fresh BBO, with the PAPER commission/slippage settings applied for
sensitivity analysis. Nothing in this module submits, changes or closes orders.
"""
from __future__ import annotations

import logging
import math
import os
import time
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

logger = logging.getLogger("Piluitus.StrategyShadow")
_MOSCOW_TZ = timezone(timedelta(hours=3), name="MSK")


def _number(metrics: Mapping[str, Any], key: str, default: float = 0.0) -> float:
    value = metrics.get(key, default)
    if isinstance(value, bool):
        return float(value)
    if not isinstance(value, (int, float, str)):
        return default
    try:
        numeric = float(value)
    except (TypeError, ValueError, OverflowError):
        return default
    return numeric if math.isfinite(numeric) else default


def _cost_settings() -> tuple[float, float]:
    try:
        commission_pct = min(1.0, max(0.0, float(os.getenv("PILUITUS_PAPER_COMMISSION_PCT", "0"))))
    except (TypeError, ValueError, OverflowError):
        commission_pct = 0.0
    try:
        slippage_bps = min(100.0, max(0.0, float(os.getenv("PILUITUS_PAPER_SLIPPAGE_BPS", "0"))))
    except (TypeError, ValueError, OverflowError):
        slippage_bps = 0.0
    if not math.isfinite(commission_pct):
        commission_pct = 0.0
    if not math.isfinite(slippage_bps):
        slippage_bps = 0.0
    return commission_pct, slippage_bps


def _apply_adverse_slippage(price: float, transaction_side: str, slippage_bps: float) -> float:
    rate = max(0.0, slippage_bps) / 10_000.0
    adjusted = price * (1.0 + rate if transaction_side == "BUY" else 1.0 - rate)
    return adjusted if math.isfinite(adjusted) and adjusted > 0.0 else price


class ShadowForwardTracker:
    """Label candidate forward performance at fixed horizons using fresh executable BBO."""

    def __init__(
        self,
        horizons_seconds: tuple[int, ...] = (60, 180, 300, 600),
        *,
        max_label_lag_sec: float = 30.0,
        on_time_label_lag_sec: float = 10.0,
        max_pending_per_ticker: int = 100,
    ) -> None:
        normalized = tuple(sorted({int(x) for x in horizons_seconds if int(x) > 0}))
        if not normalized:
            raise ValueError("At least one positive shadow horizon is required.")
        self.horizons_seconds = normalized
        self.max_label_lag_sec = max(1.0, float(max_label_lag_sec))
        self.on_time_label_lag_sec = max(0.0, float(on_time_label_lag_sec))
        # This is a warning threshold, not a hard cap. Dropping the oldest
        # candidate here silently destroys forward-label coverage precisely
        # during high-activity periods. Keep every candidate until its horizons
        # are labelled and expose sustained backlog through a rate-limited log.
        self.max_pending_per_ticker = max(1, int(max_pending_per_ticker))
        self._pending: dict[str, list[dict[str, Any]]] = {}
        self._pending_backlog_warned: set[str] = set()
        self.max_segment_groups = 1200
        self._label_stats: dict[str, dict[str, float | int]] = {
            "eligible": self._new_stats_bucket(),
            "ineligible": self._new_stats_bucket(),
        }
        # Bounded cross-sections let PAPER research compare direction, ticker,
        # market regime, score band, horizon and Moscow trading hour.
        self._segment_stats: dict[str, dict[str, float | int]] = {}

    @staticmethod
    def _new_stats_bucket() -> dict[str, float | int]:
        return {
            "labels": 0, "ok": 0, "late": 0, "no_fresh_bbo": 0,
            "positive": 0, "negative": 0, "net_sum_pct": 0.0, "gross_sum_pct": 0.0,
            "net_positive_sum_pct": 0.0, "net_negative_sum_pct": 0.0,
            "mfe_sum_pct": 0.0, "mae_sum_pct": 0.0,
        }

    @staticmethod
    def _accumulate_stats(bucket: dict[str, float | int], label: Mapping[str, Any]) -> None:
        bucket["labels"] = int(bucket["labels"]) + 1
        result = str(label.get("result") or "")
        if result == "OK":
            bucket["ok"] = int(bucket["ok"]) + 1
            net = _number(label, "net_executable_return_pct", math.nan)
            gross = _number(label, "gross_executable_return_pct", math.nan)
            if math.isfinite(net):
                bucket["net_sum_pct"] = float(bucket["net_sum_pct"]) + net
                if net > 0.0:
                    bucket["positive"] = int(bucket["positive"]) + 1
                    bucket["net_positive_sum_pct"] = float(bucket["net_positive_sum_pct"]) + net
                elif net < 0.0:
                    bucket["negative"] = int(bucket["negative"]) + 1
                    bucket["net_negative_sum_pct"] = float(bucket["net_negative_sum_pct"]) + abs(net)
            if math.isfinite(gross):
                bucket["gross_sum_pct"] = float(bucket["gross_sum_pct"]) + gross
            bucket["mfe_sum_pct"] = float(bucket["mfe_sum_pct"]) + _number(label, "mfe_executable_pct")
            bucket["mae_sum_pct"] = float(bucket["mae_sum_pct"]) + _number(label, "mae_executable_pct")
        elif result == "LABEL_LATE":
            bucket["late"] = int(bucket["late"]) + 1
        elif result == "NO_FRESH_BBO":
            bucket["no_fresh_bbo"] = int(bucket["no_fresh_bbo"]) + 1

    @staticmethod
    def _score_band(score: float) -> str:
        value = min(100.0, max(0.0, score))
        lower = 90 if value >= 90 else int(value // 10) * 10
        upper = 100 if lower == 90 else lower + 9
        return f"{lower:02d}-{upper:02d}"

    @staticmethod
    def _fresh_bbo(metrics: Mapping[str, Any]) -> tuple[bool, dict[str, Any]]:
        bid = _number(metrics, "bid")
        ask = _number(metrics, "ask")
        bid_age = _number(metrics, "bid_quote_age_sec", math.inf)
        ask_age = _number(metrics, "ask_quote_age_sec", math.inf)
        max_age = min(5.0, max(0.25, _number(metrics, "bbo_max_age_sec", 2.0)))
        if _number(metrics, "bid_ask_available") < 0.5 or _number(metrics, "spread_available") < 0.5:
            unavailable_reason = "MISSING_BBO_FLAGS"
        elif bid <= 0.0:
            unavailable_reason = "MISSING_BID"
        elif ask <= 0.0:
            unavailable_reason = "MISSING_ASK"
        elif ask < bid:
            unavailable_reason = "CROSSED_BBO"
        elif bid_age < 0.0 or bid_age > max_age:
            unavailable_reason = "BID_STALE"
        elif ask_age < 0.0 or ask_age > max_age:
            unavailable_reason = "ASK_STALE"
        else:
            unavailable_reason = "OK"
        valid = unavailable_reason == "OK"
        return valid, {
            "bid": bid,
            "ask": ask,
            "bid_age_sec": bid_age,
            "ask_age_sec": ask_age,
            "max_age_sec": max_age,
            "unavailable_reason": unavailable_reason,
        }

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
            "atr_pct": _number(metrics, "bar_atr_pct"),
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
            "mfe_executable_pct": 0.0,
            "mae_executable_pct": 0.0,
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
            "trades60=%d flow30=%s spread_ATR=%s",
            candidate_id, symbol, side, row["score"], row["shadow_eligible"],
            "ASK" if side == "BUY" else "BID", entry_fill, commission_pct, slippage_bps,
            row["trade_count_60s"],
            f'{row["trade_flow_imbalance_30s"]:+.3f}' if math.isfinite(row["trade_flow_imbalance_30s"]) else "NA",
            f'{row["spread_atr_ratio"]:.3f}' if math.isfinite(row["spread_atr_ratio"]) else "NA",
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

    @staticmethod
    def _llm_label_fields(row: Mapping[str, Any]) -> dict[str, Any]:
        return {
            "llm_linked": bool(row.get("llm_linked", False)),
            "llm_attribution_state": str(row.get("llm_attribution_state") or "UNKNOWN_LEGACY"),
            "llm_not_called_reason": row.get("llm_not_called_reason"),
            "llm_verdict_id": row.get("llm_verdict_id"),
            "llm_model_decision": str(row.get("llm_model_decision") or "NOT_LINKED"),
            "llm_final_decision": str(row.get("llm_final_decision") or "NOT_LINKED"),
            "llm_signal_status": str(row.get("llm_signal_status") or "NOT_LINKED"),
            "llm_confidence": row.get("llm_confidence"),
            "llm_reason_code": str(row.get("llm_reason_code") or "NOT_LINKED"),
            "llm_local_guard": row.get("llm_local_guard"),
            "llm_execution_status": str(row.get("llm_execution_status") or "NOT_LINKED"),
        }

    @staticmethod
    def _executable_return_pct(row: Mapping[str, Any], exit_raw: float) -> tuple[float, float, float]:
        side = str(row["side"])
        exit_fill = _apply_adverse_slippage(
            exit_raw,
            "SELL" if side == "BUY" else "BUY",
            float(row["slippage_bps"]),
        )
        entry_fill = float(row["entry_fill_price"])
        entry_fill_price = entry_fill
        sign = 1.0 if side == "BUY" else -1.0
        gross_rub_per_share = (exit_fill - entry_fill) * sign
        fee_rub_per_share = (
            entry_fill + exit_fill
        ) * float(row["commission_pct_per_side"]) / 100.0
        net_rub_per_share = gross_rub_per_share - fee_rub_per_share
        gross_pct = gross_rub_per_share / entry_fill_price * 100.0
        net_pct = net_rub_per_share / entry_fill_price * 100.0
        return gross_pct, net_pct, exit_fill

    def observe_market_tick(self, ticker: str, metrics: Mapping[str, Any]) -> list[dict[str, Any]]:
        """Update excursions and emit any horizon labels reached on a fresh quote."""
        symbol = str(ticker or "").strip().upper().split("@")[0]
        pending = self._pending.get(symbol)
        if not symbol or not pending:
            return []
        now_mono = time.monotonic()
        fresh, quote = self._fresh_bbo(metrics) if isinstance(metrics, Mapping) else (False, {})
        completed: list[dict[str, Any]] = []
        survivors: list[dict[str, Any]] = []

        for row in pending:
            elapsed = max(0.0, now_mono - float(row["created_monotonic"]))
            side = str(row["side"])
            exit_raw = quote.get("bid", 0.0) if side == "BUY" else quote.get("ask", 0.0)
            mid = (
                (quote["bid"] + quote["ask"]) / 2.0 if fresh else 0.0
            )
            if fresh:
                gross_pct, net_pct, exit_fill = self._executable_return_pct(row, float(exit_raw))
                row["mfe_executable_pct"] = max(float(row["mfe_executable_pct"]), net_pct)
                row["mae_executable_pct"] = min(float(row["mae_executable_pct"]), net_pct)
                row["last_bbo_unavailable_reason"] = None
            else:
                gross_pct = net_pct = exit_fill = math.nan
                row["no_fresh_bbo_observations"] = int(row.get("no_fresh_bbo_observations", 0)) + 1
                row["last_bbo_unavailable_reason"] = str(quote.get("unavailable_reason") or "UNKNOWN")

            remaining: list[int] = []
            for horizon in row["remaining_horizons"]:
                if elapsed >= horizon and fresh and elapsed <= horizon + self.max_label_lag_sec:
                    sign = 1.0 if side == "BUY" else -1.0
                    mid_return_pct = (
                        (mid / float(row["entry_mid"]) - 1.0) * 100.0 * sign
                    )
                    label_lag = round(max(0.0, elapsed - horizon), 3)
                    label_quality = (
                        "ON_TIME" if label_lag <= self.on_time_label_lag_sec else "DELAYED"
                    )
                    label = {
                        "candidate_id": row["candidate_id"],
                        **self._llm_label_fields(row),
                        "ticker": symbol,
                        "side": side,
                        "entry_time_msk": row["created_at_msk"],
                        "entry_hour_msk": row["entry_hour_msk"],
                        "result": "OK",
                        "label_quality": label_quality,
                        "horizon_sec": horizon,
                        "elapsed_sec": round(elapsed, 3),
                        "label_lag_sec": label_lag,
                        "entry_executable_price": round(float(row["entry_executable_price"]), 6),
                        "entry_fill_price": round(float(row["entry_fill_price"]), 6),
                        "exit_executable_price": round(float(exit_raw), 6),
                        "exit_fill_price": round(float(exit_fill), 6),
                        "exit_bid_age_sec": round(float(quote["bid_age_sec"]), 3),
                        "exit_ask_age_sec": round(float(quote["ask_age_sec"]), 3),
                        "exit_bbo_max_age_sec": round(float(quote["max_age_sec"]), 3),
                        "exit_spread_pct": _number(metrics, "spread_pct"),
                        "mid_return_pct": round(mid_return_pct, 6),
                        "gross_executable_return_pct": round(gross_pct, 6),
                        "net_executable_return_pct": round(net_pct, 6),
                        "mfe_executable_pct": round(float(row["mfe_executable_pct"]), 6),
                        "mae_executable_pct": round(float(row["mae_executable_pct"]), 6),
                        "score": row["score"],
                        "shadow_eligible": row["shadow_eligible"],
                        "regime": row["regime"],
                        "hard_blocks": list(row.get("hard_blocks", [])),
                        "reasons": list(row.get("reasons", [])),
                        "price_change_pct": row["price_change_pct"],
                        "price_vs_vwap_pct": row["price_vs_vwap_pct"],
                        "m1_pattern_confirmed": row["m1_pattern_confirmed"],
                        "m1_pattern": row["m1_pattern"],
                        "m1_signal_gap_atr": row["m1_signal_gap_atr"],
                        "m1_breakout_extension_atr": row["m1_breakout_extension_atr"],
                        "queue_imbalance": row["queue_imbalance"],
                        "queue_imbalance_5s": row["queue_imbalance_5s"],
                        "queue_imbalance_5s_coverage_sec": row["queue_imbalance_5s_coverage_sec"],
                        "queue_imbalance_5s_samples": row["queue_imbalance_5s_samples"],
                        "queue_imbalance_5s_positive_time_pct": row["queue_imbalance_5s_positive_time_pct"],
                        "ofi_5s": row["ofi_5s"],
                        "ofi_5s_available": row["ofi_5s_available"],
                        "ofi_5s_samples": row["ofi_5s_samples"],
                        "ofi_5s_coverage_sec": row["ofi_5s_coverage_sec"],
                        "spread_atr_ratio": row["spread_atr_ratio"],
                        "trade_count_60s": row["trade_count_60s"],
                        "trade_flow_imbalance_30s": row["trade_flow_imbalance_30s"],
                        "trade_history_coverage_sec": row["trade_history_coverage_sec"],
                        "trade_realized_volatility_60s_pct": row["trade_realized_volatility_60s_pct"],
                        "trade_volume_5s": row["trade_volume_5s"],
                        "trade_volume_baseline_5s": row["trade_volume_baseline_5s"],
                        "commission_pct_per_side": row["commission_pct_per_side"],
                        "slippage_bps": row["slippage_bps"],
                    }
                    completed.append(label)
                    logger.info(
                        "[STRATEGY SHADOW OUTCOME] id=%s ticker=%s side=%s horizon=%ss "
                        "elapsed=%.2fs lag=%.2fs quality=%s result=OK net=%.5f%% gross=%.5f%% "
                        "mid=%.5f%% MFE=%.5f%% MAE=%.5f%% score=%.1f eligible=%s "
                        "regime=%s entry_time_msk=%s hard_blocks=%s trades60=%d flow30=%s spread_ATR=%s",
                        label["candidate_id"], symbol, side, horizon, elapsed,
                        label["label_lag_sec"], label_quality, net_pct, gross_pct, mid_return_pct,
                        row["mfe_executable_pct"], row["mae_executable_pct"],
                        row["score"], row["shadow_eligible"], row["regime"],
                        row["created_at_msk"], ",".join(row.get("hard_blocks", [])) or "-",
                        row["trade_count_60s"],
                        f'{row["trade_flow_imbalance_30s"]:+.3f}' if math.isfinite(row["trade_flow_imbalance_30s"]) else "NA",
                        f'{row["spread_atr_ratio"]:.3f}' if math.isfinite(row["spread_atr_ratio"]) else "NA",
                    )
                elif elapsed > horizon + self.max_label_lag_sec:
                    label = {
                        "candidate_id": row["candidate_id"],
                        **self._llm_label_fields(row),
                        "ticker": symbol,
                        "side": side,
                        "entry_time_msk": row["created_at_msk"],
                        "entry_hour_msk": row["entry_hour_msk"],
                        "result": "LABEL_LATE" if fresh else "NO_FRESH_BBO",
                        "label_quality": "LABEL_LATE" if fresh else "NO_FRESH_BBO",
                        "horizon_sec": horizon,
                        "elapsed_sec": round(elapsed, 3),
                        "label_lag_sec": round(max(0.0, elapsed - horizon), 3),
                        "score": row["score"],
                        "shadow_eligible": row["shadow_eligible"],
                        "regime": row["regime"],
                        "hard_blocks": list(row.get("hard_blocks", [])),
                        "reasons": list(row.get("reasons", [])),
                        "price_change_pct": row["price_change_pct"],
                        "price_vs_vwap_pct": row["price_vs_vwap_pct"],
                        "m1_pattern_confirmed": row["m1_pattern_confirmed"],
                        "m1_pattern": row["m1_pattern"],
                        "m1_signal_gap_atr": row["m1_signal_gap_atr"],
                        "m1_breakout_extension_atr": row["m1_breakout_extension_atr"],
                        "queue_imbalance": row["queue_imbalance"],
                        "queue_imbalance_5s": row["queue_imbalance_5s"],
                        "queue_imbalance_5s_coverage_sec": row["queue_imbalance_5s_coverage_sec"],
                        "queue_imbalance_5s_samples": row["queue_imbalance_5s_samples"],
                        "queue_imbalance_5s_positive_time_pct": row["queue_imbalance_5s_positive_time_pct"],
                        "ofi_5s": row["ofi_5s"],
                        "ofi_5s_available": row["ofi_5s_available"],
                        "ofi_5s_samples": row["ofi_5s_samples"],
                        "ofi_5s_coverage_sec": row["ofi_5s_coverage_sec"],
                        "spread_atr_ratio": row["spread_atr_ratio"],
                        "trade_count_60s": row["trade_count_60s"],
                        "trade_flow_imbalance_30s": row["trade_flow_imbalance_30s"],
                        "trade_history_coverage_sec": row["trade_history_coverage_sec"],
                        "trade_realized_volatility_60s_pct": row["trade_realized_volatility_60s_pct"],
                        "trade_volume_5s": row["trade_volume_5s"],
                        "trade_volume_baseline_5s": row["trade_volume_baseline_5s"],
                        "bbo_unavailable_reason": (
                            str(quote.get("unavailable_reason") or row.get("last_bbo_unavailable_reason") or "UNKNOWN")
                            if not fresh else "LABEL_LATE"
                        ),
                        "no_fresh_bbo_observations": int(row.get("no_fresh_bbo_observations", 0)),
                        "entry_bid_age_sec": row["entry_bid_age_sec"],
                        "entry_ask_age_sec": row["entry_ask_age_sec"],
                    }
                    completed.append(label)
                    logger.warning(
                        "[STRATEGY SHADOW OUTCOME] id=%s ticker=%s side=%s horizon=%ss "
                        "elapsed=%.2fs quality=%s reason=%s invalid_observations=%d result=%s "
                        "regime=%s score=%.1f entry_time_msk=%s hard_blocks=%s",
                        label["candidate_id"], symbol, side, horizon, elapsed,
                        label["label_quality"], label["bbo_unavailable_reason"],
                        label["no_fresh_bbo_observations"], label["result"], row["regime"],
                        row["score"], row["created_at_msk"],
                        ",".join(row.get("hard_blocks", [])) or "-",
                    )
                else:
                    remaining.append(horizon)
            row["remaining_horizons"] = remaining
            if remaining:
                survivors.append(row)

        if survivors:
            self._pending[symbol] = survivors
            if len(survivors) < self.max_pending_per_ticker:
                self._pending_backlog_warned.discard(symbol)
        else:
            self._pending.pop(symbol, None)
            self._pending_backlog_warned.discard(symbol)
        self.restore_completed_labels(completed)
        return completed

    def restore_completed_labels(self, labels: Any) -> int:
        """Restore cumulative research statistics from persisted completed labels.

        Replaying the same stored batch more than once would double-count it;
        callers must load the bounded outcome journal exactly once at startup.
        """
        if not isinstance(labels, (list, tuple)):
            return 0
        restored = 0
        for label in labels:
            if not isinstance(label, Mapping):
                continue
            if str(label.get("result") or "") not in {"OK", "LABEL_LATE", "NO_FRESH_BBO"}:
                continue
            if not label.get("candidate_id") or not label.get("ticker"):
                continue
            if str(label.get("side") or "").upper() not in {"BUY", "SELL"}:
                continue
            # Eligibility is part of the persisted outcome schema. Do not
            # silently classify incomplete legacy/corrupt rows as ineligible.
            if not isinstance(label.get("shadow_eligible"), bool):
                continue
            self._accumulate_completed_label(label)
            restored += 1
        return restored

    def _accumulate_completed_label(self, label: Mapping[str, Any]) -> None:
        eligible = bool(label.get("shadow_eligible"))
        self._accumulate_stats(
            self._label_stats["eligible" if eligible else "ineligible"], label
        )
        side = str(label.get("side") or "UNKNOWN").upper()
        ticker = str(label.get("ticker") or "UNKNOWN").upper()
        regime = str(label.get("regime") or "UNKNOWN")
        score_band = self._score_band(_number(label, "score"))
        horizon = int(_number(label, "horizon_sec"))
        hour_msk = str(label.get("entry_hour_msk") or "UNKNOWN")
        eligibility = "eligible" if eligible else "ineligible"
        
        spread_atr = _number(label, "spread_atr_ratio", math.nan)
        if not math.isfinite(spread_atr) or spread_atr < 0.0:
            spread_atr_bucket = "unavailable"
        elif spread_atr <= 0.10:
            spread_atr_bucket = "le_0.10"
        elif spread_atr <= 0.20:
            spread_atr_bucket = "0.10-0.20"
        elif spread_atr <= 0.35:
            spread_atr_bucket = "0.20-0.35"
        elif spread_atr <= 0.50:
            spread_atr_bucket = "0.35-0.50"
        else:
            spread_atr_bucket = "gt_0.50"
        
        m1_gap = _number(label, "m1_signal_gap_atr", math.nan)
        if not math.isfinite(m1_gap) or m1_gap < 0.0:
            m1_gap_bucket = "unavailable"
        elif m1_gap <= 0.05:
            m1_gap_bucket = "le_0.05"
        elif m1_gap <= 0.10:
            m1_gap_bucket = "0.05-0.10"
        elif m1_gap <= 0.25:
            m1_gap_bucket = "0.10-0.25"
        elif m1_gap <= 0.35:
            m1_gap_bucket = "0.25-0.35"
        else:
            m1_gap_bucket = "gt_0.35"
        
        trade_count = int(max(0.0, _number(label, "trade_count_60s")))
        if trade_count < 10:
            trade_count_bucket = "lt_10"
        elif trade_count < 20:
            trade_count_bucket = "10-19"
        elif trade_count < 50:
            trade_count_bucket = "20-49"
        else:
            trade_count_bucket = "ge_50"
        
        queue_value = _number(label, "queue_imbalance_5s", math.nan)
        if not math.isfinite(queue_value):
            queue_alignment = "unavailable"
        else:
            signed_queue = queue_value if side == "BUY" else -queue_value
            if signed_queue >= 0.25:
                queue_alignment = "strong_aligned"
            elif signed_queue >= 0.10:
                queue_alignment = "aligned"
            elif signed_queue > -0.10:
                queue_alignment = "neutral"
            else:
                queue_alignment = "opposed"

        ofi_value = _number(label, "ofi_5s", math.nan)
        if not bool(label.get("ofi_5s_available", False)) or not math.isfinite(ofi_value):
            ofi_alignment = "unavailable"
        else:
            signed_ofi = ofi_value if side == "BUY" else -ofi_value
            if signed_ofi >= 0.05:
                ofi_alignment = "aligned"
            elif signed_ofi <= -0.05:
                ofi_alignment = "opposed"
            else:
                ofi_alignment = "neutral"

        flow = _number(label, "trade_flow_imbalance_30s", math.nan)
        if not math.isfinite(flow):
            flow_alignment = "unavailable"
        else:
            signed_flow = flow if side == "BUY" else -flow
            if signed_flow >= 0.25:
                flow_alignment = "strong_aligned"
            elif signed_flow >= 0.10:
                flow_alignment = "aligned"
            elif signed_flow > -0.10:
                flow_alignment = "neutral"
            else:
                flow_alignment = "opposed"
        
        segment_keys = (
            f"side={side}",
            f"ticker={ticker}",
            f"regime={regime}",
            f"score_band={score_band}",
            f"horizon={horizon}s",
            f"hour_msk={hour_msk}",
            f"spread_atr_bucket={spread_atr_bucket}",
            f"trade_count_bucket={trade_count_bucket}",
            f"flow_alignment={flow_alignment}",
            f"side_flow_alignment={side}|{flow_alignment}",
            f"queue_alignment_5s={queue_alignment}",
            f"side_queue_alignment_5s={side}|{queue_alignment}",
            f"ofi_alignment_5s={ofi_alignment}",
            f"side_ofi_alignment_5s={side}|{ofi_alignment}",
            f"side_regime={side}|{regime}",
            f"side_score_band={side}|{score_band}",
            f"side_spread_atr_bucket={side}|{spread_atr_bucket}",
            f"side_trade_count_bucket={side}|{trade_count_bucket}",
            f"m1_pattern={str(label.get('m1_pattern') or 'NONE')}",
            f"side_m1_pattern={side}|{str(label.get('m1_pattern') or 'NONE')}",
            f"m1_gap_bucket={m1_gap_bucket}",
            f"side_m1_gap_bucket={side}|{m1_gap_bucket}",
            f"side_eligibility={side}|{eligibility}",
            f"ticker_eligibility={ticker}|{eligibility}",
            f"llm_linked={bool(label.get('llm_linked', False))}",
            f"llm_attribution_state={str(label.get('llm_attribution_state') or 'UNKNOWN_LEGACY')}",
            f"llm_not_called_reason={str(label.get('llm_not_called_reason') or 'NONE')}",
            f"llm_model_decision={str(label.get('llm_model_decision') or 'NOT_LINKED')}",
            f"llm_final_decision={str(label.get('llm_final_decision') or 'NOT_LINKED')}",
            f"llm_signal_status={str(label.get('llm_signal_status') or 'NOT_LINKED')}",
            f"llm_reason_code={str(label.get('llm_reason_code') or 'NOT_LINKED')}",
            f"llm_execution_status={str(label.get('llm_execution_status') or 'NOT_LINKED')}",
            f"llm_confidence_bucket={self._score_band(_number(label, 'llm_confidence', 0.0)) if label.get('llm_confidence') is not None else 'unavailable'}",
            # Interactions are essential: a confidence band or decision can
            # behave very differently at 60s versus 600s. These bounded keys
            # let the scorecard compare them without mixing horizons.
            f"horizon_llm_model_decision={horizon}s|{str(label.get('llm_model_decision') or 'NOT_LINKED')}",
            f"horizon_llm_final_decision={horizon}s|{str(label.get('llm_final_decision') or 'NOT_LINKED')}",
            f"horizon_llm_confidence={horizon}s|{self._score_band(_number(label, 'llm_confidence', 0.0)) if label.get('llm_confidence') is not None else 'unavailable'}",
            f"horizon_llm_decision_confidence={horizon}s|{str(label.get('llm_model_decision') or 'NOT_LINKED')}|{self._score_band(_number(label, 'llm_confidence', 0.0)) if label.get('llm_confidence') is not None else 'unavailable'}",
            f"horizon_llm_execution={horizon}s|{str(label.get('llm_execution_status') or 'NOT_LINKED')}",
            *(f"hard_block={reason}" for reason in label.get("hard_blocks", [])),
        )
        for key in segment_keys:
            if key not in self._segment_stats:
                if len(self._segment_stats) >= self.max_segment_groups:
                    continue
                self._segment_stats[key] = self._new_stats_bucket()
            self._accumulate_stats(self._segment_stats[key], label)

    def statistics_snapshot(self) -> dict[str, Any]:
        """Cumulative outcomes by eligibility and bounded research cross-sections."""
        def summarize(values: Mapping[str, float | int]) -> dict[str, float | int | str | None]:
            row = dict(values)
            count = int(row["ok"])
            row["positive_rate_pct"] = (
                round(int(row["positive"]) / count * 100.0, 2) if count else 0.0
            )
            loss_sum = float(row.get("net_negative_sum_pct", 0.0))
            profit_sum = float(row.get("net_positive_sum_pct", 0.0))
            row["profit_factor"] = round(profit_sum / loss_sum, 4) if loss_sum > 0.0 else None
            positive_count = int(row.get("positive", 0))
            negative_count = int(row.get("negative", 0))
            row["average_win_pct"] = (
                round(profit_sum / positive_count, 6) if positive_count else 0.0
            )
            row["average_loss_pct"] = (
                round(loss_sum / negative_count, 6) if negative_count else 0.0
            )
            row["sample_quality"] = (
                "INSUFFICIENT_LT_30" if count < 30 else
                "PRELIMINARY_30_99" if count < 100 else
                "DEVELOPING_100_299" if count < 300 else
                "LARGE_300_PLUS"
            )
            for metric, total in (
                ("average_net_return_pct", "net_sum_pct"),
                ("average_gross_return_pct", "gross_sum_pct"),
                ("average_mfe_pct", "mfe_sum_pct"),
                ("average_mae_pct", "mae_sum_pct"),
            ):
                row[metric] = round(float(row[total]) / count, 6) if count else 0.0
            return row

        snapshot: dict[str, Any] = {
            name: summarize(values) for name, values in self._label_stats.items()
        }
        snapshot["segments"] = {
            key: summarize(values) for key, values in self._segment_stats.items()
        }
        # Expose the primary horizons directly for compact diagnostic comparisons.
        snapshot["by_horizon"] = {
            key.split("=", 1)[1]: summarize(values)
            for key, values in self._segment_stats.items()
            if key.startswith("horizon=")
        }
        commission_pct, slippage_bps = _cost_settings()
        costs_configured = commission_pct > 0.0 and slippage_bps > 0.0
        snapshot["cost_model"] = {
            "commission_pct_per_side": commission_pct,
            "slippage_bps": slippage_bps,
            "status": "CONFIGURED" if costs_configured else "INCOMPLETE",
            "note": (
                "Both costs are configured; verify commission against the active Finam tariff."
                if costs_configured else
                "Net returns are not fully calibrated: configure the actual Finam commission and a justified additional-slippage assumption."
            ),
        }
        return snapshot

    def pending_count(self, ticker: str | None = None) -> int:
        if ticker is None:
            return sum(len(rows) for rows in self._pending.values())
        symbol = str(ticker or "").strip().upper().split("@")[0]
        return len(self._pending.get(symbol, []))
