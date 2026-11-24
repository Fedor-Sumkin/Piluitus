"""Fresh-market observation and forward-horizon label generation for Shadow."""
from __future__ import annotations

import math
import time
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from .shadow_outcomes_primitives import _apply_adverse_slippage, _number, logger


class ShadowMarketObservationMixin:
    """Track executable excursions and emit labels only under the original rules."""

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

    @staticmethod
    def _excursion_label_fields(row: Mapping[str, Any]) -> dict[str, Any]:
        """Snapshot executable-side excursions and the quality of quote observations."""
        return {
            "mfe_executable_pct": round(_number(row, "mfe_executable_pct"), 6),
            "mae_executable_pct": round(_number(row, "mae_executable_pct"), 6),
            "mfe_gross_executable_pct": round(_number(row, "mfe_gross_executable_pct"), 6),
            "mae_gross_executable_pct": round(_number(row, "mae_gross_executable_pct"), 6),
            "time_to_mfe_sec": round(_number(row, "time_to_mfe_sec"), 3),
            "time_to_mae_sec": round(_number(row, "time_to_mae_sec"), 3),
            "mfe_at_utc": row.get("mfe_at_utc"),
            "mae_at_utc": row.get("mae_at_utc"),
            "valid_bbo_observations": int(max(0.0, _number(row, "valid_bbo_observations"))),
            "first_valid_bbo_elapsed_sec": row.get("first_valid_bbo_elapsed_sec"),
            "last_valid_bbo_elapsed_sec": row.get("last_valid_bbo_elapsed_sec"),
            "bbo_observation_span_sec": round(_number(row, "bbo_observation_span_sec"), 3),
            "no_fresh_bbo_observations": int(max(0.0, _number(row, "no_fresh_bbo_observations"))),
        }

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
        observed_at_utc = datetime.now(timezone.utc).isoformat(timespec="milliseconds")

        for row in pending:
            elapsed = max(0.0, now_mono - float(row["created_monotonic"]))
            side = str(row["side"])
            exit_raw = quote.get("bid", 0.0) if side == "BUY" else quote.get("ask", 0.0)
            mid = (
                (quote["bid"] + quote["ask"]) / 2.0 if fresh else 0.0
            )
            if fresh:
                gross_pct, net_pct, exit_fill = self._executable_return_pct(row, float(exit_raw))
                row["valid_bbo_observations"] = int(row.get("valid_bbo_observations", 0)) + 1
                if row.get("first_valid_bbo_elapsed_sec") is None:
                    row["first_valid_bbo_elapsed_sec"] = round(elapsed, 3)
                row["last_valid_bbo_elapsed_sec"] = round(elapsed, 3)
                row["bbo_observation_span_sec"] = round(
                    max(0.0, elapsed - float(row["first_valid_bbo_elapsed_sec"])), 3
                )
                if net_pct > float(row["mfe_executable_pct"]):
                    row["mfe_executable_pct"] = net_pct
                    row["time_to_mfe_sec"] = round(elapsed, 3)
                    row["mfe_at_utc"] = observed_at_utc
                if net_pct < float(row["mae_executable_pct"]):
                    row["mae_executable_pct"] = net_pct
                    row["time_to_mae_sec"] = round(elapsed, 3)
                    row["mae_at_utc"] = observed_at_utc
                if gross_pct > float(row["mfe_gross_executable_pct"]):
                    row["mfe_gross_executable_pct"] = gross_pct
                if gross_pct < float(row["mae_gross_executable_pct"]):
                    row["mae_gross_executable_pct"] = gross_pct
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
                        **self._excursion_label_fields(row),
                        "ticker": symbol,
                        "side": side,
                        "entry_time_utc": row["created_at_utc"],
                        "entry_time_msk": row["created_at_msk"],
                        "entry_hour_msk": row["entry_hour_msk"],
                        "result": "OK",
                        "label_quality": label_quality,
                        "horizon_sec": horizon,
                        "elapsed_sec": round(elapsed, 3),
                        "excursion_observation_end_elapsed_sec": round(elapsed, 3),
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
                        "score": row["score"],
                        "shadow_eligible": row["shadow_eligible"],
                        "shadow_v2_variant": row.get("shadow_v2_variant", "LEGACY_NOT_AVAILABLE"),
                        "shadow_v2_score": row.get("shadow_v2_score"),
                        "shadow_v2_eligible": row.get("shadow_v2_eligible"),
                        "shadow_v2_adjustments": dict(row.get("shadow_v2_adjustments") or {}),
                        "regime": row["regime"],
                        "market_regime": row.get("market_regime", "UNKNOWN"),
                        "volatility_regime": row.get("volatility_regime", "UNKNOWN"),
                        "liquidity_regime": row.get("liquidity_regime", "UNKNOWN"),
                        "directional_regime": row.get("directional_regime", "UNKNOWN"),
                        "atr_reference_pct": row.get("atr_reference_pct"),
                        "atr_reference_samples": row.get("atr_reference_samples", 0),
                        "atr_vs_reference_ratio": row.get("atr_vs_reference_ratio"),
                        "minimum_favorable_move_pct": row.get("minimum_favorable_move_pct"),
                        "cost_to_atr_ratio": row.get("cost_to_atr_ratio"),
                        "entry_economics": dict(row.get("entry_economics") or {}),
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
                        **self._excursion_label_fields(row),
                        "ticker": symbol,
                        "side": side,
                        "entry_time_utc": row["created_at_utc"],
                        "entry_time_msk": row["created_at_msk"],
                        "entry_hour_msk": row["entry_hour_msk"],
                        "result": "LABEL_LATE" if fresh else "NO_FRESH_BBO",
                        "label_quality": "LABEL_LATE" if fresh else "NO_FRESH_BBO",
                        "horizon_sec": horizon,
                        "elapsed_sec": round(elapsed, 3),
                        "excursion_observation_end_elapsed_sec": round(elapsed, 3),
                        "label_lag_sec": round(max(0.0, elapsed - horizon), 3),
                        "score": row["score"],
                        "shadow_eligible": row["shadow_eligible"],
                        "shadow_v2_variant": row.get("shadow_v2_variant", "LEGACY_NOT_AVAILABLE"),
                        "shadow_v2_score": row.get("shadow_v2_score"),
                        "shadow_v2_eligible": row.get("shadow_v2_eligible"),
                        "shadow_v2_adjustments": dict(row.get("shadow_v2_adjustments") or {}),
                        "regime": row["regime"],
                        "market_regime": row.get("market_regime", "UNKNOWN"),
                        "volatility_regime": row.get("volatility_regime", "UNKNOWN"),
                        "liquidity_regime": row.get("liquidity_regime", "UNKNOWN"),
                        "directional_regime": row.get("directional_regime", "UNKNOWN"),
                        "atr_reference_pct": row.get("atr_reference_pct"),
                        "atr_reference_samples": row.get("atr_reference_samples", 0),
                        "atr_vs_reference_ratio": row.get("atr_vs_reference_ratio"),
                        "minimum_favorable_move_pct": row.get("minimum_favorable_move_pct"),
                        "cost_to_atr_ratio": row.get("cost_to_atr_ratio"),
                        "entry_economics": dict(row.get("entry_economics") or {}),
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
                        "observed_bid": quote.get("bid"),
                        "observed_ask": quote.get("ask"),
                        "observed_bid_age_sec": quote.get("bid_age_sec"),
                        "observed_ask_age_sec": quote.get("ask_age_sec"),
                        "bbo_max_age_sec": quote.get("max_age_sec"),
                        "bid_ask_available": quote.get("bid_ask_available"),
                        "spread_available": quote.get("spread_available"),
                        "observed_spread_pct": quote.get("spread_pct"),
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
