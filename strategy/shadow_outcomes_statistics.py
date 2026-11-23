"""Restoration and aggregation of completed Shadow outcome labels."""
from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from .shadow_outcomes_primitives import _cost_settings, _number


class ShadowOutcomeStatisticsMixin:
    """Restore completed labels and expose cumulative bounded research statistics."""

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
        
        market_regime = str(label.get("market_regime") or "UNKNOWN")
        volatility_regime = str(label.get("volatility_regime") or "UNKNOWN")
        liquidity_regime = str(label.get("liquidity_regime") or "UNKNOWN")
        directional_regime = str(label.get("directional_regime") or "UNKNOWN")
        cost_to_atr = _number(label, "cost_to_atr_ratio", math.nan)
        if not math.isfinite(cost_to_atr) or cost_to_atr < 0.0:
            cost_to_atr_bucket = "unavailable"
        elif cost_to_atr <= 0.25:
            cost_to_atr_bucket = "le_0.25"
        elif cost_to_atr <= 0.50:
            cost_to_atr_bucket = "0.25-0.50"
        elif cost_to_atr <= 1.0:
            cost_to_atr_bucket = "0.50-1.00"
        else:
            cost_to_atr_bucket = "gt_1.00"

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
            f"market_regime={market_regime}",
            f"volatility_regime={volatility_regime}",
            f"liquidity_regime={liquidity_regime}",
            f"directional_regime={directional_regime}",
            f"cost_to_atr_bucket={cost_to_atr_bucket}",
            f"side_market_regime={side}|{market_regime}",
            f"side_cost_to_atr_bucket={side}|{cost_to_atr_bucket}",
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
            f"market_regime_queue_alignment_5s={market_regime}|{side}|{queue_alignment}",
            f"market_regime_flow_alignment_30s={market_regime}|{side}|{flow_alignment}",
            f"market_regime_ofi_alignment_5s={market_regime}|{side}|{ofi_alignment}",
            f"market_regime_signal_confluence={market_regime}|{side}|Q={queue_alignment}|FLOW={flow_alignment}|OFI={ofi_alignment}",
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
        v2_eligible = label.get("shadow_v2_eligible")
        if isinstance(v2_eligible, bool):
            v2_state = "ELIGIBLE" if v2_eligible else "INELIGIBLE"
            v2_keys = [f"shadow_v2_eligibility={v2_state}"]
            v2_score = _number(label, "shadow_v2_score", math.nan)
            if math.isfinite(v2_score):
                v2_keys.append(f"shadow_v2_score_band={self._score_band(v2_score)}")
            segment_keys = tuple(v2_keys) + segment_keys

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
                ("average_time_to_mfe_sec", "mfe_time_sum_sec"),
                ("average_time_to_mae_sec", "mae_time_sum_sec"),
                ("average_bbo_observation_span_sec", "bbo_observation_span_sum_sec"),
            ):
                row[metric] = round(float(row[total]) / count, 3 if metric.endswith("_sec") else 6) if count else 0.0
            row["average_valid_bbo_observations"] = (
                round(int(row["valid_bbo_observations_sum"]) / count, 2) if count else 0.0
            )
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
