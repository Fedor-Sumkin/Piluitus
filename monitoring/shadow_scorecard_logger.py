"""Logging for research-only strategy shadow scorecards.

This module reports completed shadow observations. It does not participate in
candidate approval, order sizing, risk checks, or execution.
"""
from __future__ import annotations

import logging


class StrategyShadowScorecardLogger:
    """Format and emit the periodic strategy-shadow research summary."""

    def __init__(self, logger: logging.Logger) -> None:
        self._logger = logger

    def log(self, tracker: object) -> None:
        shadow = tracker.statistics_snapshot()
        eligible = shadow["eligible"]
        ineligible = shadow["ineligible"]
        cost_model = shadow.get("cost_model", {})
        self._logger.info(
            "[STRATEGY SHADOW SCORECARD] cost_status=%s costs=commission %.4f%%/side slippage %.2fbps; "
            "eligible labels=%d ok=%d positive=%.1f%% avg_gross=%+.5f%% avg_net=%+.5f%% "
            "MFE=%+.5f%%@%.1fs MAE=%+.5f%%@%.1fs BBO_obs=%.1f span=%.1fs; "
            "ineligible labels=%d ok=%d positive=%.1f%% avg_gross=%+.5f%% avg_net=%+.5f%% "
            "MFE=%+.5f%%@%.1fs MAE=%+.5f%%@%.1fs BBO_obs=%.1f span=%.1fs; pending=%d",
            str(cost_model.get("status", "UNKNOWN")),
            float(cost_model.get("commission_pct_per_side", 0.0)),
            float(cost_model.get("slippage_bps", 0.0)),
            eligible["labels"], eligible["ok"], eligible["positive_rate_pct"],
            eligible["average_gross_return_pct"], eligible["average_net_return_pct"],
            eligible["average_mfe_pct"], eligible["average_time_to_mfe_sec"],
            eligible["average_mae_pct"], eligible["average_time_to_mae_sec"],
            eligible["average_valid_bbo_observations"], eligible["average_bbo_observation_span_sec"],
            ineligible["labels"], ineligible["ok"], ineligible["positive_rate_pct"],
            ineligible["average_gross_return_pct"], ineligible["average_net_return_pct"],
            ineligible["average_mfe_pct"], ineligible["average_time_to_mfe_sec"],
            ineligible["average_mae_pct"], ineligible["average_time_to_mae_sec"],
            ineligible["average_valid_bbo_observations"], ineligible["average_bbo_observation_span_sec"],
            tracker.pending_count(),
        )
        segments = shadow.get("segments", {})
        for side in ("BUY", "SELL"):
            row = segments.get(f"side={side}")
            if row and int(row.get("labels", 0)) > 0:
                self._logger.info(
                    "[STRATEGY SHADOW SIDE] side=%s labels=%d ok=%d positive=%.1f%% "
                    "avg_gross=%+.5f%% avg_net=%+.5f%% MFE=%+.5f%% MAE=%+.5f%%",
                    side, row["labels"], row["ok"], row["positive_rate_pct"],
                    row["average_gross_return_pct"], row["average_net_return_pct"],
                    row["average_mfe_pct"], row["average_mae_pct"],
                )
        ticker_rows = [
            (key, row) for key, row in segments.items()
            if key.startswith("ticker=") and int(row.get("ok", 0)) >= 5
        ]
        for key, row in sorted(ticker_rows, key=lambda item: int(item[1].get("ok", 0)), reverse=True)[:5]:
            self._logger.info(
                "[STRATEGY SHADOW TICKER] %s labels=%d ok=%d positive=%.1f%% avg_net=%+.5f%%",
                key, row["labels"], row["ok"], row["positive_rate_pct"], row["average_net_return_pct"],
            )
        for prefix in (
            "regime=", "market_regime=", "volatility_regime=", "liquidity_regime=",
            "directional_regime=", "cost_to_atr_bucket=", "score_band=", "horizon=",
            "hour_msk=", "hard_block=", "ofi_alignment_5s=", "side_ofi_alignment_5s=",
            "spread_atr_bucket=", "trade_count_bucket=", "flow_alignment=", "side_flow_alignment=",
            "market_regime_queue_alignment_5s=", "market_regime_flow_alignment_30s=",
            "market_regime_ofi_alignment_5s=", "market_regime_signal_confluence=",
            "llm_model_decision=", "llm_final_decision=", "llm_signal_status=",
            "llm_execution_status=", "llm_confidence_bucket=",
            "shadow_v2_eligibility=", "shadow_v2_score_band=",
        ):
            grouped = [
                (key, row) for key, row in segments.items()
                if key.startswith(prefix) and int(row.get("labels", 0)) >= 3
            ]
            for key, row in sorted(grouped, key=lambda item: int(item[1].get("labels", 0)), reverse=True)[:3]:
                self._logger.info(
                    "[STRATEGY SHADOW SEGMENT] %s labels=%d ok=%d positive=%.1f%% "
                    "avg_gross=%+.5f%% avg_net=%+.5f%% PF=%s",
                    key, row["labels"], row["ok"], row["positive_rate_pct"],
                    row["average_gross_return_pct"], row["average_net_return_pct"],
                    f'{row["profit_factor"]:.3f}' if row.get("profit_factor") is not None else "N/A",
                )
        
