"""Research-only strategy shadow event diagnostics.

This adapter records shadow observations and emits their diagnostics. It does
not participate in the baseline candidate or order-execution path.
"""
from __future__ import annotations

import logging
import math
from collections.abc import Callable, Mapping
from typing import Any


class StrategyShadowDiagnostics:
    """Coordinate shadow-only tracker updates, counters and log formatting."""

    def __init__(
        self,
        logger: logging.Logger,
        tracker: Any,
        increment_counter: Callable[[str, int], None],
        guard_reason_bucket: Callable[[str], str],
        evaluate_candidate: Callable[[str, Mapping[str, Any]], dict[str, Any]],
    ) -> None:
        self._logger = logger
        self._tracker = tracker
        self._increment_counter = increment_counter
        self._guard_reason_bucket = guard_reason_bucket
        self._evaluate_candidate = evaluate_candidate

    def mark_llm_not_called(self, candidate_id: object, reason_code: str) -> None:
        """Keep known pre-LLM rejects distinct from missing candidate/verdict links."""
        identifier = str(candidate_id or "").strip()
        if not identifier:
            return
        if self._tracker.mark_llm_not_called(identifier, reason_code=reason_code):
            self._increment_counter("shadow_llm_not_called", 1)
            self._increment_counter(
                "shadow_llm_not_called_" + self._guard_reason_bucket(reason_code), 1
            )

    def log_candidate(
        self, ticker: str, direction: str, metrics: dict[str, Any]
    ) -> dict[str, Any]:
        """Evaluate and log a research-only strategy score."""
        result = self._evaluate_candidate(direction, metrics)
        observation_id = self._tracker.record_candidate(
            ticker, direction, metrics, result
        )
        result["candidate_id"] = observation_id
        self._increment_counter("strategy_shadow_evaluated", 1)
        self._increment_counter(
            "strategy_shadow_pass" if result.get("eligible") else "strategy_shadow_reject",
            1,
        )
        shadow_block_keys = {
            "bbo_unavailable": "shadow_block_bbo_unavailable",
            "bbo_stale": "shadow_block_bbo_stale",
            "order_book_not_confirmed": "shadow_block_order_book_not_confirmed",
            "atr_not_ready": "shadow_block_atr_not_ready",
            "spread_above_0.50_pct": "shadow_block_spread",
            "spread_unavailable_or_invalid": "shadow_block_spread_unavailable",
            "spread_atr_unavailable": "shadow_block_spread_atr_unavailable",
            "spread_to_atr_above_limit": "shadow_block_spread_atr_too_wide",
            "bbo_prices_invalid": "shadow_block_bbo_prices_invalid",
            "queue_trade_flow_conflict": "shadow_block_queue_trade_flow_conflict",
            "trade_flow_unavailable_or_stale": "shadow_block_trade_flow_unavailable",
        }
        observed_blocks = result.get("hard_blocks", [])
        counted_blocks: set[str] = set()
        unknown_block_count = 0
        for block in observed_blocks:
            counter = shadow_block_keys.get(block)
            if counter:
                self._increment_counter(counter, 1)
                counted_blocks.add(block)
            else:
                unknown_block_count += 1
        if unknown_block_count:
            self._increment_counter("shadow_block_other", unknown_block_count)
        raw_components = result.get("components", {})
        if isinstance(raw_components, dict):
            component_text = ",".join(
                f"{key}={float(value):.1f}"
                for key, value in sorted(raw_components.items())
                if isinstance(value, (int, float)) and math.isfinite(float(value))
            ) or "-"
        else:
            component_text = "-"
        self._logger.info(
            "🧪 [STRATEGY SHADOW] %s id=%s side=%s eligible=%s score=%.1f/%.0f regime=%s "
            "queue_now=%s queue_5s=%s trade_flow_30s=%s OFI_5s=%s/%s spread_ATR=%s components=%s hard_blocks=%s reasons=%s",
            ticker, observation_id or "-", direction, bool(result.get("eligible")),
            float(result.get("score", 0.0)), float(result.get("threshold", 70.0)),
            result.get("regime", "UNKNOWN"), result.get("queue_imbalance"),
            result.get("queue_imbalance_5s"), result.get("trade_flow_imbalance_30s"),
            result.get("ofi_5s"), result.get("ofi_directional_alignment"),
            result.get("spread_atr_ratio"), component_text,
            ",".join(result.get("hard_blocks", [])) or "-",
            ",".join(result.get("reasons", [])) or "-",
        )
        return result
