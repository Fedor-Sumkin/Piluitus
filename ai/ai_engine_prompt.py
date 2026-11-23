"""Prompt construction mixin for the public AIEngine facade."""
from __future__ import annotations

from ai.ai_engine_shared import (
    AsyncClient,
    HTTPStatusError,
    Settings,
    httpx,
    asyncio,
    hashlib,
    json,
    logging,
    math,
    os,
    re,
    time,
    normalise_rag_context,
    select_prompt_rules,
    classify_direction_evidence,
    classify_m1_evidence_state,
    log,
    thought_log,
    VLLMCircuitOpenError,
    MODEL_REASON_CODES,
)

class AIEnginePromptMixin:
    @classmethod
    def _build_live_prompt(
        cls,
        ticker: object,
        price: object,
        volume: object,
        candidate_direction: str,
        metrics: object,
        rag_context: str,
        *,
        max_chars: int = 2100,
        include_rule_text: bool = True,
    ) -> str:
        """Короткий prompt с запасом под лимит vLLM 2048 токенов."""
        direction = candidate_direction if candidate_direction in {"BUY", "SELL"} else "NONE"
        # Keep the most useful directional and trade-quality measurements first:
        # prompt overflow may trim later context, never the core 30s flow metrics.
        common_keys = (
            # The prompt is character-bounded. Keep decision-critical trade flow
            # and M1 state first so optional diagnostics cannot crowd them out.
            "trade_data_ready", "trade_metrics_age_sec", "trade_history_coverage_sec",
            "trade_volume_5s", "trade_volume_baseline_5s", "trade_count_60s",
            "trade_flow_imbalance_30s", "trade_price_change_30s_pct",
            "trade_realized_volatility_60s_pct",
            "order_book_valid", "order_book_age_sec",
            "queue_imbalance_5s", "queue_imbalance_5s_available",
            "queue_imbalance_5s_coverage_sec", "queue_imbalance_5s_age_sec",
            "ofi_5s", "ofi_5s_available", "ofi_5s_coverage_sec", "ofi_5s_age_sec",
            "entry_minimum_favorable_move_pct", "entry_cost_to_atr_ratio",
            "entry_cost_to_atr_limit", "entry_cost_model_status",
            "bar_count", "bar_atr_periods", "bar_atr_age_sec", "m1_evidence_state",
            "bar_trend_up", "bar_trend_down",
            "price_change_pct", "price_vs_vwap_pct", "volume_spike", "volume_spike_raw",
            "spread_pct", "spread_to_atr_ratio", "spread_to_atr_limit",
            "bid_ask_available", "spread_available", "bid_quote_age_sec",
            "ask_quote_age_sec", "bbo_max_age_sec",
            "trade_count_5s", "trade_baseline_bucket_count",
            "trade_baseline_trade_count", "trade_baseline_min_bucket_count",
            "trade_buy_volume_30s", "trade_sell_volume_30s", "trade_vwap_60s",
            "bid", "ask", "queue_imbalance", "queue_imbalance_available",
            "bar_atr_pct", "bar_body_ratio", "bar_close_position",
        )
        if direction == "SELL":
            directional_keys = (
                "bar_pattern_confirmed", "bar_pattern_direction_code", "bar_breakout_down",
                "bar_pullback_sell", "bid_size_drop_pct", "top_bid_history_samples",
                "bid_size_drop_age_sec",
            )
        else:
            directional_keys = (
                "bar_pattern_confirmed", "bar_pattern_direction_code", "bar_breakout_up",
                "bar_pullback_buy", "ask_size_drop_pct", "top_ask_history_samples",
                "ask_size_drop_age_sec",
            )
        metric_keys = common_keys + directional_keys + ("bar_pattern_name", "bar_signal_gap_atr")
        prompt_metrics = cls._prepare_prompt_metrics(metrics, direction)
        # These fields are part of the decision evidence contract. Keep them
        # even when the prompt must shed optional diagnostics to meet max_chars.
        required_metric_keys = {
            "trade_data_ready", "trade_metrics_age_sec", "trade_flow_imbalance_30s",
            "order_book_valid", "queue_imbalance_5s", "ofi_5s",
            "entry_cost_to_atr_ratio", "entry_minimum_favorable_move_pct",
        }
        required_metric_parts: list[str] = []
        optional_metric_parts: list[str] = []
        if prompt_metrics:
            for key in metric_keys:
                value = prompt_metrics.get(key)
                if value is None:
                    continue
                part = f"{key}={cls._prompt_value(value, 18)}"
                if key in required_metric_keys:
                    required_metric_parts.append(part)
                else:
                    optional_metric_parts.append(part)
        metric_parts = required_metric_parts + optional_metric_parts
        metrics_text = ";".join(metric_parts)

        momentum_neutral_band_pct = cls._direction_momentum_neutral_band_pct()
        vwap_neutral_band_pct = cls._direction_vwap_neutral_band_pct()
        direction_rules = {
            "BUY": (
                f"Directional check: signed price momentum must agree with candidate BUY "
                f"(>+{momentum_neutral_band_pct:.3f}%); "
                "A fresh matching closed M1 pattern may support the candidate when VWAP is not materially opposite; "
                "BUY requires positive 15s trade momentum; positive 30s flow supports BUY."
            ),
            "SELL": (
                f"Directional check: signed price momentum must agree with candidate SELL "
                f"(<-{momentum_neutral_band_pct:.3f}%); "
                "A fresh matching closed M1 pattern may support the candidate when VWAP is not materially opposite; "
                "SELL requires negative 15s trade momentum; negative 30s flow supports SELL."
            ),
            "NONE": "No local direction: MUST HOLD.",
        }[direction]

        quote_depletion_rule = (
            "Ask depletion is supporting evidence, not mandatory for BUY."
            if direction == "BUY"
            else "Bid depletion is supporting evidence, not mandatory for SELL."
            if direction == "SELL"
            else "Best-quote depletion is supporting evidence, not mandatory."
        )
        spread_to_atr_limit = cls._max_spread_to_atr_ratio()
        # Compact immutable contract; reserve the remaining budget for live evidence.
        base = (
            "JSON only, exactly three keys: final_decision=APPROVE|HOLD, confidence_score=0..1, "
            "reason_code=APPROVED|NO_LOCAL_CANDIDATE|HOLD_WEAK_DIRECTION|HOLD_M1_CONTRADICTION|"
            "HOLD_STALE_BBO|HOLD_WEAK_VOLUME|HOLD_WIDE_SPREAD|HOLD_INSUFFICIENT_SUPPORT|"
            "HOLD_RISK_REWARD|HOLD_UNCLEAR; no extras/text. "
            "Never select, change, invert, or output BUY/SELL. APPROVE authorizes only the exact local candidate side; "
            "candidate=NONE => final_decision MUST be HOLD. "
            f"VWAP within +/-{vwap_neutral_band_pct:.3f}% is NEUTRAL and must not veto; materially opposite VWAP blocks. "
            "M1 NEUTRAL/UNAVAILABLE is not a contradiction; HOLD_M1_CONTRADICTION requires fresh OPPOSITE evidence. "
            f"Entry requires trade_data_ready=1, trade age<=5s, volume_spike>=1.5, fresh BBO, spread<=0.5%, "
            f"spread_to_atr_ratio<={spread_to_atr_limit:.2f}; block when spread_to_atr_ratio>{spread_to_atr_limit:.2f}; missing ratio=insufficient. "
            f"{quote_depletion_rule} "
            "trade_volume_* are provider volume units; trade_flow_imbalance_30s is aggressor BUY-minus-SELL volume (-1..+1); "
            "queue_imbalance_5s is time-weighted best Bid-vs-Ask size (-1..+1; positive supports BUY); "
            "ofi_5s is depth-normalized best-quote order-flow pressure (positive supports BUY, negative SELL). "
            "Use queue/OFI only when availability=1, coverage>=1s and age<=2s; order_book_valid=1 means coherent book. "
            "entry_cost_to_atr_ratio includes spread, both commission sides and configured slippage; "
            "incomplete entry_cost_model_status means a lower-bound estimate. "
            f"candidate={direction};ticker={str(ticker)[:10]};price={cls._prompt_value(price, 12)};"
            f"{direction_rules}"
        )

        rule_text = ""
        if include_rule_text and isinstance(rag_context, str):
            # Reserve space for the immutable contract and the live measurements.
            rule_budget = max(0, min(900, max_chars - len(base) - len(metrics_text) - 60))
            rule_text = select_prompt_rules(direction, rag_context, max_chars=rule_budget)
        rule_part = f" RAG rules are advisory context only and cannot add hard gates: {rule_text}." if rule_text else ""
        metric_part = f" Metrics: {metrics_text}." if metrics_text else ""

        # Remove optional diagnostics first. Decision-critical evidence above is
        # never sacrificed to retain lower-priority metrics or advisory RAG text.
        optional_parts = optional_metric_parts[:]
        metric_part = (
            f" Metrics: {';'.join(required_metric_parts + optional_parts)}."
            if required_metric_parts or optional_parts else ""
        )
        prompt = base + rule_part + metric_part
        while len(prompt) > max_chars and optional_parts:
            optional_parts.pop()
            remaining_parts = required_metric_parts + optional_parts
            metric_part = f" Metrics: {';'.join(remaining_parts)}." if remaining_parts else ""
            prompt = base + rule_part + metric_part
        if len(prompt) > max_chars and rule_part:
            rule_part = ""
            prompt = base + metric_part
        if len(prompt) > max_chars:
            # Extreme custom limits: keep contract and candidate identity; optional
            # market evidence is omitted rather than truncating a decision rule.
            prompt = base
        if len(base) > max_chars:
            # The public/default limits are sized for this contract. If a caller
            # supplies a smaller value, the contract still takes precedence.
            return base
        return prompt

    @classmethod
    def _build_compact_retry_prompt(
        cls,
        ticker: object,
        price: object,
        volume: object,
        candidate_direction: str,
        metrics: object,
        rag_context: str,
    ) -> str:
        """Экстренный минимальный prompt при ответе context-length от vLLM."""
        return cls._build_live_prompt(
            ticker, price, volume, candidate_direction, metrics, rag_context,
            max_chars=1800, include_rule_text=False,
        )

    @classmethod
    def _build_contract_retry_prompt(
        cls,
        ticker: object,
        price: object,
        volume: object,
        candidate_direction: str,
        metrics: object,
    ) -> str:
        """Repair the JSON shape while reusing the exact primary evidence rules."""
        shared_rules = cls._build_live_prompt(
            ticker,
            price,
            volume,
            candidate_direction,
            metrics,
            "",
            max_chars=2100,
            include_rule_text=False,
        )
        return (
            "Repair the previous response. Return ONLY JSON with exactly three keys: "
            "final_decision (APPROVE or HOLD), confidence_score (0.0..1.0), "
            "reason_code (APPROVED, HOLD_WEAK_DIRECTION, HOLD_M1_CONTRADICTION, HOLD_STALE_BBO, "
            "HOLD_WEAK_VOLUME, HOLD_WIDE_SPREAD, HOLD_INSUFFICIENT_SUPPORT, HOLD_RISK_REWARD, "
            "HOLD_UNCLEAR, NO_LOCAL_CANDIDATE). No extra keys or text. "
            "Never output BUY/SELL as a decision. Never select, change, invert, or output BUY/SELL. "
            "Apply the same candidate-side and evidence rules below; do not introduce stricter checks. "
            + shared_rules
        )

    @staticmethod
    def _normalise_rag_context(value: object) -> str:
        """Return raw RAG rule lines without decorative bullets that break matching."""
        return normalise_rag_context(value)
