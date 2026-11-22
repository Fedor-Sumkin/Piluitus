"""Composition root for market observability and market runtime dependencies.

This adapter gathers the established strategy, execution and monitoring callbacks
and wires them into MarketApplicationRuntime. It does not implement trading
policy; all decision behavior remains in its domain modules.
"""
from __future__ import annotations

import time
from collections.abc import Callable, Mapping
from typing import Any

from ai.dashboard_verdicts import (
    build_verdict_explanation,
    finite_float,
    nonnegative_int,
    normalise_execution_status,
)
from execution.outcome_publisher import publish_execution_outcome
from execution.outcome_reporting import normalize_execution_fill
from execution.verdict_publisher import publish_market_verdict
from monitoring.market_observability_runtime import MarketObservabilityRuntime
from monitoring.trade_flow_diagnostics import guard_reason_bucket
from monitoring.trading_event_logger import log_trading_event_to_file
from monitoring.logging_setup import update_numpy_impulse_status
from portfolio.paper_accounting import parse_finite_float
from risk.risk_limits import validate_daily_loss_limit
from runtime.market_application_runtime import MarketApplicationRuntime
from runtime.market_event_normalizer import build_market_candidate_payload
from strategy.candidate_dedup import candidate_is_expired
from strategy.candidate_recovery import refresh_candidate_bbo_metrics
from strategy.decision_diagnostics import build_llm_feature_snapshot
from strategy.entry_decision_pipeline import prepare_entry_execution_plan
from strategy.llm_context import attach_llm_rule_context
from strategy.llm_response import build_llm_error_response, process_llm_response
from strategy.post_llm_validation import validate_post_llm_entry
from strategy.scalping_strategy import evaluate_scalping_candidate
from strategy.signal_guard import (
    classify_direction_evidence,
    infer_candidate_direction,
    validate_atr_readiness,
    validate_entry_evidence,
)
from strategy.verdict_records import (
    build_default_signal_reason,
    build_llm_verdict_record,
    build_pre_llm_risk_block_verdict,
    classify_local_evidence_diagnostic,
    enrich_ai_analysis_with_verdict,
    link_shadow_candidate_verdict,
)


class MarketRuntimeCompositionRuntime:
    """Wire observation services and their callbacks into the market runtime."""

    def __init__(
        self,
        *,
        dependencies: Mapping[str, Any],
        paper_session_composition_runtime: Any,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.dependencies = dict(dependencies)
        self.paper_session_composition_runtime = paper_session_composition_runtime
        self.monotonic = monotonic
        self.parse_finite_float = parse_finite_float
        self.market_observability_runtime: Any | None = None
        self.market_application_runtime: Any | None = None

    def _pending_bbo_count(self) -> int:
        """Read pending-BBO count after ingestion has been composed."""
        runtime = self.market_application_runtime
        components = getattr(runtime, "ingestion_components", None)
        store = getattr(components, "pending_bbo_candidate_store", None)
        count = getattr(store, "count", None)
        return int(count()) if callable(count) else 0

    def _build_callbacks(self) -> dict[str, Any]:
        if self.market_observability_runtime is None:
            raise RuntimeError(
                "Market observability must be created before market callbacks."
            )
        observability = self.market_observability_runtime
        execution_engine = self.dependencies["execution_engine"]
        return {
            "trade_flow_increment": observability.increment_trade_flow,
            "guard_reason_bucket": guard_reason_bucket,
            "record_position_order_book_event": observability.record_position_order_book_event,
            "refresh_bbo_metrics": refresh_candidate_bbo_metrics,
            "persist_shadow_outcomes": self.paper_session_composition_runtime.persist_shadow_outcomes,
            "infer_candidate_direction": infer_candidate_direction,
            "paper_mode_provider": lambda: bool(
                getattr(execution_engine, "paper_mode", False)
            ),
            "validate_entry_evidence": validate_entry_evidence,
            "validate_atr_readiness": validate_atr_readiness,
            "mark_shadow_llm_not_called": observability.mark_shadow_llm_not_called,
            "log_shadow_candidate": observability.log_shadow_candidate,
            "trade_flow_guard_block": observability.guard_block,
            "log_prefilter_blocked": observability.log_prefilter_blocked,
            "update_numpy_impulse_status": update_numpy_impulse_status,
            "build_candidate_payload": build_market_candidate_payload,
            "classify_direction": classify_direction_evidence,
            "candidate_is_expired": candidate_is_expired,
            "normalise_execution_status": normalise_execution_status,
            "nonnegative_int": nonnegative_int,
            "normalize_execution_fill": normalize_execution_fill,
            "publish_execution_outcome": publish_execution_outcome,
            "validate_daily_loss_limit": validate_daily_loss_limit,
            "parse_finite_float": parse_finite_float,
            "log_trading_event": log_trading_event_to_file,
            "attach_llm_rule_context": attach_llm_rule_context,
            "build_llm_feature_snapshot": build_llm_feature_snapshot,
            "build_llm_error_response": build_llm_error_response,
            "process_llm_response": process_llm_response,
            "validate_post_llm_entry": validate_post_llm_entry,
            "prepare_entry_execution_plan": prepare_entry_execution_plan,
            "finite_float": finite_float,
            "build_default_signal_reason": build_default_signal_reason,
            "build_verdict_explanation": build_verdict_explanation,
            "classify_local_evidence_diagnostic": classify_local_evidence_diagnostic,
            "build_pre_llm_risk_block_verdict": build_pre_llm_risk_block_verdict,
            "build_llm_verdict_record": build_llm_verdict_record,
            "link_shadow_candidate_verdict": link_shadow_candidate_verdict,
            "enrich_ai_analysis_with_verdict": enrich_ai_analysis_with_verdict,
            "publish_market_verdict": publish_market_verdict,
        }

    def create(self) -> "MarketRuntimeCompositionRuntime":
        """Compose telemetry first, then the ingestion/worker/background graph."""
        if self.market_application_runtime is not None:
            raise RuntimeError(
                "MarketRuntimeCompositionRuntime.create() may only run once."
            )

        dependencies = self.dependencies
        self.market_observability_runtime = MarketObservabilityRuntime(
            logger=dependencies["logger"],
            shadow_tracker=dependencies["shadow_tracker"],
            execution_engine=dependencies["execution_engine"],
            screener=dependencies["screener"],
            parse_finite_float=parse_finite_float,
            guard_reason_bucket=guard_reason_bucket,
            evaluate_candidate=evaluate_scalping_candidate,
            pending_bbo_count_provider=self._pending_bbo_count,
            monotonic=self.monotonic,
        )
        self.market_application_runtime = MarketApplicationRuntime(
            dependencies=dependencies,
            callbacks=self._build_callbacks(),
        ).create()
        return self
