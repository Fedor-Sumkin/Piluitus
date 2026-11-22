"""Shared dependency state for market-worker queue processing."""
from __future__ import annotations

import asyncio
import os
import time
from datetime import datetime
from typing import Any, Callable
from uuid import uuid4


class MarketWorkerProcessingBase:
    """Own the stable constructor and injected dependencies for processing."""

    def __init__(
        self,
        *,
        db: Any,
        ai: Any,
        anomaly_queue: Any,
        execution_engine: Any,
        risk_manager: Any,
        session_lock: Any,
        session_generation_provider: Callable[[], int],
        candidate_is_expired: Callable[..., bool],
        candidate_ttl_sec: float,
        trade_flow_increment: Callable[[str], None],
        logger: Any,
        settings: Any,
        validate_daily_loss_limit: Callable[[Any], float],
        parse_finite_float: Callable[[Any], float | None],
        background_operations: Any,
        log_trading_event: Callable[..., Any],
        attach_llm_rule_context: Callable[..., Any],
        build_llm_feature_snapshot: Callable[[Any], dict[str, Any]],
        build_llm_error_response: Callable[..., dict[str, Any]],
        process_llm_response: Callable[..., tuple[dict[str, Any], dict[str, Any]]],
        validate_post_llm_entry: Callable[..., dict[str, Any]],
        screener: Any,
        queue_flow_conflict_reason: Callable[..., str],
        prepare_entry_execution_plan: Callable[..., dict[str, Any]],
        lot_sizes: dict[str, int],
        trade_flow_guard_block: Callable[[str], None],
        finite_float: Callable[[Any], float | None],
        nonnegative_int: Callable[[Any], int | None],
        build_default_signal_reason: Callable[..., str],
        build_verdict_explanation: Callable[..., tuple[str, str]],
        classify_local_evidence_diagnostic: Callable[..., str],
        build_pre_llm_risk_block_verdict: Callable[..., dict[str, Any]],
        build_llm_verdict_record: Callable[..., dict[str, Any]],
        link_shadow_candidate_verdict: Callable[..., bool],
        shadow_tracker: Any,
        enrich_ai_analysis_with_verdict: Callable[..., None],
        verdict_cache: list[dict[str, Any]],
        max_cache_size: int,
        manager: Any,
        balance_snapshot: dict[str, Any],
        publish_market_verdict: Callable[..., Any],
        execute_and_publish: Callable[..., Any],
        release_ai_ticker: Callable[[str], None],
        paper_session_generation_lock: Any,
        getenv: Callable[..., str] = os.getenv,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Any] = asyncio.sleep,
        now_local: Callable[[], datetime] | None = None,
        uuid_factory: Callable[[], Any] = uuid4,
    ) -> None:
        self.db = db
        self.ai = ai
        self.anomaly_queue = anomaly_queue
        self.execution_engine = execution_engine
        self.risk_manager = risk_manager
        self.session_lock = session_lock
        self.session_generation_provider = session_generation_provider
        self.candidate_is_expired = candidate_is_expired
        self.candidate_ttl_sec = float(candidate_ttl_sec)
        self.trade_flow_increment = trade_flow_increment
        self.logger = logger
        self.settings = settings
        self.validate_daily_loss_limit = validate_daily_loss_limit
        self.parse_finite_float = parse_finite_float
        self.background_operations = background_operations
        self.log_trading_event = log_trading_event
        self.attach_llm_rule_context = attach_llm_rule_context
        self.build_llm_feature_snapshot = build_llm_feature_snapshot
        self.build_llm_error_response = build_llm_error_response
        self.process_llm_response = process_llm_response
        self.validate_post_llm_entry = validate_post_llm_entry
        self.screener = screener
        self.queue_flow_conflict_reason = queue_flow_conflict_reason
        self.prepare_entry_execution_plan = prepare_entry_execution_plan
        self.lot_sizes = lot_sizes
        self.trade_flow_guard_block = trade_flow_guard_block
        self.finite_float = finite_float
        self.nonnegative_int = nonnegative_int
        self.build_default_signal_reason = build_default_signal_reason
        self.build_verdict_explanation = build_verdict_explanation
        self.classify_local_evidence_diagnostic = classify_local_evidence_diagnostic
        self.build_pre_llm_risk_block_verdict = build_pre_llm_risk_block_verdict
        self.build_llm_verdict_record = build_llm_verdict_record
        self.link_shadow_candidate_verdict = link_shadow_candidate_verdict
        self.shadow_tracker = shadow_tracker
        self.enrich_ai_analysis_with_verdict = enrich_ai_analysis_with_verdict
        self.verdict_cache = verdict_cache
        self.max_cache_size = int(max_cache_size)
        self.manager = manager
        self.balance_snapshot = balance_snapshot
        self.publish_market_verdict = publish_market_verdict
        self.execute_and_publish = execute_and_publish
        self.release_ai_ticker = release_ai_ticker
        self.paper_session_generation_lock = paper_session_generation_lock
        self.getenv = getenv
        self.monotonic = monotonic
        self.sleep = sleep
        self.now_local = now_local or (lambda: datetime.now().astimezone())
        self.uuid_factory = uuid_factory
        self._last_risk_block_verdict_at = 0.0
