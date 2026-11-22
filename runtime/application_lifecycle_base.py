"""Shared constructor and injected services for application lifecycle orchestration."""
from __future__ import annotations

from typing import Any, Callable


class ApplicationLifecycleBase:
    """Keep lifecycle dependency wiring in one stable constructor."""

    def __init__(
        self,
        *,
        logger: Any,
        execution_engine: Any,
        deposit: float,
        db_manager: Any,
        paper_performance_tracker: Any,
        shadow_tracker: Any,
        paper_shadow_labels: list[dict[str, Any]],
        risk_manager: Any,
        paper_bbo_replay_recorder_provider: Callable[[], Any],
        restore_shadow_outcomes: Callable[[], Any],
        restore_paper_performance: Callable[[], Any],
        ai_engine: Any,
        quarantine_unreconciled_paper_positions: Callable[[], Any],
        max_cache_size: int,
        set_verdict_cache: Callable[[list[dict[str, Any]]], None],
        worker_factory: Callable[[], Any],
        finam_metadata_refresh_loop: Callable[[], Any],
        start_news_updater: Callable[[], Any],
        start_tables_sync_loop: Callable[[], Any],
        telegram_bot_module: Any,
        broker: Any,
        market_broker_init: Callable[[], Any],
        set_market_task: Callable[[Any], None],
        background_operations: Any,
        trade_flow_maybe_log: Callable[..., Any],
        task_supervisor_factory: Callable[[Any], Any],
    ) -> None:
        self.logger = logger
        self.execution_engine = execution_engine
        self.deposit = float(deposit)
        self.db_manager = db_manager
        self.paper_performance_tracker = paper_performance_tracker
        self.shadow_tracker = shadow_tracker
        self.paper_shadow_labels = paper_shadow_labels
        self.risk_manager = risk_manager
        self.paper_bbo_replay_recorder_provider = paper_bbo_replay_recorder_provider
        self.restore_shadow_outcomes = restore_shadow_outcomes
        self.restore_paper_performance = restore_paper_performance
        self.ai_engine = ai_engine
        self.quarantine_unreconciled_paper_positions = quarantine_unreconciled_paper_positions
        self.max_cache_size = int(max_cache_size)
        self.set_verdict_cache = set_verdict_cache
        self.worker_factory = worker_factory
        self.finam_metadata_refresh_loop = finam_metadata_refresh_loop
        self.start_news_updater = start_news_updater
        self.start_tables_sync_loop = start_tables_sync_loop
        self.telegram_bot_module = telegram_bot_module
        self.broker = broker
        self.market_broker_init = market_broker_init
        self.set_market_task = set_market_task
        self.background_operations = background_operations
        self.trade_flow_maybe_log = trade_flow_maybe_log
        self.task_supervisor_factory = task_supervisor_factory
