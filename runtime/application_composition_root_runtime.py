"""High-level application composition root.

This module binds the already-created core, shared-state, PAPER-session and
market runtimes to the FastAPI lifecycle and route composition. It only wires
dependencies; domain behavior and trading policy stay in their owning modules.
"""
from __future__ import annotations

import time
from typing import Any, Callable

from config import LOT_SIZES, PROJECT_ROOT
from interface import tg_bot
from monitoring.background_tasks import BackgroundTaskSupervisor
from risk.risk_limits import validate_daily_loss_limit
from runtime.application_composition_runtime import ApplicationCompositionRuntime
from storage.db_manager import DBManager
from storage.paper_session_maintenance_runtime import PAPER_CLEAR_CONFIRMATION
from storage.shadow_outcomes import SHADOW_OUTCOMES_STORAGE_LIMIT
from strategy.bbo_replay import BBO_REPLAY_STORAGE_KEY, BBO_REPLAY_STORAGE_LIMIT
from strategy.bbo_replay_simulator import replay_exit_batch, replay_exit_scenarios
from strategy.paper_execution_reconciliation import build_actual_vs_replay_reconciliation
from strategy.paper_reconciliation import build_shadow_reconciliation
from strategy.paper_performance import (
    PAPER_CLOSED_TRADES_STORAGE_LIMIT,
    paper_performance_tracker,
)
from strategy.shadow_validation import (
    build_shadow_cost_sensitivity,
    build_shadow_walk_forward_validation,
)


class ApplicationCompositionRootRuntime:
    """Build FastAPI runtime dependency maps from composed application services."""

    def __init__(
        self,
        *,
        application_core_runtime: Any,
        application_shared_state_runtime: Any,
        paper_session_composition_runtime: Any,
        market_runtime_composition: Any,
        settings: Any,
        deposit: float,
        logger: Any,
        verdict_cache: list[dict[str, Any]],
        max_cache_size: int,
        set_verdict_cache: Callable[[list[dict[str, Any]]], None],
        set_market_task: Callable[[Any], None],
        advance_session_generation: Callable[[], None],
    ) -> None:
        self.application_core_runtime = application_core_runtime
        self.application_shared_state_runtime = application_shared_state_runtime
        self.paper_session_composition_runtime = paper_session_composition_runtime
        self.market_runtime_composition = market_runtime_composition
        self.settings = settings
        self.deposit = float(deposit)
        self.logger = logger
        self.verdict_cache = verdict_cache
        self.max_cache_size = int(max_cache_size)
        self.set_verdict_cache = set_verdict_cache
        self.set_market_task = set_market_task
        self.advance_session_generation = advance_session_generation

        self.application_composition_runtime: Any | None = None
        self._created = False

    def create(self) -> "ApplicationCompositionRootRuntime":
        """Bind application-facing dependencies and create the FastAPI app."""
        if self._created:
            raise RuntimeError(
                "ApplicationCompositionRootRuntime.create() may only run once."
            )

        core = self.application_core_runtime
        shared = self.application_shared_state_runtime
        paper_session = self.paper_session_composition_runtime
        market = self.market_runtime_composition
        dashboard = core.dashboard_application_runtime
        market_app = market.market_application_runtime
        observability = market.market_observability_runtime
        ingestion = market_app.ingestion_components
        background_services = market_app.market_background_services
        execution_engine = core.execution_engine
        db_manager = core.db_manager
        broker = core.broker
        ai_engine = core.ai_engine
        risk_manager = core.risk_manager
        screener = core.screener
        anomaly_queue = shared.anomaly_queue
        connection_manager = shared.connection_manager
        paper_shadow_labels = shared.paper_shadow_labels

        lifecycle_dependencies = {
            "logger": self.logger,
            "execution_engine": execution_engine,
            "deposit": self.deposit,
            "db_manager": db_manager,
            "paper_performance_tracker": paper_performance_tracker,
            "shadow_tracker": shared.shadow_tracker,
            "paper_shadow_labels": paper_shadow_labels,
            "risk_manager": risk_manager,
            "paper_bbo_replay_recorder_provider": (
                lambda: shared.bbo_replay_recorder
            ),
            "restore_shadow_outcomes": paper_session.restore_shadow_outcomes,
            "restore_paper_performance": paper_session.restore_paper_performance,
            "ai_engine": ai_engine,
            "quarantine_unreconciled_paper_positions": (
                lambda: execution_engine.quarantine_unreconciled_paper_positions(
                    db_manager
                )
            ),
            "max_cache_size": self.max_cache_size,
            "set_verdict_cache": self.set_verdict_cache,
            "worker_factory": lambda: market_app.market_worker_factory.create(
                db_manager, ai_engine, broker
            ),
            "finam_metadata_refresh_loop": (
                background_services._finam_metadata_refresh_loop
            ),
            "start_news_updater": background_services._start_news_updater,
            "start_tables_sync_loop": background_services._start_tables_sync_loop,
            "telegram_bot_module": tg_bot,
            "broker": broker,
            "market_broker_init": background_services.background_broker_init,
            "set_market_task": self.set_market_task,
            "background_operations": shared.background_operations,
            "trade_flow_maybe_log": observability.maybe_log_trade_flow,
            "task_supervisor_factory": BackgroundTaskSupervisor,
        }

        dashboard_route_dependencies = {
            "anomaly_queue": anomaly_queue,
            "priority_queue_item_factory": shared.priority_queue_item_factory,
            "validate_daily_loss_limit": validate_daily_loss_limit,
            "default_daily_loss_limit": self.settings.DAILY_LOSS_LIMIT_PCT,
            "template_dir": PROJECT_ROOT / "interface" / "templates",
            "manager": connection_manager,
            "last_ai_verdicts_cache": self.verdict_cache,
            "process_market_event": market_app.process_market_event,
        }

        paper_operations_dependencies = {
            "db_manager": db_manager,
            "is_paper_trade": DBManager.is_paper_trade,
            "execution_engine": execution_engine,
            "risk_manager": risk_manager,
            "screener": screener,
            "paper_performance_tracker": paper_performance_tracker,
            "shadow_tracker": shared.shadow_tracker,
            "paper_shadow_labels": paper_shadow_labels,
            "paper_session_lock": shared.paper_session_lock,
            "connection_manager": connection_manager,
            "get_open_positions": dashboard.get_dashboard_open_positions,
            "refresh_balance": dashboard.refresh_balance_snapshot,
            "anomaly_queue": anomaly_queue,
            "ai_candidate_deduplicator": ingestion.ai_candidate_deduplicator,
            "last_ai_verdicts_cache": self.verdict_cache,
            "trade_flow_counters": observability.trade_flow_counters,
            "pending_bbo_candidates": ingestion.pending_bbo_candidates,
            "pending_bbo_lock": ingestion.pending_bbo_lock,
            "position_order_book_diagnostics": (
                observability.position_order_book_diagnostics
            ),
            "bbo_replay_storage_key": BBO_REPLAY_STORAGE_KEY,
            "bbo_replay_storage_limit": BBO_REPLAY_STORAGE_LIMIT,
            "paper_closed_trades_storage_limit": PAPER_CLOSED_TRADES_STORAGE_LIMIT,
            "shadow_outcomes_storage_limit": SHADOW_OUTCOMES_STORAGE_LIMIT,
            "lot_sizes": LOT_SIZES,
            "parse_finite_float": market.parse_finite_float,
            "replay_exit_batch": replay_exit_batch,
            "replay_exit_scenarios": replay_exit_scenarios,
            "build_shadow_reconciliation": build_shadow_reconciliation,
            "build_actual_vs_replay_reconciliation": (
                build_actual_vs_replay_reconciliation
            ),
            "build_shadow_cost_sensitivity": build_shadow_cost_sensitivity,
            "build_shadow_walk_forward_validation": (
                build_shadow_walk_forward_validation
            ),
            "deposit": self.deposit,
            "logger": self.logger,
            "paper_clear_confirmation": PAPER_CLEAR_CONFIRMATION,
            "monotonic": time.monotonic,
        }

        paper_operations_callbacks = {
            "advance_session_generation": self.advance_session_generation,
            "reset_balance_guard_signature": dashboard.reset_balance_guard_signature,
            "rotate_bbo_replay_session": paper_session.rotate_bbo_replay_session,
            "get_bbo_replay_session_id": (
                lambda: shared.bbo_replay_recorder.session_id
            ),
        }

        composition_runtime = ApplicationCompositionRuntime(
            lifecycle_dependencies=lifecycle_dependencies,
            app_title="PILUITUS v35.2 — AI Trading Core",
            dashboard_application_runtime=dashboard,
            dashboard_route_dependencies=dashboard_route_dependencies,
            paper_operations_dependencies=paper_operations_dependencies,
            paper_operations_callbacks=paper_operations_callbacks,
            static_dir=PROJECT_ROOT / "static",
        ).create()

        self.application_composition_runtime = composition_runtime
        self._created = True
        return self
