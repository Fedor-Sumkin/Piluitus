import asyncio
import json
import re
import os
import sys
import time as time_module
import shutil
import unicodedata
from contextlib import asynccontextmanager
from typing import Any, List, Mapping, Union, cast
from aiogram import Bot 

from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, time
from uuid import uuid4
from types import SimpleNamespace
from config import (
    Settings,
    PROJECT_ROOT,
    TICKERS_TO_TRADE,
    FINAM_TRADE_LOT_SIZES,
    LOT_SIZES,
)
from risk.risk_limits import validate_daily_loss_limit

# --- PILUITUS‑V3 modules ------------------------------------------------
from storage.db_manager import DBManager
from runtime.application_core_runtime import ApplicationCoreRuntime
from runtime.paper_session_composition_runtime import PaperSessionCompositionRuntime
from storage.shadow_outcomes import SHADOW_OUTCOMES_STORAGE_LIMIT
from runtime.paper_operations_runtime import PAPER_CLEAR_CONFIRMATION
from runtime.priority_queue import PriorityItem, PriorityQueueItemFactory
from runtime.application_composition_runtime import ApplicationCompositionRuntime
from runtime.market_runtime_composition_runtime import MarketRuntimeCompositionRuntime
from strategy.shadow_outcomes import ShadowForwardTracker
from strategy.bbo_replay import (
    BBOReplayRecorder,
    BBO_REPLAY_STORAGE_KEY,
    BBO_REPLAY_STORAGE_LIMIT,
)
from strategy.bbo_replay_simulator import replay_exit_batch, replay_exit_scenarios
from strategy.paper_reconciliation import build_shadow_reconciliation
from strategy.paper_execution_reconciliation import build_actual_vs_replay_reconciliation
from strategy.shadow_validation import (
    build_shadow_cost_sensitivity,
    build_shadow_walk_forward_validation,
)
from strategy.paper_performance import (
    PAPER_CLOSED_TRADES_STORAGE_KEY,
    PAPER_CLOSED_TRADES_STORAGE_LIMIT,
    paper_performance_tracker,
)
from interface import tg_bot
from interface.websocket_manager import ConnectionManager
from ai.news_parser import MarketNewsParser

# --------------------------------------------------------------
#  ENVIRONMENT & INITIALIZATION
# --------------------------------------------------------------
load_dotenv()

from monitoring.background_tasks import BackgroundOperationTracker
from monitoring.background_tasks import BackgroundTaskSupervisor
from monitoring.logging_setup import logger, terminal_status




FINAM_TOKEN = os.getenv("FINAM_TOKEN", "")
# PAPER orders are recorded against an internal simulator identity, never a Finam client/account ID.
CLIENT_ID = "PILUITUS_PAPER" if Settings.PAPER_TRADING else os.getenv("FINAM_CLIENT_ID", "UNKNOWN_ACCOUNT")
DEPOSIT = float(Settings.TRADE_DEPOSIT)

# --------------------------------------------------------------
#  1️⃣  GLOBAL OBJECTS & PRIORITIZED CONTOUR
# --------------------------------------------------------------
application_core_runtime = ApplicationCoreRuntime(
    settings=Settings,
    finam_token=FINAM_TOKEN,
    deposit=DEPOSIT,
    lot_sizes=LOT_SIZES,
    logger=logger,
    set_balance_snapshot=lambda snapshot: globals().__setitem__(
        "_dashboard_balance_snapshot", snapshot
    ),
).create()

db_manager = application_core_runtime.db_manager
ai_engine = application_core_runtime.ai_engine
broker = application_core_runtime.broker
screener = application_core_runtime.screener
execution_engine = application_core_runtime.execution_engine
risk_manager = application_core_runtime.risk_manager
dashboard_application_runtime = application_core_runtime.dashboard_application_runtime

dashboard_runtime = dashboard_application_runtime.dashboard_runtime
_dashboard_balance_snapshot = dashboard_application_runtime.balance_snapshot

# Preserve stable providers used by risk, market, PAPER APIs and the dashboard.
_current_paper_open_pnl = dashboard_application_runtime.current_paper_open_pnl
_current_paper_assets_cost = dashboard_application_runtime.current_paper_assets_cost

# Глобальные кэши и лимиты ОЗУ под High-Load тики
last_ai_verdicts_cache: list[dict[str, Any]] = []
MAX_CACHE_SIZE = 100  # Защита ОЗУ от утечек памяти

# Асинхронная очередь с приоритетами. Второй элемент — уникальный sequence ID:
# PriorityQueue сравнивает tuple поэлементно; без уникального ID при совпадении
# priority/timestamp/event_type Python пытался сравнивать payload-словари и падал
# с TypeError: '<' not supported between instances of 'dict' and 'dict'.
anomaly_queue: asyncio.PriorityQueue[PriorityItem] = asyncio.PriorityQueue(maxsize=1000)
priority_queue_item_factory = PriorityQueueItemFactory()
# Serializes order publication with PAPER maintenance; generation invalidates old queued/in-flight AI events.
_paper_session_lock = asyncio.Lock()
_paper_session_generation = 0
bg_market_task = None

# Forward labels are research-only and bounded in memory; all observations are logged.
scalping_shadow_tracker = ShadowForwardTracker()
paper_bbo_replay_recorder = BBOReplayRecorder()
execution_engine.bbo_replay_session_id = paper_bbo_replay_recorder.session_id
execution_engine.paper_exit_event_recorder = paper_bbo_replay_recorder.record_exit_trigger
paper_shadow_labels: list[dict[str, Any]] = []

paper_session_composition_runtime = PaperSessionCompositionRuntime(
    db_manager=db_manager,
    execution_engine=execution_engine,
    shadow_tracker=scalping_shadow_tracker,
    bbo_replay_recorder=paper_bbo_replay_recorder,
    bbo_replay_recorder_factory=BBOReplayRecorder,
    paper_performance_tracker=paper_performance_tracker,
    paper_shadow_labels=paper_shadow_labels,
    logger=logger,
    closed_trades_storage_key=PAPER_CLOSED_TRADES_STORAGE_KEY,
    closed_trades_storage_limit=PAPER_CLOSED_TRADES_STORAGE_LIMIT,
    shadow_outcomes_storage_limit=SHADOW_OUTCOMES_STORAGE_LIMIT,
    set_bbo_replay_recorder=lambda recorder: globals().__setitem__(
        "paper_bbo_replay_recorder", recorder
    ),
).create()
shadow_outcome_persistence = paper_session_composition_runtime.shadow_outcome_persistence
paper_session_runtime = paper_session_composition_runtime.paper_session_runtime

# Keep the established callback names consumed by market and lifecycle runtimes.
_persist_shadow_outcomes = paper_session_composition_runtime.persist_shadow_outcomes
_restore_shadow_outcomes = paper_session_composition_runtime.restore_shadow_outcomes
_rotate_paper_bbo_replay_session = paper_session_composition_runtime.rotate_bbo_replay_session
_restore_paper_performance = paper_session_composition_runtime.restore_paper_performance


# Dashboard callbacks are owned by the application composition runtime.
_latest_screener_prices = dashboard_application_runtime.latest_screener_prices
_get_dashboard_open_positions = dashboard_application_runtime.get_dashboard_open_positions
_refresh_dashboard_balance_snapshot = dashboard_application_runtime.refresh_balance_snapshot


# Все 88 инструментов остаются подписанными для сбора сравнимой статистики.
# В LLM разрешается передавать только события от выбранного локальным рейтингом top-N.
screener.set_universe(TICKERS_TO_TRADE)

# --------------------------------------------------------------
#  2️⃣  НЕБЛОКИРУЮЩИЙ ДИСКОВЫЙ БОРТОВОЙ ЖУРНАЛ ТЕСТОВ
# --------------------------------------------------------------
_background_operations = BackgroundOperationTracker(logger)
# --------------------------------------------------------------
#  3️⃣  CONNECTION MANAGER (WebSocket)
# --------------------------------------------------------------
manager = ConnectionManager()
# --------------------------------------------------------------
#  4️⃣  ПОЛИМОРФНЫЙ ТИКЕРНЫЙ ОБРАБОТЧИК С АВТО-ЗАВЕРШЕНИЕМ ПОЗИЦИЙ
# --------------------------------------------------------------
# --------------------------------------------------------------
#  5️⃣  MARKET WORKER И ФОНОВЫЕ СЕРВИСЫ: ЕДИНЫЙ COMPOSITION ROOT
market_runtime_composition = MarketRuntimeCompositionRuntime(
    dependencies={
        "broker": broker,
        "screener": screener,
        "execution_engine": execution_engine,
        "db_manager": db_manager,
        "anomaly_queue": anomaly_queue,
        "queue_item_factory": priority_queue_item_factory,
        "shadow_tracker": scalping_shadow_tracker,
        "logger": logger,
        "terminal_status": terminal_status,
        "recorder_provider": lambda: paper_bbo_replay_recorder,
        "session_generation_provider": lambda: _paper_session_generation,
        "environment": os.environ,
        "client_id": CLIENT_ID,
        "ai_engine": ai_engine,
        "risk_manager": risk_manager,
        "session_lock": _paper_session_lock,
        "settings": Settings,
        "background_operations": _background_operations,
        "manager": manager,
        "get_open_positions": _get_dashboard_open_positions,
        "refresh_balance_snapshot": _refresh_dashboard_balance_snapshot,
        "refresh_dashboard_balance_snapshot": _refresh_dashboard_balance_snapshot,
        "verdict_cache": last_ai_verdicts_cache,
        "max_cache_size": MAX_CACHE_SIZE,
        "balance_snapshot": _dashboard_balance_snapshot,
        "tickers": TICKERS_TO_TRADE,
        "finam_trade_lot_sizes": FINAM_TRADE_LOT_SIZES,
    },
    paper_session_composition_runtime=paper_session_composition_runtime,
    monotonic=time_module.monotonic,
).create()

market_observability_runtime = market_runtime_composition.market_observability_runtime
market_application_runtime = market_runtime_composition.market_application_runtime
_parse_finite_float = market_runtime_composition.parse_finite_float

_trade_flow_counters = market_observability_runtime.trade_flow_counters
position_order_book_diagnostics = market_observability_runtime.position_order_book_diagnostics
shadow_scorecard_logger = market_observability_runtime.shadow_scorecard_logger
trade_flow_report_logger = market_observability_runtime.trade_flow_report_logger
strategy_shadow_diagnostics = market_observability_runtime.strategy_shadow_diagnostics
prefilter_block_logger = market_observability_runtime.prefilter_block_logger
_record_position_order_book_event = market_observability_runtime.record_position_order_book_event
_log_open_position_bbo_diagnostics = market_observability_runtime.log_open_position_bbo_diagnostics
_trade_flow_inc = market_observability_runtime.increment_trade_flow
_trade_flow_maybe_log = market_observability_runtime.maybe_log_trade_flow
_trade_flow_guard_block = market_observability_runtime.guard_block
_mark_shadow_llm_not_called = market_observability_runtime.mark_shadow_llm_not_called
_log_scalping_shadow = market_observability_runtime.log_shadow_candidate
_log_prefilter_blocked = market_observability_runtime.log_prefilter_blocked

market_ingestion_components = market_application_runtime.ingestion_components
market_worker_factory = market_application_runtime.market_worker_factory
market_background_services = market_application_runtime.market_background_services
_ai_candidate_deduplicator = market_ingestion_components.ai_candidate_deduplicator
_AI_CANDIDATE_TTL_SEC = market_ingestion_components.ai_candidate_ttl_sec
_pending_bbo_candidates = market_ingestion_components.pending_bbo_candidates
_pending_bbo_lock = market_ingestion_components.pending_bbo_lock
m1_history_warmup = market_ingestion_components.m1_history_warmup
pending_bbo_candidate_store = market_ingestion_components.pending_bbo_candidate_store
pending_bbo_runtime = market_ingestion_components.pending_bbo_runtime
market_auxiliary_event_router = market_ingestion_components.market_auxiliary_event_router
market_candidate_pipeline = market_ingestion_components.market_candidate_pipeline
market_data_event_runtime = market_ingestion_components.market_data_event_runtime
process_market_event = market_application_runtime.process_market_event

# Keep stable main-module helper names for callers and operator diagnostics.
_paper_queue_flow_conflict_reason = market_application_runtime.paper_queue_flow_conflict_reason
_reserve_ai_ticker = market_application_runtime.reserve_ai_ticker
_release_ai_ticker = market_application_runtime.release_ai_ticker
_warmup_selected_m1_history = market_application_runtime.warmup_selected_m1_history
_schedule_m1_history_warmup = market_application_runtime.schedule_m1_history_warmup
_drop_pending_bbo_candidate = market_application_runtime.drop_pending_bbo_candidate
_cancel_pending_bbo_candidate = market_application_runtime.cancel_pending_bbo_candidate
_expire_pending_bbo_candidates = market_application_runtime.expire_pending_bbo_candidates
_store_pending_bbo_candidate = market_application_runtime.store_pending_bbo_candidate
_resume_pending_bbo_candidate = market_application_runtime.resume_pending_bbo_candidate

#  6️⃣  ФОНОВЫЕ ЦИКЛЫ ОБНОВЛЕНИЯ И СИНХРОНИЗАЦИИ ТАБЛИЦ
market_background_services = market_application_runtime.market_background_services


background_broker_init = market_background_services.background_broker_init
_start_news_updater = market_background_services._start_news_updater
_finam_metadata_refresh_loop = market_background_services._finam_metadata_refresh_loop
_start_tables_sync_loop = market_background_services._start_tables_sync_loop


# --------------------------------------------------------------
#  7️⃣  FASTAPI APPLICATION: ЕДИНЫЙ COMPOSITION ROOT
# --------------------------------------------------------------
BASE_DIR = PROJECT_ROOT
TEMPLATE_DIR = BASE_DIR / "interface" / "templates"
STATIC_DIR = BASE_DIR / "static"


def _advance_paper_session_generation() -> None:
    global _paper_session_generation
    _paper_session_generation += 1


_reset_balance_guard_signature = dashboard_application_runtime.reset_balance_guard_signature

application_composition_runtime = ApplicationCompositionRuntime(
    lifecycle_dependencies={
        "logger": logger,
        "execution_engine": execution_engine,
        "deposit": DEPOSIT,
        "db_manager": db_manager,
        "paper_performance_tracker": paper_performance_tracker,
        "shadow_tracker": scalping_shadow_tracker,
        "paper_shadow_labels": paper_shadow_labels,
        "risk_manager": risk_manager,
        "paper_bbo_replay_recorder_provider": lambda: paper_bbo_replay_recorder,
        "restore_shadow_outcomes": _restore_shadow_outcomes,
        "restore_paper_performance": _restore_paper_performance,
        "ai_engine": ai_engine,
        "quarantine_unreconciled_paper_positions": lambda: execution_engine.quarantine_unreconciled_paper_positions(db_manager),
        "max_cache_size": MAX_CACHE_SIZE,
        "set_verdict_cache": lambda values: globals().__setitem__("last_ai_verdicts_cache", values),
        "worker_factory": lambda: market_worker_factory.create(db_manager, ai_engine, broker),
        "finam_metadata_refresh_loop": _finam_metadata_refresh_loop,
        "start_news_updater": _start_news_updater,
        "start_tables_sync_loop": _start_tables_sync_loop,
        "telegram_bot_module": tg_bot,
        "broker": broker,
        "market_broker_init": background_broker_init,
        "set_market_task": lambda task: globals().__setitem__("bg_market_task", task),
        "background_operations": _background_operations,
        "trade_flow_maybe_log": _trade_flow_maybe_log,
        "task_supervisor_factory": BackgroundTaskSupervisor,
    },
    app_title="PILUITUS v35.2 — AI Trading Core",
    dashboard_application_runtime=dashboard_application_runtime,
    dashboard_route_dependencies={
        "anomaly_queue": anomaly_queue,
        "priority_queue_item_factory": priority_queue_item_factory,
        "validate_daily_loss_limit": validate_daily_loss_limit,
        "default_daily_loss_limit": Settings.DAILY_LOSS_LIMIT_PCT,
        "template_dir": TEMPLATE_DIR,
        "manager": manager,
        "last_ai_verdicts_cache": last_ai_verdicts_cache,
        "process_market_event": process_market_event,
    },
    paper_operations_dependencies={
        "db_manager": db_manager,
        "is_paper_trade": DBManager.is_paper_trade,
        "execution_engine": execution_engine,
        "risk_manager": risk_manager,
        "screener": screener,
        "paper_performance_tracker": paper_performance_tracker,
        "shadow_tracker": scalping_shadow_tracker,
        "paper_shadow_labels": paper_shadow_labels,
        "paper_session_lock": _paper_session_lock,
        "connection_manager": manager,
        "get_open_positions": _get_dashboard_open_positions,
        "refresh_balance": _refresh_dashboard_balance_snapshot,
        "anomaly_queue": anomaly_queue,
        "ai_candidate_deduplicator": _ai_candidate_deduplicator,
        "last_ai_verdicts_cache": last_ai_verdicts_cache,
        "trade_flow_counters": _trade_flow_counters,
        "pending_bbo_candidates": _pending_bbo_candidates,
        "pending_bbo_lock": _pending_bbo_lock,
        "position_order_book_diagnostics": position_order_book_diagnostics,
        "bbo_replay_storage_key": BBO_REPLAY_STORAGE_KEY,
        "bbo_replay_storage_limit": BBO_REPLAY_STORAGE_LIMIT,
        "paper_closed_trades_storage_limit": PAPER_CLOSED_TRADES_STORAGE_LIMIT,
        "shadow_outcomes_storage_limit": SHADOW_OUTCOMES_STORAGE_LIMIT,
        "lot_sizes": LOT_SIZES,
        "parse_finite_float": market_runtime_composition.parse_finite_float,
        "replay_exit_batch": replay_exit_batch,
        "replay_exit_scenarios": replay_exit_scenarios,
        "build_shadow_reconciliation": build_shadow_reconciliation,
        "build_actual_vs_replay_reconciliation": build_actual_vs_replay_reconciliation,
        "build_shadow_cost_sensitivity": build_shadow_cost_sensitivity,
        "build_shadow_walk_forward_validation": build_shadow_walk_forward_validation,
        "deposit": DEPOSIT,
        "logger": logger,
        "paper_clear_confirmation": PAPER_CLEAR_CONFIRMATION,
        "monotonic": time_module.monotonic,
    },
    paper_operations_callbacks={
        "advance_session_generation": _advance_paper_session_generation,
        "reset_balance_guard_signature": _reset_balance_guard_signature,
        "rotate_bbo_replay_session": _rotate_paper_bbo_replay_session,
        "get_bbo_replay_session_id": lambda: paper_bbo_replay_recorder.session_id,
    },
    static_dir=STATIC_DIR,
).create()

application_lifecycle_runtime = application_composition_runtime.application_lifecycle_runtime
lifespan = application_composition_runtime.lifespan
app = application_composition_runtime.app
paper_operations_runtime = application_composition_runtime.paper_operations_runtime

paper_api_helpers = paper_operations_runtime.paper_api_helpers
paper_research_runtime = paper_operations_runtime.paper_research_runtime
paper_session_maintenance_runtime = paper_operations_runtime.paper_session_maintenance_runtime
_require_paper_mode = paper_operations_runtime.require_paper_mode
_paper_trade_rows = paper_operations_runtime.paper_trade_rows
_broadcast_paper_tables = paper_operations_runtime.broadcast_paper_tables
_broadcast_paper_session_reset = paper_operations_runtime.broadcast_paper_session_reset


if __name__ == "__main__":
    import uvicorn
    try:
        uvicorn.run(
            app,
            # Keep Uvicorn from reconfiguring/closing PILUITUS' durable file handlers.
            log_config=None,
            host="127.0.0.1",
            port=8080,
            reload=False,
            workers=1,
        )
    except KeyboardInterrupt:
        # Ctrl+C is an expected operator shutdown, not an application failure.
        logger.info("🛑 PILUITUS остановлен оператором (Ctrl+C).")
