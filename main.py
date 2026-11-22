import asyncio
import json
import math
import re
import os
import sys
import time as time_module
import shutil
import unicodedata
from contextlib import asynccontextmanager
from typing import Any, List, Mapping, Union, cast
from aiogram import Bot 

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
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
from ai.ai_engine import AIEngine
from storage.db_manager import DBManager
from storage.shadow_outcomes import (
    ShadowOutcomePersistence,
    SHADOW_OUTCOMES_STORAGE_LIMIT,
)
from storage.paper_research_runtime import PaperResearchRuntime
from exchange.finam_client import FinamBrokerClient
from strategy.screener_funnel import HFTMemoryScreener
from execution.execution_layer import BaseExecutionEngine
from execution.outcome_reporting import normalize_execution_fill
from execution.outcome_publisher import publish_execution_outcome
from execution.verdict_publisher import publish_market_verdict
from risk.risk_manager import RiskManager
from runtime.priority_queue import PriorityItem, PriorityQueueItemFactory
from runtime.market_event_normalizer import build_market_candidate_payload
from runtime.application_lifecycle_runtime import ApplicationLifecycleRuntime
from runtime.market_application_runtime import MarketApplicationRuntime
from strategy.llm_context import attach_llm_rule_context
from strategy.llm_response import build_llm_error_response, process_llm_response
from strategy.post_llm_validation import validate_post_llm_entry
from strategy.entry_decision_pipeline import prepare_entry_execution_plan
from strategy.verdict_records import (
    build_pre_llm_risk_block_verdict,
    build_default_signal_reason,
    classify_local_evidence_diagnostic,
    build_llm_verdict_record,
    link_shadow_candidate_verdict,
    enrich_ai_analysis_with_verdict,
)
from strategy.signal_guard import (
    validate_entry_evidence,
    infer_candidate_direction, validate_atr_readiness,
    classify_direction_evidence,
)
from strategy.candidate_dedup import candidate_is_expired
from strategy.candidate_recovery import (
    refresh_candidate_bbo_metrics,
)
from strategy.scalping_strategy import evaluate_scalping_candidate, has_queue_trade_flow_conflict
from strategy.market_data_runtime import paper_queue_flow_conflict_reason
from portfolio.dashboard_runtime import DashboardRuntime
from interface.paper_api_helpers import PaperAPIHelpers
from interface.dashboard_routes import create_dashboard_router
from interface.paper_session_routes import create_paper_session_router
from interface.paper_research_routes import create_paper_research_router
from storage.paper_session_runtime import PaperSessionRuntime
from storage.paper_session_maintenance_runtime import (
    PAPER_CLEAR_CONFIRMATION,
    PaperSessionMaintenanceRuntime,
)
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
from ai.dashboard_verdicts import (
    build_verdict_explanation,
    finite_float,
    nonnegative_int,
    normalise_execution_status,
)
from strategy.decision_diagnostics import (
    build_llm_feature_snapshot,
)
from interface import tg_bot
from interface.websocket_manager import ConnectionManager
from ai.news_parser import MarketNewsParser
from monitoring.trade_flow_diagnostics import (
    bbo_resume_reason_bucket as _bbo_resume_reason_bucket,
    guard_reason_bucket as _guard_reason_bucket,
)
from monitoring.market_observability_runtime import MarketObservabilityRuntime

# --------------------------------------------------------------
#  ENVIRONMENT & INITIALIZATION
# --------------------------------------------------------------
load_dotenv()

from monitoring.background_tasks import BackgroundOperationTracker
from monitoring.background_tasks import BackgroundTaskSupervisor
from monitoring.logging_setup import (
    RUN_LOG_PATHS,
    logger,
    terminal_status,
    update_numpy_impulse_status,
)
from monitoring.trading_event_logger import log_trading_event_to_file

from portfolio.paper_accounting import (
    parse_finite_float as _parse_finite_float,
)



FINAM_TOKEN = os.getenv("FINAM_TOKEN", "")
# PAPER orders are recorded against an internal simulator identity, never a Finam client/account ID.
CLIENT_ID = "PILUITUS_PAPER" if Settings.PAPER_TRADING else os.getenv("FINAM_CLIENT_ID", "UNKNOWN_ACCOUNT")
DEPOSIT = float(Settings.TRADE_DEPOSIT)

# --------------------------------------------------------------
#  1️⃣  GLOBAL OBJECTS & PRIORITIZED CONTOUR
# --------------------------------------------------------------
db_manager = DBManager()
ai_engine = AIEngine()
broker = FinamBrokerClient(token=FINAM_TOKEN, is_demo=Settings.FINAM_DEMO)
screener = HFTMemoryScreener(window_size=30)
execution_engine = BaseExecutionEngine(
    finam_broker_client=broker,
    screener=screener,
    initial_capital=DEPOSIT,
    max_open_stop_risk_pct=Settings.MAX_OPEN_STOP_RISK_PCT,
)
# Keep Finam ORDER_BOOK subscriptions for every active PAPER position, even
# when its ticker rotates out of the screener's Top-N list.
broker.open_positions_provider = execution_engine.get_open_position_tickers
risk_manager = RiskManager(
    max_risk_per_trade=Settings.MAX_RISK_PCT,
    daily_loss_limit=Settings.DAILY_LOSS_LIMIT_PCT,
    initial_deposit=DEPOSIT,
    drawdown_warning_pct=Settings.DRAWDOWN_WARNING_PCT,
    max_drawdown_pct=Settings.MAX_DRAWDOWN_PCT,
    loss_streak_3_pause_sec=Settings.LOSS_STREAK_3_PAUSE_SEC,
    loss_streak_5_pause_sec=Settings.LOSS_STREAK_5_PAUSE_SEC,
    loss_streak_hard_stop=Settings.LOSS_STREAK_HARD_STOP,
)

if not math.isclose(
    float(execution_engine.initial_capital),
    float(risk_manager.initial_deposit),
    rel_tol=0.0,
    abs_tol=0.01,
):
    raise RuntimeError(
        "Несогласованный стартовый капитал: ExecutionLayer и RiskManager должны использовать единый Settings.TRADE_DEPOSIT."
    )
logger.info(
    "💰 [CAPITAL SYNC] source=%s; TRADE_DEPOSIT=%.2f ₽; ExecutionLayer=%.2f ₽; RiskManager=%.2f ₽.",
    Settings.TRADE_DEPOSIT_SOURCE,
    DEPOSIT, execution_engine.initial_capital, risk_manager.initial_deposit,
)


dashboard_runtime = DashboardRuntime(
    execution_engine=execution_engine,
    db_manager=db_manager,
    screener=screener,
    lot_sizes=LOT_SIZES,
    logger=logger,
)


def _current_paper_open_pnl() -> float:
    return dashboard_runtime.current_paper_open_pnl()


def _current_paper_assets_cost() -> float:
    return dashboard_runtime.current_paper_assets_cost()


# In PAPER, in-memory positions are the live source of floating PnL and book cost.
# Avoid adding the same OPEN journal rows a second time from Redis.
if execution_engine.paper_mode:
    risk_manager.open_pnl_provider = _current_paper_open_pnl
    risk_manager.open_assets_cost_provider = _current_paper_assets_cost

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

# One composition boundary owns the observational diagnostics used across runtimes.
market_observability_runtime = MarketObservabilityRuntime(
    logger=logger,
    shadow_tracker=scalping_shadow_tracker,
    execution_engine=execution_engine,
    screener=screener,
    parse_finite_float=_parse_finite_float,
    guard_reason_bucket=_guard_reason_bucket,
    evaluate_candidate=evaluate_scalping_candidate,
    pending_bbo_count_provider=lambda: getattr(
        globals().get("pending_bbo_candidate_store"), "count", lambda: 0
    )(),
    monotonic=time_module.monotonic,
)
_trade_flow_counters = market_observability_runtime.trade_flow_counters
position_order_book_diagnostics = market_observability_runtime.position_order_book_diagnostics
shadow_scorecard_logger = market_observability_runtime.shadow_scorecard_logger
trade_flow_report_logger = market_observability_runtime.trade_flow_report_logger
strategy_shadow_diagnostics = market_observability_runtime.strategy_shadow_diagnostics
prefilter_block_logger = market_observability_runtime.prefilter_block_logger
shadow_outcome_persistence = ShadowOutcomePersistence(
    db_manager, scalping_shadow_tracker, paper_shadow_labels, logger
)


paper_session_runtime = PaperSessionRuntime(
    db_manager=db_manager,
    execution_engine=execution_engine,
    shadow_outcome_persistence=shadow_outcome_persistence,
    bbo_replay_recorder=paper_bbo_replay_recorder,
    bbo_replay_recorder_factory=BBOReplayRecorder,
    paper_performance_tracker=paper_performance_tracker,
    paper_shadow_labels=paper_shadow_labels,
    logger=logger,
    closed_trades_storage_key=PAPER_CLOSED_TRADES_STORAGE_KEY,
    closed_trades_storage_limit=PAPER_CLOSED_TRADES_STORAGE_LIMIT,
)


async def _persist_shadow_outcomes(
    labels: list[dict[str, Any]], *, source: str
) -> None:
    await paper_session_runtime.persist_shadow_outcomes(labels, source=source)


async def _restore_shadow_outcomes() -> None:
    await paper_session_runtime.restore_shadow_outcomes()


async def _rotate_paper_bbo_replay_session() -> None:
    global paper_bbo_replay_recorder
    paper_bbo_replay_recorder = (
        await paper_session_runtime.rotate_bbo_replay_session()
    )


async def _restore_paper_performance() -> None:
    await paper_session_runtime.restore_paper_performance()


# Preserve the callback names consumed by the market and worker runtimes.
_record_position_order_book_event = market_observability_runtime.record_position_order_book_event
_log_open_position_bbo_diagnostics = market_observability_runtime.log_open_position_bbo_diagnostics
_trade_flow_inc = market_observability_runtime.increment_trade_flow
_trade_flow_maybe_log = market_observability_runtime.maybe_log_trade_flow
_trade_flow_guard_block = market_observability_runtime.guard_block
_mark_shadow_llm_not_called = market_observability_runtime.mark_shadow_llm_not_called
_log_scalping_shadow = market_observability_runtime.log_shadow_candidate
_log_prefilter_blocked = market_observability_runtime.log_prefilter_blocked


# Последний рассчитанный срез баланса для UI. Не изменяет внутреннюю риск-модель.
_dashboard_balance_snapshot = {
    "equity": round(DEPOSIT, 2),
    "initial_deposit": round(DEPOSIT, 2),
    "free_margin": round(DEPOSIT, 2),
    "margin_used": 0.0,
    "exposure_cap": round(DEPOSIT * 0.15, 2),
    "exposure_cap_pct": 0.15,
    "single_position_cap": round(DEPOSIT * 0.05, 2),
    "unreconciled_positions_count": 0,
    "pnl": 0.0,
    "realized_pnl": 0.0,
    "open_pnl": 0.0,
    "active_positions_count": 0,
}

def _latest_screener_prices() -> dict[str, float]:
    return dashboard_runtime.latest_screener_prices()


async def _get_dashboard_open_positions() -> list[dict]:
    return await dashboard_runtime.get_dashboard_open_positions()


async def _refresh_dashboard_balance_snapshot(positions: list[dict] | None = None) -> dict:
    global _dashboard_balance_snapshot
    _dashboard_balance_snapshot = await dashboard_runtime.refresh_balance_snapshot(
        positions,
        risk_manager=risk_manager,
        initial_deposit=DEPOSIT,
    )
    return dict(_dashboard_balance_snapshot)


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
paper_api_helpers = PaperAPIHelpers(
    execution_engine=execution_engine,
    connection_manager=manager,
    get_open_positions=_get_dashboard_open_positions,
    refresh_balance=_refresh_dashboard_balance_snapshot,
)

# --------------------------------------------------------------
#  4️⃣  ПОЛИМОРФНЫЙ ТИКЕРНЫЙ ОБРАБОТЧИК С АВТО-ЗАВЕРШЕНИЕМ ПОЗИЦИЙ
# --------------------------------------------------------------
async def process_market_event(event: Union[SimpleNamespace, dict]):
    await market_data_event_runtime.process_event(event)


# --------------------------------------------------------------
#  5️⃣  MARKET WORKER И ФОНОВЫЕ СЕРВИСЫ: ЕДИНЫЙ COMPOSITION ROOT
market_application_runtime = MarketApplicationRuntime(
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
    callbacks={
        "trade_flow_increment": _trade_flow_inc,
        "guard_reason_bucket": _guard_reason_bucket,
        "record_position_order_book_event": _record_position_order_book_event,
        "refresh_bbo_metrics": refresh_candidate_bbo_metrics,
        "persist_shadow_outcomes": _persist_shadow_outcomes,
        "infer_candidate_direction": infer_candidate_direction,
        "paper_mode_provider": lambda: bool(getattr(execution_engine, "paper_mode", False)),
        "validate_entry_evidence": validate_entry_evidence,
        "validate_atr_readiness": validate_atr_readiness,
        "mark_shadow_llm_not_called": _mark_shadow_llm_not_called,
        "log_shadow_candidate": _log_scalping_shadow,
        "trade_flow_guard_block": _trade_flow_guard_block,
        "log_prefilter_blocked": _log_prefilter_blocked,
        "update_numpy_impulse_status": update_numpy_impulse_status,
        "build_candidate_payload": build_market_candidate_payload,
        "classify_direction": classify_direction_evidence,
        "candidate_is_expired": candidate_is_expired,
        "normalise_execution_status": normalise_execution_status,
        "nonnegative_int": nonnegative_int,
        "normalize_execution_fill": normalize_execution_fill,
        "publish_execution_outcome": publish_execution_outcome,
        "validate_daily_loss_limit": validate_daily_loss_limit,
        "parse_finite_float": _parse_finite_float,
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
    },
).create()

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


async def background_broker_init():
    await market_background_services.background_broker_init()


async def _start_news_updater() -> None:
    await market_background_services._start_news_updater()


async def _finam_metadata_refresh_loop() -> None:
    await market_background_services._finam_metadata_refresh_loop()


async def _start_tables_sync_loop():
    await market_background_services._start_tables_sync_loop()


# --------------------------------------------------------------
#  7️⃣  FastAPI LIFESPAN
# --------------------------------------------------------------
application_lifecycle_runtime = ApplicationLifecycleRuntime(
    logger=logger,
    execution_engine=execution_engine,
    deposit=DEPOSIT,
    db_manager=db_manager,
    paper_performance_tracker=paper_performance_tracker,
    shadow_tracker=scalping_shadow_tracker,
    paper_shadow_labels=paper_shadow_labels,
    risk_manager=risk_manager,
    paper_bbo_replay_recorder_provider=lambda: paper_bbo_replay_recorder,
    restore_shadow_outcomes=_restore_shadow_outcomes,
    restore_paper_performance=_restore_paper_performance,
    ai_engine=ai_engine,
    quarantine_unreconciled_paper_positions=lambda: execution_engine.quarantine_unreconciled_paper_positions(db_manager),
    max_cache_size=MAX_CACHE_SIZE,
    set_verdict_cache=lambda values: globals().__setitem__("last_ai_verdicts_cache", values),
    worker_factory=lambda: market_worker_factory.create(db_manager, ai_engine, broker),
    finam_metadata_refresh_loop=_finam_metadata_refresh_loop,
    start_news_updater=_start_news_updater,
    start_tables_sync_loop=_start_tables_sync_loop,
    telegram_bot_module=tg_bot,
    broker=broker,
    market_broker_init=background_broker_init,
    set_market_task=lambda task: globals().__setitem__("bg_market_task", task),
    background_operations=_background_operations,
    trade_flow_maybe_log=_trade_flow_maybe_log,
    task_supervisor_factory=BackgroundTaskSupervisor,
)
lifespan = application_lifecycle_runtime.lifespan

app = FastAPI(title="PILUITUS v35.2 — AI Trading Core", lifespan=lifespan)

BASE_DIR = PROJECT_ROOT
TEMPLATE_DIR = BASE_DIR / "interface" / "templates"

app.include_router(
    create_dashboard_router(
        anomaly_queue=anomaly_queue,
        priority_queue_item_factory=priority_queue_item_factory,
        validate_daily_loss_limit=validate_daily_loss_limit,
        default_daily_loss_limit=Settings.DAILY_LOSS_LIMIT_PCT,
        template_dir=TEMPLATE_DIR,
        manager=manager,
        logger=logger,
        get_dashboard_open_positions=_get_dashboard_open_positions,
        refresh_dashboard_balance_snapshot=_refresh_dashboard_balance_snapshot,
        db_manager=db_manager,
        execution_engine=execution_engine,
        last_ai_verdicts_cache=last_ai_verdicts_cache,
        process_market_event=process_market_event,
    )
)

# Dashboard HTTP/WebSocket endpoints are owned by the interface package.


def _require_paper_mode() -> None:
    paper_api_helpers.require_paper_mode()


def _paper_trade_rows(snapshot: dict) -> list[dict]:
    return paper_api_helpers.paper_trade_rows(snapshot)


async def _broadcast_paper_tables() -> None:
    await paper_api_helpers.broadcast_paper_tables()


paper_research_runtime = PaperResearchRuntime(
    db_manager=db_manager,
    is_paper_trade=DBManager.is_paper_trade,
    execution_engine=execution_engine,
    screener=screener,
    paper_performance_tracker=paper_performance_tracker,
    paper_shadow_labels=paper_shadow_labels,
    paper_session_lock=_paper_session_lock,
    broadcast_paper_tables=_broadcast_paper_tables,
    bbo_replay_storage_key=BBO_REPLAY_STORAGE_KEY,
    bbo_replay_storage_limit=BBO_REPLAY_STORAGE_LIMIT,
    paper_closed_trades_storage_limit=PAPER_CLOSED_TRADES_STORAGE_LIMIT,
    shadow_outcomes_storage_limit=SHADOW_OUTCOMES_STORAGE_LIMIT,
    lot_sizes=LOT_SIZES,
    parse_finite_float=_parse_finite_float,
    replay_exit_batch=replay_exit_batch,
    replay_exit_scenarios=replay_exit_scenarios,
    build_shadow_reconciliation=build_shadow_reconciliation,
    build_actual_vs_replay_reconciliation=build_actual_vs_replay_reconciliation,
    build_shadow_cost_sensitivity=build_shadow_cost_sensitivity,
    build_shadow_walk_forward_validation=build_shadow_walk_forward_validation,
)


app.include_router(create_paper_research_router(paper_research_runtime))


def _advance_paper_session_generation() -> None:
    global _paper_session_generation
    _paper_session_generation += 1


def _reset_balance_guard_signature() -> None:
    dashboard_runtime.reset_balance_guard_signature()


async def _broadcast_paper_session_reset(payload: dict[str, Any]) -> None:
    await manager.broadcast(json.dumps(payload, ensure_ascii=False))


paper_session_maintenance_runtime = PaperSessionMaintenanceRuntime(
    db_manager=db_manager,
    execution_engine=execution_engine,
    risk_manager=risk_manager,
    paper_session_lock=_paper_session_lock,
    anomaly_queue=anomaly_queue,
    ai_candidate_deduplicator=_ai_candidate_deduplicator,
    paper_performance_tracker=paper_performance_tracker,
    shadow_tracker=scalping_shadow_tracker,
    paper_shadow_labels=paper_shadow_labels,
    last_ai_verdicts_cache=last_ai_verdicts_cache,
    trade_flow_counters=_trade_flow_counters,
    pending_bbo_candidates=_pending_bbo_candidates,
    pending_bbo_lock=_pending_bbo_lock,
    position_order_book_diagnostics=position_order_book_diagnostics,
    require_paper_mode=_require_paper_mode,
    advance_session_generation=_advance_paper_session_generation,
    reset_balance_guard_signature=_reset_balance_guard_signature,
    rotate_bbo_replay_session=_rotate_paper_bbo_replay_session,
    refresh_dashboard_balance_snapshot=_refresh_dashboard_balance_snapshot,
    broadcast_paper_tables=_broadcast_paper_tables,
    broadcast_paper_session_reset=_broadcast_paper_session_reset,
    get_bbo_replay_session_id=lambda: paper_bbo_replay_recorder.session_id,
    deposit=DEPOSIT,
    logger=logger,
    paper_clear_confirmation=PAPER_CLEAR_CONFIRMATION,
    monotonic=time_module.monotonic,
)
app.include_router(
    create_paper_session_router(paper_session_maintenance_runtime)
)


STATIC_DIR = BASE_DIR / "static"
if STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


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
