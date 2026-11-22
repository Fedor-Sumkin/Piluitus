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
from runtime.market_background_services import MarketBackgroundServicesRuntime
from runtime.application_lifecycle_runtime import ApplicationLifecycleRuntime
from runtime.market_worker import MarketWorkerFactory
from runtime.market_ingestion_runtime import MarketIngestionRuntimeFactory
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
    TradeFlowCounters,
    bbo_resume_reason_bucket as _bbo_resume_reason_bucket,
    guard_reason_bucket as _guard_reason_bucket,
)

# --------------------------------------------------------------
#  ENVIRONMENT & INITIALIZATION
# --------------------------------------------------------------
load_dotenv()

from monitoring.background_tasks import BackgroundOperationTracker
from monitoring.background_tasks import BackgroundTaskSupervisor
from monitoring.position_order_book_diagnostics import PositionOrderBookDiagnostics
from monitoring.shadow_scorecard_logger import StrategyShadowScorecardLogger
from monitoring.trade_flow_report import TradeFlowReportLogger
from monitoring.strategy_shadow_diagnostics import StrategyShadowDiagnostics
from monitoring.prefilter_diagnostics import PrefilterBlockLogger
from monitoring.logging_setup import (
    RUN_LOG_PATHS,
    logger,
    terminal_status,
    update_numpy_impulse_status,
)
from monitoring.trading_event_logger import log_trading_event_to_file

prefilter_block_logger = PrefilterBlockLogger(logger)

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

# Диагностический накопитель; окно отчёта остаётся 60 секунд.
_trade_flow_counters = TradeFlowCounters(time_module.monotonic())
# Forward labels are research-only and bounded in memory; all observations are logged.
scalping_shadow_tracker = ShadowForwardTracker()
paper_bbo_replay_recorder = BBOReplayRecorder()
execution_engine.bbo_replay_session_id = paper_bbo_replay_recorder.session_id
execution_engine.paper_exit_event_recorder = paper_bbo_replay_recorder.record_exit_trigger
paper_shadow_labels: list[dict[str, Any]] = []
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


# One outstanding AI event per ticker prevents repeated nearly identical
# anomaly events from filling the queue while inference is still running.
# Per-position ORDER_BOOK diagnostics are observational and owned by monitoring.
position_order_book_diagnostics = PositionOrderBookDiagnostics(logger)
shadow_scorecard_logger = StrategyShadowScorecardLogger(logger)
trade_flow_report_logger = TradeFlowReportLogger(logger)


def _record_position_order_book_event(ticker: str, result: dict[str, Any]) -> None:
    position_order_book_diagnostics.record(
        ticker, result, globals().get("execution_engine")
    )


def _log_open_position_bbo_diagnostics() -> None:
    position_order_book_diagnostics.log_open_positions(
        globals().get("execution_engine"),
        globals().get("screener"),
        _parse_finite_float,
    )


def _trade_flow_inc(key: str, amount: int = 1) -> None:
    _trade_flow_counters.increment(key, amount)
    _trade_flow_maybe_log()


def _trade_flow_maybe_log(force: bool = False) -> None:
    counts = _trade_flow_counters.snapshot_if_due(
        now=time_module.monotonic(), interval_sec=60.0, force=force
    )
    if counts is None:
        return

    pending_bbo = pending_bbo_candidate_store.count()
    trade_flow_report_logger.log(counts, pending_bbo=pending_bbo)
    shadow_scorecard_logger.log(scalping_shadow_tracker)
    _log_open_position_bbo_diagnostics()


def _trade_flow_guard_block(reason: str) -> None:
    _trade_flow_inc("guard_blocked")
    lowered = str(reason or "").lower()
    if "bid/ask" in lowered or "котировк" in lowered or "актуальн" in lowered:
        bucket = "guard_quotes"
    elif "спред" in lowered:
        bucket = "guard_spread"
    elif "размер лучшей заявки" in lowered or "сокращение" in lowered or "истории размера" in lowered:
        bucket = "guard_size"
    elif "всплеск" in lowered or "объём" in lowered or "объем" in lowered:
        bucket = "guard_volume"
    elif "импульс" in lowered or "vwap" in lowered or "цена не" in lowered:
        bucket = "guard_direction"
    elif "объяснение" in lowered or "текст" in lowered or "поврежд" in lowered or "неподтверждён" in lowered:
        bucket = "guard_text"
    else:
        bucket = "guard_other"
    _trade_flow_inc(bucket)


strategy_shadow_diagnostics = StrategyShadowDiagnostics(
    logger,
    scalping_shadow_tracker,
    _trade_flow_inc,
    _guard_reason_bucket,
    evaluate_scalping_candidate,
)


def _mark_shadow_llm_not_called(candidate_id: object, reason_code: str) -> None:
    strategy_shadow_diagnostics.mark_llm_not_called(candidate_id, reason_code)


def _log_scalping_shadow(ticker: str, direction: str, metrics: dict[str, Any]) -> dict[str, Any]:
    return strategy_shadow_diagnostics.log_candidate(ticker, direction, metrics)


def _log_prefilter_blocked(ticker: str, reason: str, *, interval_sec: float = 15.0) -> None:
    """Rate-limit identical prefilter warnings per ticker and reason category."""
    prefilter_block_logger.log(ticker, reason, interval_sec=interval_sec)


def _paper_queue_flow_conflict_reason(
    direction: str, metrics: dict[str, Any]
) -> str:
    return paper_queue_flow_conflict_reason(
        direction,
        metrics,
        paper_mode=bool(Settings.PAPER_TRADING),
        enabled=os.getenv("PILUITUS_PAPER_QUEUE_FLOW_CONFLICT_GUARD", "1"),
        conflict_detector=has_queue_trade_flow_conflict,
        finite_float=finite_float,
    )


def _reserve_ai_ticker(
    ticker: str,
    direction: str = "NONE",
    metrics: dict[str, Any] | None = None,
) -> bool:
    pattern = (
        str((metrics or {}).get("bar_pattern_name") or "FLOW")
        if isinstance(metrics, dict) else "FLOW"
    )
    return _ai_candidate_deduplicator.reserve(ticker, direction, pattern)


def _release_ai_ticker(ticker: str) -> None:
    _ai_candidate_deduplicator.release(ticker)


async def _warmup_selected_m1_history(
    tickers: tuple[str, ...] | list[str]
) -> None:
    await m1_history_warmup.warmup_selected_m1_history(tickers)


def _schedule_m1_history_warmup(
    tickers: tuple[str, ...] | list[str]
) -> None:
    m1_history_warmup.schedule(tickers)


def _drop_pending_bbo_candidate(
    ticker: str, expected: dict[str, Any] | None = None
) -> bool:
    return pending_bbo_runtime.drop(ticker, expected)


def _cancel_pending_bbo_candidate(
    ticker: str, expected: dict[str, Any] | None = None
) -> bool:
    return pending_bbo_runtime.cancel(ticker, expected)


def _expire_pending_bbo_candidates(now: float | None = None) -> None:
    pending_bbo_runtime.expire(now)


def _store_pending_bbo_candidate(
    ticker: str,
    payload: dict[str, Any],
    received_monotonic: object | None = None,
) -> None:
    pending_bbo_runtime.store_candidate(
        ticker, payload, received_monotonic=received_monotonic
    )


async def _resume_pending_bbo_candidate(
    ticker: str,
    *,
    trigger: str = "ORDER_BOOK",
) -> None:
    await pending_bbo_runtime.resume(ticker, trigger=trigger)



# Market-ingestion state and its collaborating runtimes are composed as one unit.
market_ingestion_components = MarketIngestionRuntimeFactory(
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
        "monotonic": time_module.monotonic,
        "sleep": asyncio.sleep,
        "environment": os.environ,
    },
    callbacks={
        "trade_flow_increment": _trade_flow_inc,
        "guard_reason_bucket": _guard_reason_bucket,
        "record_position_order_book_event": _record_position_order_book_event,
        "refresh_bbo_metrics": refresh_candidate_bbo_metrics,
        "persist_shadow_outcomes": _persist_shadow_outcomes,
        "resume_pending_bbo_candidate": _resume_pending_bbo_candidate,
        "infer_candidate_direction": infer_candidate_direction,
        "paper_mode_provider": lambda: bool(getattr(execution_engine, "paper_mode", False)),
        "validate_entry_evidence": validate_entry_evidence,
        "validate_atr_readiness": validate_atr_readiness,
        "queue_flow_conflict_reason": _paper_queue_flow_conflict_reason,
        "reserve_ai_ticker": _reserve_ai_ticker,
        "release_ai_ticker": _release_ai_ticker,
        "mark_shadow_llm_not_called": _mark_shadow_llm_not_called,
        "log_shadow_candidate": _log_scalping_shadow,
        "trade_flow_guard_block": _trade_flow_guard_block,
        "log_prefilter_blocked": _log_prefilter_blocked,
        "update_numpy_impulse_status": update_numpy_impulse_status,
        "store_pending_bbo_candidate": _store_pending_bbo_candidate,
        "build_candidate_payload": build_market_candidate_payload,
        "classify_direction": classify_direction_evidence,
        "schedule_m1_history_warmup": _schedule_m1_history_warmup,
    },
).create()

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
#  5️⃣  MARKET WORKER С ПРИОРИТЕТНЫМ ПЕРЕХВАТОМ И КОНТУРОМ RAG
# --------------------------------------------------------------
market_worker_factory = MarketWorkerFactory(
    client_id=CLIENT_ID,
    execution_runtime_dependencies={
        "execution_engine": execution_engine,
        "risk_manager": risk_manager,
        "session_lock": _paper_session_lock,
        "session_generation_provider": lambda: _paper_session_generation,
        "candidate_is_expired": candidate_is_expired,
        "candidate_ttl_sec": _AI_CANDIDATE_TTL_SEC,
        "trade_flow_increment": _trade_flow_inc,
        "logger": logger,
        "shadow_tracker": scalping_shadow_tracker,
        "normalise_execution_status": normalise_execution_status,
        "nonnegative_int": nonnegative_int,
        "normalize_execution_fill": normalize_execution_fill,
        "lot_sizes": FINAM_TRADE_LOT_SIZES,
        "publish_execution_outcome": publish_execution_outcome,
        "verdict_cache": last_ai_verdicts_cache,
        "get_open_positions": _get_dashboard_open_positions,
        "refresh_balance_snapshot": _refresh_dashboard_balance_snapshot,
        "manager": manager,
    },
    processing_runtime_dependencies={
        "anomaly_queue": anomaly_queue,
        "execution_engine": execution_engine,
        "risk_manager": risk_manager,
        "session_lock": _paper_session_lock,
        "session_generation_provider": lambda: _paper_session_generation,
        "candidate_is_expired": candidate_is_expired,
        "candidate_ttl_sec": _AI_CANDIDATE_TTL_SEC,
        "trade_flow_increment": _trade_flow_inc,
        "logger": logger,
        "settings": Settings,
        "validate_daily_loss_limit": validate_daily_loss_limit,
        "parse_finite_float": _parse_finite_float,
        "background_operations": _background_operations,
        "log_trading_event": log_trading_event_to_file,
        "attach_llm_rule_context": attach_llm_rule_context,
        "build_llm_feature_snapshot": build_llm_feature_snapshot,
        "build_llm_error_response": build_llm_error_response,
        "process_llm_response": process_llm_response,
        "validate_post_llm_entry": validate_post_llm_entry,
        "screener": screener,
        "queue_flow_conflict_reason": _paper_queue_flow_conflict_reason,
        "prepare_entry_execution_plan": prepare_entry_execution_plan,
        "lot_sizes": FINAM_TRADE_LOT_SIZES,
        "trade_flow_guard_block": _trade_flow_guard_block,
        "finite_float": finite_float,
        "nonnegative_int": nonnegative_int,
        "build_default_signal_reason": build_default_signal_reason,
        "build_verdict_explanation": build_verdict_explanation,
        "classify_local_evidence_diagnostic": classify_local_evidence_diagnostic,
        "build_pre_llm_risk_block_verdict": build_pre_llm_risk_block_verdict,
        "build_llm_verdict_record": build_llm_verdict_record,
        "link_shadow_candidate_verdict": link_shadow_candidate_verdict,
        "shadow_tracker": scalping_shadow_tracker,
        "enrich_ai_analysis_with_verdict": enrich_ai_analysis_with_verdict,
        "verdict_cache": last_ai_verdicts_cache,
        "max_cache_size": MAX_CACHE_SIZE,
        "manager": manager,
        "balance_snapshot": _dashboard_balance_snapshot,
        "publish_market_verdict": publish_market_verdict,
        "release_ai_ticker": _release_ai_ticker,
        "paper_session_generation_lock": _paper_session_lock,
    },
)

# --------------------------------------------------------------
#  6️⃣  ФОНОВЫЕ ЦИКЛЫ ОБНОВЛЕНИЯ И СИНХРОНИЗАЦИИ ТАБЛИЦ
# --------------------------------------------------------------
market_background_services = MarketBackgroundServicesRuntime(
    broker=broker,
    execution_engine=execution_engine,
    logger=logger,
    tickers=TICKERS_TO_TRADE,
    finam_trade_lot_sizes=FINAM_TRADE_LOT_SIZES,
    screener=screener,
    process_market_event=process_market_event,
    db_manager=db_manager,
    manager=manager,
    get_dashboard_open_positions=_get_dashboard_open_positions,
    refresh_dashboard_balance_snapshot=_refresh_dashboard_balance_snapshot,
)


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
