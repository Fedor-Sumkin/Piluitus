import asyncio
import json
import math
import re
import os
import sys
import time as time_module
import threading
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
from execution.market_worker_execution_runtime import MarketWorkerExecutionRuntime
from runtime.market_worker_processing_runtime import MarketWorkerProcessingRuntime
from execution.outcome_reporting import normalize_execution_fill
from execution.outcome_publisher import publish_execution_outcome
from execution.verdict_publisher import publish_market_verdict
from risk.risk_manager import RiskManager
from runtime.priority_queue import PriorityItem, PriorityQueueItemFactory
from runtime.market_event_normalizer import build_market_candidate_payload
from runtime.market_data_event_runtime import MarketDataEventRuntime
from runtime.market_background_services import MarketBackgroundServicesRuntime
from runtime.market_event_router import (
    MarketAuxiliaryEventRouter,
    PaperBBOReplayAdapter,
)
from strategy.market_candidate_pipeline import MarketCandidatePipeline
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
from strategy.candidate_dedup import CandidateDeduplicator, candidate_is_expired
from strategy.candidate_recovery import (
    refresh_candidate_bbo_metrics,
)
from strategy.scalping_strategy import evaluate_scalping_candidate, has_queue_trade_flow_conflict
from strategy.market_data_runtime import (
    M1HistoryWarmup,
    PendingBBOCandidateStore,
    PendingBBOCandidateRuntime,
    paper_queue_flow_conflict_reason,
)
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
from portfolio.dashboard_accounting import build_dashboard_balance_snapshot



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
m1_history_warmup = M1HistoryWarmup(
    broker=broker,
    screener=screener,
    logger=logger,
    retry_sec=30.0,
    monotonic=time_module.monotonic,
    sleep=asyncio.sleep,
)
try:
    _AI_CANDIDATE_COOLDOWN_SEC = max(
        0.5, float(os.getenv("PILUITUS_AI_CANDIDATE_COOLDOWN_SEC", "8.0"))
    )
except (TypeError, ValueError, OverflowError):
    _AI_CANDIDATE_COOLDOWN_SEC = 8.0
_ai_candidate_deduplicator = CandidateDeduplicator(_AI_CANDIDATE_COOLDOWN_SEC)
try:
    _AI_CANDIDATE_TTL_SEC = max(1.0, float(os.getenv("PILUITUS_AI_CANDIDATE_TTL_SEC", "5.0")))
except (TypeError, ValueError, OverflowError):
    _AI_CANDIDATE_TTL_SEC = 5.0
_pending_bbo_candidates: dict[str, dict[str, Any]] = {}
_pending_bbo_lock = threading.Lock()
try:
    _PENDING_BBO_CANDIDATE_TTL_SEC = min(5.0, max(0.25, float(os.getenv("PILUITUS_PENDING_BBO_TTL_SEC", "1.5"))))
except (TypeError, ValueError):
    _PENDING_BBO_CANDIDATE_TTL_SEC = 1.5


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


pending_bbo_candidate_store = PendingBBOCandidateStore(
    ttl_sec=_PENDING_BBO_CANDIDATE_TTL_SEC,
    trade_flow_increment=_trade_flow_inc,
    logger=logger,
    candidates=_pending_bbo_candidates,
    lock=_pending_bbo_lock,
    monotonic=time_module.monotonic,
)


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


pending_bbo_runtime = PendingBBOCandidateRuntime(
    store=pending_bbo_candidate_store,
    screener=screener,
    execution_engine=execution_engine,
    anomaly_queue=anomaly_queue,
    queue_item_factory=priority_queue_item_factory,
    refresh_bbo_metrics=refresh_candidate_bbo_metrics,
    infer_candidate_direction=infer_candidate_direction,
    paper_mode_provider=lambda: bool(getattr(execution_engine, "paper_mode", False)),
    validate_entry_evidence=validate_entry_evidence,
    validate_atr_readiness=validate_atr_readiness,
    queue_flow_conflict_reason=_paper_queue_flow_conflict_reason,
    reserve_ai_ticker=_reserve_ai_ticker,
    release_ai_ticker=_release_ai_ticker,
    mark_shadow_llm_not_called=_mark_shadow_llm_not_called,
    log_shadow_candidate=_log_scalping_shadow,
    trade_flow_increment=_trade_flow_inc,
    guard_reason_bucket=_guard_reason_bucket,
    trade_flow_guard_block=_trade_flow_guard_block,
    log_prefilter_blocked=_log_prefilter_blocked,
    update_numpy_impulse_status=update_numpy_impulse_status,
    session_generation_provider=lambda: _paper_session_generation,
    logger=logger,
    monotonic=time_module.monotonic,
)


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
_balance_guard_last_signature: tuple[int, bool] | None = None

def _latest_screener_prices() -> dict[str, float]:
    return dashboard_runtime.latest_screener_prices()


async def _get_dashboard_open_positions() -> list[dict]:
    return await dashboard_runtime.get_dashboard_open_positions()


async def _refresh_dashboard_balance_snapshot(positions: list[dict] | None = None) -> dict:
    """Build a non-mark-to-market dashboard snapshot.

    Free cash = configured initial deposit + realized net PnL - acquisition cost
    of currently open assets. The assets field uses entry_price * quantity and
    therefore stays unchanged when market quotes move. Total PnL also includes
    floating PnL from open positions. The original deposit remains unchanged for
    risk limits; equity reports deposit plus total (realized + floating) PnL.
    """
    global _dashboard_balance_snapshot
    if positions is None:
        positions = await _get_dashboard_open_positions()

    try:
        initial_deposit = float(DEPOSIT)
        if not math.isfinite(initial_deposit) or initial_deposit < 0:
            raise ValueError("invalid configured deposit")
    except (TypeError, ValueError, OverflowError):
        initial_deposit = float(getattr(risk_manager, "initial_deposit", 0.0) or 0.0)

    try:
        pnl = float(getattr(risk_manager, "total_session_pnl", getattr(risk_manager, "session_pnl", 0.0)))
        if not math.isfinite(pnl):
            pnl = 0.0
    except (TypeError, ValueError, OverflowError):
        pnl = 0.0

    try:
        realized_pnl = float(getattr(risk_manager, "realized_pnl", 0.0) or 0.0)
        if not math.isfinite(realized_pnl):
            realized_pnl = 0.0
    except (TypeError, ValueError, OverflowError):
        realized_pnl = 0.0

    try:
        open_pnl = float(getattr(risk_manager, "open_pnl", pnl - realized_pnl) or 0.0)
        if not math.isfinite(open_pnl):
            open_pnl = pnl - realized_pnl
    except (TypeError, ValueError, OverflowError):
        open_pnl = pnl - realized_pnl

    # Old PAPER positions remain reserved until an operator reviews them.
    unresolved_map = (
        getattr(execution_engine, "_unreconciled_paper_positions", {})
        if getattr(execution_engine, "paper_mode", True) else {}
    )
    unresolved_positions = list(unresolved_map.values()) if isinstance(unresolved_map, dict) else []
    calculation = build_dashboard_balance_snapshot(
        positions=positions,
        unresolved_positions=unresolved_positions,
        initial_deposit=initial_deposit,
        total_pnl=pnl,
        realized_pnl=realized_pnl,
        open_pnl=open_pnl,
        max_gross_exposure_pct=getattr(execution_engine, "max_gross_exposure_pct", 0.15),
        max_single_position_pct=getattr(execution_engine, "max_single_position_pct", 0.05),
        lot_sizes=LOT_SIZES,
    )
    _dashboard_balance_snapshot = calculation["snapshot"]
    assets_used = calculation["assets_used"]
    exposure_cap = calculation["exposure_cap"]
    max_gross_pct = calculation["max_gross_pct"]

    global _balance_guard_last_signature
    signature = calculation["guard_signature"]
    if signature != _balance_guard_last_signature:
        if unresolved_positions:
            logger.critical(
                "🛑 [PAPER RECONCILIATION GUARD] %d старых PAPER-позиций на сверке; их себестоимость %.2f ₽ учтена в активах. Новые входы заблокированы.",
                len(unresolved_positions), calculation["unresolved_assets_cost"],
            )
        if calculation["over_cap"]:
            logger.error(
                "🚨 [BALANCE GUARD] Себестоимость активов %.2f ₽ превышает лимит задействования %.2f ₽ (%.0f%% депозита). Новые входы будут заблокированы.",
                assets_used, exposure_cap, max_gross_pct * 100,
            )
        _balance_guard_last_signature = signature
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
market_auxiliary_event_router = MarketAuxiliaryEventRouter(
    screener=screener,
    execution_engine=execution_engine,
    db_manager=db_manager,
    logger=logger,
    trade_flow_increment=_trade_flow_inc,
    record_position_order_book_event=_record_position_order_book_event,
    bbo_replay=PaperBBOReplayAdapter(
        recorder_provider=lambda: paper_bbo_replay_recorder,
        screener=screener,
        monotonic=time_module.monotonic,
    ),
    refresh_bbo_metrics=refresh_candidate_bbo_metrics,
    shadow_tracker=scalping_shadow_tracker,
    persist_shadow_outcomes=_persist_shadow_outcomes,
    resume_pending_bbo_candidate=_resume_pending_bbo_candidate,
    monotonic=time_module.monotonic,
)



market_candidate_pipeline = MarketCandidatePipeline(
    execution_engine=execution_engine,
    anomaly_queue=anomaly_queue,
    queue_item_factory=priority_queue_item_factory,
    trade_flow_increment=_trade_flow_inc,
    infer_direction=infer_candidate_direction,
    classify_direction=classify_direction_evidence,
    log_shadow_candidate=_log_scalping_shadow,
    queue_flow_conflict_reason=_paper_queue_flow_conflict_reason,
    validate_entry_evidence=validate_entry_evidence,
    paper_mode_provider=lambda: bool(getattr(execution_engine, "paper_mode", False)),
    guard_reason_bucket=_guard_reason_bucket,
    store_pending_bbo_candidate=_store_pending_bbo_candidate,
    mark_shadow_llm_not_called=_mark_shadow_llm_not_called,
    trade_flow_guard_block=_trade_flow_guard_block,
    log_prefilter_blocked=_log_prefilter_blocked,
    validate_atr_readiness=validate_atr_readiness,
    reserve_ai_ticker=_reserve_ai_ticker,
    build_candidate_payload=build_market_candidate_payload,
    release_ai_ticker=_release_ai_ticker,
    update_numpy_impulse_status=update_numpy_impulse_status,
    logger=logger,
    monotonic=time_module.monotonic,
)


market_data_event_runtime = MarketDataEventRuntime(
    execution_engine=execution_engine,
    db_manager=db_manager,
    screener=screener,
    auxiliary_event_router=market_auxiliary_event_router,
    candidate_pipeline=market_candidate_pipeline,
    pending_bbo_runtime=pending_bbo_runtime,
    shadow_tracker=scalping_shadow_tracker,
    persist_shadow_outcomes=_persist_shadow_outcomes,
    schedule_m1_history_warmup=_schedule_m1_history_warmup,
    trade_flow_increment=_trade_flow_inc,
    terminal_status=terminal_status,
    logger=logger,
    session_generation_provider=lambda: _paper_session_generation,
    monotonic=time_module.monotonic,
)


async def process_market_event(event: Union[SimpleNamespace, dict]):
    await market_data_event_runtime.process_event(event)


# --------------------------------------------------------------
#  5️⃣  MARKET WORKER С ПРИОРИТЕТНЫМ ПЕРЕХВАТОМ И КОНТУРОМ RAG
# --------------------------------------------------------------
class MarketWorker:
    def __init__(self, db: DBManager, ai: AIEngine, broker_client):
        self.db = db
        self.ai = ai
        self.broker = broker_client
        self.client_id = CLIENT_ID
        self.execution_runtime = MarketWorkerExecutionRuntime(
            db=self.db,
            client_id=self.client_id,
            execution_engine=execution_engine,
            risk_manager=risk_manager,
            session_lock=_paper_session_lock,
            session_generation_provider=lambda: _paper_session_generation,
            candidate_is_expired=candidate_is_expired,
            candidate_ttl_sec=_AI_CANDIDATE_TTL_SEC,
            trade_flow_increment=_trade_flow_inc,
            logger=logger,
            shadow_tracker=scalping_shadow_tracker,
            normalise_execution_status=normalise_execution_status,
            nonnegative_int=nonnegative_int,
            normalize_execution_fill=normalize_execution_fill,
            lot_sizes=FINAM_TRADE_LOT_SIZES,
            publish_execution_outcome=publish_execution_outcome,
            verdict_cache=last_ai_verdicts_cache,
            get_open_positions=_get_dashboard_open_positions,
            refresh_balance_snapshot=_refresh_dashboard_balance_snapshot,

            manager=manager,
        )
        self.processing_runtime = MarketWorkerProcessingRuntime(
            db=self.db,
            ai=self.ai,
            anomaly_queue=anomaly_queue,
            execution_engine=execution_engine,
            risk_manager=risk_manager,
            session_lock=_paper_session_lock,
            session_generation_provider=lambda: _paper_session_generation,
            candidate_is_expired=candidate_is_expired,
            candidate_ttl_sec=_AI_CANDIDATE_TTL_SEC,
            trade_flow_increment=_trade_flow_inc,
            logger=logger,
            settings=Settings,
            validate_daily_loss_limit=validate_daily_loss_limit,
            parse_finite_float=_parse_finite_float,
            background_operations=_background_operations,
            log_trading_event=log_trading_event_to_file,
            attach_llm_rule_context=attach_llm_rule_context,
            build_llm_feature_snapshot=build_llm_feature_snapshot,
            build_llm_error_response=build_llm_error_response,
            process_llm_response=process_llm_response,
            validate_post_llm_entry=validate_post_llm_entry,
            screener=screener,
            queue_flow_conflict_reason=_paper_queue_flow_conflict_reason,
            prepare_entry_execution_plan=prepare_entry_execution_plan,
            lot_sizes=FINAM_TRADE_LOT_SIZES,
            trade_flow_guard_block=_trade_flow_guard_block,
            finite_float=finite_float,
            nonnegative_int=nonnegative_int,
            build_default_signal_reason=build_default_signal_reason,
            build_verdict_explanation=build_verdict_explanation,
            classify_local_evidence_diagnostic=classify_local_evidence_diagnostic,
            build_pre_llm_risk_block_verdict=build_pre_llm_risk_block_verdict,
            build_llm_verdict_record=build_llm_verdict_record,
            link_shadow_candidate_verdict=link_shadow_candidate_verdict,
            shadow_tracker=scalping_shadow_tracker,
            enrich_ai_analysis_with_verdict=enrich_ai_analysis_with_verdict,
            verdict_cache=last_ai_verdicts_cache,
            max_cache_size=MAX_CACHE_SIZE,
            manager=manager,
            balance_snapshot=_dashboard_balance_snapshot,
            publish_market_verdict=publish_market_verdict,
            execute_and_publish=self._execute_and_publish,
            release_ai_ticker=_release_ai_ticker,
            paper_session_generation_lock=_paper_session_lock,
        )

    async def _execute_and_publish(
        self,
        *,
        verdict_id: str,
        ticker: str,
        lots: int,
        side: str,
        price: float,
        ai_analysis: dict,
        session_generation: int,
        candidate_created_monotonic: object | None = None,
        shadow_candidate_id: str | None = None,
    ) -> None:
        await self.execution_runtime.execute_and_publish(
            verdict_id=verdict_id,
            ticker=ticker,
            lots=lots,
            side=side,
            price=price,
            ai_analysis=ai_analysis,
            session_generation=session_generation,
            candidate_created_monotonic=candidate_created_monotonic,
            shadow_candidate_id=shadow_candidate_id,
        )

    async def _execute_and_publish_locked(
        self,
        *,
        verdict_id: str,
        ticker: str,
        lots: int,
        side: str,
        price: float,
        ai_analysis: dict,
        candidate_created_monotonic: object | None = None,
        shadow_candidate_id: str | None = None,
    ) -> None:
        await self.execution_runtime.execute_and_publish_locked(
            verdict_id=verdict_id,
            ticker=ticker,
            lots=lots,
            side=side,
            price=price,
            ai_analysis=ai_analysis,
            candidate_created_monotonic=candidate_created_monotonic,
            shadow_candidate_id=shadow_candidate_id,
        )


    async def _publish_pre_llm_risk_block(
        self, event_payload: dict, session_generation: int
    ) -> None:
        await self.processing_runtime.publish_pre_llm_risk_block(
            event_payload, session_generation
        )

    async def _publish_pre_llm_risk_block_locked(self, event_payload: dict) -> None:
        await self.processing_runtime.publish_pre_llm_risk_block_locked(event_payload)

    async def start_processing(self):
        await self.processing_runtime.start_processing()

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
@asynccontextmanager
async def lifespan(app: FastAPI):
    global bg_market_task, last_ai_verdicts_cache
    background_task_supervisor = BackgroundTaskSupervisor(logger)
    gross_exposure_pct = execution_engine.max_gross_exposure_pct * 100.0
    single_position_pct = execution_engine.max_single_position_pct * 100.0
    logger.info(
        "🚀 Запуск PILUITUS v35.2 | депозит=%.2f ₽ | общий лимит активов=%.1f%% (%.2f ₽), "
        "одна позиция=%.1f%% (%.2f ₽).",
        DEPOSIT, gross_exposure_pct, DEPOSIT * gross_exposure_pct / 100.0,
        single_position_pct, DEPOSIT * single_position_pct / 100.0,
    )

    try:
        await db_manager.connect()
        if getattr(execution_engine, "paper_mode", True):
            # Every process start is a new PAPER measurement session. Remove only
            # PAPER execution/risk journals; DBManager refuses to proceed if LIVE
            # records are present in the shared store.
            try:
                reset_result = await db_manager.clear_paper_session()
                paper_performance_tracker.clear()
                scalping_shadow_tracker.clear()
                paper_shadow_labels.clear()
                risk_manager._balance_initialized = False
                risk_manager.block_trading = False
                risk_manager._blocked_date = None
                risk_manager.total_session_pnl = 0.0
                risk_manager.realized_pnl = 0.0
                risk_manager.open_pnl = 0.0
                risk_manager.current_equity = DEPOSIT
                risk_manager.free_margin = DEPOSIT
                risk_manager.high_water_mark = DEPOSIT
                risk_manager.current_drawdown_pct = 0.0
                risk_manager._drawdown_warning_active = False
                risk_manager._loss_streak_count = 0
                risk_manager._loss_pause_signature = None
                risk_manager._cached_trading_allowed = None
                risk_manager._last_risk_snapshot_monotonic = 0.0
                logger.warning(
                    "🧹 [PAPER SESSION START] previous PAPER execution/risk data cleared; "
                    "fresh session_id=%s; storage=%s",
                    getattr(execution_engine, "session_id", "unknown"),
                    reset_result,
                )
            except Exception:
                execution_engine._maintenance_active = True
                execution_engine._maintenance_reason = (
                    "Автоматическая очистка предыдущей PAPER-сессии не завершилась. "
                    "Новые входы заблокированы до устранения причины."
                )
                logger.critical(
                    "⛔ [PAPER SESSION START] reset failed; trading remains paused to avoid "
                    "mixing old and new session statistics.",
                    exc_info=True,
                )
        paper_bbo_replay_recorder.start(db_manager)
        logger.info(
            "[PAPER BBO REPLAY] recorder started; selected Top-N + open positions, "
            "queue_limit=%d batch_size=%d storage_limit=%d",
            paper_bbo_replay_recorder.queue_limit,
            paper_bbo_replay_recorder.batch_size,
            paper_bbo_replay_recorder.storage_limit,
        )
        await _restore_shadow_outcomes()
        if not getattr(execution_engine, "paper_mode", True):
            await _restore_paper_performance()
        maintenance_state = await db_manager.get_json("piluitus:paper:maintenance")
        if isinstance(maintenance_state, dict) and bool(maintenance_state.get("active")):
            execution_engine._maintenance_active = True
            execution_engine._maintenance_reason = str(maintenance_state.get("reason") or "PAPER-сессия приостановлена для обслуживания.")
            logger.warning("⏸️ Восстановлена сохранённая пауза PAPER-сессии. Нажмите «Возобновить PAPER» на странице обслуживания.")
    except Exception as exc:
        logger.critical(f"❌ Критическая ошибка старта БД в Lifespan: {exc}")

    # В v17 функция существовала, но не вызывалась при запуске: JSON мог не попасть в Redis.
    try:
        from ai.knowledge_parser import TechnicalKnowledgeParser
        await TechnicalKnowledgeParser.warm_up_rag_cache(db_manager)
    except Exception as rag_start_err:
        logger.error("⚠️ Не удалось загрузить базу ai_market_knowledge.json в RAG: %s", rag_start_err, exc_info=True)

    try:
        # Normal PAPER startup archives prior-session positions during reset.
        # This is a defensive fail-closed check for residual records if reset was
        # interrupted or a nonstandard PAPER record escaped the reset inventory.
        quarantined = await execution_engine.quarantine_unreconciled_paper_positions(db_manager)
        if quarantined:
            logger.warning("В старом PAPER-журнале найдено позиций для ручной проверки: %d", quarantined)
    except Exception as stale_paper_err:
        logger.error("Не удалось проверить старые PAPER-позиции: %s", stale_paper_err, exc_info=True)

    try:
        redis_inst = getattr(db_manager, "redis", getattr(db_manager, "_redis", None))
        if redis_inst is not None:
            cached_verdicts = await redis_inst.lrange("piluitus:verdicts:history", 0, MAX_CACHE_SIZE - 1)
            if cached_verdicts:
                last_ai_verdicts_cache = [json.loads(v) for v in cached_verdicts][::-1]
                logger.info(f"📚 Из Redis успешно восстановлено {len(last_ai_verdicts_cache)} вердиктов.")
    except Exception as redis_restore_err:
        logger.error(f"⚠️ Не удалось восстановить историю вердиктов из Redis: {redis_restore_err}")

    worker = MarketWorker(db=db_manager, ai=ai_engine, broker_client=broker)
    async def _run_worker_safely():
        try:
            await worker.start_processing()
        except Exception as e:
            logger.critical(f"❌ КРИТИЧЕСКОЕ ПАДЕНИЕ MarketWorker В ЦИКЛЕ ТОРГОВЛИ: {e}", exc_info=True)

    # Запускаем строго по одной асинхронной задаче на каждый воркер (дублирование удалено)
    background_task_supervisor.spawn(_run_worker_safely(), name="market-worker")
    background_task_supervisor.spawn(_finam_metadata_refresh_loop(), name="finam-metadata-refresh")
    background_task_supervisor.spawn(_start_news_updater(), name="news-updater")
    background_task_supervisor.spawn(_start_tables_sync_loop(), name="tables-sync")

    from interface import tg_bot as tb
    tb.global_broker_ref = broker
    tb.global_execution_engine_ref = execution_engine

    if tg_bot.bot and tg_bot.dp:
        logger.info("🤖 Подключаем Telegram‑бот к API...")
        bot: Bot = cast(Bot, tg_bot.bot)          
        dp = tg_bot.dp                             

        from interface.telegram_runtime import run_polling_with_retry

        async def _start_bot_safely() -> None:
            await run_polling_with_retry(
                lambda: dp.start_polling(bot, skip_updates=True),
                logger=logger,
            )

        background_task_supervisor.spawn(_start_bot_safely(), name="telegram-polling")
        logger.info(
            "🤖 Telegram polling запланирован; доступность API проверяется в фоновой задаче."
        )
    else:
        logger.warning("⚠️ Токен Telegram не найден – режим управления отключён.")

    bg_market_task = background_task_supervisor.spawn(background_broker_init(), name="broker-init-and-streams")

    try:
        yield
    finally:
        logger.info("🛑 Начинаем штатную остановку фоновых задач PILUITUS...")
        await background_task_supervisor.cancel_and_drain()

        # Producers are stopped; finish all retained verdict writes and order publications
        # before closing the replay writer or the event loop.
        try:
            await _background_operations.drain()
        except Exception as operation_drain_err:
            logger.error(
                "[BACKGROUND OPERATIONS] shutdown drain failed: %s",
                operation_drain_err,
                exc_info=True,
            )

        # Stop producers first (broker/market workers), then drain the replay writer.
        try:
            await paper_bbo_replay_recorder.close()
            logger.info("[PAPER BBO REPLAY] shutdown drain: %s", paper_bbo_replay_recorder.status())
        except Exception as replay_close_err:
            logger.error("[PAPER BBO REPLAY] shutdown drain failed: %s", replay_close_err, exc_info=True)

        _trade_flow_maybe_log(force=True)
        try:
            await ai_engine.close()
        except Exception as close_err:
            logger.warning("Ошибка закрытия AIEngine: %s", close_err, exc_info=True)
        logger.info("🛑 Фоновые задачи остановлены; AIEngine закрыт.")


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
    global _balance_guard_last_signature
    _balance_guard_last_signature = None


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
