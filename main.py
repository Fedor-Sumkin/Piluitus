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

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Body, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime, time
from uuid import uuid4
from types import SimpleNamespace
from fastapi.templating import Jinja2Templates
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
from risk.risk_manager import RiskManager
from runtime.priority_queue import PriorityItem, PriorityQueueItemFactory
from runtime.market_event_normalizer import (
    build_market_candidate_payload,
    market_event_type,
    normalize_market_quote_event,
    normalize_market_ticker,
    normalize_order_book_event,
)
from strategy.signal_guard import (
    validate_entry_evidence,
    infer_candidate_direction, validate_atr_readiness,
    classify_direction_evidence,
)
from strategy.candidate_dedup import CandidateDeduplicator, candidate_is_expired
from ai.rag_prompt_utils import select_prompt_rules
from strategy.candidate_recovery import (
    refresh_candidate_bbo_metrics,
    refresh_post_llm_evidence,
)
from strategy.scalping_strategy import evaluate_scalping_candidate, has_queue_trade_flow_conflict
from strategy.market_data_runtime import (
    M1HistoryWarmup,
    PendingBBOCandidateStore,
    paper_queue_flow_conflict_reason,
)
from portfolio.dashboard_runtime import DashboardRuntime
from interface.paper_api_helpers import PaperAPIHelpers
from storage.paper_session_runtime import PaperSessionRuntime
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
    classify_hold_reason_bucket,
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
tick_counter = 0
spinner_frames = ["|", "/", "-", "\\"]

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
_last_funnel_log_at = 0.0
_last_funnel_log_key: tuple[str, ...] | None = None
_last_funnel_ready = False

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

    with _pending_bbo_lock:
        pending_bbo = len(_pending_bbo_candidates)
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
    return pending_bbo_candidate_store.drop(ticker, expected)


def _cancel_pending_bbo_candidate(
    ticker: str, expected: dict[str, Any] | None = None
) -> bool:
    return pending_bbo_candidate_store.cancel(ticker, expected)


def _expire_pending_bbo_candidates(now: float | None = None) -> None:
    pending_bbo_candidate_store.expire(now)


def _store_pending_bbo_candidate(
    ticker: str,
    payload: dict[str, Any],
    received_monotonic: object | None = None,
) -> None:
    pending_bbo_candidate_store.store(
        ticker, payload, received_monotonic=received_monotonic
    )


async def _resume_pending_bbo_candidate(
    ticker: str,
    *,
    trigger: str = "ORDER_BOOK",
) -> None:
    """Revalidate a short-lived candidate after a coherent book or fresh quote event."""
    key = str(ticker or "").strip().upper().split("@")[0]
    with _pending_bbo_lock:
        pending = _pending_bbo_candidates.get(key)
    if pending is None:
        return

    normalized_trigger = str(trigger or "ORDER_BOOK").strip().upper()
    _trade_flow_inc("bbo_resume_attempted")
    _trade_flow_inc(
        "bbo_resume_from_quotes" if normalized_trigger == "QUOTES" else "bbo_resume_from_book"
    )

    now = time_module.monotonic()
    created_at = float(pending.get("created_at", 0.0) or 0.0)
    if created_at <= 0.0 or now - created_at > _PENDING_BBO_CANDIDATE_TTL_SEC:
        _drop_pending_bbo_candidate(key, pending)
        _trade_flow_inc("bbo_expired")
        _trade_flow_inc("bbo_resume_expired")
        return

    payload = pending.get("payload")
    if not isinstance(payload, dict):
        _trade_flow_inc("bbo_resume_invalid_payload")
        _cancel_pending_bbo_candidate(key, pending)
        return
    if (
        getattr(execution_engine, "_maintenance_active", False)
        or payload.get("paper_session_generation") != _paper_session_generation
    ):
        _mark_shadow_llm_not_called(payload.get("shadow_candidate_id"), "PAPER_SESSION_CHANGED")
        _trade_flow_inc("bbo_resume_session_cancelled")
        _cancel_pending_bbo_candidate(key, pending)
        return

    funnel_status = screener.get_status()
    if not funnel_status.get("ready"):
        _mark_shadow_llm_not_called(payload.get("shadow_candidate_id"), "FUNNEL_NOT_READY")
        _trade_flow_inc("bbo_resume_funnel_not_ready")
        _cancel_pending_bbo_candidate(key, pending)
        return
    if key not in set(funnel_status.get("selected_tickers", [])):
        _mark_shadow_llm_not_called(payload.get("shadow_candidate_id"), "TOP15_DESELECTED")
        _trade_flow_inc("bbo_resume_not_selected")
        _cancel_pending_bbo_candidate(key, pending)
        return

    active_tickers = execution_engine.get_open_position_tickers()
    if key in active_tickers or key in getattr(execution_engine, "_opening_tickers", set()):
        _mark_shadow_llm_not_called(payload.get("shadow_candidate_id"), "POSITION_ALREADY_OPEN")
        _trade_flow_inc("bbo_resume_active_position")
        _cancel_pending_bbo_candidate(key, pending)
        _trade_flow_inc("active_position_skipped")
        return

    metrics = payload.get("metrics")
    if not isinstance(metrics, dict):
        _mark_shadow_llm_not_called(payload.get("shadow_candidate_id"), "INVALID_METRICS_PAYLOAD")
        _trade_flow_inc("bbo_resume_invalid_payload")
        _cancel_pending_bbo_candidate(key, pending)
        return

    book_ready, book_reason = refresh_candidate_bbo_metrics(screener, key, metrics, now=now)
    if not book_ready:
        _trade_flow_inc("bbo_resume_" + _bbo_resume_reason_bucket(book_reason))
        logger.debug(
            "[PRE-LLM BBO RESUME WAIT] %s trigger=%s: %s",
            key, normalized_trigger, book_reason,
        )
        return

    direction = infer_candidate_direction(metrics)
    original_direction = str(payload.get("candidate_direction", "")).upper()
    if direction not in {"BUY", "SELL"} or direction != original_direction:
        _mark_shadow_llm_not_called(payload.get("shadow_candidate_id"), "DIRECTION_CHANGED")
        _trade_flow_inc("bbo_resume_direction_changed")
        _cancel_pending_bbo_candidate(key, pending)
        _trade_flow_inc("direction_prefilter_skipped")
        return

    if not payload.get("shadow_candidate_id"):
        shadow_result = _log_scalping_shadow(key, direction, metrics)
        payload["shadow_candidate_id"] = shadow_result.get("candidate_id")

    queue_flow_reason = _paper_queue_flow_conflict_reason(direction, metrics)
    if queue_flow_reason:
        _mark_shadow_llm_not_called(payload.get("shadow_candidate_id"), "QUEUE_TRADE_FLOW_CONFLICT")
        _trade_flow_inc("bbo_resume_evidence_blocked")
        _trade_flow_guard_block(queue_flow_reason)
        _log_prefilter_blocked(key, queue_flow_reason)
        _cancel_pending_bbo_candidate(key, pending)
        return

    evidence_ok, evidence_reason = validate_entry_evidence(
        direction, metrics, paper_mode=bool(getattr(execution_engine, "paper_mode", False))
    )
    if not evidence_ok:
        if _guard_reason_bucket(evidence_reason) == "quotes":
            _trade_flow_inc("bbo_resume_evidence_quotes")
            return
        _mark_shadow_llm_not_called(payload.get("shadow_candidate_id"), "SIGNAL_EVIDENCE_BLOCKED")
        _trade_flow_inc("bbo_resume_evidence_blocked")
        _cancel_pending_bbo_candidate(key, pending)
        _trade_flow_inc("prefilter_blocked")
        _trade_flow_guard_block(evidence_reason)
        _log_prefilter_blocked(key, evidence_reason)
        return

    atr_ready, atr_reason = validate_atr_readiness(metrics)
    if not atr_ready:
        _mark_shadow_llm_not_called(payload.get("shadow_candidate_id"), "ATR_NOT_READY")
        _trade_flow_inc("bbo_resume_atr_blocked")
        _trade_flow_inc("atr_unready_candidates")
        _cancel_pending_bbo_candidate(key, pending)
        _log_prefilter_blocked(key, atr_reason, interval_sec=60.0)
        return

    # A same-side tick may have refreshed the pending payload while this
    # coroutine was validating the book. Never enqueue stale evidence or remove
    # the newer payload; a later event will revalidate that newer candidate.
    with _pending_bbo_lock:
        still_current = (
            _pending_bbo_candidates.get(key) is pending
            and pending.get("payload") is payload
        )
    if not still_current:
        _mark_shadow_llm_not_called(payload.get("shadow_candidate_id"), "PAYLOAD_SUPERSEDED")
        _trade_flow_inc("bbo_resume_payload_changed")
        return

    if not _reserve_ai_ticker(key, payload.get("candidate_direction"), metrics):
        _mark_shadow_llm_not_called(payload.get("shadow_candidate_id"), "AI_DEDUPLICATED")
        _trade_flow_inc("bbo_resume_deduplicated")
        _cancel_pending_bbo_candidate(key, pending)
        _trade_flow_inc("candidate_deduplicated")
        return

    payload["bid"] = metrics.get("bid", 0.0)
    payload["ask"] = metrics.get("ask", 0.0)
    payload["bid_size"] = metrics.get("bid_size", 0.0)
    payload["ask_size"] = metrics.get("ask_size", 0.0)
    try:
        anomaly_queue.put_nowait(priority_queue_item_factory.create(1, "MARKET_TICK", payload))
        _drop_pending_bbo_candidate(key, pending)
        _trade_flow_inc("bbo_resumed")
        _trade_flow_inc("anomalies_queued")
        _trade_flow_inc("prefilter_pass_buy" if direction == "BUY" else "prefilter_pass_sell")
        logger.info(
            "✅ [PRE-LLM RESUME] %s: свежий BBO и ORDER_BOOK подтверждены; "
            "кандидат %s передан в LLM-очередь (trigger=%s).",
            key, direction, normalized_trigger,
        )
        update_numpy_impulse_status(key)
    except asyncio.QueueFull:
        _mark_shadow_llm_not_called(payload.get("shadow_candidate_id"), "AI_QUEUE_FULL")
        _release_ai_ticker(key)
        _drop_pending_bbo_candidate(key, pending)
        _trade_flow_inc("bbo_resume_queue_full")
        _trade_flow_inc("queue_dropped")
        logger.error("Очередь AI-сигналов заполнена; восстановленный кандидат %s отброшен.", key)


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
async def process_market_event(event: Union[SimpleNamespace, dict]):
    """Обновляет локальную воронку по каждой котировке и направляет в LLM только top-N."""
    global tick_counter, _last_funnel_log_at, _last_funnel_log_key, _last_funnel_ready
    event_session_generation = _paper_session_generation
    if getattr(execution_engine, "_maintenance_active", False):
        return
    _expire_pending_bbo_candidates()
    try:
        # Instrument trades update the trade-flow accumulator independently of QUOTES.
        if market_event_type(event) == "INSTRUMENT_TRADE":
            trade_ticker = normalize_market_ticker(event.get("ticker"))
            if not trade_ticker:
                _trade_flow_inc("instrument_trades_invalid")
                return
            trade_result = screener.push_trade(
                ticker=trade_ticker, price=event.get("price", 0.0),
                size=event.get("size", 0.0), trade_id=event.get("trade_id"),
                side=event.get("side"), event_timestamp=event.get("timestamp"),
                received_monotonic=event.get("received_monotonic"),
            )
            if trade_result.get("accepted"):
                _trade_flow_inc("instrument_trades_received")
            else:
                trade_reason = str(trade_result.get("reason") or "rejected")
                _trade_flow_inc("instrument_trades_" + trade_reason)
                # Rejected trade events never invalidate the independent QUOTES-driven M1 history.
            return

        # ORDER_BOOK events have no last-trade price. Route them before the QUOTES
        # normalizer, otherwise price<=0 would silently discard every book update.
        if market_event_type(event) == "ORDER_BOOK":
            try:
                book_event = normalize_order_book_event(event)
                book_ticker = book_event["ticker"]
                if not book_ticker:
                    _trade_flow_inc("orderbook_unusable")
                    return
                book_result = await screener.push_order_book(**book_event)
            except (TypeError, ValueError, KeyError):
                _trade_flow_inc("orderbook_unusable")
                return
            _record_position_order_book_event(book_ticker, book_result)
            _record_paper_bbo_replay(
                book_ticker,
                source="FINAM_ORDER_BOOK",
                event_timestamp=book_event.get("order_book_timestamp"),
                received_monotonic=book_event.get("received_monotonic"),
                book_result=book_result,
            )
            if book_result.get("updated") and book_result.get("valid", True):
                _trade_flow_inc("orderbook_updates")
                _trade_flow_inc("orderbook_side_touches", int(book_result.get("bbo_side_touch_count", 0) or 0))
                if book_result.get("heartbeat_only"):
                    _trade_flow_inc("orderbook_heartbeats")
                shadow_metrics: dict[str, Any] = {}
                shadow_bbo_ready, _shadow_bbo_reason = refresh_candidate_bbo_metrics(
                    screener, book_ticker, shadow_metrics, now=time_module.monotonic()
                )
                if shadow_bbo_ready:
                    shadow_labels = scalping_shadow_tracker.observe_market_tick(book_ticker, shadow_metrics)
                    if shadow_labels:
                        await _persist_shadow_outcomes(shadow_labels, source="ORDER_BOOK")
                await _resume_pending_bbo_candidate(book_ticker, trigger="ORDER_BOOK")
                # ORDER_BOOK can refresh BBO independently of QUOTES. Re-run the
                # PAPER exit monitor only after a coherent book confirmation.
                if execution_engine.paper_mode and book_ticker in execution_engine.paper_positions:
                    last_trade_price = execution_engine._latest_price(
                        book_ticker, execution_engine.paper_positions.get(book_ticker)
                    )
                    execution_engine.schedule_paper_position_monitor(
                        book_ticker,
                        last_trade_price if last_trade_price > 0.0 else None,
                        db_manager,
                        event_source="ORDER_BOOK",
                        event_timestamp=book_event.get("order_book_timestamp"),
                    )
            elif book_result.get("updated"):
                _trade_flow_inc("orderbook_invalid")
                logger.debug(
                    "ORDER_BOOK %s заблокирован: %s",
                    book_ticker, book_result.get("reason", book_result.get("book_reason", "invalid")),
                )
            elif book_result.get("out_of_order"):
                _trade_flow_inc("orderbook_out_of_order")
            else:
                _trade_flow_inc("orderbook_unusable")
                if book_result.get("reason"):
                    logger.debug("ORDER_BOOK %s не принят: %s", book_ticker, book_result["reason"])
            return

        quote_event = normalize_market_quote_event(event)
        ticker = quote_event.ticker
        price = quote_event.price
        volume = quote_event.volume
        bid = quote_event.bid
        ask = quote_event.ask
        bid_size = quote_event.bid_size
        ask_size = quote_event.ask_size
        bid_present = quote_event.bid_present
        ask_present = quote_event.ask_present
        bid_size_present = quote_event.bid_size_present
        ask_size_present = quote_event.ask_size_present
        day_volume = quote_event.day_volume
        supplied_metrics = quote_event.metrics
        event_timestamp = quote_event.timestamp
        received_monotonic = quote_event.received_monotonic
        if price <= 0.0 or not ticker or volume < 0.0:
            return
        # Quote ticks do not cancel deferred candidates. They may refresh the
        # two-sided quote confirmation clocks; a pending candidate can resume on
        # QUOTES only while a recently confirmed coherent ORDER_BOOK is still fresh.

        tick_counter += 1
        current_frame = spinner_frames[tick_counter % len(spinner_frames)]
        terminal_status.update(
            f"⚡ [WS ПОТОК ФИНАМ] {current_frame} "
            f"Тиков: {tick_counter:,} | {ticker} {price}₽"
        )

        # Учитываем каждый инструмент в рейтинге, а не только аномальные тики.
        analysis = await screener.push_tick(
            ticker=ticker,
            price=price,
            volume=volume,
            bid=bid,
            ask=ask,
            bid_size=bid_size,
            ask_size=ask_size,
            day_volume=day_volume,
            bid_present=bid_present,
            ask_present=ask_present,
            bid_size_present=bid_size_present,
            ask_size_present=ask_size_present,
            event_timestamp=event_timestamp,
            received_monotonic=received_monotonic,
        )

        _trade_flow_inc("quotes_received")
        accepted_quote_sides = analysis.get("quote_side_updates", {})
        _record_paper_bbo_replay(
            ticker,
            source="FINAM_QUOTES",
            event_timestamp=event_timestamp,
            received_monotonic=received_monotonic,
            sides_present=bool(
                isinstance(accepted_quote_sides, Mapping)
                and (
                    (bid_present and accepted_quote_sides.get("bid"))
                    or (ask_present and accepted_quote_sides.get("ask"))
                )
            ),
        )
        # ORDER_BOOK and QUOTES use independent provider streams. A valid recent
        # book can be confirmed by the newest two-sided QUOTES packet between
        # book deltas; this prevents a pending candidate from expiring merely
        # because no new best-level ORDER_BOOK event arrived within its 1.5s TTL.
        with _pending_bbo_lock:
            has_pending_bbo = ticker in _pending_bbo_candidates
        if has_pending_bbo:
            await _resume_pending_bbo_candidate(ticker, trigger="QUOTES")
        quote_metrics_for_shadow = analysis.get("metrics", {})
        if isinstance(quote_metrics_for_shadow, dict):
            shadow_labels = scalping_shadow_tracker.observe_market_tick(ticker, quote_metrics_for_shadow)
            if shadow_labels:
                await _persist_shadow_outcomes(shadow_labels, source="QUOTES")
        funnel_status = screener.get_status()
        terminal_status.update_funnel(screener.get_status_text())
        if analysis.get("selected") and analysis.get("selection_ready"):
            _trade_flow_inc("selected_quotes")
        now_mono = time_module.monotonic()
        selected_snapshot = tuple(funnel_status.get("selected_tickers", []))
        if funnel_status.get("ready") and selected_snapshot:
            _schedule_m1_history_warmup(selected_snapshot)
        if not funnel_status["ready"]:
            _last_funnel_ready = False
            if _last_funnel_log_at == 0.0 or now_mono - _last_funnel_log_at >= 30.0:
                logger.info(
                    "🎯 Воронка прогревается: котировки получены по %d/%d инструментам; пригодны для рейтинга %d; цель top-%d.",
                    funnel_status["observed_count"], funnel_status["universe_count"], funnel_status["eligible_count"], funnel_status["top_n"],
                )
                _last_funnel_log_at = now_mono
        elif selected_snapshot:
            first_ready = not _last_funnel_ready
            selection_changed = _last_funnel_log_key is None or selected_snapshot != _last_funnel_log_key
            cooldown_elapsed = _last_funnel_log_at == 0.0 or now_mono - _last_funnel_log_at >= 30.0
            if first_ready or (selection_changed and cooldown_elapsed):
                logger.info(
                    "🎯 ВОРОНКА ГОТОВА: %d → %d тикеров для LLM: %s",
                    funnel_status["universe_count"], len(selected_snapshot), ", ".join(selected_snapshot),
                )
                _last_funnel_log_at = now_mono
                _last_funnel_log_key = selected_snapshot
            _last_funnel_ready = True

        logger.debug(
            "📥 [ВХОДЯЩИЙ ТИК] %s price=%s anomaly=%s selected=%s rank=%s score=%s",
            ticker, price, analysis["is_anomaly"], analysis.get("selected", False),
            analysis.get("metrics", {}).get("funnel_rank", 0),
            analysis.get("metrics", {}).get("funnel_score", 0.0),
        )

        # Аномалия сама по себе не запускает LLM. Сначала локально определяем
        # направление, затем проверяем измеряемые условия входа. Модель вызывается
        # только для кандидата, который не был заранее отклонён численным guard.
        if analysis["is_anomaly"] and analysis.get("selection_ready") and analysis.get("selected"):
            _trade_flow_inc("selected_anomalies")
            metrics_payload = analysis.get("metrics", supplied_metrics if isinstance(supplied_metrics, dict) else {})
            if not isinstance(metrics_payload, dict):
                metrics_payload = {}

            # Existing positions are monitored independently on every market event.
            # Do not spend another LLM call proposing a second entry in the same ticker.
            active_tickers = execution_engine.get_open_position_tickers()
            ticker_opening = ticker in getattr(execution_engine, "_opening_tickers", set())
            if ticker in active_tickers or ticker_opening:
                _trade_flow_inc("active_position_skipped")
            else:
                candidate_direction = infer_candidate_direction(metrics_payload)
                if candidate_direction is None:
                    _trade_flow_inc("direction_prefilter_skipped")
                    _log_prefilter_blocked(
                        ticker,
                        "Локальное направление не подтверждено: нет импульса цены либо VWAP показывает сильное встречное движение.",
                    )
                else:
                    direction_state = classify_direction_evidence(candidate_direction, metrics_payload)
                    metrics_payload["direction_evidence_state"] = direction_state
                    _trade_flow_inc("local_candidate_buy" if candidate_direction == "BUY" else "local_candidate_sell")
                    logger.debug(
                        "[DIRECTION GATE] %s side=%s state=%s price_change=%.5f%% vs_vwap=%.5f%%",
                        ticker, candidate_direction, direction_state,
                        float(metrics_payload.get("price_change_pct", 0.0) or 0.0),
                        float(metrics_payload.get("price_vs_vwap_pct", 0.0) or 0.0),
                    )
                    shadow_result = _log_scalping_shadow(ticker, candidate_direction, metrics_payload)
                    shadow_candidate_id = shadow_result.get("candidate_id")
                    queue_flow_reason = _paper_queue_flow_conflict_reason(candidate_direction, metrics_payload)
                    if queue_flow_reason:
                        evidence_ok, evidence_reason = False, queue_flow_reason
                    else:
                        evidence_ok, evidence_reason = validate_entry_evidence(
                            candidate_direction, metrics_payload,
                            paper_mode=bool(getattr(execution_engine, "paper_mode", False)),
                        )
                    if not evidence_ok:
                        if _guard_reason_bucket(evidence_reason) == "quotes":
                            pending_payload = build_market_candidate_payload(
                                ticker=ticker,
                                candidate_created_monotonic=time_module.monotonic(),
                                price=price,
                                volume=volume,
                                bid=bid,
                                ask=ask,
                                bid_size=bid_size,
                                ask_size=ask_size,
                                day_volume=day_volume,
                                candidate_direction=candidate_direction,
                                shadow_candidate_id=shadow_candidate_id,
                                paper_session_generation=event_session_generation,
                                metrics=metrics_payload,
                            )
                            _store_pending_bbo_candidate(ticker, pending_payload, received_monotonic)
                        else:
                            _mark_shadow_llm_not_called(shadow_candidate_id, "SIGNAL_EVIDENCE_BLOCKED")
                            _trade_flow_inc("prefilter_blocked")
                            _trade_flow_guard_block(evidence_reason)
                            _log_prefilter_blocked(ticker, evidence_reason)
                    else:
                        atr_ready, atr_reason = validate_atr_readiness(metrics_payload)
                        if not atr_ready:
                            _mark_shadow_llm_not_called(shadow_candidate_id, "ATR_NOT_READY")
                            # ATR is a hard pre-LLM prerequisite. Re-evaluate on a
                            # later fresh tick instead of spending inference only to
                            # reject an APPROVE after the model has answered.
                            _trade_flow_inc("atr_unready_candidates")
                            _log_prefilter_blocked(ticker, atr_reason, interval_sec=60.0)
                        elif not _reserve_ai_ticker(ticker, candidate_direction, metrics_payload):
                            _mark_shadow_llm_not_called(shadow_candidate_id, "AI_DEDUPLICATED")
                            _trade_flow_inc("candidate_deduplicated")
                        else:
                            event_payload = build_market_candidate_payload(
                                ticker=ticker,
                                candidate_created_monotonic=time_module.monotonic(),
                                price=price,
                                volume=volume,
                                bid=bid,
                                ask=ask,
                                bid_size=bid_size,
                                ask_size=ask_size,
                                day_volume=day_volume,
                                candidate_direction=candidate_direction,
                                shadow_candidate_id=shadow_candidate_id,
                                paper_session_generation=event_session_generation,
                                metrics=metrics_payload,
                            )
                            try:
                                anomaly_queue.put_nowait(priority_queue_item_factory.create(1, "MARKET_TICK", event_payload))
                                _trade_flow_inc("anomalies_queued")
                                _trade_flow_inc("prefilter_pass_buy" if candidate_direction == "BUY" else "prefilter_pass_sell")
                                update_numpy_impulse_status(ticker)
                            except asyncio.QueueFull:
                                _mark_shadow_llm_not_called(shadow_candidate_id, "AI_QUEUE_FULL")
                                _release_ai_ticker(ticker)
                                _trade_flow_inc("queue_dropped")
                                logger.error("Очередь AI-сигналов заполнена; аномалия %s отброшена.", ticker)

        # Обновление позиции продолжается по всем 88 котировкам, а не только по top-N.
        execution_engine.schedule_paper_position_monitor(
            ticker,
            price,
            db_manager,
            event_source="LAST_TRADE",
            event_timestamp=event_timestamp,
        )

    except Exception as e:
        logger.error("💥 Ошибка в обработчике тиков: %s", e, exc_info=True)
# --------------------------------------------------------------
#  5️⃣  MARKET WORKER С ПРИОРИТЕТНЫМ ПЕРЕХВАТОМ И КОНТУРОМ RAG
# --------------------------------------------------------------
class MarketWorker:
    def __init__(self, db: DBManager, ai: AIEngine, broker_client):
        self.db = db
        self.ai = ai
        self.broker = broker_client
        self.client_id = CLIENT_ID
        self._last_risk_block_verdict_at = 0.0

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
        """Serialize order execution with PAPER reset and reject stale/pre-reset signals."""
        async with _paper_session_lock:
            if (
                getattr(execution_engine, "_maintenance_active", False)
                or session_generation != _paper_session_generation
            ):
                logger.info("⏭️ Старый сигнал %s по %s отброшен: PAPER-сессия изменилась/на обслуживании.", verdict_id, ticker)
                return
            await self._execute_and_publish_locked(
                verdict_id=verdict_id, ticker=ticker, lots=lots, side=side,
                price=price, ai_analysis=ai_analysis,
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
        """Execute only a fresh, risk-approved candidate and publish its final result."""
        try:
            # The pre-LLM TTL check cannot protect against a slow inference call.
            # Recheck the original market-evidence age immediately before execution.
            if candidate_is_expired(
                candidate_created_monotonic,
                ttl_sec=_AI_CANDIDATE_TTL_SEC,
            ):
                try:
                    candidate_age_sec = max(
                        0.0,
                        time_module.monotonic() - float(candidate_created_monotonic),
                    )
                except (TypeError, ValueError, OverflowError):
                    candidate_age_sec = -1.0
                result = {
                    "status": "BLOCKED",
                    "message": (
                        "SIGNAL_EXPIRED: рыночные данные кандидата устарели во время LLM/очереди; "
                        "вход отменён без исполнения."
                    ),
                }
                _trade_flow_inc("execution_stale_signal_blocked")
                logger.warning(
                    "[EXECUTION STALE SIGNAL BLOCK] %s %s: age=%.3fs ttl=%.3fs; "
                    "устаревший сигнал не передан исполнителю.",
                    side, ticker, candidate_age_sec, _AI_CANDIDATE_TTL_SEC,
                )
            # Re-read the complete risk state immediately before execution.
            # A candidate may have waited in an async task while another trade
            # closed and activated the session hard stop.
            elif not await risk_manager.is_trading_allowed(self.db, force_refresh=True):
                result = {
                    "status": "BLOCKED",
                    "message": (
                        "RISK_FINAL_GATE: RiskManager запретил исполнение после "
                        "повторной проверки; ранее одобренный сигнал отброшен."
                    ),
                }
                _trade_flow_inc("risk_final_gate_blocked")
                logger.warning(
                    "🛑 [RISK FINAL GATE] %s %s: отложенный сигнал %s заблокирован перед исполнением.",
                    side, ticker, verdict_id,
                )
            else:
                result = await execution_engine.execute_order(
                    ticker=ticker,
                    lots=lots,
                    side=side,
                    price=price,
                    client_id=self.client_id,
                    db=self.db,
                    ai_analysis=ai_analysis,
                )
            if not isinstance(result, dict):
                result = {"status": "FAILED", "message": "Исполнитель вернул некорректный ответ."}
        except Exception as exc:
            logger.error("Ошибка исполнения %s %s: %s", side, ticker, exc, exc_info=True)
            result = {"status": "FAILED", "message": str(exc)}

        raw_status = str(result.get("status", "FAILED")).upper()
        result_mode = str(result.get("mode") or ("PAPER" if getattr(execution_engine, "paper_mode", True) else "LIVE")).upper()
        execution_status = normalise_execution_status(raw_status, result_mode)
        if shadow_candidate_id:
            scalping_shadow_tracker.update_execution_result(
                shadow_candidate_id, execution_status=execution_status
            )
        result_message = str(result.get("message", ""))
        outcome_requested_lots = nonnegative_int(result.get("requested_lots"))
        if outcome_requested_lots is None or outcome_requested_lots <= 0:
            outcome_requested_lots = max(0, int(lots))
        outcome_filled_lots = nonnegative_int(result.get("lots"))
        if outcome_filled_lots is None:
            outcome_filled_lots = (
                outcome_requested_lots if execution_status in {"OPENED", "ACCEPTED"} else 0
            )
        logger.info(
            "[EXECUTION OUTCOME] %s %s: status=%s mode=%s requested_lots=%d filled_lots=%d message=%s",
            side, ticker, execution_status, result_mode, outcome_requested_lots, outcome_filled_lots,
            result_message.replace(chr(10), " ").replace(chr(13), " ")[:240] or "-",
        )
        if "BBO_ENTRY_BLOCK" in result_message:
            _trade_flow_inc("execution_bbo_blocked")
        if "SPREAD_ATR_ENTRY_BLOCK" in result_message:
            _trade_flow_inc("execution_spread_atr_blocked")
        if execution_status in {"OPENED", "ACCEPTED"}:
            # Count actual executor outcomes, not merely queued AI decisions.
            _trade_flow_inc("orders_scheduled")
            _trade_flow_inc("orders_opened_buy" if side == "BUY" else "orders_opened_sell")
        order_id = str(result.get("orderId") or result.get("order_id") or "")
        raw_actual_lots = result.get("lots")
        raw_actual_shares = result.get("shares")
        raw_actual_notional = result.get("notional_entry_rub")
        actual_lots = nonnegative_int(raw_actual_lots)
        actual_shares = nonnegative_int(raw_actual_shares)
        actual_notional = finite_float(raw_actual_notional)
        requested_lots_from_executor = nonnegative_int(result.get("requested_lots"))
        if execution_status in {"SKIPPED", "BLOCKED", "FAILED"}:
            if "Лимиты портфеля:" in result_message:
                _trade_flow_inc("portfolio_cap_blocked")
            elif "ENTRY_SESSION_BLOCK" in result_message:
                _trade_flow_inc("entry_session_blocked")
            elif "BBO_ENTRY_BLOCK" not in result_message and "SPREAD_ATR_ENTRY_BLOCK" not in result_message and "AI Decision is HOLD" not in result_message:
                _trade_flow_inc("execution_other_blocked")
        if (
            execution_status in {"OPENED", "ACCEPTED"}
            and requested_lots_from_executor is not None
            and actual_lots is not None
            and actual_lots < requested_lots_from_executor
        ):
            _trade_flow_inc("portfolio_size_reduced")
        if execution_status in {"OPENED", "ACCEPTED"} and raw_actual_lots is None:
            actual_lots = int(lots)
        if execution_status in {"OPENED", "ACCEPTED"}:
            logger.info(
                "📌 [EXECUTION RESULT] %s %s: requested=%s lots; actual=%s lots/%s shares; notional=%.2f ₽; status=%s",
                side, ticker, requested_lots_from_executor if requested_lots_from_executor is not None else lots,
                actual_lots if actual_lots is not None else lots,
                actual_shares if actual_shares is not None else 0,
                actual_notional if actual_notional is not None else 0.0, execution_status,
            )
        if (
            execution_status in {"OPENED", "ACCEPTED"}
            and raw_actual_shares is None
            and actual_lots is not None
        ):
            actual_shares = actual_lots * max(int(FINAM_TRADE_LOT_SIZES.get(ticker, 0) or 0), 0)
        if (
            execution_status in {"OPENED", "ACCEPTED"}
            and raw_actual_notional is None
            and actual_shares is not None
        ):
            actual_notional = round(actual_shares * price, 2)

        update = {
            "verdict_id": verdict_id,
            "ticker": ticker,
            "execution_status": execution_status,
            "execution_message": str(result.get("message") or (
                "PAPER-позиция зарегистрирована." if execution_status == "OPENED"
                else "Брокер принял заявку; факт исполнения должен быть подтверждён отдельно." if execution_status == "ACCEPTED"
                else "Заявка не открыта."
            )),
            "order_id": order_id or None,
            "executed_lots": actual_lots,
            "executed_shares": actual_shares,
            "actual_notional_rub": actual_notional,
            "execution_timestamp": datetime.now().astimezone().isoformat(timespec="milliseconds"),
        }

        # Update in-memory history so a browser refresh shows the final outcome.
        for cached in last_ai_verdicts_cache:
            if isinstance(cached, dict) and cached.get("verdict_id") == verdict_id:
                cached.update(update)
                break

        # Persist execution outcome separately. This avoids list-index races when
        # several asynchronous orders finish while newer verdicts are LPUSH'ed.
        try:
            redis_inst = getattr(self.db, "redis", getattr(self.db, "_redis", None))
            if redis_inst is not None:
                await redis_inst.set(
                    f"piluitus:verdict:execution:{verdict_id}",
                    json.dumps(update, ensure_ascii=False),
                    ex=7 * 24 * 60 * 60,
                )
        except Exception as persist_exc:
            logger.warning("Не удалось сохранить статус исполнения вердикта %s: %s", verdict_id, persist_exc)

        try:
            positions = await _get_dashboard_open_positions()
            balance = await _refresh_dashboard_balance_snapshot(positions)
            update["active_positions"] = positions
            update["balance"] = balance
            await manager.broadcast(json.dumps({"type": "EXECUTION_UPDATE", **update}, ensure_ascii=False))
            await manager.broadcast(json.dumps({
                "type": "TABLES_UPDATE",
                "active_positions": positions,
                "closed_trades": getattr(execution_engine, "closed_trades", [])[-30:],
                "balance": balance,
            }, ensure_ascii=False))
        except Exception as broadcast_exc:
            logger.error("Не удалось отправить статус исполнения в веб-интерфейс: %s", broadcast_exc, exc_info=True)

        if execution_status in {"OPENED", "ACCEPTED"}:
            logger.info(
                "📡 [DASHBOARD EXECUTION] %s %s %s: %s лот(а), %s ₽, order=%s",
                side, ticker, execution_status, actual_lots, actual_notional, order_id or "N/A",
            )
        else:
            logger.warning(
                "📡 [DASHBOARD EXECUTION] %s %s: %s — %s",
                side, ticker, execution_status, update["execution_message"],
            )

    async def _publish_pre_llm_risk_block(self, event_payload: dict, session_generation: int) -> None:
        """Publish a local candidate rejected by RiskManager if its PAPER session is still current."""
        async with _paper_session_lock:
            if (
                getattr(execution_engine, "_maintenance_active", False)
                or session_generation != _paper_session_generation
            ):
                return
            await self._publish_pre_llm_risk_block_locked(event_payload)

    async def _publish_pre_llm_risk_block_locked(self, event_payload: dict) -> None:
        """Persist and broadcast the pre-LLM risk block."""
        now_mono = time_module.monotonic()
        # RiskManager is a global gate. Avoid filling the verdict panel with one
        # identical card per ticker while the same global stop remains active.
        if now_mono - self._last_risk_block_verdict_at < 10.0:
            return
        self._last_risk_block_verdict_at = now_mono
        ticker = str(event_payload.get("ticker", "UNKNOWN")).strip().upper()
        price = event_payload.get("price", 0.0)
        metrics = event_payload.get("metrics", {})
        if not isinstance(metrics, dict):
            metrics = {}
        candidate = str(event_payload.get("candidate_direction", "HOLD")).upper()
        reason = "RiskManager остановил торговлю до запроса к ИИ; исполнитель не вызывался."
        tactic, critic = build_verdict_explanation("HOLD", candidate, metrics, reason)
        now_local = datetime.now().astimezone()
        verdict_id = uuid4().hex
        verdict = {
            "verdict_id": verdict_id,
            "timestamp": now_local.strftime("%H:%M:%S"),
            "timestamp_iso": now_local.isoformat(timespec="milliseconds"),
            "ticker": ticker,
            "price": price,
            "decision": "HOLD",
            "candidate_decision": candidate,
            "confidence": None,
            "decision_source": "RISK_MANAGER",
            "signal_status": "BLOCKED" if candidate in {"BUY", "SELL"} else "NO_ENTRY",
            "signal_reason": reason,
            "execution_status": "NOT_REQUESTED",
            "execution_message": "Не запрашивалось: торговля остановлена RiskManager.",
            "tactic_opinion": tactic,
            "critic_opinion": critic,
        }
        last_ai_verdicts_cache.append(verdict)
        if len(last_ai_verdicts_cache) > MAX_CACHE_SIZE:
            last_ai_verdicts_cache.pop(0)
        try:
            redis_inst = getattr(self.db, "redis", getattr(self.db, "_redis", None))
            if redis_inst is not None:
                await redis_inst.lpush("piluitus:verdicts:history", json.dumps(verdict, ensure_ascii=False))
                await redis_inst.ltrim("piluitus:verdicts:history", 0, MAX_CACHE_SIZE - 1)
        except Exception as persist_exc:
            logger.warning("Не удалось сохранить RiskManager-вердикт %s: %s", ticker, persist_exc)
        try:
            payload = {"type": "MARKET_UPDATE", **verdict, "balance": dict(_dashboard_balance_snapshot)}
            await manager.broadcast(json.dumps(payload, ensure_ascii=False))
        except Exception as broadcast_exc:
            logger.error("Не удалось отправить RiskManager-вердикт в веб-интерфейс: %s", broadcast_exc)

    async def start_processing(self):
        global last_ai_verdicts_cache
        logger.info("🤖 ИИ‑конвейер воркеров Piluitus v35.2 успешно запущен.")

        while True:
            queue_item_acquired = False
            pending_ticker = ""
            try:
                priority, _sequence_id, _timestamp_id, event_type, event_payload = await anomaly_queue.get()
                queue_item_acquired = True
                if event_type == "MARKET_TICK" and isinstance(event_payload, dict):
                    pending_ticker = str(event_payload.get("ticker", "")).strip().upper()
                    if (
                        getattr(execution_engine, "_maintenance_active", False)
                        or event_payload.get("paper_session_generation") != _paper_session_generation
                    ):
                        continue
                    created_at = event_payload.get("candidate_created_monotonic")
                    if created_at is not None and candidate_is_expired(
                        created_at, ttl_sec=_AI_CANDIDATE_TTL_SEC
                    ):
                        _trade_flow_inc("candidate_expired")
                        logger.info(
                            "[CANDIDATE TTL] %s side=%s expired before LLM; evidence age exceeded %.1fs.",
                            pending_ticker,
                            event_payload.get("candidate_direction", "NONE"),
                            _AI_CANDIDATE_TTL_SEC,
                        )
                        continue

                if priority == 0:
                    logger.warning(f"🚨 [ПРИОРИТЕТНЫЙ ПЕРЕХВАТ] Выполнение экстренной команды: {event_type}")
                    if event_type == "CMD_CLOSE_ALL":
                        await execution_engine.close_all_positions(db=self.db) 
                    elif event_type == "CMD_SET_RISK_LIMIT":
                        try:
                            risk_manager.daily_loss_limit = validate_daily_loss_limit(
                                event_payload.get("value", Settings.DAILY_LOSS_LIMIT_PCT)
                            )
                        except ValueError as exc:
                            logger.error("[RISK LIMIT] Некорректное значение в очереди: %s", exc)
                    continue

                ticker   = event_payload.get("ticker") or event_payload.get("security_code") or "UNKNOWN"
                price    = event_payload.get("price") or 0.0
                metrics  = event_payload.get("metrics", {})
                if not isinstance(metrics, dict):
                    metrics = {}
                candidate_direction_for_diag = str(event_payload.get("candidate_direction") or "").upper()
                llm_input_features = build_llm_feature_snapshot(metrics)
                volume_spike = _parse_finite_float(metrics.get("volume_spike"))
                volume_5s = _parse_finite_float(metrics.get("trade_volume_5s"))
                volume_baseline_5s = _parse_finite_float(metrics.get("trade_volume_baseline_5s"))
                trade_count_60s = _parse_finite_float(metrics.get("trade_count_60s"))
                trade_data_ready = (_parse_finite_float(metrics.get("trade_data_ready")) or 0.0) >= 0.5
                volume_spike_text = f"{volume_spike:.2f}×" if volume_spike is not None else "Н/Д"
                if trade_data_ready and volume_5s is not None and volume_baseline_5s is not None:
                    baseline_buckets = int(_parse_finite_float(metrics.get("trade_baseline_bucket_count")) or 0)
                    trades_5s = int(_parse_finite_float(metrics.get("trade_count_5s")) or 0)
                    volume_text = (
                        f"Всплеск объёма: {volume_spike_text}; "
                        f"5с={volume_5s:.3f} ед. провайдера, медианная база={volume_baseline_5s:.3f}; "
                        f"сделок 5с={trades_5s}, сделок 60с={int(trade_count_60s or 0)}, "
                        f"активных 5с-интервалов базы={baseline_buckets}."
                    )
                else:
                    volume_text = f"Объём сделок: Н/Д — история потока не готова; spike={volume_spike_text}."

                trade_realized_volatility = _parse_finite_float(
                    metrics.get("trade_realized_volatility_60s_pct")
                )
                m1_atr_pct = _parse_finite_float(metrics.get("bar_atr_pct"))
                m1_atr_periods = _parse_finite_float(metrics.get("bar_atr_periods")) or 0.0
                if trade_data_ready and trade_realized_volatility is not None:
                    volatility_text = (
                        f"Реализованная волатильность по сделкам за 60с: "
                        f"{trade_realized_volatility:.5f}%."
                    )
                elif m1_atr_pct is not None and m1_atr_pct > 0.0 and m1_atr_periods >= 5.0:
                    volatility_text = f"M1 ATR ({int(m1_atr_periods)} закрытых баров): {m1_atr_pct:.5f}%."
                else:
                    volatility_text = "Волатильность: Н/Д — нет подтверждённой истории."

                _background_operations.spawn(
                    log_trading_event_to_file(
                        ticker=ticker,
                        price=price,
                        decision="В ОЧЕРЕДИ (ПРОВЕРКА РИСКОВ)",
                        confidence=0.0,
                        tactic=volume_text,
                        critic=volatility_text,
                        candidate_id=str(event_payload.get("shadow_candidate_id") or "") or None,
                    ),
                    name=f"trading-log-queue:{ticker}",
                )

                trading_allowed = await risk_manager.is_trading_allowed(self.db)
                if (
                    getattr(execution_engine, "_maintenance_active", False)
                    or event_payload.get("paper_session_generation") != _paper_session_generation
                ):
                    continue
                if not trading_allowed:
                    _trade_flow_inc("risk_blocked")
                    _background_operations.spawn(
                        log_trading_event_to_file(
                            ticker,
                            price,
                            "RISK_BLOCK",
                            0.0,
                            "Торговля заблокирована.",
                            "RiskManager запретил торговлю до запроса к ИИ.",
                            candidate_id=str(event_payload.get("shadow_candidate_id") or "") or None,
                        ),
                        name=f"trading-log-risk-block:{ticker}",
                    )
                    await self._publish_pre_llm_risk_block(
                        event_payload, int(event_payload.get("paper_session_generation", -1))
                    )
                    continue

                try:
                    from ai.knowledge_parser import TechnicalKnowledgeParser

                    rag_context_str = await TechnicalKnowledgeParser.get_knowledge_context(
                        db=self.db,
                        category="ScalpingRules",
                        market_evidence=metrics if isinstance(metrics, dict) else {},
                    )
                    event_payload["rag_context"] = [rag_context_str]
                    retrieved_rule_ids = re.findall(
                        r"\[(?:RISK|BUY|SELL)\s*/\s*([A-Z0-9_]+)\]",
                        rag_context_str,
                    )
                    # The prompt receives always-on RISK rules plus only the
                    # selected local candidate's directional rules. They remain
                    # advisory context; deterministic guards own hard entry gates.
                    prompt_rule_text = select_prompt_rules(
                        candidate_direction_for_diag, rag_context_str, max_chars=900
                    )
                    prompt_rule_ids = re.findall(
                        r"\[(?:RISK|BUY|SELL)\s*/\s*([A-Z0-9_]+)\]",
                        prompt_rule_text,
                    )
                    logger.info(
                        "[LLM CONTEXT] ticker=%s candidate=%s retrieved_rule_ids=%s prompt_rule_ids=%s",
                        ticker, candidate_direction_for_diag,
                        ",".join(retrieved_rule_ids) if retrieved_rule_ids else "NONE",
                        ",".join(prompt_rule_ids) if prompt_rule_ids else "NONE",
                    )
                except Exception as exc:
                    logger.error("⚠️ Ошибка сборки RAG-контекста в main.py: %s", exc, exc_info=True)
                    event_payload["rag_context"] = [
                        "Нет проверенного контекста правил; направленный вход должен быть HOLD."
                    ]

                logger.info(
                    "🧭 [LLM INPUT] ticker=%s candidate=%s features=%s",
                    ticker, candidate_direction_for_diag,
                    json.dumps(llm_input_features, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                )
                try:
                    _trade_flow_inc("llm_calls")
                    ai_response = await asyncio.wait_for(
                        self.ai.analyze_market_state(event_payload),
                        timeout=float(Settings.VLLM_TIMEOUT) * 2.0 + 1.0,
                    )
                except asyncio.TimeoutError as exc:
                    ai_response = {
                        "final_decision": "HOLD",
                        "model_decision": "HOLD",
                        "model_raw_decision": "INVALID_RESPONSE",
                        "model_contract_violation": False,
                        "contract_error_type": None,
                        "error_type": "worker_timeout",
                        "request_error": True,
                        "request_error_type": "worker_timeout",
                        "confidence_score": 0.0,
                        "confidence_valid": False,
                        "tactic_opinion": "Превышено общее время ожидания ИИ; торговый вход заблокирован.",
                        "critic_opinion": str(exc),
                    }
                except Exception as exc:
                    ai_response = {
                        "final_decision": "HOLD",
                        "model_decision": "HOLD",
                        "model_raw_decision": "INVALID_RESPONSE",
                        "model_contract_violation": False,
                        "contract_error_type": None,
                        "error_type": "worker_error",
                        "request_error": True,
                        "request_error_type": "worker_error",
                        "confidence_score": 0.0,
                        "confidence_valid": False,
                        "tactic_opinion": "Ошибка обработки запроса ИИ; торговый вход заблокирован.",
                        "critic_opinion": str(exc),
                    }

                if (
                    getattr(execution_engine, "_maintenance_active", False)
                    or event_payload.get("paper_session_generation") != _paper_session_generation
                ):
                    continue

                ai_analysis = {"final_decision": "HOLD", "confidence_score": 0.0, "confidence_valid": False, "tactic_opinion": "", "critic_opinion": ""}
                if isinstance(ai_response, list) and len(ai_response) > 0:
                    first_item = next(iter(ai_response), None)
                    if isinstance(first_item, dict):
                        ai_analysis = first_item
                elif isinstance(ai_response, dict):
                    ai_analysis = ai_response

                if ai_analysis.get("contract_retry_attempted"):
                    _trade_flow_inc("llm_contract_retry")
                if ai_analysis.get("contract_retry_recovered"):
                    _trade_flow_inc("llm_contract_retry_recovered")
                if ai_analysis.get("contract_retry_failed"):
                    _trade_flow_inc("llm_contract_retry_failed")

                request_error = bool(ai_analysis.get("request_error", False))
                request_error_type = str(ai_analysis.get("request_error_type") or "request_error")
                if request_error:
                    _trade_flow_inc("llm_request_error")
                    if "timeout" in request_error_type:
                        _trade_flow_inc("llm_timeout")

                # LLM returns only APPROVE/HOLD. Trade direction comes exclusively
                # from the deterministic local candidate and cannot be changed by the model.
                model_decision = str(
                    ai_analysis.get("model_decision", ai_analysis.get("final_decision", "HOLD"))
                ).strip().upper()
                candidate_direction = str(event_payload.get("candidate_direction", "")).strip().upper()
                model_reason_code = str(
                    ai_analysis.get("model_reason_code")
                    or ("NO_LOCAL_CANDIDATE" if candidate_direction not in {"BUY", "SELL"} else "NOT_PROVIDED")
                ).strip().upper()
                if model_decision == "HOLD" and candidate_direction in {"BUY", "SELL"} and not request_error:
                    hold_bucket = classify_hold_reason_bucket(model_reason_code)
                    _trade_flow_inc(f"llm_hold_{candidate_direction.lower()}_{hold_bucket}")
                candidate_decision = candidate_direction if candidate_direction in {"BUY", "SELL"} else "HOLD"
                model_contract_violation = bool(ai_analysis.get("model_contract_violation", False))
                if not request_error:
                    if (
                        model_decision == "APPROVE"
                        and candidate_direction in {"BUY", "SELL"}
                        and not model_contract_violation
                    ):
                        _trade_flow_inc("llm_model_approve")
                    elif model_decision == "HOLD" and not model_contract_violation:
                        _trade_flow_inc("llm_model_hold")
                if (
                    model_decision == "APPROVE"
                    and candidate_direction in {"BUY", "SELL"}
                    and not model_contract_violation
                    and not request_error
                ):
                    decision = candidate_direction
                else:
                    decision = "HOLD"
                    if request_error:
                        logger.error(
                            "🛑 [LLM REQUEST ERROR] %s: запрос к vLLM не завершён; "
                            "type=%s candidate=%s; вход запрещён.",
                            ticker, request_error_type, candidate_direction or "NONE",
                        )
                    elif model_decision != "HOLD" or model_contract_violation:
                        model_contract_violation = True
                        _trade_flow_inc("llm_contract_violation")
                        logger.error(
                            "🛑 [LLM CONTRACT] %s: ответ не прошёл контракт APPROVE/HOLD; "
                            "raw=%s error_type=%s retry_attempted=%s retry_failed=%s candidate=%s; вход запрещён.",
                            ticker, ai_analysis.get("model_raw_decision", model_decision),
                            ai_analysis.get("contract_error_type", "unknown"),
                            ai_analysis.get("contract_retry_attempted", False),
                            ai_analysis.get("contract_retry_failed", False),
                            candidate_direction or "NONE",
                        )
                ai_analysis["model_decision"] = model_decision
                ai_analysis["final_decision"] = decision
                confidence_valid = bool(ai_analysis.get("confidence_valid", "confidence_score" in ai_analysis))
                try:
                    confidence = float(ai_analysis.get("confidence_score", 0.0)) * 100.0
                    if not math.isfinite(confidence) or not 0.0 <= confidence <= 100.0:
                        confidence_valid = False
                        confidence = 0.0
                except (TypeError, ValueError):
                    confidence_valid = False
                    confidence = 0.0
                latency_val = float(ai_analysis.get("latency", 0.0) or 0.0)
                if decision not in {"BUY", "SELL", "HOLD"}:
                    decision = "HOLD"
                    confidence = 0.0
                candidate_confidence = confidence if confidence_valid else None
                if decision == "BUY":
                    _trade_flow_inc("llm_approve_buy")
                elif decision == "SELL":
                    _trade_flow_inc("llm_approve_sell")
                else:
                    if not request_error:
                        _trade_flow_inc("llm_hold")
                        if candidate_direction == "BUY":
                            _trade_flow_inc("llm_hold_buy")
                        elif candidate_direction == "SELL":
                            _trade_flow_inc("llm_hold_sell")
                # Rebuild executable BBO and trade-flow evidence after LLM latency.
                # Pre-LLM metrics remain available separately for attribution, but
                # stale quote sides must never authorize a new entry.
                evidence = metrics if isinstance(metrics, dict) else {}
                post_llm_refresh: dict[str, Any] = {}
                if candidate_direction in {"BUY", "SELL"}:
                    evidence, post_llm_refresh = refresh_post_llm_evidence(
                        screener, ticker, evidence
                    )
                    evidence["direction_evidence_state"] = classify_direction_evidence(
                        candidate_direction, evidence
                    )
                    if (
                        post_llm_refresh.get("trade_refresh_status") == "PASS"
                        and post_llm_refresh.get("price_refreshed")
                    ):
                        refreshed_price = _parse_finite_float(evidence.get("current_price"))
                        if refreshed_price is not None and refreshed_price > 0.0:
                            price = refreshed_price
                    if isinstance(metrics, dict):
                        metrics.update(evidence)

                valid_evidence = True
                block_reason = ""
                if decision in {"BUY", "SELL"} and candidate_direction in {"BUY", "SELL"} and decision != candidate_direction:
                    valid_evidence = False
                    block_reason = (
                        f"Противоположное направление запрещено: локально подтверждён {candidate_direction}, "
                        f"модель предложила {decision}."
                    )
                elif decision in {"BUY", "SELL"}:
                    queue_flow_reason = _paper_queue_flow_conflict_reason(decision, evidence)
                    if queue_flow_reason:
                        valid_evidence, block_reason = False, queue_flow_reason
                    else:
                        valid_evidence, block_reason = validate_entry_evidence(
                            decision, evidence,
                            paper_mode=bool(getattr(execution_engine, "paper_mode", False)),
                        )
                post_llm_features = build_llm_feature_snapshot(evidence)
                post_llm_evidence_validation = (
                    "NOT_APPLICABLE"
                    if decision not in {"BUY", "SELL"}
                    else "PASS" if valid_evidence else "BLOCKED"
                )
                post_llm_evidence_reason = (
                    block_reason if block_reason
                    else "PASS" if decision in {"BUY", "SELL"}
                    else "NOT_APPLICABLE"
                )
                signal_reason = (
                    f"Ошибка запроса к vLLM ({request_error_type}); вход запрещён."
                    if request_error else
                    "LLM нарушила контракт APPROVE/HOLD; вход запрещён."
                    if model_contract_violation else ""
                )
                if decision in {"BUY", "SELL"} and not valid_evidence:
                    requested_decision = decision
                    signal_reason = block_reason or "Измеряемые условия входа не пройдены."
                    _trade_flow_inc("post_llm_evidence_blocked")
                    _trade_flow_guard_block(signal_reason)
                    logger.warning(
                        "🛑 [SIGNAL GUARD] Заблокирован кандидат %s по %s: %s",
                        requested_decision, ticker, signal_reason,
                    )
                    decision = "HOLD"
                    confidence = 0.0
                    ai_analysis["final_decision"] = decision
                    ai_analysis["confidence_score"] = 0.0

                if decision in {"BUY", "SELL"} and (not confidence_valid or confidence < 75.0):
                    _trade_flow_inc("confidence_blocked")
                    _trade_flow_inc("post_llm_confidence_blocked")
                    requested_decision = decision
                    if not confidence_valid:
                        reason = "Модель не вернула корректную числовую confidence_score; вход заблокирован."
                    else:
                        reason = f"Оценка модели {confidence:.0f}% ниже порога входа 75%; новая позиция не открыта."
                    signal_reason = reason
                    decision = "HOLD"
                    # Keep the model's original candidate score visible; the critic
                    # text explains that the candidate fell below the entry threshold.
                    ai_analysis["final_decision"] = decision
                    ai_analysis["confidence_score"] = confidence / 100.0

                execution_status = "NOT_REQUESTED"
                execution_message = "Новая позиция не запрашивалась."
                requested_lots = 0
                requested_notional = 0.0
                lot_multiplier = max(int(FINAM_TRADE_LOT_SIZES.get(ticker, 0) or 0), 0)
                verdict_id = uuid4().hex

                if decision in ("BUY", "SELL") and confidence >= 75.0:
                    logger.warning(f"🎯 СИГНАЛ УТВЕРЖДЁН ИИ: {ticker} → {decision} ({confidence:.1f}%)")
                    atr_value = finite_float(metrics.get("bar_atr")) or 0.0
                    atr_periods = nonnegative_int(metrics.get("bar_atr_periods")) or 0
                    entry_bbo_available = (finite_float(metrics.get("spread_available")) or 0.0) >= 0.5
                    entry_bid = (finite_float(metrics.get("bid")) or 0.0) if entry_bbo_available else 0.0
                    entry_ask = (finite_float(metrics.get("ask")) or 0.0) if entry_bbo_available else 0.0
                    entry_spread_pct = (finite_float(metrics.get("spread_pct")) or 0.0) if entry_bbo_available else 0.0
                    entry_bbo_age_sec = (finite_float(metrics.get("bbo_age_sec")) or 0.0) if entry_bbo_available else 0.0
                    stop_loss, take_profit = execution_engine.calculate_atr_stop(
                        ticker,
                        price,
                        decision,
                        atr_value=atr_value,
                        atr_periods=atr_periods,
                        bid=entry_bid,
                        ask=entry_ask,
                    )
                    if stop_loss <= 0.0 or take_profit <= 0.0:
                        try:
                            min_atr_periods = min(14, max(2, int(os.getenv("PILUITUS_ATR_MIN_PERIODS", "5"))))
                        except (TypeError, ValueError):
                            min_atr_periods = 5
                        execution_status = "BLOCKED"
                        execution_message = (
                            f"Недостаточно закрытых M1-диапазонов для ATR: {atr_periods}/{min_atr_periods}; "
                            "новая позиция не открыта."
                        )
                        _trade_flow_inc("atr_history_blocked")
                        ai_analysis["atr_entry_ready"] = False
                        logger.warning("Сигнал %s по %s заблокирован после LLM: %s", decision, ticker, execution_message)
                    else:
                        ai_analysis["atr_entry_ready"] = True
                        ai_analysis["stop_loss"] = stop_loss
                        ai_analysis["take_profit"] = take_profit
                        ai_analysis["atr_at_entry"] = atr_value
                        ai_analysis["atr_periods_at_entry"] = atr_periods
                        ai_analysis["atr_age_sec_at_entry"] = finite_float(metrics.get("bar_atr_age_sec")) if isinstance(metrics, dict) else None
                        ai_analysis["atr_multiplier"] = finite_float(os.getenv("ATR_MULTIPLIER", "1.5")) or 1.5
                        ai_analysis["entry_bid"] = entry_bid
                        ai_analysis["entry_ask"] = entry_ask
                        ai_analysis["entry_spread_pct"] = entry_spread_pct
                        ai_analysis["entry_bbo_age_sec"] = entry_bbo_age_sec
                        requested_lots = risk_manager.calculate_lot_size(ticker, price, stop_loss)
                        if requested_lots <= 0:
                            _trade_flow_inc("zero_lot")
                            execution_status = "BLOCKED"
                            if lot_multiplier <= 0:
                                _trade_flow_inc("finam_lot_unavailable")
                                execution_message = (
                                    f"{'биржевой lot_size' if Settings.PAPER_TRADING else 'Finam account trade_lot_size'} для {ticker} не подтверждён; "
                                    "новая позиция не открыта."
                                )
                                logger.warning(
                                    "Сигнал %s по %s заблокирован: lot_size не подтверждён.",
                                    decision, ticker,
                                )
                            else:
                                execution_message = "Риск на минимальный лот превышает установленный лимит."
                                logger.warning(
                                    "Сигнал %s по %s пропущен: риск на минимальный лот превышает лимит.",
                                    decision, ticker,
                                )
                        else:
                            requested_notional = requested_lots * lot_multiplier * price
                            logger.info(
                                "🧮 [POSITION SIZE] %s %s: lots=%d shares=%d price=%.4f notional=%.2f ₽ planned_risk=%.2f ₽ ATR=%.6f (%d M1 ranges) stop=%.4f%%",
                                decision, ticker, requested_lots, requested_lots * lot_multiplier, price, requested_notional,
                                abs(price - stop_loss) * requested_lots * lot_multiplier,
                                atr_value, atr_periods, abs(price - stop_loss) / price * 100.0,
                            )
                            execution_status = "PENDING"
                            execution_message = "Заявка передана в контур исполнения; ожидается результат."

                # Do not expose free-form LLM reasoning as evidence. User-facing text
                # is generated only from locally measured values and guard outcomes.
                if decision == "HOLD" and not signal_reason:
                    if not confidence_valid:
                        signal_reason = "Ответ модели не содержит корректной числовой оценки; вход запрещён."
                    elif model_decision == "HOLD" and candidate_decision in {"BUY", "SELL"}:
                        signal_reason = "Модель отклонила локальный кандидат: вердикт HOLD."
                    elif candidate_decision in {"BUY", "SELL"}:
                        signal_reason = "Кандидат ИИ не прошёл локальные проверки входа."
                    else:
                        signal_reason = "Модель не предложила направленный вход."
                tactic_text, critic_text = build_verdict_explanation(
                    decision, candidate_decision, evidence, signal_reason
                )
                if block_reason:
                    local_evidence_diagnostic = block_reason
                elif candidate_direction not in {"BUY", "SELL"}:
                    local_evidence_diagnostic = "NO_DIRECTIONAL_CANDIDATE"
                elif request_error:
                    local_evidence_diagnostic = f"LLM_REQUEST_ERROR:{request_error_type}"
                elif model_contract_violation:
                    local_evidence_diagnostic = "LLM_CONTRACT_VIOLATION"
                elif model_decision == "HOLD":
                    local_evidence_diagnostic = f"MODEL_HOLD:{model_reason_code}"
                elif not confidence_valid:
                    local_evidence_diagnostic = "CONFIDENCE_INVALID"
                elif decision == "HOLD" and confidence < 75.0:
                    local_evidence_diagnostic = f"CONFIDENCE_BELOW_THRESHOLD:{confidence:.1f}"
                elif decision == "HOLD":
                    local_evidence_diagnostic = "POST_LLM_GUARD_BLOCK"
                else:
                    local_evidence_diagnostic = "PRE_LLM_VALIDATED"
                logger.info(
                    "[LLM DECISION] %s candidate=%s candidate_id=%s verdict_id=%s model=%s final=%s "
                    "model_confidence=%s final_confidence=%s confidence_valid=%s "
                    "contract_violation=%s model_reason_code=%s model_reason_raw=%s "
                    "reason_validation=%s m1=%s direction_gate=%s prompt_chars=%s prompt_fingerprint=%s "
                    "reason=%s local_evidence=%s pre_llm_features=%s post_llm_features=%s "
                    "post_llm_evidence_validation=%s post_llm_evidence_reason=%s post_llm_refresh=%s",
                    ticker,
                    candidate_direction or "NONE",
                    str(event_payload.get("shadow_candidate_id") or "NONE"),
                    verdict_id,
                    model_decision,
                    decision,
                    f"{candidate_confidence:.1f}%" if candidate_confidence is not None else "invalid",
                    f"{confidence:.1f}%" if confidence_valid else "invalid",
                    confidence_valid,
                    model_contract_violation,
                    model_reason_code,
                    ai_analysis.get("model_reason_code_raw", "NOT_PROVIDED"),
                    ai_analysis.get("model_reason_validation", "UNKNOWN"),
                    ai_analysis.get("m1_evidence_state", "UNAVAILABLE"),
                    metrics.get("direction_evidence_state", "UNAVAILABLE"),
                    ai_analysis.get("prompt_chars", "N/A"),
                    ai_analysis.get("prompt_fingerprint", "N/A"),
                    signal_reason or ("APPROVED" if decision in {"BUY", "SELL"} else "MODEL_HOLD"),
                    local_evidence_diagnostic,
                    json.dumps(llm_input_features, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                    json.dumps(post_llm_features, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                    post_llm_evidence_validation,
                    post_llm_evidence_reason,
                    json.dumps(post_llm_refresh, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                )

                signal_status = (
                    "APPROVED" if decision in {"BUY", "SELL"}
                    else "BLOCKED" if candidate_decision in {"BUY", "SELL"}
                    else "NO_ENTRY"
                )
                now_local = datetime.now().astimezone()
                verdict_data = {
                    "verdict_id": verdict_id,
                    "shadow_candidate_id": event_payload.get("shadow_candidate_id"),
                    "timestamp": now_local.strftime("%H:%M:%S"),
                    "timestamp_iso": now_local.isoformat(timespec="milliseconds"),
                    "ticker": ticker,
                    "price": price,
                    "decision": decision,
                    "candidate_decision": candidate_decision,
                    "model_decision": model_decision,
                    # Keep model confidence separate from the post-LLM final gate.
                    "confidence": round(candidate_confidence, 1) if candidate_confidence is not None else None,
                    "model_confidence": round(candidate_confidence, 1) if candidate_confidence is not None else None,
                    "final_confidence": round(confidence, 1) if confidence_valid else None,
                    "signal_status": signal_status,
                    "signal_reason": signal_reason,
                    "execution_status": execution_status,
                    "execution_message": execution_message,
                    "requested_lots": requested_lots or None,
                    "requested_notional_rub": round(requested_notional, 2) if requested_notional else None,
                    "executed_lots": None,
                    "executed_shares": None,
                    "actual_notional_rub": None,
                    "order_id": None,
                    "tactic_opinion": tactic_text,
                    "critic_opinion": critic_text,
                    "local_evidence_reason": local_evidence_diagnostic,
                    "llm_input_features": llm_input_features,
                    "pre_llm_features": llm_input_features,
                    "post_llm_features": post_llm_features,
                    "post_llm_evidence_validation": post_llm_evidence_validation,
                    "post_llm_evidence_reason": post_llm_evidence_reason,
                    "post_llm_refresh": post_llm_refresh,
                    "llm_candidate_direction": candidate_decision,
                    "llm_model_decision": model_decision,
                    "llm_reason_code": model_reason_code,
                    "llm_reason_code_raw": ai_analysis.get("model_reason_code_raw", "NOT_PROVIDED"),
                    "llm_reason_validation": ai_analysis.get("model_reason_validation", "UNKNOWN"),
                    "llm_m1_evidence_state": ai_analysis.get("m1_evidence_state", "UNAVAILABLE"),
                    "llm_prompt_chars": ai_analysis.get("prompt_chars"),
                    "llm_prompt_fingerprint": ai_analysis.get("prompt_fingerprint"),
                }
                shadow_candidate_id = str(event_payload.get("shadow_candidate_id") or "")
                if shadow_candidate_id:
                    linked = scalping_shadow_tracker.attach_llm_verdict(
                        shadow_candidate_id,
                        verdict_id=verdict_id,
                        model_decision=model_decision,
                        final_decision=decision,
                        signal_status=signal_status,
                        confidence=candidate_confidence,
                        reason_code=model_reason_code,
                        local_guard=local_evidence_diagnostic,
                        execution_status=execution_status,
                    )
                    verdict_data["shadow_linked"] = bool(linked)
                    if not linked:
                        logger.warning(
                            "[STRATEGY SHADOW LINK MISS] candidate_id=%s verdict_id=%s ticker=%s",
                            shadow_candidate_id, verdict_id, ticker,
                        )
                else:
                    verdict_data["shadow_linked"] = False

                ai_analysis["final_decision"] = decision
                ai_analysis["confidence_score"] = (confidence / 100.0) if confidence_valid else 0.0
                ai_analysis.update({
                    "shadow_candidate_id": shadow_candidate_id or None,
                    "llm_verdict_id": verdict_id,
                    "llm_model_decision": model_decision,
                    "llm_final_decision": decision,
                    "llm_confidence_pct": candidate_confidence,
                    "llm_final_confidence_pct": round(confidence, 1) if confidence_valid else None,
                    "llm_signal_status": signal_status,
                    "llm_reason_code": model_reason_code,
                    "llm_local_guard": local_evidence_diagnostic,
                    "candidate_direction": candidate_decision,
                    "llm_shadow_linked": bool(verdict_data.get("shadow_linked", False)),
                    "pre_llm_features": llm_input_features,
                    "post_llm_features": post_llm_features,
                    "post_llm_evidence_validation": post_llm_evidence_validation,
                    "post_llm_evidence_reason": post_llm_evidence_reason,
                    "post_llm_refresh": post_llm_refresh,
                    # Persist point-in-time regime labels with the trade. Missing
                    # source fields remain UNKNOWN rather than being inferred later.
                    "market_regime": metrics.get("market_regime", metrics.get("regime", "UNKNOWN")),
                    "volatility_regime": metrics.get("volatility_regime", "UNKNOWN"),
                    "liquidity_regime": metrics.get("liquidity_regime", "UNKNOWN"),
                    "directional_regime": metrics.get("directional_regime", "UNKNOWN"),
                })
                ai_analysis["tactic_opinion"] = tactic_text
                ai_analysis["critic_opinion"] = critic_text

                _background_operations.spawn(
                    log_trading_event_to_file(
                        ticker=ticker,
                        price=price,
                        decision=f"ВЕРДИКТ ИИ: {decision} (кандидат={candidate_decision}, исполнение={execution_status})",
                        confidence=round(candidate_confidence, 1) if candidate_confidence is not None else 0.0,
                        tactic=tactic_text,
                        critic=critic_text,
                        candidate_id=shadow_candidate_id or None,
                        verdict_id=verdict_id,
                        model_confidence=candidate_confidence,
                        final_confidence=round(confidence, 1) if confidence_valid else None,
                    ),
                    name=f"trading-log-verdict:{ticker}:{verdict_id}",
                )

                async with _paper_session_lock:
                    if (
                        getattr(execution_engine, "_maintenance_active", False)
                        or event_payload.get("paper_session_generation") != _paper_session_generation
                    ):
                        continue
                    last_ai_verdicts_cache.append(verdict_data)
                    if len(last_ai_verdicts_cache) > MAX_CACHE_SIZE:
                        last_ai_verdicts_cache.pop(0)

                    redis_inst = getattr(self.db, "redis", None) or getattr(self.db, "_redis", None)
                    if redis_inst is None:
                        logger.error(
                            "[LLM VERDICT PERSIST ERROR] verdict_id=%s candidate_id=%s ticker=%s: "
                            "Redis/Memurai client unavailable; verdict remains only in memory.",
                            verdict_id,
                            str(event_payload.get("shadow_candidate_id") or "NONE"),
                            ticker,
                        )
                    else:
                        try:
                            await redis_inst.lpush(
                                "piluitus:verdicts:history",
                                json.dumps(verdict_data, ensure_ascii=False),
                            )
                            await redis_inst.ltrim(
                                "piluitus:verdicts:history", 0, MAX_CACHE_SIZE - 1
                            )
                        except Exception as redis_err:
                            logger.error(
                                "[LLM VERDICT PERSIST ERROR] verdict_id=%s candidate_id=%s ticker=%s error=%s",
                                verdict_id,
                                str(event_payload.get("shadow_candidate_id") or "NONE"),
                                ticker,
                                redis_err,
                                exc_info=True,
                            )

                    current_balance = dict(_dashboard_balance_snapshot)
                    payload_for_ws = {
                        "type": "MARKET_UPDATE",
                        **verdict_data,
                        "balance": current_balance,
                    }

                    async def _safe_broadcast(payload: dict):
                        try:
                            json_str = json.dumps(payload, ensure_ascii=False)
                            await manager.broadcast(json_str)
                        except Exception as broadcast_err:
                            logger.error(f"Ошибка WebSocket бродкаста: {broadcast_err}")

                    await _safe_broadcast(payload_for_ws)

                    if execution_status == "PENDING":
                        _background_operations.spawn(
                            self._execute_and_publish(
                                verdict_id=verdict_id,
                                ticker=ticker,
                                lots=requested_lots,
                                side=decision,
                                price=price,
                                ai_analysis=dict(ai_analysis),
                                session_generation=int(event_payload.get("paper_session_generation", -1)),
                                candidate_created_monotonic=event_payload.get("candidate_created_monotonic"),
                                shadow_candidate_id=shadow_candidate_id or None,
                            ),
                            name=f"paper-execution:{ticker}:{verdict_id}",
                        )

            except Exception as outer_exc:
                logger.error("[MarketWorker] КРИТИЧЕСКИЙ СБОЙ КОНВЕЙЕРА: %r", outer_exc, exc_info=True)

            finally:
                # Подтверждаем только элемент, который worker действительно извлёк.
                # ACK выполняется ровно один раз на всех путях (включая continue).
                if queue_item_acquired:
                    anomaly_queue.task_done()
                if pending_ticker:
                    _release_ai_ticker(pending_ticker)
                await asyncio.sleep(0.001)
# --------------------------------------------------------------
#  6️⃣  ФОНОВЫЕ ЦИКЛЫ ОБНОВЛЕНИЯ И СИНХРОНИЗАЦИИ ТАБЛИЦ
# --------------------------------------------------------------
async def background_broker_init():
    """
    Поддерживает Finam market-data stream независимо от дня недели.
    Торговые интервалы каждого инструмента проверяются отдельно через Finam schedule API;
    это также позволяет получать данные в предусмотренные MOEX сессии выходного дня.
    """
    logger.info("🔌 Проверка брокерского соединения Finam; календарь сессий будет проверяться по каждому инструменту...")
    try:
        await broker.connect()
        if bool(getattr(execution_engine, "paper_mode", True)):
            logger.info(
                "🧪 [FINAM ACCOUNT] PAPER работает на внутреннем псевдосчёте; "
                "Finam используется только для авторизации и рыночных данных. Account discovery не требуется."
            )
        else:
            selected_account_id = await broker.discover_accounts()
            if selected_account_id:
                logger.info(
                    "🏦 [FINAM ACCOUNT] account_id успешно определён из текущей сессии; "
                    "source=%s; account_id=%s.",
                    getattr(broker, "account_id_source", "session"),
                    broker._mask_account_id(selected_account_id),
                )
            else:
                logger.error(
                    "⛔ [FINAM ACCOUNT] LIVE-счёт не определён; LIVE-входы заблокированы."
                )

        # Verify both the authenticated endpoint and the schedule parser before
        # the first candidate reaches the executor. SBER is used only as a probe;
        # every actual order is checked against its own instrument schedule.
        try:
            schedule_probe = await execution_engine._market_session_status("SBER")
            logger.info(
                "📅 [SESSION CALENDAR PROBE] SBER@MISX: known=%s, open=%s, entry_allowed=%s, "
                "reason=%s, next_session=%s; actual entries are checked per instrument.",
                schedule_probe.get("known"),
                schedule_probe.get("is_open"),
                schedule_probe.get("entry_allowed"),
                schedule_probe.get("reason"),
                schedule_probe.get("next_session_start", "-"),
            )
            if not schedule_probe.get("known"):
                logger.warning(
                    "⛔ [SESSION CALENDAR] расписание Finam не подтверждено; "
                    "новые входы останутся заблокированы до успешной проверки."
                )
        except Exception as schedule_probe_exc:
            logger.error(
                "❌ [SESSION CALENDAR PROBE] ошибка проверки расписания Finam: %s",
                type(schedule_probe_exc).__name__,
                exc_info=True,
            )

        bridge_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=50000)
        async def _run_market_stream():
            await broker.start_market_data_stream(
                tickers=TICKERS_TO_TRADE,
                queue=bridge_queue,
                screener=screener,
            )

        async def _bridge_worker():
            while True:
                try:
                    raw_event = await bridge_queue.get()
                    try:
                        await process_market_event(raw_event)
                    finally:
                        bridge_queue.task_done()
                except Exception as bridge_err:
                    logger.error("❌ Ошибка моста WebSocket-котировок: %s", bridge_err, exc_info=True)
                    await asyncio.sleep(0.1)

        # TaskGroup ties both stream tasks to this parent task.
        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(_run_market_stream(), name="finam-market-stream")
            task_group.create_task(_bridge_worker(), name="finam-bridge-worker")
            logger.info("⏳ Задача Finam WebSocket запущена; ожидаем HANDSHAKE_SUCCESS.")
    except Exception as exc:
        logger.error("❌ Ошибка инициализации API Finam: %s", exc, exc_info=True)

    while True:
        await asyncio.sleep(3600)

# --------------------------------------------------------------
#  6️⃣.5️⃣ ВСПОМОГАТЕЛЬНЫЕ ФОНОВЫЕ ИНИЦИАЛИЗАТОРЫ (Перемещены вверх для линтера)
# --------------------------------------------------------------
async def _start_news_updater() -> None:
    async def _broadcast(message: str) -> None:
        await manager.broadcast(message)
    await MarketNewsParser.update_news_cycle(db_manager, broadcast_callback=_broadcast)

async def _finam_metadata_refresh_loop() -> None:
    """Refresh exchange lots for PAPER or account-specific lots for LIVE."""
    from risk.risk_manager import RiskManager

    try:
        refresh_sec = max(300.0, float(os.getenv("FINAM_METADATA_REFRESH_SEC", "900")))
    except (TypeError, ValueError, OverflowError):
        refresh_sec = 900.0
    try:
        account_retry_sec = min(
            300.0,
            max(15.0, float(os.getenv("FINAM_ACCOUNT_DISCOVERY_RETRY_SEC", "60"))),
        )
    except (TypeError, ValueError, OverflowError):
        account_retry_sec = 60.0
    while True:
        # background_broker_init owns authentication. Do not send REST requests
        # until a JWT has been obtained successfully.
        if not str(getattr(broker, "jwt_token", "") or "").strip():
            await asyncio.sleep(1.0)
            continue
        paper_mode = bool(getattr(execution_engine, "paper_mode", True))
        account_id = str(getattr(broker, "account_id", "") or "").strip()
        if paper_mode:
            # PAPER lot sizing uses the public MOEX ISS TQBR specification.
            # It does not require a Finam account or read-only account context.
            # Keep account discovery exclusive to LIVE brokerage execution.
            if callable(getattr(broker, "get_moex_asset", None)):
                logger.info(
                    "[FINAM METADATA WORKER] PAPER lot refresh uses public MOEX ISS TQBR; "
                    "Finam account context is not required."
                )
            else:
                logger.warning(
                    "[FINAM METADATA WORKER] public MOEX ISS provider unavailable; "
                    "unconfirmed PAPER lots will remain blocked."
                )
        elif not account_id:
            discover = getattr(broker, "discover_accounts", None)
            if callable(discover):
                try:
                    account_id = str(
                        await discover() or getattr(broker, "account_id", "") or ""
                    ).strip()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "[FINAM METADATA WORKER] account discovery failed: %s",
                        type(exc).__name__,
                    )
        if not paper_mode and (not account_id or account_id.upper() == "UNKNOWN_ACCOUNT"):
            logger.warning(
                "⛔ [FINAM METADATA WORKER] LIVE account_id не получен; LIVE-входы остаются заблокированы."
            )
            await asyncio.sleep(account_retry_sec)
            continue
        try:
            await RiskManager.update_finam_lot_sizes(
                finam_broker_client=broker, paper_mode=paper_mode
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error(
                "❌ [FINAM METADATA WORKER] refresh failed: %s",
                type(exc).__name__, exc_info=True,
            )
        confirmed = sum(
            1 for ticker in TICKERS_TO_TRADE
            if int(FINAM_TRADE_LOT_SIZES.get(str(ticker).strip().upper(), 0) or 0) > 0
        )
        # Avoid repeating 88 requests every 180 seconds after persistent provider-side HTTP 400.
        retry_delay = refresh_sec
        if confirmed:
            logger.info(
                "📦 [FINAM METADATA WORKER] confirmed %s lots=%d/%d; next refresh in %.0fs.",
                "PAPER exchange" if paper_mode else "LIVE account",
                confirmed, len(TICKERS_TO_TRADE), refresh_sec,
            )
        await asyncio.sleep(retry_delay)


async def _start_tables_sync_loop():
    """Периодически обновляет позиции и фактическую оценку задействованного капитала."""
    logger.info("📊 Воркер синхронизации таблиц и капитала запущен.")
    while True:
        try:
            await asyncio.sleep(1.0)

            active_positions = await _get_dashboard_open_positions()
            balance = await _refresh_dashboard_balance_snapshot(active_positions)
            raw_closed = getattr(execution_engine, "closed_trades", getattr(execution_engine, "_closed_trades", []))

            payload = {
                "type": "TABLES_UPDATE",
                "active_positions": active_positions,
                "closed_trades": raw_closed[-30:] if isinstance(raw_closed, list) else [],
                "balance": balance,
            }
            await manager.broadcast(json.dumps(payload, ensure_ascii=False))
        except Exception as e:
            logger.error("❌ Ошибка воркера синхронизации таблиц/капитала: %s", e, exc_info=True)
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

# 🔥 HTTP-ЭНДПОИНТ ДЛЯ ПЕРЕХВАТА ИНТЕРАКТИВНЫХ КОМАНД С ВЫСШИМ ПРИОРИТЕТОМ (Priority 0)
@app.post("/api/v1/control/close_all")
async def http_emergency_close_all():
    """Экстренное принудительное закрытие всех открытых позиций фронтендом с обходом HFT-очереди."""
    await anomaly_queue.put(priority_queue_item_factory.create(0, "CMD_CLOSE_ALL", {}))
    return {"status": "queued", "priority": "high_emergency"}

@app.post("/api/v1/control/risk_limit")
async def http_update_risk_limit(payload: dict = Body(...)):
    """Динамическое изменение лимитов просадки робота с высшим приоритетом обработки."""
    try:
        new_limit = validate_daily_loss_limit(payload.get("value", Settings.DAILY_LOSS_LIMIT_PCT))
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    await anomaly_queue.put(priority_queue_item_factory.create(0, "CMD_SET_RISK_LIMIT", {"value": new_limit}))
    return {"status": "queued", "priority": "high_emergency", "new_target_limit": new_limit}

# --------------------------------------------------------------
#  8️⃣  FastAPI ROUTING & WEBSOCKET ENDPOINT
# --------------------------------------------------------------
BASE_DIR = PROJECT_ROOT
TEMPLATE_DIR = BASE_DIR / "interface" / "templates"
templates = Jinja2Templates(directory=str(TEMPLATE_DIR))
PAPER_CLEAR_CONFIRMATION = "ОЧИСТИТЬ PAPER-СЕССИЮ"


def _require_paper_mode() -> None:
    paper_api_helpers.require_paper_mode()


def _paper_trade_rows(snapshot: dict) -> list[dict]:
    return paper_api_helpers.paper_trade_rows(snapshot)


async def _broadcast_paper_tables() -> None:
    await paper_api_helpers.broadcast_paper_tables()


@app.get("/maintenance", response_class=HTMLResponse)
async def read_maintenance_page():
    page_path = TEMPLATE_DIR / "maintenance.html"
    if not page_path.is_file():
        return HTMLResponse("<h1>maintenance.html не найден</h1>", status_code=404)
    return FileResponse(page_path)


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


@app.get("/api/v1/paper/performance")
async def get_paper_performance():
    """Return realized PAPER trade statistics and exact-ID Shadow comparisons."""
    return paper_research_runtime.get_performance()


@app.get("/api/v1/paper/replay/batch")
async def replay_paper_exit_batch(
    limit: int = 30,
    ticker: str | None = None,
    session_id: str | None = None,
    portfolio_capital_rub: float | None = None,
    max_allocation_pct: float = 15.0,
):
    return await paper_research_runtime.replay_batch(
        limit=limit,
        ticker=ticker,
        session_id=session_id,
        portfolio_capital_rub=portfolio_capital_rub,
        max_allocation_pct=max_allocation_pct,
    )


@app.get("/api/v1/paper/reconciliation/shadow")
async def get_paper_shadow_reconciliation(limit: int = 100):
    return paper_research_runtime.get_shadow_reconciliation(limit=limit)


@app.get("/api/v1/paper/shadow/walk-forward")
async def get_paper_shadow_walk_forward(
    folds: int = 5,
    embargo_seconds: float = 30.0,
    limit: int = 20000,
):
    return paper_research_runtime.get_shadow_walk_forward(
        folds=folds,
        embargo_seconds=embargo_seconds,
        limit=limit,
    )


@app.get("/api/v1/paper/shadow/cost-sensitivity")
async def get_paper_shadow_cost_sensitivity(limit: int = 20000):
    return paper_research_runtime.get_shadow_cost_sensitivity(limit=limit)


@app.get("/api/v1/paper/research/report")
async def get_paper_research_report(limit: int = 30):
    return await paper_research_runtime.get_research_report(limit=limit)


@app.get("/api/v1/paper/replay/{order_id}")
async def replay_paper_exit_scenarios(order_id: str):
    return await paper_research_runtime.replay_trade(order_id)


@app.get("/api/v1/paper/reconciliation")
async def get_paper_reconciliation():
    return await paper_research_runtime.get_reconciliation()


@app.post("/api/v1/paper/reconciliation/{order_id}")
async def reconcile_paper_position(
    order_id: str,
    payload: dict[str, object] = Body(...),
):
    return await paper_research_runtime.reconcile_position(order_id, payload)


@app.post("/api/v1/paper/session/clear")
async def clear_paper_session(payload: dict[str, object] = Body(...)):
    global _paper_session_generation, last_ai_verdicts_cache, _balance_guard_last_signature
    _require_paper_mode()
    if str(payload.get("confirmation", "")).strip() != PAPER_CLEAR_CONFIRMATION:
        raise HTTPException(status_code=400, detail=f"Для подтверждения введите точную фразу: {PAPER_CLEAR_CONFIRMATION}")

    async with _paper_session_lock:
        execution_engine._maintenance_active = True
        execution_engine._maintenance_reason = "PAPER-сессия приостановлена для полной очистки. Проверьте результат и вручную возобновите PAPER."
        _paper_session_generation += 1
        # Persist the pause first. Any later failure must leave the bot paused
        # across process restarts rather than silently resuming a partial reset.
        await db_manager.set_json("piluitus:paper:maintenance", {
            "active": True,
            "reason": execution_engine._maintenance_reason,
            "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        })

        # Purge queued market events but retain emergency priority-zero commands.
        emergency_items = []
        while True:
            try:
                queued_item = anomaly_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                if isinstance(queued_item, tuple) and queued_item and queued_item[0] == 0:
                    emergency_items.append(queued_item)
            finally:
                anomaly_queue.task_done()
        for item in emergency_items:
            anomaly_queue.put_nowait(item)

        _ai_candidate_deduplicator.clear()

        if execution_engine._opening_tickers or execution_engine._lock_closing:
            raise HTTPException(
                status_code=409,
                detail="PAPER-операция ещё выполняется. Торговля оставлена на паузе; обновите страницу и повторите очистку.",
            )

        monitor_task = execution_engine._monitor_worker_task
        if monitor_task is not None and not monitor_task.done():
            monitor_task.cancel()
            await asyncio.gather(monitor_task, return_exceptions=True)
        execution_engine._monitor_worker_task = None
        execution_engine._pending_monitor_prices.clear()

        try:
            storage_result = await db_manager.clear_paper_session()
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        removed_memory_positions = len(execution_engine.paper_positions)
        removed_memory_closed = len(execution_engine.closed_trades)
        removed_memory_performance = paper_performance_tracker.clear()
        removed_memory_shadow_pending = scalping_shadow_tracker.clear()
        removed_memory_shadow_labels = len(paper_shadow_labels)
        paper_shadow_labels.clear()
        removed_memory_stale = len(execution_engine._unreconciled_paper_positions)
        execution_engine.paper_positions.clear()
        execution_engine.closed_trades.clear()
        execution_engine._unreconciled_paper_positions.clear()
        execution_engine._ticker_cooldowns.clear()
        execution_engine._recent_stop_losses.clear()
        execution_engine._hydrated_stop_loss_keys.clear()
        execution_engine._reserved_notional.clear()
        execution_engine._reserved_stop_risk.clear()

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

        last_ai_verdicts_cache.clear()
        _trade_flow_counters.reset(time_module.monotonic())
        with _pending_bbo_lock:
            _pending_bbo_candidates.clear()
        position_order_book_diagnostics.clear()
        try:
            await _rotate_paper_bbo_replay_session()
        except Exception as replay_exc:
            execution_engine._maintenance_active = True
            execution_engine._maintenance_reason = (
                "PAPER-сессия очищена, но новый BBO replay-сеанс не запустился; торговля оставлена на паузе."
            )
            logger.critical(
                "[PAPER BBO REPLAY SESSION RESET ERROR] trading remains paused: %s",
                replay_exc, exc_info=True,
            )
            raise HTTPException(status_code=500, detail=execution_engine._maintenance_reason) from replay_exc
        _balance_guard_last_signature = None
        balance = await _refresh_dashboard_balance_snapshot([])

        await manager.broadcast(json.dumps({
            "type": "PAPER_SESSION_RESET",
            "balance": balance,
            "active_positions": [],
            "closed_trades": [],
        }, ensure_ascii=False))

        result = {
            "status": "ok",
            "maintenance_active": True,
            "message": "PAPER-сессия очищена. Для защиты от старых сигналов торговый конвейер оставлен на паузе.",
            "shadow_pending_discarded": removed_memory_shadow_pending,
            "shadow_labels_discarded": removed_memory_shadow_labels,
            "bbo_replay_session_id": paper_bbo_replay_recorder.session_id,
            "storage": storage_result,
            "memory": {
                "removed_open_positions": removed_memory_positions,
                "removed_closed_trades": removed_memory_closed,
                "removed_performance_records": removed_memory_performance,
                "removed_stale_positions": removed_memory_stale,
            },
            "balance": balance,
        }
        logger.warning("🧹 Полная очистка PAPER-сессии завершена; торговля приостановлена до ручного возобновления.")
        return result


@app.post("/api/v1/paper/session/cleanup-stale-and-resume")
async def cleanup_stale_and_resume_paper_session():
    """Remove only quarantined STALE_REVIEW PAPER rows, then resume PAPER atomically."""
    global _paper_session_generation, _balance_guard_last_signature
    _require_paper_mode()

    async with _paper_session_lock:
        if execution_engine._opening_tickers or execution_engine._lock_closing:
            raise HTTPException(
                status_code=409,
                detail="Есть незавершённая операция исполнения. Очистка и запуск отменены.",
            )

        snapshot_before = await db_manager.get_paper_session_snapshot()
        if (
            snapshot_before.get("live_trade_count", 0)
            or snapshot_before.get("live_history_count", 0)
            or snapshot_before.get("live_active_order_count", 0)
        ):
            raise HTTPException(
                status_code=409,
                detail="В хранилище обнаружены LIVE-записи. Автоматический запуск PAPER заблокирован; данные не изменены.",
            )

        execution_engine._maintenance_active = True
        execution_engine._maintenance_reason = (
            "PAPER приостановлен на время удаления только STALE_REVIEW-записей."
        )
        _paper_session_generation += 1
        await db_manager.set_json("piluitus:paper:maintenance", {
            "active": True,
            "reason": execution_engine._maintenance_reason,
            "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        })

        # Drop queued market events so old candidates cannot run immediately after resume.
        # Priority-zero emergency commands are retained.
        emergency_items = []
        while True:
            try:
                queued_item = anomaly_queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            try:
                if isinstance(queued_item, tuple) and queued_item and queued_item[0] == 0:
                    emergency_items.append(queued_item)
            finally:
                anomaly_queue.task_done()
        for item in emergency_items:
            anomaly_queue.put_nowait(item)

        _ai_candidate_deduplicator.clear()

        try:
            storage_result = await db_manager.clear_unreconciled_paper_positions()
        except RuntimeError as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

        removed_ids = {
            str(order_id) for order_id in storage_result.get("position_ids", [])
        }
        removed_memory_stale = sum(
            1 for order_id in removed_ids
            if order_id in execution_engine._unreconciled_paper_positions
        )
        for order_id in removed_ids:
            execution_engine._unreconciled_paper_positions.pop(order_id, None)

        snapshot_after = await db_manager.get_paper_session_snapshot()
        if snapshot_after.get("paper_stale_count", 0) > 0:
            raise HTTPException(
                status_code=409,
                detail=(
                    f"Осталось STALE_REVIEW-позиций: {snapshot_after['paper_stale_count']}. "
                    "PAPER оставлен на паузе; проверьте журнал."
                ),
            )
        if (
            snapshot_after.get("live_trade_count", 0)
            or snapshot_after.get("live_history_count", 0)
            or snapshot_after.get("live_active_order_count", 0)
        ):
            raise HTTPException(
                status_code=409,
                detail="После очистки обнаружены LIVE-записи. PAPER оставлен на паузе.",
            )

        # No unresolved PAPER records remain in persistent storage. Clear any
        # matching in-memory quarantine remnants but preserve current open positions,
        # closed history, verdicts and risk state.
        removed_memory_stale += len(execution_engine._unreconciled_paper_positions)
        execution_engine._unreconciled_paper_positions.clear()
        # Persist the resume before lifting the in-memory maintenance lock.
        # If storage fails, this process must remain paused.
        await db_manager.set_json("piluitus:paper:maintenance", {
            "active": False,
            "reason": "",
            "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        })
        execution_engine._maintenance_active = False
        execution_engine._maintenance_reason = ""

        _balance_guard_last_signature = None
        await _broadcast_paper_tables()
        result = {
            "status": "ok",
            "maintenance_active": False,
            "message": (
                "STALE_REVIEW-записи удалены. PAPER-конвейер автоматически возобновлён; "
                "закрытая история, вердикты, риск-состояние и текущие позиции в памяти сохранены."
            ),
            "storage": storage_result,
            "memory": {"removed_stale_positions": removed_memory_stale},
            "preserved": {
                "paper_history_rows": snapshot_after.get("paper_history_count", 0),
                "verdict_history_rows": snapshot_after.get("verdict_history_count", 0),
                "verdict_execution_keys": snapshot_after.get("verdict_execution_count", 0),
                "active_memory_positions": len(execution_engine.paper_positions),
            },
        }
        logger.warning(
            "🧹 [PAPER STALE CLEANUP + RESUME] deleted=%d; memory_stale=%d; PAPER resumed.",
            storage_result.get("deleted_stale_positions", 0), removed_memory_stale,
        )
        return result


@app.post("/api/v1/paper/session/resume")
async def resume_paper_session():
    _require_paper_mode()
    async with _paper_session_lock:
        if execution_engine._opening_tickers or execution_engine._lock_closing:
            raise HTTPException(status_code=409, detail="Выполняется операция исполнения; PAPER пока нельзя возобновить.")
        snapshot = await db_manager.get_paper_session_snapshot()
        if snapshot.get("paper_stale_count", 0) > 0:
            raise HTTPException(status_code=409, detail="Сначала завершите сверку всех STALE_REVIEW-позиций.")
        if snapshot.get("live_trade_count", 0) or snapshot.get("live_history_count", 0) or snapshot.get("live_active_order_count", 0):
            raise HTTPException(status_code=409, detail="В хранилище есть LIVE-записи. Возобновление PAPER заблокировано до проверки хранилища.")
        execution_engine._maintenance_active = False
        execution_engine._maintenance_reason = ""
        await db_manager.set_json("piluitus:paper:maintenance", {
            "active": False,
            "reason": "",
            "updated_at": datetime.now().astimezone().isoformat(timespec="seconds"),
        })
        logger.warning("▶️ PAPER-сессия вручную возобновлена оператором.")
        await _broadcast_paper_tables()
        return {"status": "ok", "maintenance_active": False, "message": "PAPER-торговля возобновлена. Новые сигналы будут приниматься."}


@app.get("/", response_class=HTMLResponse)
async def read_root():
    index_path = TEMPLATE_DIR / "index.html"          
    if not index_path.is_file():
        return HTMLResponse(
            content="<h1>404 – index.html не найден</h1><p>Поместите файл в папку interface/templates.</p>",
            status_code=404,
        )
    return FileResponse(index_path)

STATIC_DIR = BASE_DIR / "static"
if STATIC_DIR.is_dir():
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.websocket("/ws/frontend")
async def websocket_frontend_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        logger.info("🔌 К WS‑шлюзу подключился веб-интерфейс. Отправка стартового пакета...")

        initial_positions = await _get_dashboard_open_positions()
        initial_balance = await _refresh_dashboard_balance_snapshot(initial_positions)

        cached_news = []
        try:
            redis_inst = getattr(db_manager, "redis", getattr(db_manager, "_redis", None))
            if redis_inst is not None:
                news_raw = await redis_inst.get("piluitus:news:latest")
                if news_raw:
                    cached_news = json.loads(news_raw)
        except Exception:
            pass

        raw_closed = getattr(execution_engine, "closed_trades", getattr(execution_engine, "_closed_trades", []))
        verdicts_for_client = [dict(item) for item in last_ai_verdicts_cache if isinstance(item, dict)]
        try:
            redis_inst = getattr(db_manager, "redis", getattr(db_manager, "_redis", None))
            if redis_inst is not None:
                for verdict in verdicts_for_client:
                    verdict_id = verdict.get("verdict_id")
                    if not verdict_id:
                        continue
                    execution_raw = await redis_inst.get(f"piluitus:verdict:execution:{verdict_id}")
                    if execution_raw:
                        execution_update = json.loads(execution_raw)
                        if isinstance(execution_update, dict):
                            verdict.update(execution_update)
        except Exception as history_exc:
            logger.debug("Не удалось обогатить историю статусами исполнения: %s", history_exc)

        await websocket.send_json({
            "type": "INIT_DATA",
            "portfolio": {},
            "balance": initial_balance,
            "news": cached_news,
            "verdicts_history": verdicts_for_client,
            "active_positions": initial_positions,
            "closed_trades": raw_closed[-30:] if isinstance(raw_closed, list) else []
        })

        while True:
            data = await websocket.receive_text()
            message = json.loads(data)

            if isinstance(message, list):
                for single_message in message:
                    if isinstance(single_message, dict):
                        target_ticker = single_message.get("security_code") or single_message.get("ticker")
                        price_val     = single_message.get("price") or single_message.get("last_price")

                        if target_ticker and price_val is not None:
                            mock_event = SimpleNamespace(
                                security_code=str(target_ticker),
                                price=float(price_val),
                                volume=int(single_message.get("volume", 1000)),
                            )
                            asyncio.create_task(process_market_event(mock_event))
                await asyncio.sleep(0.0001)

            elif isinstance(message, dict):
                # ПЕРЕХВАТ ИНТЕРАКТИВНЫХ WS-КОМАНД ИЗ ИНТЕРФЕЙСА (Мгновенный перевод на Priority 0)
                msg_type = message.get("type")
                if msg_type == "USER_COMMAND_CLOSE_ALL":
                    await anomaly_queue.put(priority_queue_item_factory.create(0, "CMD_CLOSE_ALL", {}))
                    continue
                elif msg_type == "USER_COMMAND_SET_RISK":
                    await anomaly_queue.put(priority_queue_item_factory.create(0, "CMD_SET_RISK_LIMIT", {"value": message.get("value", 0.10)}))
                    continue

                target_ticker = message.get("security_code") or message.get("ticker")
                price_val     = message.get("price") or message.get("last_price")

                if target_ticker and price_val is not None:
                    mock_event = SimpleNamespace(
                        security_code=str(target_ticker),
                        price=float(price_val),
                        volume=int(message.get("volume", 1000)),
                    )
                    asyncio.create_task(process_market_event(mock_event))
            else:
                logger.error(f"❌ Получено вебсокет-сообщение неизвестного типа: {type(message)}")

    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as ws_err:
        logger.error(f"⚠️ Ошибка WebSocket: {ws_err}")


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
