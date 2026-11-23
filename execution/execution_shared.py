from __future__ import annotations

import logging
import os
import inspect
from collections import deque
from collections.abc import Iterable, Mapping
from typing import Any, Protocol, cast
import math
import time
import numpy as np
from datetime import datetime, timezone, timedelta, time as datetime_time
from storage.db_manager import DBManager
import asyncio

from execution.execution_primitives import (
    BBOExitQuote,
    TradeRecord,
    _as_string_mapping,
    _finite_or_default,
)
from execution.paper_position_exit_runtime import PaperPositionExitRuntimeMixin
from uuid import uuid4

# Единый справочник лотности хранится в config.py.
from config import ASSET_METADATA, FINAM_TRADE_LOT_SIZES, LOT_SIZES, Settings, PAPER_TRADING
from strategy.signal_guard import validate_spread_to_atr
from strategy.market_context import validate_entry_economics
from strategy.paper_performance import (
    PAPER_CLOSED_TRADES_STORAGE_KEY,
    PAPER_CLOSED_TRADES_STORAGE_LIMIT,
    paper_performance_tracker,
)
from strategy.paper_entry_diagnostics import build_entry_execution_diagnostics
from strategy.paper_exit_diagnostics import build_exit_execution_diagnostics

logger = logging.getLogger("Piluitus.ExecutionLayer")


class BrokerClientProtocol(Protocol):
    async def place_order(
        self, ticker: str, lots: int, side: str, price: float, client_id: str
    ) -> object: ...

    async def get_asset_schedule(self, ticker: str) -> dict[str, Any] | None: ...


class MinuteBarPatternProtocol(Protocol):
    def get_atr_snapshot(self, ticker: str, period: int = 14) -> dict[str, float]: ...


class ScreenerProtocol(Protocol):
    history: dict[str, dict[str, Any]]
    quotes: dict[str, dict[str, Any]]

    @property
    def minute_bar_patterns(self) -> MinuteBarPatternProtocol: ...


_MOSCOW_TZ = timezone(timedelta(hours=3), name="MSK")


def _parse_schedule_datetime(value: object, session_date) -> datetime | None:
    """Parse Finam REST Timestamp, ISO datetime or local HH:MM[:SS] safely."""
    parsed: datetime | None = None
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, Mapping):
        seconds_raw = value.get("seconds")
        nanos_raw = value.get("nanos", 0)
        if seconds_raw is not None:
            try:
                seconds = float(seconds_raw) + float(nanos_raw or 0.0) / 1_000_000_000.0
                if math.isfinite(seconds):
                    parsed = datetime.fromtimestamp(seconds, tz=timezone.utc)
            except (TypeError, ValueError, OverflowError, OSError):
                parsed = None
        if parsed is None and value.get("value") is not None:
            return _parse_schedule_datetime(value.get("value"), session_date)
    elif isinstance(value, (int, float)) and not isinstance(value, bool):
        try:
            seconds = float(value)
            if seconds >= 1e18:
                seconds /= 1_000_000_000.0
            elif seconds >= 1e15:
                seconds /= 1_000_000.0
            elif seconds >= 1e12:
                seconds /= 1_000.0
            if math.isfinite(seconds):
                parsed = datetime.fromtimestamp(seconds, tz=timezone.utc)
        except (TypeError, ValueError, OverflowError, OSError):
            parsed = None
    elif isinstance(value, str):
        raw = value.strip()
        if not raw:
            return None
        try:
            numeric = float(raw)
        except ValueError:
            numeric = math.nan
        if math.isfinite(numeric):
            return _parse_schedule_datetime(numeric, session_date)
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            try:
                clock = datetime_time.fromisoformat(raw)
                parsed = datetime.combine(session_date, clock)
            except ValueError:
                return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=_MOSCOW_TZ)
    try:
        return parsed.astimezone(_MOSCOW_TZ)
    except (ValueError, OverflowError):
        return None


def _decision_attribution_fields(ai_analysis: object) -> dict[str, object]:
    """Return bounded, typed decision identifiers to persist with an opened trade."""
    if not isinstance(ai_analysis, Mapping):
        return {
            "shadow_candidate_id": None,
            "llm_verdict_id": None,
            "llm_model_decision": "UNKNOWN",
            "llm_final_decision": "UNKNOWN",
            "llm_confidence_pct": None,
            "llm_final_confidence_pct": None,
            "llm_signal_status": "UNKNOWN",
            "llm_reason_code": "UNKNOWN",
            "llm_local_guard": None,
            "candidate_direction": "UNKNOWN",
            "llm_shadow_linked": False,
            "pre_llm_features": {},
            "post_llm_features": {},
            "post_llm_evidence_validation": "UNKNOWN",
            "post_llm_evidence_reason": None,
            "post_llm_refresh": {},
        }

    def bounded_text(key: str, default: str | None = None, limit: int = 160) -> str | None:
        raw = ai_analysis.get(key)
        if raw is None:
            return default
        value = str(raw).strip()
        return value[:limit] if value else default

    def bounded_snapshot(key: str, max_items: int = 96) -> dict[str, object]:
        raw = ai_analysis.get(key)
        if not isinstance(raw, Mapping):
            return {}
        snapshot: dict[str, object] = {}
        for raw_name, raw_value in list(raw.items())[:max_items]:
            name = str(raw_name).strip()[:48]
            if not name:
                continue
            if isinstance(raw_value, bool):
                snapshot[name] = raw_value
            elif isinstance(raw_value, str):
                snapshot[name] = raw_value[:160]
            elif isinstance(raw_value, (int, float)):
                number = _finite_or_default(raw_value, math.nan)
                if math.isfinite(number):
                    snapshot[name] = round(number, 8)
        return snapshot

    confidence_raw = ai_analysis.get("llm_confidence_pct")
    confidence = _finite_or_default(confidence_raw, math.nan)
    if not math.isfinite(confidence) or not 0.0 <= confidence <= 100.0:
        confidence_value: float | None = None
    else:
        confidence_value = round(confidence, 3)

    final_confidence_raw = ai_analysis.get("llm_final_confidence_pct")
    final_confidence = _finite_or_default(final_confidence_raw, math.nan)
    if not math.isfinite(final_confidence) or not 0.0 <= final_confidence <= 100.0:
        final_confidence_value: float | None = None
    else:
        final_confidence_value = round(final_confidence, 3)

    return {
        "shadow_candidate_id": bounded_text("shadow_candidate_id", limit=64),
        "llm_verdict_id": bounded_text("llm_verdict_id", limit=96),
        "llm_model_decision": bounded_text("llm_model_decision", "UNKNOWN", 32),
        "llm_final_decision": bounded_text("llm_final_decision", "UNKNOWN", 32),
        "llm_confidence_pct": confidence_value,
        "llm_final_confidence_pct": final_confidence_value,
        "llm_signal_status": bounded_text("llm_signal_status", "UNKNOWN", 32),
        "llm_reason_code": bounded_text("llm_reason_code", "UNKNOWN", 96),
        "llm_local_guard": bounded_text("llm_local_guard", limit=240),
        "candidate_direction": bounded_text("candidate_direction", "UNKNOWN", 16),
        "llm_shadow_linked": bool(ai_analysis.get("llm_shadow_linked", False)),
        "pre_llm_features": bounded_snapshot("pre_llm_features"),
        "post_llm_features": bounded_snapshot("post_llm_features"),
        "post_llm_evidence_validation": bounded_text("post_llm_evidence_validation", "UNKNOWN", 32),
        "post_llm_evidence_reason": bounded_text("post_llm_evidence_reason", limit=240),
        "post_llm_refresh": bounded_snapshot("post_llm_refresh", max_items=16),
        "market_regime": bounded_text("market_regime", "UNKNOWN", 32),
        "volatility_regime": bounded_text("volatility_regime", "UNKNOWN", 32),
        "liquidity_regime": bounded_text("liquidity_regime", "UNKNOWN", 32),
        "directional_regime": bounded_text("directional_regime", "UNKNOWN", 32),
    }


def _extract_order_id(response: object) -> str:
    """Safely read an order identifier from an untyped broker response."""
    response_map = _as_string_mapping(response)
    if response_map is None:
        return ""
    raw_order_id = response_map.get("orderId") or response_map.get("order_id") or ""
    return str(raw_order_id).strip()


# Hard safety ceilings: legacy .env values such as 1.0/0.20 are clamped to these.
MAX_ALLOWED_GROSS_EXPOSURE_PCT = 0.15
MAX_ALLOWED_SINGLE_POSITION_PCT = 0.03

__all__ = [
    "logging", "os", "inspect", "deque", "Iterable", "Mapping", "Any", "Protocol",
    "cast", "math", "time", "np", "datetime", "timezone", "timedelta", "datetime_time",
    "DBManager", "asyncio", "BBOExitQuote", "TradeRecord", "_as_string_mapping",
    "_finite_or_default", "uuid4", "ASSET_METADATA", "FINAM_TRADE_LOT_SIZES",
    "LOT_SIZES", "Settings", "PAPER_TRADING", "validate_spread_to_atr",
    "validate_entry_economics", "PAPER_CLOSED_TRADES_STORAGE_KEY",
    "PAPER_CLOSED_TRADES_STORAGE_LIMIT", "paper_performance_tracker",
    "build_entry_execution_diagnostics", "build_exit_execution_diagnostics",
    "logger", "BrokerClientProtocol", "MinuteBarPatternProtocol", "ScreenerProtocol",
    "_MOSCOW_TZ", "_parse_schedule_datetime", "_decision_attribution_fields",
    "_extract_order_id", "MAX_ALLOWED_GROSS_EXPOSURE_PCT",
    "MAX_ALLOWED_SINGLE_POSITION_PCT",
]
