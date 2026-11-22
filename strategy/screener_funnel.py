"""Локальная численная воронка для отбора наиболее перспективных тикеров.

Все котировки продолжают поступать по полной вселенной инструментов. Рейтинг
считается локально; LLM получает только аномалии по тикерам из выбранного top-N.
Сохраняем совместимую структуру ``history`` для execution_layer/evolution_agent.
"""
from __future__ import annotations

 
import threading
import time
from typing import Any

from strategy.minute_bar_patterns import MinuteBarPatternEngine
from strategy.screener_funnel_order_book import ScreenerFunnelOrderBookMixin
from strategy.screener_funnel_selection import ScreenerFunnelSelectionMixin
from strategy.screener_funnel_state import ScreenerFunnelStateMixin
from strategy.screener_funnel_tick import ScreenerFunnelTickMixin
from strategy.screener_funnel_trade_flow import ScreenerFunnelTradeFlowMixin
from strategy.screener_funnel_primitives import (
    _best_quote_ofi_delta,
    _env_float,
    _env_int,
    _event_timestamp_to_monotonic,
    _ofi_window,
    _queue_imbalance_window,
    logger,
)



class HFTMemoryScreener(\n    ScreenerFunnelSelectionMixin,\n    ScreenerFunnelOrderBookMixin,\n    ScreenerFunnelTickMixin,\n    ScreenerFunnelTradeFlowMixin,\n    ScreenerFunnelStateMixin,\n):
    """Короткая тик-история + локальный cross-sectional рейтинг top-N."""

    def __init__(
        self,
        window_size: int = 30,
        top_n: int | None = None,
        min_samples: int | None = None,
        selection_interval: float | None = None,
        warmup_timeout: float | None = None,
        minimum_coverage: float | None = None,
        stale_after: float | None = None,
        max_spread_pct: float | None = None,
        bbo_max_age_sec: float | None = None,
    ):
        self.window_size = max(8, int(window_size))
        self.top_n = max(1, int(top_n if top_n is not None else _env_int("PILUITUS_FUNNEL_TOP_N", 15)))
        # This calibration phase deliberately caps score-only churn at one
        # replacement per refresh, even if an older .env still says 3 or more.
        self.max_replacements_per_refresh = max(
            0, min(1, _env_int("PILUITUS_FUNNEL_MAX_REPLACEMENTS_PER_REFRESH", 1))
        )
        self.retention_score_gap = _env_float("PILUITUS_FUNNEL_RETENTION_SCORE_GAP", 5.0)
        self.min_selected_hold_sec = max(
            0.0, _env_float("PILUITUS_FUNNEL_MIN_HOLD_SEC", 60.0)
        )
        self.replacement_confirmations = max(
            1, _env_int("PILUITUS_FUNNEL_REPLACEMENT_CONFIRMATIONS", 2)
        )
        self.min_samples = max(
            3,
            int(min_samples if min_samples is not None else _env_int("PILUITUS_FUNNEL_MIN_SAMPLES", 8)),
        )
        self.selection_interval = max(
            15.0,
            float(selection_interval if selection_interval is not None else _env_float("PILUITUS_FUNNEL_REFRESH_SEC", 15.0)),
        )
        self.warmup_timeout = max(
            5.0,
            float(warmup_timeout if warmup_timeout is not None else _env_float("PILUITUS_FUNNEL_WARMUP_SEC", 90.0)),
        )
        self.minimum_coverage = min(
            1.0,
            max(0.1, float(minimum_coverage if minimum_coverage is not None else _env_float("PILUITUS_FUNNEL_COVERAGE", 0.60))),
        )
        self.stale_after = max(
            5.0,
            float(stale_after if stale_after is not None else _env_float("PILUITUS_FUNNEL_STALE_SEC", 60.0)),
        )
        self.max_spread_pct = max(
            0.01,
            float(max_spread_pct if max_spread_pct is not None else _env_float("PILUITUS_FUNNEL_MAX_SPREAD_PCT", 0.50)),
        )
        # A quote may omit unchanged fields. Retain the last explicitly observed
        # bid/ask only for a short TTL; never treat old prices as current forever.
        self.size_signal_ttl_sec = _env_float("PILUITUS_SIZE_SIGNAL_MAX_AGE_SEC", 2.0, minimum=0.25)
        configured_bbo_age = (
            float(bbo_max_age_sec) if bbo_max_age_sec is not None
            else _env_float("PILUITUS_BBO_MAX_AGE_SEC", 2.0, minimum=0.25)
        )
        self.bbo_max_age_sec = min(5.0, max(0.25, configured_bbo_age))
        # Quote updates can remain fresh while the last traded price is stale.
        self.price_event_max_age_sec = _env_float(
            "PILUITUS_FUNNEL_PRICE_EVENT_MAX_AGE_SEC", 10.0, minimum=1.0
        )

        # Keep the historical array layout compatible with ATR calculations.
        self.history: dict[str, dict[str, Any]] = {}
        self.quotes: dict[str, dict[str, Any]] = {}
        self.base_metrics: dict[str, dict[str, float]] = {}
        self.universe: list[str] = []
        self._universe_set: set[str] = set()
        self.selected_tickers: list[str] = []
        self.selection_ready = False
        self.eligible_count = 0
        self._ranking: dict[str, dict[str, float | int]] = {}
        self._started_at = time.monotonic()
        self._last_selection_update = 0.0
        self._selected_since: dict[str, float] = {}
        self._replacement_streak: dict[str, int] = {}
        self._lock = threading.RLock()
        self._last_selection_log_key: tuple[str, ...] | None = None
        # Closed 1-minute OHLC patterns are measured locally; no LLM is used to detect them.
        self.minute_bar_patterns = MinuteBarPatternEngine()
        self.trade_data_mode = False

                "ready": bool(self.selection_ready),
                "universe_count": len(self.universe) if self.universe else len(self.history),
                "eligible_count": int(self.eligible_count),
                "observed_count": sum(1 for ticker in self.universe if int(self.history.get(ticker, {}).get("count", 0)) > 0),
                "selected_count": len(self.selected_tickers),
                "selected_tickers": list(self.selected_tickers),
                "top_n": self.top_n,
            }

    def get_status_text(self) -> str:
        status = self.get_status()
        if status["ready"] and status["selected_count"]:
            names = "/".join(status["selected_tickers"][:3])
            return f"🎯 Funnel {status['universe_count']}→{status['selected_count']} {names}"
        return (
            f"🎯 Funnel warmup {status['eligible_count']}/{status['universe_count']} "
            f"→{status['top_n']}"
        )
