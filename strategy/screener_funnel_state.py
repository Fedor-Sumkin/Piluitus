"""Screener funnel State methods."""
from __future__ import annotations

from collections import deque
import time

import numpy as np

from strategy.screener_funnel_primitives import logger



class ScreenerFunnelStateMixin:
    """Composable behavior over the single HFTMemoryScreener state owner."""

    @staticmethod
    def normalize_ticker(ticker: str) -> str:
        return str(ticker or "").strip().upper().split("@")[0]

    def register_ticker(self, ticker: str) -> None:
        ticker = self.normalize_ticker(ticker)
        if not ticker or ticker in self.history:
            return
        self.history[ticker] = {
            "prices": np.zeros(self.window_size, dtype=np.float64),
            "volumes": np.zeros(self.window_size, dtype=np.float64),
            "count": 0,
            "trade_events": deque(maxlen=5000),
            "trade_seen_ids": deque(maxlen=10000),
            "trade_seen_set": set(),
        }
        self.quotes[ticker] = {
            "bid": 0.0,
            "ask": 0.0,
            "bid_size": 0.0,
            "ask_size": 0.0,
            "bid_levels": [],
            "ask_levels": [],
            "day_volume": 0.0,
            "last_seen": 0.0,
            "last_bid_seen": 0.0,
            "last_ask_seen": 0.0,
            # Confirmation clocks are distinct from level-change clocks. A valid
            # ORDER_BOOK delta confirms the maintained book even when the BBO
            # itself did not change; QUOTES confirms only explicitly present sides.
            "last_bid_confirmed_seen": 0.0,
            "last_ask_confirmed_seen": 0.0,
            "last_order_book_seen": 0.0,
            "last_order_book_event_seen": 0.0,
            # Event-time watermarks remain set after an explicit zero clears a
            # side, so an older packet cannot resurrect a withdrawn quote.
            "last_bid_event_seen": 0.0,
            "last_ask_event_seen": 0.0,
            "last_bid_quotes_event_seen": 0.0,
            "last_ask_quotes_event_seen": 0.0,
            "last_bid_book_event_seen": 0.0,
            "last_ask_book_event_seen": 0.0,
            "last_bid_source_received_seen": 0.0,
            "last_ask_source_received_seen": 0.0,
            "last_bid_size_event_seen": 0.0,
            "last_ask_size_event_seen": 0.0,
            "last_order_book_received": 0.0,
            "order_book_valid": False,
            "order_book_last_reason": "NO_ORDER_BOOK",
            # Confirmation clocks used by entry/exit guards. Event watermarks
            # above remain the last actual side-level update for ordering/audit.
            "last_bbo_seen": 0.0,
            "last_bid_size_seen": 0.0,
            "last_ask_size_seen": 0.0,
            "bid_size_drop_pct": 0.0,
            "ask_size_drop_pct": 0.0,
            "bid_size_baseline": 0.0,
            "ask_size_baseline": 0.0,
            "bid_size_current": 0.0,
            "ask_size_current": 0.0,
            "bid_size_drop_at": 0.0,
            "ask_size_drop_at": 0.0,
            "top_bid_history_samples": 0,
            "top_ask_history_samples": 0,
            "tick_times": deque(maxlen=self.window_size),
            "price_events": deque(maxlen=5000),
            # Наблюдения размера только лучшего уровня котировки (не полный стакан).
            # QUOTES и ORDER_BOOK могут пополнять одну короткую историю; размер
            # учитывается только при фактическом наличии соответствующего поля.
            "bid_size_history": deque(maxlen=self.window_size),
            "ask_size_history": deque(maxlen=self.window_size),
            "queue_imbalance_history": deque(maxlen=1024),
            "ofi_history": deque(maxlen=4096),
        }
        self.base_metrics[ticker] = {
            "volume_spike": 1.0,
            "vwap": 0.0,
            "std_dev": 0.0,
            "mean_price": 0.0,
        }

    def set_universe(self, tickers: list[str]) -> None:
        """Задать полный список котировок, из которого выбирается top-N."""
        normalized = list(dict.fromkeys(self.normalize_ticker(t) for t in tickers if self.normalize_ticker(t)))
        with self._lock:
            self.universe = normalized
            self._universe_set = set(normalized)
            for ticker in normalized:
                self.register_ticker(ticker)
            self.selected_tickers = []
            self.selection_ready = False
            self.eligible_count = 0
            self._ranking.clear()
            self._started_at = time.monotonic()
            self._last_selection_update = 0.0
            self._selected_since.clear()
            self._replacement_streak.clear()
            self._last_selection_log_key = None
        logger.info(
            "🎯 Локальная воронка инициализирована: universe=%d, top_n=%d, min_samples=%d, coverage=%.0f%%",
            len(normalized), min(self.top_n, len(normalized)), self.min_samples, self.minimum_coverage * 100,
        )

    @staticmethod

