"""Screener funnel TradeFlow methods."""
from __future__ import annotations

import math
import time
from collections import deque
from typing import Any

import numpy as np

from strategy.screener_funnel_primitives import (
    _env_int,
    logger,
)



class ScreenerFunnelTradeFlowMixin:
    """Composable behavior over the single HFTMemoryScreener state owner."""

    def enable_trade_data_mode(self) -> None:
        """Require unique INSTRUMENT_TRADES for live volume/VWAP evidence."""
        with self._lock:
            self.trade_data_mode = True
            for state in self.history.values():
                state["volumes"].fill(0.0)
            logger.info("📈 Trade-flow mode enabled: QUOTES.lastSize excluded from volume/VWAP.")

    def reset_trade_history(self, ticker: str) -> None:
        """Invalidate rolling trade-flow evidence without erasing the independent QUOTES M1 history."""
        symbol = self.normalize_ticker(ticker)
        if not symbol:
            return
        with self._lock:
            state = self.history.get(symbol)
            if isinstance(state, dict):
                state.get("trade_events", deque()).clear()
                state.get("trade_seen_ids", deque()).clear()
                state.get("trade_seen_set", set()).clear()
            quote = self.quotes.get(symbol)
            if isinstance(quote, dict):
                quote["trade_metrics"] = {}
                quote["last_trade_event_seen"] = 0.0
                quote.pop("last_trade_price", None)

    def reset_trade_subscription(self, ticker: str) -> None:
        """Reset rolling trade-flow on subscription changes without erasing closed M1 bars."""
        symbol = self.normalize_ticker(ticker)
        if not symbol:
            return
        with self._lock:
            state = self.history.get(symbol)
            if isinstance(state, dict):
                state.get("trade_events", deque()).clear()
                state.get("trade_seen_ids", deque()).clear()
                state.get("trade_seen_set", set()).clear()
            quote = self.quotes.get(symbol)
            if isinstance(quote, dict):
                quote["trade_metrics"] = {}
                quote["last_trade_event_seen"] = 0.0
                quote.pop("last_trade_price", None)

    def push_trade(
        self, ticker: str, price: float, size: float, trade_id: object = None,
        side: object = None, event_timestamp: object = None,
        received_monotonic: float | None = None,
    ) -> dict[str, Any]:
        """Record one unique exchange trade and refresh rolling trade-flow metrics."""
        symbol = self.normalize_ticker(ticker)
        try:
            px, qty = float(price), float(size)
        except (TypeError, ValueError, OverflowError):
            return {"accepted": False, "reason": "invalid_numeric"}
        if not symbol or not math.isfinite(px) or not math.isfinite(qty) or px <= 0.0 or qty <= 0.0:
            return {"accepted": False, "reason": "invalid_trade"}
        now = time.monotonic()
        received = now
        if received_monotonic is not None:
            try:
                candidate = float(received_monotonic)
                if math.isfinite(candidate) and 0.0 < candidate <= now + 0.05:
                    received = min(now, candidate)
            except (TypeError, ValueError, OverflowError):
                pass
        event_seen = self._event_timestamp_to_monotonic(event_timestamp, received)
        if event_seen < now - 120.0 or event_seen > now + 0.05:
            return {"accepted": False, "reason": "stale_or_future_trade"}
        raw_id = str(trade_id or "").strip()
        side_text = str(side or "").strip().upper()
        side_norm = "BUY" if ("BUY" in side_text or side_text == "1") else "SELL" if ("SELL" in side_text or side_text == "2") else "UNKNOWN"
        fingerprint = raw_id or f"{event_seen:.6f}|{px:.8f}|{qty:.8f}|{side_norm}"
        with self._lock:
            self.register_ticker(symbol)
            state = self.history[symbol]
            seen = state["trade_seen_set"]
            if fingerprint in seen:
                return {"accepted": False, "reason": "duplicate_trade"}
            ids = state["trade_seen_ids"]
            if len(ids) == ids.maxlen:
                seen.discard(ids[0])
            ids.append(fingerprint)
            seen.add(fingerprint)
            events = state["trade_events"]
            if events and event_seen < events[-1][0] - 0.001:
                return {"accepted": False, "reason": "out_of_order_trade"}
            events.append((event_seen, px, qty, side_norm))
            # M1 OHLC is built exclusively from the full-universe QUOTES stream.
            # INSTRUMENT_TRADES has a different subscription scope and may omit or
            # use a different timestamp clock; feeding it into the candle builder
            # can reset the M1 clock domain and erase otherwise valid history.
            metrics = self._trade_metrics_locked(state, now)
            self.quotes[symbol]["trade_metrics"] = metrics
            self.quotes[symbol]["last_trade_event_seen"] = event_seen
            self.quotes[symbol]["last_trade_price"] = px
            return {"accepted": True, **metrics}

    def refresh_trade_metrics(self, ticker: str, now: float | None = None) -> dict[str, Any]:
        """Revalidate trade-flow freshness immediately before a post-LLM decision."""
        symbol = self.normalize_ticker(ticker)
        if not symbol:
            return {"trade_data_ready": 0.0, "trade_metrics_age_sec": -1.0}
        current = time.monotonic() if now is None else float(now)
        with self._lock:
            state = self.history.get(symbol)
            quote = self.quotes.get(symbol)
            if not state or not quote:
                return {"trade_data_ready": 0.0, "trade_metrics_age_sec": -1.0}
            metrics = self._trade_metrics_locked(state, current)
            quote["trade_metrics"] = metrics
            latest = state["trade_events"][-1] if state.get("trade_events") else None
            last_price = float(latest[1]) if latest else 0.0
            vwap = float(metrics.get("trade_vwap_60s", 0.0) or 0.0)
            return {
                **metrics,
                "current_price": last_price,
                "vwap": vwap,
                "price_vs_vwap_pct": ((last_price - vwap) / vwap * 100.0) if last_price > 0 and vwap > 0 else 0.0,
            }

    @staticmethod

    @staticmethod
    def _trade_metrics_locked(state: dict[str, Any], now: float) -> dict[str, Any]:
        events = state.get("trade_events")
        if not isinstance(events, deque):
            return {"trade_data_ready": 0.0, "volume_spike": 0.0}
        while events and now - events[0][0] > 120.0:
            events.popleft()
        recent60 = [item for item in events if 0.0 <= now - item[0] <= 60.0]
        recent30 = [item for item in recent60 if now - item[0] <= 30.0]
        recent5 = [item for item in recent60 if now - item[0] <= 5.0]
        prior55 = [item for item in recent60 if 5.0 < now - item[0] <= 60.0]
        total60 = sum(item[2] for item in recent60)
        total5 = sum(item[2] for item in recent5)

        # Robust 5-second baseline: median of occupied prior buckets rather than
        # a mean that can collapse toward zero for sparse instruments. Empty
        # buckets are not treated as evidence of a zero-volume normal regime.
        prior_bucket_volumes: dict[int, float] = {}
        for item in prior55:
            bucket = int(float(item[0]) // 5.0)
            prior_bucket_volumes[bucket] = prior_bucket_volumes.get(bucket, 0.0) + max(0.0, float(item[2]))
        occupied_baseline = [value for value in prior_bucket_volumes.values() if value > 0.0]
        baseline5 = float(np.median(occupied_baseline)) if occupied_baseline else 0.0

        vwap60 = sum(item[1] * item[2] for item in recent60) / total60 if total60 > 0 else 0.0
        latest = recent60[-1] if recent60 else None
        reference = next((item for item in reversed(recent60) if latest and item[0] <= latest[0] - 15.0), None)
        price_change15 = (latest[1] - reference[1]) / reference[1] * 100.0 if latest and reference and reference[1] > 0 else 0.0
        reference30 = next((item for item in reversed(recent60) if latest and item[0] <= latest[0] - 30.0), None)
        price_change30 = (latest[1] - reference30[1]) / reference30[1] * 100.0 if latest and reference30 and reference30[1] > 0 else 0.0
        trade_returns = [
            (recent60[i][1] - recent60[i - 1][1]) / recent60[i - 1][1] * 100.0
            for i in range(1, len(recent60)) if recent60[i - 1][1] > 0
        ]
        realized_volatility60 = float(np.std(trade_returns)) if len(trade_returns) >= 2 else 0.0
        buy30 = sum(item[2] for item in recent30 if item[3] == "BUY")
        sell30 = sum(item[2] for item in recent30 if item[3] == "SELL")
        known30 = buy30 + sell30
        imbalance30 = (buy30 - sell30) / known30 if known30 > 0 else 0.0
        last_age = max(0.0, now - latest[0]) if latest else math.inf
        coverage_sec = max(0.0, now - events[0][0]) if events else 0.0

        # A valid comparison needs a complete 60s window, at least three prior
        # occupied buckets and three prior prints. Otherwise the ratio is unknown,
        # not an artificially huge anomaly caused by a tiny denominator.
        min_baseline_buckets = _env_int(
            "PILUITUS_VOLUME_BASELINE_MIN_BUCKETS", 4, minimum=3
        )
        ready = bool(
            len(recent60) >= 3 and len(prior55) >= min_baseline_buckets
            and len(occupied_baseline) >= min_baseline_buckets and reference is not None
            and coverage_sec >= 60.0 and baseline5 > 0.0
            and last_age <= 5.0 and vwap60 > 0.0
        )
        raw_spike = total5 / baseline5 if ready and baseline5 > 0.0 else 0.0
        volume_spike = min(100.0, max(0.0, raw_spike))
        return {
            "trade_data_ready": float(ready), "trade_vwap_60s": float(vwap60),
            "trade_volume_5s": float(total5), "trade_volume_baseline_5s": float(baseline5),
            "volume_spike": float(volume_spike), "volume_spike_raw": float(raw_spike),
            "trade_baseline_bucket_count": int(len(occupied_baseline)),
            "trade_baseline_min_bucket_count": int(min_baseline_buckets),
            "trade_baseline_trade_count": int(len(prior55)),
            "trade_count_5s": int(len(recent5)),
            "price_change_pct": float(price_change15) if ready else 0.0,
            "trade_price_change_30s_pct": float(price_change30) if ready and reference30 else 0.0,
            "trade_realized_volatility_60s_pct": float(realized_volatility60) if ready else 0.0,
            "trade_flow_imbalance_30s": float(imbalance30),
            "trade_buy_volume_30s": float(buy30), "trade_sell_volume_30s": float(sell30),
            "trade_count_60s": int(len(recent60)),
            "trade_history_coverage_sec": float(coverage_sec),
            "trade_metrics_age_sec": float(last_age) if math.isfinite(last_age) else -1.0,
        }

