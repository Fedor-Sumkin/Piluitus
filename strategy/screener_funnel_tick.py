"""Screener funnel Tick methods."""
from __future__ import annotations

import math
import time
from collections import deque
from typing import Any

import numpy as np

from strategy.screener_funnel_primitives import (
    _ofi_window,
    _queue_imbalance_window,
    logger,
)



class ScreenerFunnelTickMixin:
    """Composable behavior over the single HFTMemoryScreener state owner."""

    def _sync_push_tick(
        self,
        ticker: str,
        price: float,
        volume: float,
        bid: float = 0.0,
        ask: float = 0.0,
        bid_size: float = 0.0,
        ask_size: float = 0.0,
        day_volume: float = 0.0,
        bid_present: bool | None = None,
        ask_present: bool | None = None,
        bid_size_present: bool | None = None,
        ask_size_present: bool | None = None,
        event_timestamp: object | None = None,
        received_monotonic: float | None = None,
    ) -> dict:
        ticker = self.normalize_ticker(ticker)
        numeric_values = (price, volume, bid, ask, bid_size, ask_size, day_volume)
        if not ticker or not all(np.isfinite(value) for value in numeric_values) or price <= 0 or volume < 0:
            safe_price = float(price) if np.isfinite(price) and price > 0 else 0.0
            return {
                "is_anomaly": False,
                "selected": False,
                "selection_ready": self.selection_ready,
                "metrics": {"current_price": safe_price, "vwap": 0.0, "std_dev": 0.0, "volume_spike": 0.0, "mean_price": 0.0},
            }

        now = time.monotonic()
        with self._lock:
            if self.universe and ticker not in self._universe_set:
                # Не позволяем неожиданному/тестовому тикеру повлиять на ранжирование 88 инструментов.
                return {
                    "is_anomaly": False,
                    "selected": False,
                    "selection_ready": self.selection_ready,
                    "metrics": {"current_price": price, "vwap": price, "std_dev": 0.0, "volume_spike": 1.0, "mean_price": price},
                }

            self.register_ticker(ticker)
            # M1 OHLC uses observed last-price updates from QUOTES across the full
            # 88-symbol universe. QUOTES.lastSize is never used for volume/VWAP;
            # selected INSTRUMENT_TRADES may enrich the same price-only candle.
            bar_pattern_metrics = self.minute_bar_patterns.update(
                ticker, price, now=now, event_timestamp=event_timestamp
            )
            data = self.history[ticker]
            prices = data["prices"]
            volumes = data["volumes"]
            count = int(data["count"])

            if count < self.window_size:
                history_prices = prices[:count].copy()
                history_volumes = volumes[:count].copy()
                prices[count] = price
                volumes[count] = 0.0 if self.trade_data_mode else volume
                data["count"] = count + 1
                current_window_prices = prices[:count + 1]
                current_window_volumes = volumes[:count + 1]
            else:
                history_prices = prices.copy()
                history_volumes = volumes.copy()
                prices[:-1] = prices[1:]
                volumes[:-1] = volumes[1:]
                prices[-1] = price
                volumes[-1] = 0.0 if self.trade_data_mode else volume
                current_window_prices = prices
                current_window_volumes = volumes

            mean_price = float(np.mean(current_window_prices))
            total_volume = float(np.sum(current_window_volumes))
            vwap = float(np.sum(current_window_prices * current_window_volumes) / total_volume) if total_volume > 0 else price
            if history_prices.size >= 2:
                std_dev = float(np.std(history_prices))
                mean_volume = float(np.mean(history_volumes))
                volume_spike = volume / mean_volume if mean_volume > 0 else 1.0
            else:
                std_dev = price * 0.001
                volume_spike = 1.0
            if std_dev <= 0:
                std_dev = price * 0.0005

            trade_metrics = self._trade_metrics_locked(data, now) if self.trade_data_mode else {}
            if self.trade_data_mode:
                self.quotes[ticker]["trade_metrics"] = trade_metrics
                trade_ready = bool(trade_metrics.get("trade_data_ready", 0.0))
                vwap = float(trade_metrics.get("trade_vwap_60s", 0.0) or 0.0) if trade_ready else price
                volume_spike = float(trade_metrics.get("volume_spike", 0.0) or 0.0) if trade_ready else 0.0
                trade_volatility = float(trade_metrics.get("trade_realized_volatility_60s_pct", 0.0) or 0.0)
                if trade_ready and trade_volatility > 0.0:
                    std_dev = max(price * trade_volatility / 100.0, price * 0.0001)

            # Порог согласован с активным правилом RAG (>=1.5x). Это только
            # поиск кандидата; окончательный guard по стакану/спреду/риску остаётся.
            base_anomaly = (volume_spike >= 1.5) and (abs(price - vwap) > (1.5 * std_dev))
            # A completed, measured 1m breakout/pullback is also a candidate event.
            # It still must pass BBO, spread, size/risk guards before execution.
            is_anomaly = bool(
                (not self.trade_data_mode or bool(trade_metrics.get("trade_data_ready", 0.0)))
                and (base_anomaly or float(bar_pattern_metrics.get("bar_pattern_confirmed", 0.0)) >= 0.5)
            )
            base = {"vwap": vwap, "std_dev": std_dev, "volume_spike": float(volume_spike), "mean_price": mean_price}
            self.base_metrics[ticker] = base

            quote = self.quotes[ticker]
            # Default flags preserve compatibility with tests and internal producers
            # that provide numeric values without explicit field-presence metadata.
            bid_present = (bid > 0) if bid_present is None else bool(bid_present)
            ask_present = (ask > 0) if ask_present is None else bool(ask_present)
            bid_size_present = (bid_size > 0) if bid_size_present is None else bool(bid_size_present)
            ask_size_present = (ask_size > 0) if ask_size_present is None else bool(ask_size_present)

            # QUOTES and ORDER_BOOK have independent event-time watermarks.
            # Their envelope timestamps can differ slightly even when the later
            # received packet is the newest observation. Keep ordering within
            # each source, then use captured ingress time to arbitrate sources.
            received_at = now
            if received_monotonic is not None:
                try:
                    supplied_received = float(received_monotonic)
                    if math.isfinite(supplied_received) and 0.0 < supplied_received <= now + 0.05:
                        received_at = min(now, supplied_received)
                except (TypeError, ValueError, OverflowError):
                    pass
            event_seen_at = self._event_timestamp_to_monotonic(event_timestamp, received_at)
            prior_bid_event = float(quote.get("last_bid_quotes_event_seen", 0.0) or 0.0)
            prior_ask_event = float(quote.get("last_ask_quotes_event_seen", 0.0) or 0.0)
            accept_bid_event = bool(
                bid_present and (prior_bid_event <= 0.0 or event_seen_at >= prior_bid_event)
            )
            accept_ask_event = bool(
                ask_present and (prior_ask_event <= 0.0 or event_seen_at >= prior_ask_event)
            )
            accept_bid_update = bool(
                accept_bid_event
                and received_at >= float(quote.get("last_bid_source_received_seen", 0.0) or 0.0)
            )
            accept_ask_update = bool(
                accept_ask_event
                and received_at >= float(quote.get("last_ask_source_received_seen", 0.0) or 0.0)
            )
            if accept_bid_event:
                quote["last_bid_quotes_event_seen"] = event_seen_at
            if accept_ask_event:
                quote["last_ask_quotes_event_seen"] = event_seen_at
            if accept_bid_update:
                quote["last_bid_source_received_seen"] = received_at
                quote["last_bid_event_seen"] = event_seen_at
                quote["bid"] = float(bid) if bid > 0.0 else 0.0
                quote["last_bid_seen"] = event_seen_at if bid > 0.0 else 0.0
                quote["last_bid_confirmed_seen"] = event_seen_at if bid > 0.0 else 0.0
            if accept_ask_update:
                quote["last_ask_source_received_seen"] = received_at
                quote["last_ask_event_seen"] = event_seen_at
                quote["ask"] = float(ask) if ask > 0.0 else 0.0
                quote["last_ask_seen"] = event_seen_at if ask > 0.0 else 0.0
                quote["last_ask_confirmed_seen"] = event_seen_at if ask > 0.0 else 0.0

            quote_bid = float(quote.get("bid", 0.0) or 0.0)
            quote_ask = float(quote.get("ask", 0.0) or 0.0)
            if quote_bid > 0.0 and quote_ask > 0.0 and quote_ask < quote_bid:
                quote.setdefault("queue_imbalance_history", deque(maxlen=1024)).clear()
                quote.setdefault("ofi_history", deque(maxlen=4096)).clear()
                quote["last_bid_confirmed_seen"] = 0.0
                quote["last_ask_confirmed_seen"] = 0.0
                quote["last_bbo_seen"] = 0.0
                quote["order_book_valid"] = False
                quote["order_book_last_reason"] = "CROSSED_QUOTES_BBO"

            # A size is a separate observed field and may arrive with a partial
            # quote. Keep its own event-time watermark so delayed packets cannot
            # roll the depth sample backwards or revive a cleared size.
            prior_bid_size_event = float(
                quote.get("last_bid_size_event_seen", quote.get("last_bid_size_seen", 0.0)) or 0.0
            )
            prior_ask_size_event = float(
                quote.get("last_ask_size_event_seen", quote.get("last_ask_size_seen", 0.0)) or 0.0
            )
            if bid_size_present and (prior_bid_size_event <= 0.0 or event_seen_at >= prior_bid_size_event):
                quote["last_bid_size_event_seen"] = event_seen_at
                self._observe_top_size(quote, "bid", float(bid_size), event_seen_at)
            if ask_size_present and (prior_ask_size_event <= 0.0 or event_seen_at >= prior_ask_size_event):
                quote["last_ask_size_event_seen"] = event_seen_at
                self._observe_top_size(quote, "ask", float(ask_size), event_seen_at)

            current_bid_size = self._current_top_size(quote, "bid", now)
            current_ask_size = self._current_top_size(quote, "ask", now)
            bid_size_drop_pct = self._current_size_drop(quote, "bid", now)
            ask_size_drop_pct = self._current_size_drop(quote, "ask", now)
            bid_size_history_samples = int(quote.get("top_bid_history_samples", 0))
            ask_size_history_samples = int(quote.get("top_ask_history_samples", 0))
            if day_volume > 0:
                quote["day_volume"] = float(day_volume)
            quote["last_seen"] = now
            quote["tick_times"].append(now)
            price_events = quote["price_events"]
            if not price_events or abs(float(price_events[-1][1]) - float(price)) > 1e-12:
                price_events.append((now, float(price)))

            bid_level_seen = float(quote.get("last_bid_seen", 0.0) or 0.0)
            ask_level_seen = float(quote.get("last_ask_seen", 0.0) or 0.0)
            bid_confirmed_seen = float(quote.get("last_bid_confirmed_seen", bid_level_seen) or 0.0)
            ask_confirmed_seen = float(quote.get("last_ask_confirmed_seen", ask_level_seen) or 0.0)
            bid_age = now - bid_confirmed_seen if bid_confirmed_seen > 0.0 else float("inf")
            ask_age = now - ask_confirmed_seen if ask_confirmed_seen > 0.0 else float("inf")
            last_bbo_seen = min(bid_confirmed_seen, ask_confirmed_seen)
            quote["last_bbo_seen"] = last_bbo_seen if last_bbo_seen > 0.0 else 0.0
            bbo_age = now - last_bbo_seen if last_bbo_seen > 0.0 else float("inf")
            bbo_fresh = (
                float(quote["bid"]) > 0 and float(quote["ask"]) > 0
                and float(quote["ask"]) >= float(quote["bid"])
                and 0.0 <= bid_age <= self.bbo_max_age_sec
                and 0.0 <= ask_age <= self.bbo_max_age_sec
            )

            # Направленный импульс относительно цены до текущего тика.
            lookback = min(5, int(history_prices.size))
            reference_price = float(history_prices[-lookback]) if lookback > 0 else 0.0
            price_change_pct = (
                (float(price) - reference_price) / reference_price * 100.0
                if reference_price > 0 else 0.0
            )
            if self.trade_data_mode:
                price_change_pct = float(trade_metrics.get("price_change_pct", 0.0) or 0.0)
            price_vs_vwap_pct = (float(price) - float(vwap)) / float(vwap) * 100.0 if vwap > 0 else 0.0
            current_bid = float(quote["bid"])
            current_ask = float(quote["ask"])
            mid = (current_bid + current_ask) / 2.0 if bbo_fresh else 0.0
            spread_pct_current = (current_ask - current_bid) / mid * 100.0 if mid > 0 else -1.0

            should_refresh = (
                self._last_selection_update <= 0.0
                or now - self._last_selection_update >= self.selection_interval
            )
            if should_refresh:
                self._refresh_selection(now)

            selected_set = set(self.selected_tickers)
            selected = ticker in selected_set
            feature = self._ranking.get(ticker, {})
            bid_size_seen = float(quote.get("last_bid_size_seen", 0.0) or 0.0)
            ask_size_seen = float(quote.get("last_ask_size_seen", 0.0) or 0.0)
            bid_size_age = now - bid_size_seen if bid_size_seen > 0.0 else float("inf")
            ask_size_age = now - ask_size_seen if ask_size_seen > 0.0 else float("inf")
            bid_drop_at = float(quote.get("bid_size_drop_at", 0.0) or 0.0)
            ask_drop_at = float(quote.get("ask_size_drop_at", 0.0) or 0.0)
            bid_drop_age = now - bid_drop_at if bid_drop_at > 0.0 else -1.0
            ask_drop_age = now - ask_drop_at if ask_drop_at > 0.0 else -1.0
            queue_size_total = current_bid_size + current_ask_size
            queue_imbalance_available = bool(
                quote.get("order_book_valid", False)
                and current_bid_size > 0.0
                and current_ask_size > 0.0
                and queue_size_total > 0.0
            )
            queue_imbalance = (
                (current_bid_size - current_ask_size) / queue_size_total
                if queue_imbalance_available else 0.0
            )
            queue_imbalance_age = max(bid_size_age, ask_size_age)
            queue_window = _queue_imbalance_window(
                quote.get("queue_imbalance_history", ()), now,
                window_sec=5.0, min_samples=3, min_coverage_sec=1.0,
            )
            ofi_window = _ofi_window(
                quote.get("ofi_history", ()), now,
                window_sec=5.0, min_samples=3, min_coverage_sec=1.0,
                max_age_sec=self.bbo_max_age_sec,
            )
            metrics: dict[str, Any] = {
                "current_price": float(price),
                "vwap": float(vwap),
                "trade_data_mode": float(self.trade_data_mode),
                "trade_data_ready": float(trade_metrics.get("trade_data_ready", 0.0)) if self.trade_data_mode else 0.0,
                "trade_vwap_60s": float(trade_metrics.get("trade_vwap_60s", 0.0)) if self.trade_data_mode else 0.0,
                "trade_volume_5s": float(trade_metrics.get("trade_volume_5s", 0.0)) if self.trade_data_mode else None,
                "trade_volume_baseline_5s": float(trade_metrics.get("trade_volume_baseline_5s", 0.0)) if self.trade_data_mode else None,
                "volume_spike_raw": float(trade_metrics.get("volume_spike_raw", 0.0)) if self.trade_data_mode else None,
                "trade_baseline_bucket_count": int(trade_metrics.get("trade_baseline_bucket_count", 0)) if self.trade_data_mode else None,
                "trade_baseline_min_bucket_count": int(trade_metrics.get("trade_baseline_min_bucket_count", 0)) if self.trade_data_mode else None,
                "trade_baseline_trade_count": int(trade_metrics.get("trade_baseline_trade_count", 0)) if self.trade_data_mode else None,
                "trade_count_5s": int(trade_metrics.get("trade_count_5s", 0)) if self.trade_data_mode else None,
                "trade_flow_imbalance_30s": float(trade_metrics.get("trade_flow_imbalance_30s", 0.0)) if self.trade_data_mode else 0.0,
                "trade_buy_volume_30s": float(trade_metrics.get("trade_buy_volume_30s", 0.0)) if self.trade_data_mode else 0.0,
                "trade_sell_volume_30s": float(trade_metrics.get("trade_sell_volume_30s", 0.0)) if self.trade_data_mode else 0.0,
                "trade_price_change_30s_pct": float(trade_metrics.get("trade_price_change_30s_pct", 0.0)) if self.trade_data_mode else None,
                "trade_realized_volatility_60s_pct": float(trade_metrics.get("trade_realized_volatility_60s_pct", 0.0)) if self.trade_data_mode else None,
                "trade_history_coverage_sec": float(trade_metrics.get("trade_history_coverage_sec", 0.0)) if self.trade_data_mode else None,
                "trade_count_60s": int(trade_metrics.get("trade_count_60s", 0)) if self.trade_data_mode else 0,
                "trade_metrics_age_sec": float(trade_metrics.get("trade_metrics_age_sec", -1.0)) if self.trade_data_mode else -1.0,
                "std_dev": float(std_dev),
                "volume_spike": float(volume_spike),
                "mean_price": float(mean_price),
                "price_change_pct": float(price_change_pct),
                "price_vs_vwap_pct": float(price_vs_vwap_pct),
                "bid_size_drop_pct": float(bid_size_drop_pct),
                "ask_size_drop_pct": float(ask_size_drop_pct),
                "bid_size_baseline": float(quote.get("bid_size_baseline", 0.0) or 0.0),
                "ask_size_baseline": float(quote.get("ask_size_baseline", 0.0) or 0.0),
                "bid_size_current": float(quote.get("bid_size_current", current_bid_size) or 0.0),
                "ask_size_current": float(quote.get("ask_size_current", current_ask_size) or 0.0),
                "top_bid_history_samples": int(bid_size_history_samples),
                "top_ask_history_samples": int(ask_size_history_samples),
                "bid_size_drop_age_sec": round(bid_drop_age, 3) if bid_drop_age >= 0.0 else -1.0,
                "ask_size_drop_age_sec": round(ask_drop_age, 3) if ask_drop_age >= 0.0 else -1.0,
                "bid_size_age_sec": round(bid_size_age, 3) if math.isfinite(bid_size_age) else -1.0,
                "ask_size_age_sec": round(ask_size_age, 3) if math.isfinite(ask_size_age) else -1.0,
                "bid_size_available": float(current_bid_size > 0),
                "ask_size_available": float(current_ask_size > 0),
                "order_book_valid": float(bool(quote.get("order_book_valid", False))),
                "queue_imbalance_available": float(queue_imbalance_available),
                "queue_imbalance": float(queue_imbalance),
                "queue_imbalance_age_sec": round(queue_imbalance_age, 3) if math.isfinite(queue_imbalance_age) else -1.0,
                "queue_imbalance_5s": float(queue_window["value"]),
                "queue_imbalance_5s_available": float(queue_window["available"]),
                "queue_imbalance_5s_coverage_sec": round(float(queue_window["coverage_sec"]), 3),
                "queue_imbalance_5s_samples": int(queue_window["samples"]),
                "queue_imbalance_5s_age_sec": round(float(queue_window["age_sec"]), 3),
                "queue_imbalance_5s_positive_time_pct": round(float(queue_window["positive_time_pct"]), 2),
                "ofi_5s": float(ofi_window["value"]),
                "ofi_5s_raw": float(ofi_window["raw"]),
                "ofi_5s_available": float(ofi_window["available"]),
                "ofi_5s_samples": int(ofi_window["samples"]),
                "ofi_5s_coverage_sec": round(float(ofi_window["coverage_sec"]), 3),
                "ofi_5s_age_sec": round(float(ofi_window["age_sec"]), 3),
                "bid_ask_available": float(bbo_fresh),
                "bid_quote_age_sec": round(bid_age, 3) if math.isfinite(bid_age) else -1.0,
                "ask_quote_age_sec": round(ask_age, 3) if math.isfinite(ask_age) else -1.0,
                "bid_level_change_age_sec": round(now - bid_level_seen, 3) if bid_level_seen > 0.0 else -1.0,
                "ask_level_change_age_sec": round(now - ask_level_seen, 3) if ask_level_seen > 0.0 else -1.0,
                "order_book_age_sec": round(now - float(quote.get("last_order_book_seen", 0.0) or 0.0), 3) if float(quote.get("last_order_book_seen", 0.0) or 0.0) > 0.0 else -1.0,
                "bid_ask_age_sec": round(bbo_age, 3) if math.isfinite(bbo_age) else -1.0,
                "bbo_age_sec": round(bbo_age, 3) if math.isfinite(bbo_age) else -1.0,
                "bbo_max_age_sec": float(self.bbo_max_age_sec),
                "bid": current_bid,
                "ask": current_ask,
                "bid_size": float(current_bid_size),
                "ask_size": float(current_ask_size),
                "day_volume": float(quote["day_volume"]),
                "funnel_score": float(feature.get("score", 0.0)),
                "funnel_rank": int(feature.get("rank", 0)),
                "volatility_pct": float(feature.get("volatility_pct", 0.0)),
                "momentum_pct": float(feature.get("momentum_pct", 0.0)),
                "spread_pct": float(spread_pct_current) if spread_pct_current >= 0.0 else 0.0,
                "spread_available": float(spread_pct_current >= 0.0),
                "selected_for_ai": float(selected),
                "funnel_ready": float(self.selection_ready),
                **bar_pattern_metrics,
            }
            if float(bar_pattern_metrics.get("bar_pattern_confirmed", 0.0)) >= 0.5:
                logger.info(
                    "📈 [M1 PATTERN] %s: %s; ATR=%.4f%%, gap=%.2f ATR, body=%.0f%%, close_pos=%.0f%%.",
                    ticker,
                    str(bar_pattern_metrics.get("bar_pattern_name", "")),
                    float(bar_pattern_metrics.get("bar_atr_pct", 0.0)),
                    float(bar_pattern_metrics.get("bar_signal_gap_atr", 0.0)),
                    float(bar_pattern_metrics.get("bar_body_ratio", 0.0)) * 100.0,
                    float(bar_pattern_metrics.get("bar_close_position", 0.5)) * 100.0,
                )
            return {
                "is_anomaly": bool(is_anomaly),
                "selected": selected,
                "selection_ready": self.selection_ready,
                "eligible_count": self.eligible_count,
                "selected_tickers": list(self.selected_tickers),
                "quote_side_updates": {
                    "bid": bool(accept_bid_update),
                    "ask": bool(accept_ask_update),
                },
                "metrics": metrics,
            }

    async def push_tick(
        self,
        ticker: str,
        price: float,
        volume: float,
        bid: float = 0.0,
        ask: float = 0.0,
        bid_size: float = 0.0,
        ask_size: float = 0.0,
        day_volume: float = 0.0,
        bid_present: bool | None = None,
        ask_present: bool | None = None,
        bid_size_present: bool | None = None,
        ask_size_present: bool | None = None,
        event_timestamp: object | None = None,
        received_monotonic: float | None = None,
    ) -> dict:
        """Обновляет короткую историю и рейтинг; расчёты массива малы и локальны."""
        # Функция работает в одном event loop; короткий расчёт 30 тиков x 88 буферов
        # не требует создавать задачу в executor для каждого входящего сообщения.
        return self._sync_push_tick(
            ticker, float(price), float(volume), float(bid), float(ask),
            float(bid_size), float(ask_size), float(day_volume),
            bid_present=bid_present, ask_present=ask_present,
            bid_size_present=bid_size_present, ask_size_present=ask_size_present,
            event_timestamp=event_timestamp, received_monotonic=received_monotonic,
        )

