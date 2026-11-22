"""Screener funnel Selection methods."""
from __future__ import annotations

import math
import time

import numpy as np

from strategy.screener_funnel_primitives import logger



class ScreenerFunnelSelectionMixin:
    """Composable behavior over the single HFTMemoryScreener state owner."""

    @staticmethod
    def _percentile_ranks(values: dict[str, float]) -> dict[str, float]:
        """Percentile rank 0..1 с одинаковым средним рангом для одинаковых значений."""
        if not values:
            return {}
        ordered = sorted(values.items(), key=lambda item: item[1])
        if len(ordered) == 1:
            return {ordered[0][0]: 0.5}
        result: dict[str, float] = {}
        i = 0
        while i < len(ordered):
            j = i
            while j + 1 < len(ordered) and ordered[j + 1][1] == ordered[i][1]:
                j += 1
            avg_rank = (i + j) / 2.0
            rank = avg_rank / (len(ordered) - 1)
            for pos in range(i, j + 1):
                result[ordered[pos][0]] = rank
            i = j + 1
        return result

    def _features_for(self, ticker: str, now: float) -> dict[str, float] | None:
        data = self.history.get(ticker)
        quote = self.quotes.get(ticker)
        if not data or not quote:
            return None
        count = int(data["count"])
        if count < self.min_samples or now - float(quote["last_seen"]) > self.stale_after:
            return None

        prices = data["prices"][:count]
        volumes = data["volumes"][:count]
        if len(prices) < self.min_samples or not np.all(np.isfinite(prices)) or np.any(prices <= 0):
            return None

        returns_pct = np.diff(prices) / prices[:-1] * 100.0
        volatility_pct = float(np.std(returns_pct)) if len(returns_pct) else 0.0
        lookback = min(5, len(prices) - 1)
        base_price = float(prices[-1 - lookback])
        momentum_pct = abs(float(prices[-1]) - base_price) / base_price * 100.0 if base_price > 0 else 0.0

        if self.trade_data_mode:
            # The trade stream is subscribed only for Top-15 and therefore must
            # never bias cross-sectional ranking. Sample every symbol on the same
            # 5-second grid; event-to-event returns are not comparable when quote
            # update frequencies differ.
            momentum_pct = 0.0
            volatility_pct = 0.0
            raw_events = quote.get("price_events", ())
            price_points: list[tuple[float, float]] = []
            for item in raw_events:
                if not isinstance(item, (tuple, list)) or len(item) < 2:
                    continue
                try:
                    stamp = float(item[0])
                    value = float(item[1])
                except (TypeError, ValueError, OverflowError):
                    continue
                if (
                    math.isfinite(stamp) and math.isfinite(value) and value > 0.0
                    and 0.0 <= now - stamp <= 75.0
                ):
                    price_points.append((stamp, value))
            price_points.sort(key=lambda item: item[0])

            if price_points:
                latest_stamp, latest_price = price_points[-1]
                latest_age = now - latest_stamp
                if 0.0 <= latest_age <= self.price_event_max_age_sec:
                    # 30-second momentum uses a price observation close to the
                    # exact target time. A sparse/stale reference is unavailable,
                    # not silently stretched into a different horizon.
                    target_30 = latest_stamp - 30.0
                    reference30 = next(
                        (item for item in reversed(price_points) if item[0] <= target_30),
                        None,
                    )
                    if (
                        reference30 is not None
                        and target_30 - reference30[0] <= self.price_event_max_age_sec
                        and reference30[1] > 0.0
                    ):
                        momentum_pct = abs(
                            (latest_price - reference30[1]) / reference30[1] * 100.0
                        )

                    # Realized volatility over a fixed 60-second window. Each
                    # sample is the last observed price at/before a 5-second
                    # boundary and is accepted only within the configured age.
                    sampled: list[tuple[int, float]] = []
                    window_start = latest_stamp - 60.0
                    for bucket in range(13):
                        boundary = window_start + bucket * 5.0
                        observation = next(
                            (item for item in reversed(price_points) if item[0] <= boundary),
                            None,
                        )
                        if (
                            observation is not None
                            and 0.0 <= boundary - observation[0] <= self.price_event_max_age_sec
                        ):
                            sampled.append((bucket, observation[1]))

                    fixed_interval_returns: list[float] = []
                    for (left_bucket, left_price), (right_bucket, right_price) in zip(
                        sampled, sampled[1:]
                    ):
                        if right_bucket != left_bucket + 1 or left_price <= 0.0:
                            continue
                        fixed_interval_returns.append(
                            (right_price - left_price) / left_price * 100.0
                        )
                    if len(fixed_interval_returns) >= 8:
                        volatility_pct = math.sqrt(
                            sum(value * value for value in fixed_interval_returns)
                        )

        base = self.base_metrics.get(ticker, {})
        # The volume spike is an entry feature; it cannot rank only already-subscribed symbols.
        volume_spike = 0.0 if self.trade_data_mode else max(0.0, float(base.get("volume_spike", 1.0)))
        day_volume = max(0.0, float(quote.get("day_volume", 0.0)))
        mean_trade_volume = float(np.mean(volumes)) if len(volumes) else 0.0
        liquidity_measure = day_volume if day_volume > 0 else mean_trade_volume * len(volumes)

        bid = float(quote.get("bid", 0.0))
        ask = float(quote.get("ask", 0.0))
        bid_seen = float(quote.get("last_bid_confirmed_seen", quote.get("last_bid_seen", 0.0)) or 0.0)
        ask_seen = float(quote.get("last_ask_confirmed_seen", quote.get("last_ask_seen", 0.0)) or 0.0)
        bbo_fresh = (
            bid > 0 and ask > 0 and ask >= bid
            and bid_seen > 0 and ask_seen > 0
            and 0.0 <= now - bid_seen <= self.bbo_max_age_sec
            and 0.0 <= now - ask_seen <= self.bbo_max_age_sec
        )
        mid = (bid + ask) / 2.0 if bbo_fresh else 0.0
        spread_pct = (ask - bid) / mid * 100.0 if mid > 0 else -1.0
        tick_times = quote["tick_times"]
        recent_ticks = sum(1 for tick_time in tick_times if now - tick_time <= 30.0)

        order_book_seen = float(quote.get("last_order_book_seen", 0.0) or 0.0)
        order_book_received = float(quote.get("last_order_book_received", 0.0) or 0.0)
        return {
            "volatility_pct": max(0.0, volatility_pct),
            "momentum_pct": max(0.0, momentum_pct),
            "volume_spike": min(volume_spike, 1000.0),
            "liquidity_measure": max(0.0, liquidity_measure),
            "day_volume": day_volume,
            "order_book_event_age_sec": max(0.0, now - order_book_seen) if order_book_seen > 0.0 else -1.0,
            "order_book_receive_age_sec": max(0.0, now - order_book_received) if order_book_received > 0.0 else -1.0,
            "order_book_valid": float(bool(quote.get("order_book_valid", False))),
            "spread_pct": spread_pct,
            "tick_activity_30s": float(recent_ticks),
        }

    def _refresh_selection(self, now: float) -> None:
        candidates = {
            ticker: features
            for ticker in (self.universe or list(self.history.keys()))
            if (features := self._features_for(ticker, now)) is not None
        }
        self.eligible_count = len(candidates)
        universe_size = len(self.universe) if self.universe else len(self.history)
        minimum_for_selection = min(self.top_n, universe_size)
        # Если свежих кандидатов стало меньше top-N, не анализируем случайное
        # подмножество: снова ждём минимум полную пятнадцатку пригодных символов.
        if len(candidates) < minimum_for_selection:
            # A temporary stale quote must not flush the entire subscription set.
            # Retain only still-eligible previous members; no LLM candidate is
            # emitted until the full Top-N coverage is restored.
            self.selection_ready = False
            self._ranking = {}
            self.selected_tickers = [
                ticker for ticker in self.selected_tickers if ticker in candidates
            ]
            self._selected_since = {
                ticker: self._selected_since.get(ticker, now)
                for ticker in self.selected_tickers
            }
            self._replacement_streak.clear()
            self._last_selection_update = now
            return
        coverage_required = min(
            universe_size,
            max(min(self.top_n, universe_size), int(math.ceil(universe_size * self.minimum_coverage))),
        )
        coverage_ready = len(candidates) >= coverage_required
        timeout_ready = (now - self._started_at) >= self.warmup_timeout and len(candidates) >= min(self.top_n, universe_size)
        if coverage_ready or timeout_ready:
            self.selection_ready = True

        if not candidates:
            self._ranking = {}
            self.selected_tickers = []
            self._last_selection_update = now
            return

        # Нормируем признаки внутри наблюдаемого рынка: единицы измерения не влияют
        # на итоговый рейтинг. Главный вес — фактическая краткосрочная волатильность.
        volatility_ranks = self._percentile_ranks({t: f["volatility_pct"] for t, f in candidates.items()})
        momentum_ranks = self._percentile_ranks({t: f["momentum_pct"] for t, f in candidates.items()})
        volume_ranks = self._percentile_ranks({t: math.log1p(f["volume_spike"]) for t, f in candidates.items()})
        liquidity_ranks = self._percentile_ranks({t: math.log1p(f["liquidity_measure"]) for t, f in candidates.items()})
        activity_ranks = self._percentile_ranks({t: math.log1p(f["tick_activity_30s"]) for t, f in candidates.items()})

        ranked: dict[str, dict[str, float | int]] = {}
        for ticker, feature in candidates.items():
            spread_pct = feature["spread_pct"]
            # Нет bid/ask — нейтральная, а не максимальная оценка качества спреда.
            spread_quality = (
                max(0.0, 1.0 - spread_pct / self.max_spread_pct)
                if spread_pct >= 0.0 else 0.35
            )
            liquidity_score = 0.7 * liquidity_ranks[ticker] + 0.3 * activity_ranks[ticker]
            score = 100.0 * (
                0.40 * volatility_ranks[ticker]
                + 0.25 * momentum_ranks[ticker]
                + 0.15 * volume_ranks[ticker]
                + 0.10 * liquidity_score
                + 0.10 * spread_quality
            )
            ranked[ticker] = {
                **feature,
                "score": round(score, 3),
                "volatility_rank_pct": round(volatility_ranks[ticker] * 100.0, 1),
                "momentum_rank_pct": round(momentum_ranks[ticker] * 100.0, 1),
                "volume_rank_pct": round(volume_ranks[ticker] * 100.0, 1),
                "rank": 0,
            }

        ordered = sorted(ranked, key=lambda ticker: float(ranked[ticker]["score"]), reverse=True)
        for rank, ticker in enumerate(ordered, start=1):
            ranked[ticker]["rank"] = rank
        self._ranking = ranked

        if not self.selection_ready:
            # Preserve a previously active eligible cohort during recovery from
            # a temporary coverage dip. Initial warm-up still starts with no cohort.
            self.selected_tickers = [
                ticker for ticker in self.selected_tickers if ticker in ranked
            ]
            self._selected_since = {
                ticker: self._selected_since.get(ticker, now)
                for ticker in self.selected_tickers
            }
            self._replacement_streak.clear()
            self._last_selection_update = now
            return

        target_count = min(self.top_n, universe_size, len(ordered))
        if target_count <= 0:
            self.selected_tickers = []
            self._last_selection_update = now
            return

        # Keep the current cohort stable. Stale/ineligible members are removed
        # immediately, but score-only challengers must stay in Top-N for multiple
        # refreshes and the outgoing member must have passed its minimum hold time.
        cutoff = float(ranked[ordered[target_count - 1]]["score"])
        retention_floor = cutoff - self.retention_score_gap
        previous = list(self.selected_tickers)
        eligible_previous = [ticker for ticker in previous if ticker in ranked]
        invalid_previous = len(previous) - len(eligible_previous)
        target_set = set(ordered[:target_count])
        chosen = list(eligible_previous)
        replacements = 0

        # Initial fill and replacement of genuinely stale members are immediate:
        # these are coverage repairs, not score-driven churn.
        if len(chosen) < target_count:
            for ticker in ordered:
                if len(chosen) >= target_count:
                    break
                if ticker not in chosen:
                    chosen.append(ticker)
                    self._selected_since[ticker] = now
            self._replacement_streak.clear()
        else:
            current_challengers = target_set - set(chosen)
            self._replacement_streak = {
                ticker: streak
                for ticker, streak in self._replacement_streak.items()
                if ticker in current_challengers
            }
            for ticker in ordered[:target_count]:
                if ticker in chosen:
                    self._replacement_streak.pop(ticker, None)
                    continue
                self._replacement_streak[ticker] = self._replacement_streak.get(ticker, 0) + 1
                if self._replacement_streak[ticker] < self.replacement_confirmations:
                    continue
                replaceable = [
                    old for old in chosen
                    if old not in target_set
                    and float(ranked[old]["score"]) < retention_floor
                    and now - self._selected_since.get(old, now) >= self.min_selected_hold_sec
                ]
                if not replaceable or replacements >= self.max_replacements_per_refresh:
                    continue
                weakest = min(replaceable, key=lambda old: float(ranked[old]["score"]))
                chosen.remove(weakest)
                self._selected_since.pop(weakest, None)
                chosen.append(ticker)
                self._selected_since[ticker] = now
                self._replacement_streak.pop(ticker, None)
                replacements += 1

        self.selected_tickers = chosen
        self._selected_since = {
            ticker: self._selected_since.get(ticker, now)
            for ticker in self.selected_tickers
        }
        if set(previous) != set(chosen):
            logger.info(
                "[FUNNEL TURNOVER] previous=%d selected=%d retained=%d replaced=%d invalid_previous=%d",
                len(previous), len(chosen), len(set(previous) & set(chosen)),
                len(set(previous) - set(chosen)), invalid_previous,
            )
        self._last_selection_update = now

