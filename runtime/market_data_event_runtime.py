"""Orchestration for the complete incoming market-data event flow."""
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from runtime.market_event_normalizer import normalize_market_quote_event


class MarketDataEventRuntime:
    """Coordinate auxiliary events, quote screening, shadow and candidate admission."""

    def __init__(
        self,
        *,
        execution_engine: Any,
        db_manager: Any,
        screener: Any,
        auxiliary_event_router: Any,
        candidate_pipeline: Any,
        pending_bbo_runtime: Any,
        shadow_tracker: Any,
        persist_shadow_outcomes: Callable[..., Any],
        schedule_m1_history_warmup: Callable[[tuple[str, ...]], None],
        trade_flow_increment: Callable[[str], None],
        terminal_status: Any,
        logger: Any,
        session_generation_provider: Callable[[], int],
        monotonic: Callable[[], float],
        spinner_frames: tuple[str, ...] | list[str] = ("|", "/", "-", "\\"),
    ) -> None:
        self.execution_engine = execution_engine
        self.db_manager = db_manager
        self.screener = screener
        self.auxiliary_event_router = auxiliary_event_router
        self.candidate_pipeline = candidate_pipeline
        self.pending_bbo_runtime = pending_bbo_runtime
        self.shadow_tracker = shadow_tracker
        self.persist_shadow_outcomes = persist_shadow_outcomes
        self.schedule_m1_history_warmup = schedule_m1_history_warmup
        self.trade_flow_increment = trade_flow_increment
        self.terminal_status = terminal_status
        self.logger = logger
        self.session_generation_provider = session_generation_provider
        self.monotonic = monotonic
        self.spinner_frames = tuple(spinner_frames) or ("|",)
        self.tick_counter = 0
        self.last_funnel_log_at = 0.0
        self.last_funnel_log_key: tuple[str, ...] | None = None
        self.last_funnel_ready = False

    def _update_funnel_diagnostics(self, analysis: dict[str, Any]) -> dict[str, Any]:
        funnel_status = self.screener.get_status()
        self.terminal_status.update_funnel(self.screener.get_status_text())
        if analysis.get("selected") and analysis.get("selection_ready"):
            self.trade_flow_increment("selected_quotes")

        now_mono = self.monotonic()
        selected_snapshot = tuple(funnel_status.get("selected_tickers", []))
        if funnel_status.get("ready") and selected_snapshot:
            self.schedule_m1_history_warmup(selected_snapshot)

        if not funnel_status["ready"]:
            self.last_funnel_ready = False
            if (
                self.last_funnel_log_at == 0.0
                or now_mono - self.last_funnel_log_at >= 30.0
            ):
                self.logger.info(
                    "🎯 Воронка прогревается: котировки получены по %d/%d инструментам; "
                    "пригодны для рейтинга %d; цель top-%d.",
                    funnel_status["observed_count"],
                    funnel_status["universe_count"],
                    funnel_status["eligible_count"],
                    funnel_status["top_n"],
                )
                self.last_funnel_log_at = now_mono
        elif selected_snapshot:
            first_ready = not self.last_funnel_ready
            selection_changed = (
                self.last_funnel_log_key is None
                or selected_snapshot != self.last_funnel_log_key
            )
            cooldown_elapsed = (
                self.last_funnel_log_at == 0.0
                or now_mono - self.last_funnel_log_at >= 30.0
            )
            if first_ready or (selection_changed and cooldown_elapsed):
                self.logger.info(
                    "🎯 ВОРОНКА ГОТОВА: %d → %d тикеров для LLM: %s",
                    funnel_status["universe_count"],
                    len(selected_snapshot),
                    ", ".join(selected_snapshot),
                )
                self.last_funnel_log_at = now_mono
                self.last_funnel_log_key = selected_snapshot
            self.last_funnel_ready = True
        return funnel_status

    async def process_event(self, event: Any) -> None:
        """Process one provider packet without changing the established gate order."""
        event_session_generation = self.session_generation_provider()
        if getattr(self.execution_engine, "_maintenance_active", False):
            return

        # Expiration remains before auxiliary routing, as in the original handler.
        self.pending_bbo_runtime.expire()
        try:
            if await self.auxiliary_event_router.process_if_handled(event):
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

            # Quote ticks do not cancel deferred candidates. They may refresh quote
            # confirmation clocks; resume is possible only while a recent coherent
            # ORDER_BOOK remains fresh.
            self.tick_counter += 1
            current_frame = self.spinner_frames[
                self.tick_counter % len(self.spinner_frames)
            ]
            self.terminal_status.update(
                f"⚡ [WS ПОТОК ФИНАМ] {current_frame} "
                f"Тиков: {self.tick_counter:,} | {ticker} {price}₽"
            )

            # Every instrument contributes to the local ranking, not only anomalies.
            analysis = await self.screener.push_tick(
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

            self.trade_flow_increment("quotes_received")
            accepted_quote_sides = analysis.get("quote_side_updates", {})
            self.auxiliary_event_router.bbo_replay.record(
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
            # book may be confirmed by the newest QUOTES packet between book deltas.
            if self.pending_bbo_runtime.has_pending(ticker):
                await self.pending_bbo_runtime.resume(ticker, trigger="QUOTES")

            quote_metrics_for_shadow = analysis.get("metrics", {})
            if isinstance(quote_metrics_for_shadow, dict):
                shadow_labels = self.shadow_tracker.observe_market_tick(
                    ticker, quote_metrics_for_shadow
                )
                if shadow_labels:
                    await self.persist_shadow_outcomes(
                        shadow_labels, source="QUOTES"
                    )

            self._update_funnel_diagnostics(analysis)
            self.logger.debug(
                "📥 [ВХОДЯЩИЙ ТИК] %s price=%s anomaly=%s selected=%s rank=%s score=%s",
                ticker,
                price,
                analysis["is_anomaly"],
                analysis.get("selected", False),
                analysis.get("metrics", {}).get("funnel_rank", 0),
                analysis.get("metrics", {}).get("funnel_score", 0.0),
            )

            self.candidate_pipeline.process_candidate(
                ticker=ticker,
                price=price,
                volume=volume,
                bid=bid,
                ask=ask,
                bid_size=bid_size,
                ask_size=ask_size,
                day_volume=day_volume,
                analysis=analysis,
                supplied_metrics=supplied_metrics,
                session_generation=event_session_generation,
                received_monotonic=received_monotonic,
            )

            # Position monitoring remains active for every quote in the universe.
            self.execution_engine.schedule_paper_position_monitor(
                ticker,
                price,
                self.db_manager,
                event_source="LAST_TRADE",
                event_timestamp=event_timestamp,
            )
        except Exception as exc:
            self.logger.error(
                "💥 Ошибка в обработчике тиков: %s", exc, exc_info=True
            )
