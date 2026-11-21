"""Routing and recording adapters for auxiliary market-data events."""
from __future__ import annotations

import math
import time
from collections.abc import Callable, Mapping
from typing import Any

from runtime.market_event_normalizer import (
    market_event_type, normalize_market_ticker, normalize_order_book_event,
)


class PaperBBOReplayAdapter:
    """Translate accepted QUOTES/ORDER_BOOK states into the BBO replay journal."""

    def __init__(self, *, recorder_provider: Callable[[], Any], screener: Any,
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        self.recorder_provider = recorder_provider
        self.screener = screener
        self.monotonic = monotonic

    @staticmethod
    def _number(value: object, default: float = 0.0) -> float:
        if isinstance(value, bool):
            return default
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return default
        return number if math.isfinite(number) else default

    def record(self, ticker: str, *, source: str,
               event_timestamp: object | None,
               received_monotonic: object | None,
               book_result: Mapping[str, Any] | None = None,
               sides_present: bool = False) -> bool:
        """Capture a normalized BBO state without awaiting or mutating execution."""
        symbol = normalize_market_ticker(ticker)
        if not symbol:
            return False
        quote_map = getattr(self.screener, "quotes", {})
        quote = quote_map.get(symbol, {}) if isinstance(quote_map, Mapping) else {}
        if not isinstance(quote, Mapping):
            quote = {}

        def current(key: str) -> float:
            if isinstance(book_result, Mapping) and key in book_result:
                return self._number(book_result.get(key))
            return self._number(quote.get(key))

        bid, ask = current("bid"), current("ask")
        bid_size, ask_size = current("bid_size"), current("ask_size")
        if isinstance(book_result, Mapping):
            valid = bool(book_result.get("valid", False))
            updated = bool(book_result.get("updated", False))
            reason = str(book_result.get("reason") or quote.get("order_book_last_reason") or "")
            book_valid = bool(book_result.get("order_book_valid", book_result.get("valid", False)))
        else:
            crossed = bid > 0.0 and ask > 0.0 and ask < bid
            valid = bool((bid > 0.0 or ask > 0.0) and not crossed)
            updated = bool(sides_present)
            reason = str(quote.get("order_book_last_reason") or "")
            book_valid = bool(quote.get("order_book_valid", False))

        state = {
            "updated": updated, "valid": valid, "order_book_valid": book_valid,
            "bid": bid, "ask": ask, "bid_size": bid_size, "ask_size": ask_size,
            "reason": reason,
        }
        source_name = str(source or "FINAM_ORDER_BOOK").upper()
        source_event = {
            "source": source_name,
            "timestamp": event_timestamp,
            "received_monotonic": received_monotonic,
            "bid_confirmed_monotonic": quote.get("last_bid_confirmed_seen"),
            "ask_confirmed_monotonic": quote.get("last_ask_confirmed_seen"),
            "max_age_sec": getattr(self.screener, "bbo_max_age_sec", 2.0),
            "book_reason": reason,
        }
        if source_name == "FINAM_ORDER_BOOK":
            source_event["order_book_timestamp"] = event_timestamp

        recorder = self.recorder_provider()
        record = getattr(recorder, "record", None)
        return bool(record(symbol, state, source_event)) if callable(record) else False


class MarketAuxiliaryEventRouter:
    """Handle INSTRUMENT_TRADE and ORDER_BOOK packets outside the QUOTES pipeline."""

    def __init__(self, *, screener: Any, execution_engine: Any, db_manager: Any,
                 logger: Any, trade_flow_increment: Callable[[str, int], None],
                 record_position_order_book_event: Callable[[str, dict[str, Any]], None],
                 bbo_replay: PaperBBOReplayAdapter,
                 refresh_bbo_metrics: Callable[..., tuple[bool, str]],
                 shadow_tracker: Any, persist_shadow_outcomes: Callable[..., Any],
                 resume_pending_bbo_candidate: Callable[..., Any],
                 monotonic: Callable[[], float] = time.monotonic) -> None:
        self.screener = screener
        self.execution_engine = execution_engine
        self.db_manager = db_manager
        self.logger = logger
        self.trade_flow_increment = trade_flow_increment
        self.record_position_order_book_event = record_position_order_book_event
        self.bbo_replay = bbo_replay
        self.refresh_bbo_metrics = refresh_bbo_metrics
        self.shadow_tracker = shadow_tracker
        self.persist_shadow_outcomes = persist_shadow_outcomes
        self.resume_pending_bbo_candidate = resume_pending_bbo_candidate
        self.monotonic = monotonic

    async def process_if_handled(self, event: Any) -> bool:
        """Route one auxiliary packet; False means it belongs to the QUOTES path."""
        event_type = market_event_type(event)
        if event_type == "INSTRUMENT_TRADE":
            self._process_instrument_trade(event)
            return True
        if event_type == "ORDER_BOOK":
            await self._process_order_book(event)
            return True
        return False

    def _process_instrument_trade(self, event: Mapping[str, Any]) -> None:
        ticker = normalize_market_ticker(event.get("ticker"))
        if not ticker:
            self.trade_flow_increment("instrument_trades_invalid", 1)
            return
        result = self.screener.push_trade(
            ticker=ticker, price=event.get("price", 0.0),
            size=event.get("size", 0.0), trade_id=event.get("trade_id"),
            side=event.get("side"), event_timestamp=event.get("timestamp"),
            received_monotonic=event.get("received_monotonic"),
        )
        if result.get("accepted"):
            self.trade_flow_increment("instrument_trades_received", 1)
        else:
            reason = str(result.get("reason") or "rejected")
            self.trade_flow_increment("instrument_trades_" + reason, 1)

    async def _process_order_book(self, event: Mapping[str, Any]) -> None:
        try:
            normalized = normalize_order_book_event(dict(event))
            ticker = normalized["ticker"]
            if not ticker:
                self.trade_flow_increment("orderbook_unusable", 1)
                return
            result = await self.screener.push_order_book(**normalized)
        except (TypeError, ValueError, KeyError):
            self.trade_flow_increment("orderbook_unusable", 1)
            return

        self.record_position_order_book_event(ticker, result)
        self.bbo_replay.record(
            ticker, source="FINAM_ORDER_BOOK",
            event_timestamp=normalized.get("order_book_timestamp"),
            received_monotonic=normalized.get("received_monotonic"),
            book_result=result,
        )

        if result.get("updated") and result.get("valid", True):
            self.trade_flow_increment("orderbook_updates", 1)
            self.trade_flow_increment(
                "orderbook_side_touches", int(result.get("bbo_side_touch_count", 0) or 0)
            )
            if result.get("heartbeat_only"):
                self.trade_flow_increment("orderbook_heartbeats", 1)
            metrics: dict[str, Any] = {}
            ready, _reason = self.refresh_bbo_metrics(
                self.screener, ticker, metrics, now=self.monotonic()
            )
            if ready:
                labels = self.shadow_tracker.observe_market_tick(ticker, metrics)
                if labels:
                    await self.persist_shadow_outcomes(labels, source="ORDER_BOOK")
            await self.resume_pending_bbo_candidate(ticker, trigger="ORDER_BOOK")
            if (getattr(self.execution_engine, "paper_mode", False)
                    and ticker in getattr(self.execution_engine, "paper_positions", {})):
                position = self.execution_engine.paper_positions.get(ticker)
                last_price = self.execution_engine._latest_price(ticker, position)
                self.execution_engine.schedule_paper_position_monitor(
                    ticker, last_price if last_price > 0.0 else None, self.db_manager,
                    event_source="ORDER_BOOK",
                    event_timestamp=normalized.get("order_book_timestamp"),
                )
        elif result.get("updated"):
            self.trade_flow_increment("orderbook_invalid", 1)
            self.logger.debug(
                "ORDER_BOOK %s заблокирован: %s",
                ticker, result.get("reason", result.get("book_reason", "invalid")),
            )
        elif result.get("out_of_order"):
            self.trade_flow_increment("orderbook_out_of_order", 1)
        else:
            self.trade_flow_increment("orderbook_unusable", 1)
            if result.get("reason"):
                self.logger.debug("ORDER_BOOK %s не принят: %s", ticker, result["reason"])
