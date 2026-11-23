"""Reusable support objects for Finam's market-data WebSocket loop."""
from __future__ import annotations
import time
from typing import Any
from exchange.finam_shared import logger

class FinamQueueOverflowTracker:
    """Rate-limited counters for full market-data queues."""
    def __init__(self) -> None:
        self.counts: dict[str, int] = {"ORDER_BOOK": 0, "INSTRUMENT_TRADE": 0, "QUOTES": 0}
        self.last_log_at = 0.0
        self.last_ticker = "-"

    def record(self, event_kind: str, ticker: object) -> None:
        self.counts[event_kind] = self.counts.get(event_kind, 0) + 1
        self.last_ticker = str(ticker or "-")
        now = time.monotonic()
        if now - self.last_log_at < 5.0:
            return
        total_dropped = sum(self.counts.values())
        logger.warning(
            "[FINAM QUEUE OVERFLOW] за последние %.1fс: всего=%d, "
            "ORDER_BOOK=%d, INSTRUMENT_TRADE=%d, QUOTES=%d; последний тикер=%s. "
            "Поток не успевает обрабатываться потребителем; события теряются.",
            max(0.0, now - self.last_log_at) if self.last_log_at else 5.0,
            total_dropped, self.counts["ORDER_BOOK"],
            self.counts["INSTRUMENT_TRADE"], self.counts["QUOTES"], self.last_ticker,
        )
        for key in self.counts:
            self.counts[key] = 0
        self.last_log_at = now

class FinamOrderBookSymbolDiagnostics:
    """Maintain rolling per-symbol diagnostic counters."""
    def __init__(self) -> None:
        self._by_ticker: dict[str, dict[str, Any]] = {}

    def for_ticker(self, ticker: str) -> dict[str, Any]:
        return self._by_ticker.setdefault(ticker, {
            "window_wire": 0, "window_parse_no_update": 0,
            "window_parse_out_of_order": 0, "window_parse_unrecognized": 0,
            "window_snapshot_wait": 0, "window_parsed": 0, "window_queued": 0,
            "window_dropped": 0, "last_wire_monotonic": 0.0,
            "last_parsed_monotonic": 0.0, "last_queued_monotonic": 0.0,
        })

    def values(self):
        return self._by_ticker.values()

class FinamPositionSnapshotRefresher:
    """Refresh REST BBO and always release its in-flight symbol marker."""
    def __init__(self, broker: Any, queue: Any, in_flight: set[str]) -> None:
        self.broker, self.queue, self.in_flight = broker, queue, in_flight

    async def refresh(self, symbol: str) -> None:
        ticker = symbol.split("@", 1)[0].strip().upper()
        try:
            await self.broker.refresh_order_book_snapshot(ticker, self.queue)
        except Exception as exc:
            logger.warning("Finam REST BBO refresh %s failed: %s", ticker, exc)
        finally:
            self.in_flight.discard(ticker)
