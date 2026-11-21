"""Thread-safe diagnostics for ORDER_BOOK updates on open positions.

These counters are observational only: they do not participate in entry,
exit, sizing, or risk decisions.
"""
from __future__ import annotations

import logging
import math
import threading
import time
from collections.abc import Callable
from typing import Any


class PositionOrderBookDiagnostics:
    """Collect and periodically report ORDER_BOOK health for open positions."""

    def __init__(
        self,
        logger: logging.Logger,
        *,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self._logger = logger
        self._monotonic = monotonic
        self._lock = threading.Lock()
        self._items: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _empty_item() -> dict[str, Any]:
        return {
            "window_events": 0,
            "window_valid": 0,
            "window_invalid": 0,
            "window_out_of_order": 0,
            "window_unusable": 0,
            "window_invalid_reasons": {},
            "last_event_monotonic": 0.0,
            "last_result": "",
        }

    def record(
        self,
        ticker: str,
        result: dict[str, Any],
        execution_engine: object,
    ) -> None:
        """Record one book result only while the ticker has an active position."""
        positions = getattr(execution_engine, "paper_positions", {})
        trade = positions.get(ticker) if isinstance(positions, dict) else None
        if (
            not isinstance(trade, dict)
            or str(trade.get("status", "OPEN")).upper() not in {"OPEN", "ACTIVE"}
        ):
            return

        reason = str(result.get("reason") or result.get("book_reason") or "").strip().upper()
        if not reason:
            reason = "OK" if result.get("valid", True) else "UNKNOWN"
        now = self._monotonic()
        with self._lock:
            item = self._items.setdefault(ticker, self._empty_item())
            item["window_events"] += 1
            if result.get("updated") and result.get("valid", True):
                item["window_valid"] += 1
            elif result.get("updated"):
                item["window_invalid"] += 1
                reasons = item["window_invalid_reasons"]
                reasons[reason] = reasons.get(reason, 0) + 1
            elif result.get("out_of_order"):
                item["window_out_of_order"] += 1
            else:
                item["window_unusable"] += 1
            item["last_event_monotonic"] = now
            item["last_result"] = reason

    def log_open_positions(
        self,
        execution_engine: object,
        screener: object,
        parse_finite_float: Callable[[object], float | None],
    ) -> None:
        """Emit and reset one diagnostic window for every active PAPER position."""
        positions = getattr(execution_engine, "paper_positions", {})
        quotes = getattr(screener, "quotes", {})
        if not isinstance(positions, dict) or not positions:
            return

        now = self._monotonic()
        for raw_ticker, trade in sorted(positions.items()):
            if (
                not isinstance(trade, dict)
                or str(trade.get("status", "OPEN")).upper() not in {"OPEN", "ACTIVE"}
            ):
                continue
            ticker = str(trade.get("ticker") or raw_ticker).strip().upper().split("@")[0]
            quote = quotes.get(ticker, {}) if isinstance(quotes, dict) else {}
            if not isinstance(quote, dict):
                quote = {}

            def age_text(field: str) -> str:
                observed = parse_finite_float(quote.get(field))
                if observed is None or observed <= 0.0:
                    return "нет"
                age = now - observed
                return f"{age:.2f}s" if math.isfinite(age) and age >= 0.0 else "будущее"

            side = str(trade.get("side", "")).strip().upper()
            executable_side = "ASK" if side == "SELL" else "BID"
            executable_age = age_text(
                "last_ask_confirmed_seen" if executable_side == "ASK"
                else "last_bid_confirmed_seen"
            )
            with self._lock:
                item = self._items.setdefault(ticker, self._empty_item())
                window_events = item["window_events"]
                window_valid = item["window_valid"]
                window_invalid = item["window_invalid"]
                window_out_of_order = item["window_out_of_order"]
                window_unusable = item["window_unusable"]
                invalid_reasons = dict(item["window_invalid_reasons"])
                last_event_monotonic = float(item["last_event_monotonic"] or 0.0)
                last_result = str(item.get("last_result") or "-")
                item.update(self._empty_item())

            invalid_summary = ",".join(
                f"{reason}:{count}" for reason, count in sorted(invalid_reasons.items())
            ) or "-"
            last_event_age = (
                f"{now - last_event_monotonic:.2f}s"
                if last_event_monotonic > 0.0 and now >= last_event_monotonic else "нет"
            )
            self._logger.info(
                "📘 [OPEN POSITION BBO 60s] %s side=%s exec=%s exec_age=%s "
                "Bid=%.6f Ask=%.6f bid_age=%s ask_age=%s book_event_age=%s "
                "book_receive_age=%s book_valid=%s book_reason=%s last_position_event_age=%s "
                "last_position_result=%s events=%d valid=%d invalid=%d out_of_order=%d unusable=%d invalid_reasons=%s",
                ticker, side, executable_side, executable_age,
                parse_finite_float(quote.get("bid")) or 0.0,
                parse_finite_float(quote.get("ask")) or 0.0,
                age_text("last_bid_confirmed_seen"),
                age_text("last_ask_confirmed_seen"),
                age_text("last_order_book_event_seen"),
                age_text("last_order_book_received"),
                bool(quote.get("order_book_valid", False)),
                str(quote.get("order_book_last_reason", "NO_ORDER_BOOK")),
                last_event_age, last_result, window_events, window_valid, window_invalid,
                window_out_of_order, window_unusable, invalid_summary,
            )

    def clear(self) -> None:
        """Discard counters when the operator starts a clean PAPER session."""
        with self._lock:
            self._items.clear()
