"""Bounded in-memory lifecycle for the durable closed PAPER trade journal."""
from __future__ import annotations

from typing import Any, Mapping

from .paper_performance_primitives import (
    PAPER_CLOSED_TRADES_STORAGE_LIMIT,
    _number,
    _text,
)


class PaperPerformanceRecordsMixin:
    """Own the bounded, idempotent in-memory closed-trade collection."""

    def __init__(self, max_records: int = PAPER_CLOSED_TRADES_STORAGE_LIMIT) -> None:
        self.max_records = max(1, int(max_records))
        self._by_id: dict[str, dict[str, Any]] = {}

    @staticmethod
    def _valid_trade(trade: object) -> dict[str, Any] | None:
        if not isinstance(trade, Mapping):
            return None
        if str(trade.get("status") or "").upper() != "CLOSED":
            return None
        trade_id = _text(trade.get("id"), "")
        mode = str(trade.get("mode") or "").strip().upper()
        if mode == "LIVE" or (mode != "PAPER" and not trade_id.startswith(("PAPER_", "TRADE_"))):
            return None
        pnl = _number(trade.get("pnl"))
        side = str(trade.get("side") or "").upper()
        if not trade_id or pnl is None or side not in {"BUY", "SELL"}:
            return None
        row = dict(trade)
        row["id"] = trade_id
        row["side"] = side
        row["ticker"] = _text(trade.get("ticker"))
        row["pnl"] = round(pnl, 2)
        return row

    def record(self, trade: object) -> bool:
        row = self._valid_trade(trade)
        if row is None or row["id"] in self._by_id:
            return False
        self._by_id[row["id"]] = row
        while len(self._by_id) > self.max_records:
            oldest_id = next(iter(self._by_id))
            self._by_id.pop(oldest_id, None)
        return True

    def restore(self, trades: object) -> int:
        if not isinstance(trades, (list, tuple)):
            return 0
        # LPUSH journals are newest-first. Keep the newest copy of a duplicate ID,
        # then replay unique rows oldest-first so bounded eviction retains the newest.
        newest_unique: list[dict[str, Any]] = []
        seen_ids: set[str] = set()
        for trade in trades:
            row = self._valid_trade(trade)
            if row is None or row["id"] in seen_ids:
                continue
            seen_ids.add(row["id"])
            newest_unique.append(row)
        restored = 0
        for row in reversed(newest_unique):
            restored += int(self.record(row))
        return restored

    def clear(self) -> int:
        """Clear the in-memory view when the operator explicitly resets PAPER."""
        removed = len(self._by_id)
        self._by_id.clear()
        return removed

    @property
    def records(self) -> list[dict[str, Any]]:
        return list(self._by_id.values())
