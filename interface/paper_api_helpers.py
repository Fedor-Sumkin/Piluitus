"""Shared helpers for the PAPER operator API and dashboard broadcasts."""
from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException


class PaperAPIHelpers:
    """Small adapter around PAPER-mode checks and table broadcasts."""

    def __init__(self, execution_engine: Any, connection_manager: Any,
                 get_open_positions: Any, refresh_balance: Any) -> None:
        self.execution_engine = execution_engine
        self.connection_manager = connection_manager
        self.get_open_positions = get_open_positions
        self.refresh_balance = refresh_balance

    def require_paper_mode(self) -> None:
        if not getattr(self.execution_engine, "paper_mode", True):
            raise HTTPException(
                status_code=403,
                detail="Операция доступна только в PAPER_TRADING=True.",
            )

    @staticmethod
    def paper_trade_rows(snapshot: dict) -> list[dict]:
        rows = snapshot.get("paper_trades", [])
        return (
            [dict(row) for row in rows if isinstance(row, dict)]
            if isinstance(rows, list) else []
        )

    async def broadcast_paper_tables(self) -> None:
        positions = await self.get_open_positions()
        balance = await self.refresh_balance(positions)
        raw_closed = getattr(self.execution_engine, "closed_trades", [])
        await self.connection_manager.broadcast(json.dumps({
            "type": "TABLES_UPDATE",
            "active_positions": positions,
            "closed_trades": raw_closed[-30:] if isinstance(raw_closed, list) else [],
            "balance": balance,
        }, ensure_ascii=False))
