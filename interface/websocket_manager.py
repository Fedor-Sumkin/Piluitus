"""WebSocket client lifecycle and broadcast management."""
from __future__ import annotations

import logging
from typing import Any

from fastapi import WebSocket

logger = logging.getLogger("Piluitus.Interface.WebSocket")


class ConnectionManager:
    """Track accepted WebSocket clients and broadcast text messages."""

    def __init__(self) -> None:
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket) -> None:
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket) -> None:
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            logger.info("🔌 WS‑клиент отключён")

    async def broadcast(self, message: str) -> None:
        for connection in self.active_connections:
            try:
                await connection.send_text(message)
            except Exception:
                # Preserve the existing behavior: a failed send does not
                # interrupt delivery attempts to the remaining clients.
                pass
