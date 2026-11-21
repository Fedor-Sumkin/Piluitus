"""Tests for the extracted WebSocket connection manager."""
from __future__ import annotations

import asyncio

from interface.websocket_manager import ConnectionManager


class FakeWebSocket:
    def __init__(self, *, fail_send: bool = False) -> None:
        self.accepted = False
        self.messages: list[str] = []
        self.fail_send = fail_send

    async def accept(self) -> None:
        self.accepted = True

    async def send_text(self, message: str) -> None:
        if self.fail_send:
            raise RuntimeError("simulated websocket failure")
        self.messages.append(message)


def test_connect_accepts_and_tracks_websocket() -> None:
    async def scenario() -> None:
        manager = ConnectionManager()
        websocket = FakeWebSocket()

        await manager.connect(websocket)

        assert websocket.accepted is True
        assert manager.active_connections == [websocket]

    asyncio.run(scenario())


def test_disconnect_removes_only_registered_websocket() -> None:
    manager = ConnectionManager()
    first = FakeWebSocket()
    second = FakeWebSocket()
    manager.active_connections.extend([first, second])

    manager.disconnect(first)
    manager.disconnect(first)

    assert manager.active_connections == [second]


def test_broadcast_continues_after_one_client_fails() -> None:
    async def scenario() -> None:
        manager = ConnectionManager()
        failed = FakeWebSocket(fail_send=True)
        healthy = FakeWebSocket()
        manager.active_connections.extend([failed, healthy])

        await manager.broadcast("status")

        assert healthy.messages == ["status"]
        assert manager.active_connections == [failed, healthy]

    asyncio.run(scenario())
