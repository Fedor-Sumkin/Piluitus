"""Fail-closed HTTP Basic authentication for the PILUITUS dashboard and API."""
from __future__ import annotations

import base64
import binascii
import hmac
from typing import Any, Awaitable, Callable


class PILUITUSBasicAuthMiddleware:
    """Protect HTTP routes and browser WebSocket sessions with one Basic-auth realm."""

    def __init__(self, app: Callable[..., Awaitable[Any]], username: str, password: str):
        self.app = app
        self.username = str(username or "").strip()
        self.password = str(password or "")

    def _authorized(self, scope: dict[str, Any]) -> bool:
        if len(self.password) < 16 or not self.username:
            return False
        headers = dict(scope.get("headers", []))
        raw = headers.get(b"authorization", b"")
        if not raw.startswith(b"Basic "):
            return False
        try:
            decoded = base64.b64decode(raw[6:], validate=True).decode("utf-8")
        except (binascii.Error, UnicodeDecodeError, ValueError):
            return False
        supplied_user, separator, supplied_password = decoded.partition(":")
        if not separator:
            return False
        return hmac.compare_digest(supplied_user, self.username) and hmac.compare_digest(
            supplied_password, self.password
        )

    async def __call__(self, scope, receive, send):
        if scope["type"] not in {"http", "websocket"}:
            await self.app(scope, receive, send)
            return
        if self._authorized(scope):
            await self.app(scope, receive, send)
            return

        if scope["type"] == "websocket":
            await send({"type": "websocket.close", "code": 4401, "reason": "Dashboard authentication required"})
            return

        configured = bool(self.username and len(self.password) >= 16)
        status = 401 if configured else 503
        body = (
            b'{"detail":"Authentication required"}'
            if configured
            else b'{"detail":"Set PILUITUS_WEB_AUTH_PASSWORD to a unique password of at least 16 characters in .env"}'
        )
        headers = [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode("ascii"))]
        if configured:
            headers.append((b"www-authenticate", b'Basic realm="PILUITUS dashboard", charset="UTF-8"'))
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": body})
