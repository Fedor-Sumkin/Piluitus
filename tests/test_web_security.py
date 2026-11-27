import base64

import pytest

from interface.web_security import PILUITUSBasicAuthMiddleware


def _scope(kind="http", authorization=None):
    headers = [] if authorization is None else [(b"authorization", authorization)]
    return {"type": kind, "headers": headers}


def _basic(user, password):
    token = base64.b64encode(f"{user}:{password}".encode()).decode()
    return f"Basic {token}".encode()


async def _collector(target, message):
    target.append(message)


@pytest.mark.asyncio
async def test_http_requires_auth_and_returns_basic_challenge():
    async def app(scope, receive, send):
        raise AssertionError("unauthorized request reached the app")

    middleware = PILUITUSBasicAuthMiddleware(app, "operator", "a-very-long-test-password")
    sent = []
    await middleware(_scope(), None, lambda message: _collector(sent, message))
    assert sent[0]["status"] == 401
    assert (b"www-authenticate", b'Basic realm="PILUITUS dashboard", charset="UTF-8"') in sent[0]["headers"]


@pytest.mark.asyncio
async def test_valid_http_credentials_reach_app():
    reached = []

    async def app(scope, receive, send):
        reached.append(True)

    middleware = PILUITUSBasicAuthMiddleware(app, "operator", "a-very-long-test-password")
    await middleware(
        _scope(authorization=_basic("operator", "a-very-long-test-password")),
        None,
        None,
    )
    assert reached == [True]


@pytest.mark.asyncio
async def test_missing_or_short_password_fails_closed_with_503():
    async def app(scope, receive, send):
        raise AssertionError("unconfigured dashboard must remain closed")

    middleware = PILUITUSBasicAuthMiddleware(app, "operator", "")
    sent = []
    await middleware(_scope(), None, lambda message: _collector(sent, message))
    assert sent[0]["status"] == 503
    assert b"PILUITUS_WEB_AUTH_PASSWORD" in sent[1]["body"]


@pytest.mark.asyncio
async def test_websocket_rejects_missing_auth_and_accepts_valid_auth():
    reached = []

    async def app(scope, receive, send):
        reached.append(True)

    middleware = PILUITUSBasicAuthMiddleware(app, "operator", "a-very-long-test-password")
    rejected = []
    await middleware(_scope("websocket"), None, lambda message: _collector(rejected, message))
    assert rejected[0]["code"] == 4401

    await middleware(
        _scope("websocket", _basic("operator", "a-very-long-test-password")),
        None,
        None,
    )
    assert reached == [True]
