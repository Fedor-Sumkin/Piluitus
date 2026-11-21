from __future__ import annotations

import asyncio

from fastapi.routing import APIRoute

from interface.paper_session_routes import create_paper_session_router


class RecordingPaperSessionRuntime:
    def __init__(self):
        self.calls = []

    async def clear_paper_session(self, payload):
        self.calls.append(("clear_paper_session", payload))
        return {"operation": "clear"}

    async def cleanup_stale_and_resume(self):
        self.calls.append(("cleanup_stale_and_resume",))
        return {"operation": "cleanup"}

    async def resume_paper_session(self):
        self.calls.append(("resume_paper_session",))
        return {"operation": "resume"}


def route_endpoints(runtime):
    router = create_paper_session_router(runtime)
    return {
        (route.path, method): route.endpoint
        for route in router.routes
        if isinstance(route, APIRoute)
        for method in route.methods or ()
    }


def test_paper_session_router_preserves_all_three_http_contracts():
    endpoints = route_endpoints(RecordingPaperSessionRuntime())
    assert set(endpoints) == {
        ("/api/v1/paper/session/clear", "POST"),
        ("/api/v1/paper/session/cleanup-stale-and-resume", "POST"),
        ("/api/v1/paper/session/resume", "POST"),
    }


def test_paper_session_router_delegates_payload_and_all_operations():
    runtime = RecordingPaperSessionRuntime()
    endpoints = route_endpoints(runtime)
    payload = {"confirmation": "ОЧИСТИТЬ PAPER-СЕССИЮ"}

    async def scenario():
        clear_result = await endpoints[("/api/v1/paper/session/clear", "POST")](
            payload
        )
        cleanup_result = await endpoints[
            ("/api/v1/paper/session/cleanup-stale-and-resume", "POST")
        ]()
        resume_result = await endpoints[("/api/v1/paper/session/resume", "POST")]()
        return clear_result, cleanup_result, resume_result

    assert asyncio.run(scenario()) == (
        {"operation": "clear"},
        {"operation": "cleanup"},
        {"operation": "resume"},
    )
    assert runtime.calls == [
        ("clear_paper_session", payload),
        ("cleanup_stale_and_resume",),
        ("resume_paper_session",),
    ]
