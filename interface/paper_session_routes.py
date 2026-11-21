"""FastAPI routes for operator-controlled PAPER session maintenance."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body


def create_paper_session_router(runtime: Any) -> APIRouter:
    """Create the three guarded PAPER maintenance endpoints."""
    router = APIRouter()

    @router.post("/api/v1/paper/session/clear")
    async def clear_paper_session(payload: dict[str, object] = Body(...)):
        return await runtime.clear_paper_session(payload)

    @router.post("/api/v1/paper/session/cleanup-stale-and-resume")
    async def cleanup_stale_and_resume_paper_session():
        return await runtime.cleanup_stale_and_resume()

    @router.post("/api/v1/paper/session/resume")
    async def resume_paper_session():
        return await runtime.resume_paper_session()

    return router
