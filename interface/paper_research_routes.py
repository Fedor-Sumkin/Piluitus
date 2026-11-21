"""FastAPI routes for PAPER research, replay and reconciliation APIs."""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Body


def create_paper_research_router(runtime: Any) -> APIRouter:
    """Bind the PAPER research runtime to its stable HTTP route contract."""
    router = APIRouter()

    @router.get("/api/v1/paper/performance")
    async def get_paper_performance():
        """Return realized PAPER trade statistics and exact-ID Shadow comparisons."""
        return runtime.get_performance()

    @router.get("/api/v1/paper/replay/batch")
    async def replay_paper_exit_batch(
        limit: int = 30,
        ticker: str | None = None,
        session_id: str | None = None,
        portfolio_capital_rub: float | None = None,
        max_allocation_pct: float = 15.0,
    ):
        return await runtime.replay_batch(
            limit=limit,
            ticker=ticker,
            session_id=session_id,
            portfolio_capital_rub=portfolio_capital_rub,
            max_allocation_pct=max_allocation_pct,
        )

    @router.get("/api/v1/paper/reconciliation/shadow")
    async def get_paper_shadow_reconciliation(limit: int = 100):
        return runtime.get_shadow_reconciliation(limit=limit)

    @router.get("/api/v1/paper/shadow/walk-forward")
    async def get_paper_shadow_walk_forward(
        folds: int = 5,
        embargo_seconds: float = 30.0,
        limit: int = 20000,
    ):
        return runtime.get_shadow_walk_forward(
            folds=folds,
            embargo_seconds=embargo_seconds,
            limit=limit,
        )

    @router.get("/api/v1/paper/shadow/cost-sensitivity")
    async def get_paper_shadow_cost_sensitivity(limit: int = 20000):
        return runtime.get_shadow_cost_sensitivity(limit=limit)

    @router.get("/api/v1/paper/research/report")
    async def get_paper_research_report(limit: int = 30):
        return await runtime.get_research_report(limit=limit)

    @router.get("/api/v1/paper/replay/{order_id}")
    async def replay_paper_exit_scenarios(order_id: str):
        return await runtime.replay_trade(order_id)

    @router.get("/api/v1/paper/reconciliation")
    async def get_paper_reconciliation():
        return await runtime.get_reconciliation()

    @router.post("/api/v1/paper/reconciliation/{order_id}")
    async def reconcile_paper_position(
        order_id: str,
        payload: dict[str, object] = Body(...),
    ):
        return await runtime.reconcile_position(order_id, payload)

    return router
