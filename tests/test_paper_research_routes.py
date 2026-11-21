from __future__ import annotations

import asyncio

from fastapi.routing import APIRoute

from interface.paper_research_routes import create_paper_research_router


class RecordingPaperResearchRuntime:
    def __init__(self) -> None:
        self.calls: list[tuple[str, tuple, dict]] = []

    def _record(self, name: str, *args, **kwargs):
        self.calls.append((name, args, kwargs))
        return {"operation": name, "args": args, "kwargs": kwargs}

    def get_performance(self):
        return self._record("get_performance")

    async def replay_batch(self, **kwargs):
        return self._record("replay_batch", **kwargs)

    def get_shadow_reconciliation(self, **kwargs):
        return self._record("get_shadow_reconciliation", **kwargs)

    def get_shadow_walk_forward(self, **kwargs):
        return self._record("get_shadow_walk_forward", **kwargs)

    def get_shadow_cost_sensitivity(self, **kwargs):
        return self._record("get_shadow_cost_sensitivity", **kwargs)

    async def get_research_report(self, **kwargs):
        return self._record("get_research_report", **kwargs)

    async def replay_trade(self, *args):
        return self._record("replay_trade", *args)

    async def get_reconciliation(self):
        return self._record("get_reconciliation")

    async def reconcile_position(self, *args):
        return self._record("reconcile_position", *args)


def _route_endpoints(runtime):
    router = create_paper_research_router(runtime)
    return {
        (route.path, method): route.endpoint
        for route in router.routes
        if isinstance(route, APIRoute)
        for method in route.methods or ()
    }


def test_paper_research_router_preserves_all_nine_http_routes():
    endpoints = _route_endpoints(RecordingPaperResearchRuntime())
    assert set(endpoints) == {
        ("/api/v1/paper/performance", "GET"),
        ("/api/v1/paper/replay/batch", "GET"),
        ("/api/v1/paper/reconciliation/shadow", "GET"),
        ("/api/v1/paper/shadow/walk-forward", "GET"),
        ("/api/v1/paper/shadow/cost-sensitivity", "GET"),
        ("/api/v1/paper/research/report", "GET"),
        ("/api/v1/paper/replay/{order_id}", "GET"),
        ("/api/v1/paper/reconciliation", "GET"),
        ("/api/v1/paper/reconciliation/{order_id}", "POST"),
    }


def test_paper_research_router_forwards_replay_filters_and_defaults():
    runtime = RecordingPaperResearchRuntime()
    endpoints = _route_endpoints(runtime)

    async def scenario():
        await endpoints[("/api/v1/paper/replay/batch", "GET")]()
        await endpoints[("/api/v1/paper/replay/batch", "GET")](
            limit=12,
            ticker="SBER",
            session_id="session-1",
            portfolio_capital_rub=400000.0,
            max_allocation_pct=12.5,
        )

    asyncio.run(scenario())
    assert runtime.calls == [
        ("replay_batch", (), {
            "limit": 30, "ticker": None, "session_id": None,
            "portfolio_capital_rub": None, "max_allocation_pct": 15.0,
        }),
        ("replay_batch", (), {
            "limit": 12, "ticker": "SBER", "session_id": "session-1",
            "portfolio_capital_rub": 400000.0, "max_allocation_pct": 12.5,
        }),
    ]


def test_paper_research_router_forwards_shadow_analysis_parameters():
    runtime = RecordingPaperResearchRuntime()
    endpoints = _route_endpoints(runtime)

    async def scenario():
        await endpoints[("/api/v1/paper/reconciliation/shadow", "GET")](limit=42)
        await endpoints[("/api/v1/paper/shadow/walk-forward", "GET")](
            folds=7, embargo_seconds=45.0, limit=9000
        )
        await endpoints[("/api/v1/paper/shadow/cost-sensitivity", "GET")](limit=8000)

    asyncio.run(scenario())
    assert runtime.calls == [
        ("get_shadow_reconciliation", (), {"limit": 42}),
        ("get_shadow_walk_forward", (), {
            "folds": 7, "embargo_seconds": 45.0, "limit": 9000
        }),
        ("get_shadow_cost_sensitivity", (), {"limit": 8000}),
    ]


def test_paper_research_router_delegates_reports_and_replay_by_id():
    runtime = RecordingPaperResearchRuntime()
    endpoints = _route_endpoints(runtime)

    async def scenario():
        await endpoints[("/api/v1/paper/performance", "GET")]()
        await endpoints[("/api/v1/paper/research/report", "GET")](limit=25)
        await endpoints[("/api/v1/paper/replay/{order_id}", "GET")]("paper-123")
        await endpoints[("/api/v1/paper/reconciliation", "GET")]()

    asyncio.run(scenario())
    assert runtime.calls == [
        ("get_performance", (), {}),
        ("get_research_report", (), {"limit": 25}),
        ("replay_trade", ("paper-123",), {}),
        ("get_reconciliation", (), {}),
    ]


def test_paper_research_router_forwards_explicit_reconciliation_action():
    runtime = RecordingPaperResearchRuntime()
    endpoints = _route_endpoints(runtime)
    payload = {"action": "close", "exit_price": 101.5, "note": "operator check"}

    async def scenario():
        await endpoints[("/api/v1/paper/reconciliation/{order_id}", "POST")](
            "paper-123", payload
        )

    asyncio.run(scenario())
    assert runtime.calls == [
        ("reconcile_position", ("paper-123", payload), {})
    ]
    assert runtime.calls[0][1][1] is payload
