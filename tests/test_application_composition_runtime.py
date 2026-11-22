"""Behavioral contracts for top-level FastAPI application composition."""
from __future__ import annotations

import pytest

from runtime import application_composition_runtime as module


class _FakeApp:
    def __init__(self, *, title, lifespan, calls):
        self.title = title
        self.lifespan = lifespan
        self.calls = calls

    def include_router(self, router):
        self.calls.append(("include_router", router))

    def mount(self, path, app, *, name):
        self.calls.append(("mount", path, app, name))


class _FakeLifecycle:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.lifespan = object()


class _FakeDashboard:
    def __init__(self, calls):
        self.calls = calls

    def register_routes(self, app, **kwargs):
        self.calls.append(("dashboard", app, kwargs))
        app.include_router("dashboard")


class _FakePaperOperations:
    def __init__(self, *, dependencies, callbacks, calls):
        self.dependencies = dependencies
        self.callbacks = callbacks
        self.calls = calls

    def create(self, app):
        self.calls.append(("paper_create", app))
        app.include_router("paper_research")
        app.include_router("paper_maintenance")
        return self


class _Directory:
    def __init__(self, exists):
        self.exists = exists

    def is_dir(self):
        return self.exists


def _build_runtime(monkeypatch, *, static_exists=True):
    calls = []
    lifecycle_dependencies = {"logger": object(), "deposit": 400_000.0}
    dashboard_dependencies = {"manager": object()}
    paper_dependencies = {"execution_engine": object()}
    paper_callbacks = {"rotate": lambda: None}

    monkeypatch.setattr(module, "ApplicationLifecycleRuntime", _FakeLifecycle)

    def make_app(*, title, lifespan):
        calls.append(("fastapi", title, lifespan))
        return _FakeApp(title=title, lifespan=lifespan, calls=calls)

    monkeypatch.setattr(module, "FastAPI", make_app)
    monkeypatch.setattr(
        module,
        "PaperOperationsRuntime",
        lambda *, dependencies, callbacks: _FakePaperOperations(
            dependencies=dependencies,
            callbacks=callbacks,
            calls=calls,
        ),
    )

    def make_static_files(*, directory):
        calls.append(("static_files", directory))
        return ("static_files", directory)

    monkeypatch.setattr(module, "StaticFiles", make_static_files)

    dashboard = _FakeDashboard(calls)
    runtime = module.ApplicationCompositionRuntime(
        lifecycle_dependencies=lifecycle_dependencies,
        app_title="PILUITUS test",
        dashboard_application_runtime=dashboard,
        dashboard_route_dependencies=dashboard_dependencies,
        paper_operations_dependencies=paper_dependencies,
        paper_operations_callbacks=paper_callbacks,
        static_dir=_Directory(static_exists),
    )
    return runtime, calls, lifecycle_dependencies, dashboard_dependencies, paper_dependencies, paper_callbacks


def test_application_composition_preserves_lifecycle_and_route_registration_order(monkeypatch):
    runtime, calls, lifecycle_deps, dashboard_deps, paper_deps, paper_callbacks = _build_runtime(
        monkeypatch
    )

    result = runtime.create()

    assert result is runtime
    assert runtime.application_lifecycle_runtime.kwargs == lifecycle_deps
    assert runtime.app.title == "PILUITUS test"
    assert runtime.app.lifespan is runtime.lifespan
    assert runtime.paper_operations_runtime.dependencies == paper_deps
    assert runtime.paper_operations_runtime.dependencies["execution_engine"] is paper_deps["execution_engine"]
    assert runtime.paper_operations_runtime.callbacks == paper_callbacks
    assert runtime.paper_operations_runtime.callbacks["rotate"] is paper_callbacks["rotate"]
    assert calls[0][0] == "fastapi"
    assert calls[1] == ("dashboard", runtime.app, dashboard_deps)
    assert calls[2] == ("include_router", "dashboard")
    assert calls[3] == ("paper_create", runtime.app)
    assert calls[4:6] == [
        ("include_router", "paper_research"),
        ("include_router", "paper_maintenance"),
    ]
    assert calls[6][0] == "static_files"
    assert calls[7][0:2] == ("mount", "/static")


def test_application_composition_skips_missing_static_directory(monkeypatch):
    runtime, calls, *_ = _build_runtime(monkeypatch, static_exists=False)

    runtime.create()

    assert not any(call[0] in {"static_files", "mount"} for call in calls)


def test_application_composition_rejects_duplicate_creation(monkeypatch):
    runtime, *_ = _build_runtime(monkeypatch, static_exists=False)
    runtime.create()

    with pytest.raises(RuntimeError, match="only run once"):
        runtime.create()
