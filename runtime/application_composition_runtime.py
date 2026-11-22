"""Composition root for the FastAPI application and its runtime graph.

The module wires lifecycle, dashboard and PAPER route groups in their established
startup order. Domain services and trading policy remain injected from main.
"""
from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from runtime.application_lifecycle_runtime import ApplicationLifecycleRuntime
from runtime.paper_operations_runtime import PaperOperationsRuntime


class ApplicationCompositionRuntime:
    """Build the FastAPI app and register every application-facing runtime."""

    def __init__(
        self,
        *,
        lifecycle_dependencies: Mapping[str, Any],
        app_title: str,
        dashboard_application_runtime: Any,
        dashboard_route_dependencies: Mapping[str, Any],
        paper_operations_dependencies: Mapping[str, Any],
        paper_operations_callbacks: Mapping[str, Any],
        static_dir: Any,
    ) -> None:
        self.lifecycle_dependencies = dict(lifecycle_dependencies)
        self.app_title = str(app_title)
        self.dashboard_application_runtime = dashboard_application_runtime
        self.dashboard_route_dependencies = dict(dashboard_route_dependencies)
        self.paper_operations_dependencies = dict(paper_operations_dependencies)
        self.paper_operations_callbacks = dict(paper_operations_callbacks)
        self.static_dir = static_dir

        self.application_lifecycle_runtime: Any | None = None
        self.paper_operations_runtime: Any | None = None
        self.app: Any | None = None

    def _require_created(self) -> None:
        if (
            self.application_lifecycle_runtime is None
            or self.paper_operations_runtime is None
            or self.app is None
        ):
            raise RuntimeError("ApplicationCompositionRuntime.create() must run first.")

    @property
    def lifespan(self) -> Any:
        self._require_created()
        return self.application_lifecycle_runtime.lifespan

    def create(self) -> "ApplicationCompositionRuntime":
        """Create the app and mount Dashboard, PAPER and optional static routes.

        Registration order is intentional: dashboard routes are mounted first,
        PAPER research/maintenance routes second, and static files last.
        """
        if self.app is not None:
            raise RuntimeError("ApplicationCompositionRuntime.create() may only run once.")

        lifecycle_runtime = ApplicationLifecycleRuntime(
            **self.lifecycle_dependencies
        )
        app = FastAPI(title=self.app_title, lifespan=lifecycle_runtime.lifespan)

        self.dashboard_application_runtime.register_routes(
            app, **self.dashboard_route_dependencies
        )
        paper_operations_runtime = PaperOperationsRuntime(
            dependencies=self.paper_operations_dependencies,
            callbacks=self.paper_operations_callbacks,
        ).create(app)

        if self.static_dir.is_dir():
            app.mount(
                "/static",
                StaticFiles(directory=self.static_dir),
                name="static",
            )

        self.application_lifecycle_runtime = lifecycle_runtime
        self.paper_operations_runtime = paper_operations_runtime
        self.app = app
        return self
