"""Process-level startup and Uvicorn lifecycle for PILUITUS."""
from __future__ import annotations

import os
from collections.abc import Callable, Mapping, MutableMapping
from typing import Any

from dotenv import load_dotenv

from config import Settings
from monitoring.logging_setup import logger, terminal_status
from runtime.application_compatibility_exports import (
    build_application_compatibility_exports,
)
from runtime.application_runtime_bootstrap import ApplicationRuntimeBootstrap


class ApplicationProcessRuntime:
    """Own environment preparation, runtime construction and process serving.

    The module namespace is supplied by the root entry point so dynamic
    compatibility callbacks continue publishing into main rather than
    leaking state into this implementation module.
    """

    def __init__(
        self,
        *,
        module_namespace: MutableMapping[str, Any],
        settings: Any = Settings,
        logger_instance: Any = logger,
        terminal_status_instance: Any = terminal_status,
        environment: Mapping[str, str] | None = None,
        load_environment: Callable[[], Any] = load_dotenv,
        bootstrap_factory: Callable[..., Any] = ApplicationRuntimeBootstrap,
        compatibility_export_builder: Callable[[Any], dict[str, Any]] = (
            build_application_compatibility_exports
        ),
        server_runner: Callable[..., Any] | None = None,
    ) -> None:
        self.module_namespace = module_namespace
        self.settings = settings
        self.logger = logger_instance
        self.terminal_status = terminal_status_instance
        self.environment = environment if environment is not None else os.environ
        self.load_environment = load_environment
        self.bootstrap_factory = bootstrap_factory
        self.compatibility_export_builder = compatibility_export_builder
        self.server_runner = server_runner

        self.finam_token = ""
        self.client_id = "UNKNOWN_ACCOUNT"
        self.deposit = 0.0
        self.verdict_cache: list[dict[str, Any]] = []
        self.max_cache_size = 100
        self.application_runtime_bootstrap: Any | None = None
        self._exports: dict[str, Any] = {}
        self._creation_started = False
        self._created = False

    def _publish(self, name: str, value: Any) -> None:
        self.module_namespace[name] = value

    def _set_dashboard_balance_snapshot(self, snapshot: dict[str, Any]) -> None:
        self._publish("_dashboard_balance_snapshot", snapshot)

    def _set_paper_bbo_replay_recorder(self, recorder: Any) -> None:
        self._publish("paper_bbo_replay_recorder", recorder)

    def _set_verdict_cache(self, values: list[dict[str, Any]]) -> None:
        self._publish("last_ai_verdicts_cache", values)

    def _set_market_task(self, task: Any) -> None:
        self._publish("bg_market_task", task)

    def _set_paper_session_generation(self, generation: int) -> None:
        self._publish("_paper_session_generation", generation)

    def create(self) -> "ApplicationProcessRuntime":
        """Prepare process configuration and construct the complete runtime once."""
        if self._creation_started:
            raise RuntimeError("ApplicationProcessRuntime.create() may only run once.")
        self._creation_started = True

        # config.py already loads the project .env. Preserve main.py's
        # additional default dotenv discovery before reading process values.
        self.load_environment()
        self.finam_token = str(self.environment.get("FINAM_TOKEN", "") or "")
        self.client_id = (
            "PILUITUS_PAPER"
            if self.settings.PAPER_TRADING
            else str(self.environment.get("FINAM_CLIENT_ID", "UNKNOWN_ACCOUNT"))
        )
        self.deposit = float(self.settings.TRADE_DEPOSIT)
        self.verdict_cache = []

        bootstrap = self.bootstrap_factory(
            settings=self.settings,
            finam_token=self.finam_token,
            client_id=self.client_id,
            deposit=self.deposit,
            logger=self.logger,
            terminal_status=self.terminal_status,
            environment=self.environment,
            verdict_cache=self.verdict_cache,
            max_cache_size=self.max_cache_size,
            set_balance_snapshot=self._set_dashboard_balance_snapshot,
            set_bbo_replay_recorder=self._set_paper_bbo_replay_recorder,
            set_verdict_cache=self._set_verdict_cache,
            set_market_task=self._set_market_task,
            set_session_generation=self._set_paper_session_generation,
        ).create()
        self.application_runtime_bootstrap = bootstrap

        self._exports = {
            # Preserve imports that existed on the historical main module.
            "os": os,
            "load_dotenv": load_dotenv,
            "Settings": self.settings,
            "ApplicationRuntimeBootstrap": ApplicationRuntimeBootstrap,
            "build_application_compatibility_exports": build_application_compatibility_exports,
            "logger": self.logger,
            "terminal_status": self.terminal_status,
            "FINAM_TOKEN": self.finam_token,
            "CLIENT_ID": self.client_id,
            "DEPOSIT": self.deposit,
            "last_ai_verdicts_cache": self.verdict_cache,
            "MAX_CACHE_SIZE": self.max_cache_size,
            "_set_dashboard_balance_snapshot": self._set_dashboard_balance_snapshot,
            "_set_paper_bbo_replay_recorder": self._set_paper_bbo_replay_recorder,
            "_set_verdict_cache": self._set_verdict_cache,
            "_set_market_task": self._set_market_task,
            "_set_paper_session_generation": self._set_paper_session_generation,
            "_application_process_runtime": self,
        }
        # The bootstrap-owned setters intentionally override the initial
        # namespace publishers for recorder/cache/task updates, as before.
        self._exports.update(self.compatibility_export_builder(bootstrap))
        self._created = True
        return self

    def compatibility_exports(self) -> dict[str, Any]:
        """Return the names to publish into the root main module."""
        if not self._created:
            raise RuntimeError("ApplicationProcessRuntime.create() must run first.")
        return dict(self._exports)

    def run(self) -> None:
        """Run the same local Uvicorn process configuration as the old entry point."""
        if not self._created:
            raise RuntimeError("ApplicationProcessRuntime.create() must run first.")

        runner = self.server_runner
        if runner is None:
            import uvicorn

            runner = uvicorn.run

        try:
            runner(
                self.module_namespace["app"],
                log_config=None,
                host="127.0.0.1",
                port=8080,
                reload=False,
                workers=1,
            )
        except KeyboardInterrupt:
            self.logger.info("🛑 PILUITUS остановлен оператором (Ctrl+C).")
