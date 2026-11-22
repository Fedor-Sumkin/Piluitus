"""Behavioral tests for process startup and serving ownership."""
from __future__ import annotations

from types import SimpleNamespace

import pytest

from runtime.application_process_runtime import ApplicationProcessRuntime


class _Logger:
    def __init__(self):
        self.messages = []

    def info(self, message):
        self.messages.append(message)


class _Bootstrap:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.created = False
        self.recorder = None
        self.market_task = None
        self.verdict_cache = kwargs["verdict_cache"]

    def create(self):
        self.created = True
        return self

    def set_bbo_replay_recorder(self, value):
        self.recorder = value

    def set_market_task(self, value):
        self.market_task = value

    def set_verdict_cache(self, value):
        self.verdict_cache = value


def _build_runtime(*, settings=None, environment=None, runner=None):
    events = []
    namespace = {}
    logger = _Logger()
    settings = settings or SimpleNamespace(PAPER_TRADING=True, TRADE_DEPOSIT=400_000.0)
    environment = environment or {"FINAM_TOKEN": "test-token", "FINAM_CLIENT_ID": "live-id"}
    bootstraps = []

    def bootstrap_factory(**kwargs):
        bootstrap = _Bootstrap(**kwargs)
        bootstraps.append(bootstrap)
        return bootstrap

    app = object()

    def export_builder(bootstrap):
        return {
            "application_runtime_bootstrap": bootstrap,
            "app": app,
            "last_ai_verdicts_cache": bootstrap.verdict_cache,
            "_set_paper_bbo_replay_recorder": bootstrap.set_bbo_replay_recorder,
            "_set_market_task": bootstrap.set_market_task,
            "_set_verdict_cache": bootstrap.set_verdict_cache,
        }

    runtime = ApplicationProcessRuntime(
        module_namespace=namespace,
        settings=settings,
        logger_instance=logger,
        terminal_status_instance=object(),
        environment=environment,
        load_environment=lambda: events.append("dotenv"),
        bootstrap_factory=bootstrap_factory,
        compatibility_export_builder=export_builder,
        server_runner=runner,
    )
    return runtime, namespace, events, logger, bootstraps, app


def test_process_runtime_builds_application_and_preserves_main_namespace_callbacks():
    runtime, namespace, events, logger, bootstraps, app = _build_runtime()
    assert runtime.create() is runtime
    bootstrap = bootstraps[0]
    assert events == ["dotenv"]
    assert bootstrap.created
    assert bootstrap.kwargs["settings"] is runtime.settings
    assert bootstrap.kwargs["finam_token"] == "test-token"
    assert bootstrap.kwargs["client_id"] == "PILUITUS_PAPER"
    assert bootstrap.kwargs["deposit"] == 400_000.0
    assert bootstrap.kwargs["environment"] is runtime.environment
    assert bootstrap.kwargs["logger"] is logger
    assert bootstrap.kwargs["verdict_cache"] is runtime.verdict_cache
    assert bootstrap.kwargs["max_cache_size"] == 100

    snapshot = {"equity": 410_000}
    recorder = object()
    cache = [{"ticker": "SBER"}]
    market_task = object()
    generation = 3
    bootstrap.kwargs["set_balance_snapshot"](snapshot)
    bootstrap.kwargs["set_bbo_replay_recorder"](recorder)
    bootstrap.kwargs["set_verdict_cache"](cache)
    bootstrap.kwargs["set_market_task"](market_task)
    bootstrap.kwargs["set_session_generation"](generation)
    assert namespace["_dashboard_balance_snapshot"] is snapshot
    assert namespace["paper_bbo_replay_recorder"] is recorder
    assert namespace["last_ai_verdicts_cache"] is cache
    assert namespace["bg_market_task"] is market_task
    assert namespace["_paper_session_generation"] == generation

    exports = runtime.compatibility_exports()
    assert exports["app"] is app
    assert exports["Settings"] is runtime.settings
    assert exports["FINAM_TOKEN"] == "test-token"
    assert exports["CLIENT_ID"] == "PILUITUS_PAPER"
    assert exports["DEPOSIT"] == 400_000.0
    assert exports["_application_process_runtime"] is runtime
    assert exports["_set_paper_bbo_replay_recorder"] == bootstrap.set_bbo_replay_recorder
    with pytest.raises(RuntimeError, match="only run once"):
        runtime.create()


def test_process_runtime_uses_live_client_id_and_runs_uvicorn_with_legacy_options():
    calls = []
    runner = lambda *args, **kwargs: calls.append((args, kwargs))
    settings = SimpleNamespace(PAPER_TRADING=False, TRADE_DEPOSIT=250_000.0)
    runtime, _, _, _, bootstraps, app = _build_runtime(
        settings=settings,
        environment={"FINAM_TOKEN": "", "FINAM_CLIENT_ID": "LIVE-ACCOUNT"},
        runner=runner,
    )
    with pytest.raises(RuntimeError, match="must run first"):
        runtime.run()
    runtime.create()
    assert runtime.client_id == "LIVE-ACCOUNT"
    assert bootstraps[0].kwargs["client_id"] == "LIVE-ACCOUNT"
    runtime.run()
    assert calls == [(
        (app,),
        {
            "log_config": None,
            "host": "127.0.0.1",
            "port": 8080,
            "reload": False,
            "workers": 1,
        },
    )]


def test_process_runtime_logs_operator_keyboard_interrupt():
    def stop_server(*args, **kwargs):
        raise KeyboardInterrupt

    runtime, _, _, logger, _, _ = _build_runtime(runner=stop_server)
    runtime.create()
    runtime.run()
    assert logger.messages == ["🛑 PILUITUS остановлен оператором (Ctrl+C)."]


def test_process_runtime_requires_creation_before_exports_and_rejects_repeat_creation():
    runtime, _, _, _, _, _ = _build_runtime()
    with pytest.raises(RuntimeError, match="must run first"):
        runtime.compatibility_exports()
    runtime.create()
    with pytest.raises(RuntimeError, match="only run once"):
        runtime.create()
