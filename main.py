"""PILUITUS process entry point: configuration and application startup only."""
from __future__ import annotations

import os

from dotenv import load_dotenv

from config import Settings
from monitoring.logging_setup import logger, terminal_status
from runtime.application_runtime_bootstrap import ApplicationRuntimeBootstrap
from runtime.application_compatibility_exports import build_application_compatibility_exports


load_dotenv()

FINAM_TOKEN = os.getenv("FINAM_TOKEN", "")
CLIENT_ID = "PILUITUS_PAPER" if Settings.PAPER_TRADING else os.getenv(
    "FINAM_CLIENT_ID", "UNKNOWN_ACCOUNT"
)
DEPOSIT = float(Settings.TRADE_DEPOSIT)

last_ai_verdicts_cache: list[dict] = []
MAX_CACHE_SIZE = 100


def _set_dashboard_balance_snapshot(snapshot: dict) -> None:
    globals().__setitem__("_dashboard_balance_snapshot", snapshot)


def _set_paper_bbo_replay_recorder(recorder) -> None:
    globals().__setitem__("paper_bbo_replay_recorder", recorder)


def _set_verdict_cache(values: list[dict]) -> None:
    globals().__setitem__("last_ai_verdicts_cache", values)


def _set_market_task(task) -> None:
    globals().__setitem__("bg_market_task", task)


def _set_paper_session_generation(generation: int) -> None:
    globals().__setitem__("_paper_session_generation", generation)


application_runtime_bootstrap = ApplicationRuntimeBootstrap(
    settings=Settings,
    finam_token=FINAM_TOKEN,
    client_id=CLIENT_ID,
    deposit=DEPOSIT,
    logger=logger,
    terminal_status=terminal_status,
    verdict_cache=last_ai_verdicts_cache,
    max_cache_size=MAX_CACHE_SIZE,
    set_balance_snapshot=_set_dashboard_balance_snapshot,
    set_bbo_replay_recorder=_set_paper_bbo_replay_recorder,
    set_verdict_cache=_set_verdict_cache,
    set_market_task=_set_market_task,
    set_session_generation=_set_paper_session_generation,
).create()

globals().update(
    build_application_compatibility_exports(application_runtime_bootstrap)
)


if __name__ == "__main__":
    import uvicorn

    try:
        uvicorn.run(app, log_config=None, host="127.0.0.1", port=8080, reload=False, workers=1)
    except KeyboardInterrupt:
        logger.info("🛑 PILUITUS остановлен оператором (Ctrl+C).")
