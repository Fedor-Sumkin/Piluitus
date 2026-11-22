"""FastAPI application startup and shutdown orchestration.

This facade preserves the public runtime and lifespan contract while delegating
state restoration, background startup and service draining to focused modules.
"""
from __future__ import annotations

# Retained as a module-level compatibility attribute for older integrations.
import json
from contextlib import asynccontextmanager
from typing import Any

from .application_lifecycle_background import ApplicationLifecycleBackgroundMixin
from .application_lifecycle_base import ApplicationLifecycleBase
from .application_lifecycle_shutdown import ApplicationLifecycleShutdownMixin
from .application_lifecycle_startup import ApplicationLifecycleStartupMixin


class ApplicationLifecycleRuntime:
    """Preserve lifespan sequencing while delegating lifecycle phases."""

    # Explicit descriptor exports keep the legacy class declaration stable.
    __init__ = ApplicationLifecycleBase.__init__
    _restore_startup_state = ApplicationLifecycleStartupMixin._restore_startup_state
    _run_market_worker_safely = ApplicationLifecycleBackgroundMixin._run_market_worker_safely
    _spawn_auxiliary_background_tasks = ApplicationLifecycleBackgroundMixin._spawn_auxiliary_background_tasks
    _drain_application_services = ApplicationLifecycleShutdownMixin._drain_application_services

    @asynccontextmanager
    async def lifespan(self, app: Any):
        logger = self.logger
        execution_engine = self.execution_engine
        deposit = self.deposit
        db_manager = self.db_manager
        background_task_supervisor = self.task_supervisor_factory(logger)
        gross_exposure_pct = execution_engine.max_gross_exposure_pct * 100.0
        single_position_pct = execution_engine.max_single_position_pct * 100.0
        logger.info(
            "🚀 Запуск PILUITUS v35.2 | депозит=%.2f ₽ | общий лимит активов=%.1f%% (%.2f ₽), "
            "одна позиция=%.1f%% (%.2f ₽).",
            deposit, gross_exposure_pct, deposit * gross_exposure_pct / 100.0,
            single_position_pct, deposit * single_position_pct / 100.0,
        )

        # Startup restoration starts the recorder via self.paper_bbo_replay_recorder_provider().
        await self._restore_startup_state(
            logger=logger,
            execution_engine=execution_engine,
            deposit=deposit,
            db_manager=db_manager,
        )

        worker = self.worker_factory()
        background_task_supervisor.spawn(
            self._run_market_worker_safely(worker, logger),
            name="market-worker",
        )
        self._spawn_auxiliary_background_tasks(
            background_task_supervisor,
            logger=logger,
            execution_engine=execution_engine,
        )

        try:
            yield
        finally:
            logger.info("🛑 Начинаем штатную остановку фоновых задач PILUITUS...")
            await background_task_supervisor.cancel_and_drain()
            try:
                await self.background_operations.drain()
            except Exception as operation_drain_err:
                logger.error(
                    "[BACKGROUND OPERATIONS] shutdown drain failed: %s",
                    operation_drain_err,
                    exc_info=True,
                )
            try:
                paper_bbo_replay_recorder = self.paper_bbo_replay_recorder_provider()
                await paper_bbo_replay_recorder.close()
                logger.info("[PAPER BBO REPLAY] shutdown drain: %s", paper_bbo_replay_recorder.status())
            except Exception as replay_close_err:
                logger.error("[PAPER BBO REPLAY] shutdown drain failed: %s", replay_close_err, exc_info=True)

            await self._drain_application_services(logger)
