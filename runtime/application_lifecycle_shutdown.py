"""Graceful draining of application services after supervised tasks stop."""
from __future__ import annotations

from typing import Any


class ApplicationLifecycleShutdownMixin:
    """Drain persistent/background services after supervisor cancellation."""

    async def _drain_application_services(self, logger: Any) -> None:
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

            self.trade_flow_maybe_log(force=True)
            try:
                await self.ai_engine.close()
            except Exception as close_err:
                logger.warning("Ошибка закрытия AIEngine: %s", close_err, exc_info=True)
            logger.info("🛑 Фоновые задачи остановлены; AIEngine закрыт.")
