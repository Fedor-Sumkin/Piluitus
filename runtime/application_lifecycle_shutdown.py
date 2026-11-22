"""Graceful draining of application services after supervised tasks stop."""
from __future__ import annotations

from typing import Any


class ApplicationLifecycleShutdownMixin:
    """Drain persistent/background services after supervisor cancellation."""

    async def _drain_application_services(self, logger: Any) -> None:
        self.trade_flow_maybe_log(force=True)
        try:
            await self.ai_engine.close()
        except Exception as close_err:
            logger.warning("Ошибка закрытия AIEngine: %s", close_err, exc_info=True)
        logger.info("🛑 Фоновые задачи остановлены; AIEngine закрыт.")
