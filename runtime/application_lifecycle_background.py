"""Background worker and integration task startup for the FastAPI lifespan."""
from __future__ import annotations

from typing import Any


class ApplicationLifecycleBackgroundMixin:
    """Start worker and integration tasks in their established order."""

    async def _run_market_worker_safely(self, worker: Any, logger: Any) -> None:
        try:
            await worker.start_processing()
        except Exception as exc:
            logger.critical(
                f"❌ КРИТИЧЕСКОЕ ПАДЕНИЕ MarketWorker В ЦИКЛЕ ТОРГОВЛИ: {exc}",
                exc_info=True,
            )

    def _spawn_auxiliary_background_tasks(
        self,
        background_task_supervisor: Any,
        *,
        logger: Any,
        execution_engine: Any,
    ) -> None:
        background_task_supervisor.spawn(self.finam_metadata_refresh_loop(), name="finam-metadata-refresh")
        background_task_supervisor.spawn(self.start_news_updater(), name="news-updater")
        background_task_supervisor.spawn(self.start_tables_sync_loop(), name="tables-sync")

        from interface import tg_bot as tb
        tb.global_broker_ref = self.broker
        tb.global_execution_engine_ref = execution_engine

        if self.telegram_bot_module.bot and self.telegram_bot_module.dp:
            logger.info("🤖 Подключаем Telegram‑бот к API...")
            bot = self.telegram_bot_module.bot
            dp = self.telegram_bot_module.dp
            from interface.telegram_runtime import run_polling_with_retry

            async def _start_bot_safely() -> None:
                await run_polling_with_retry(
                    lambda: dp.start_polling(bot, skip_updates=True),
                    logger=logger,
                )

            background_task_supervisor.spawn(_start_bot_safely(), name="telegram-polling")
            logger.info("🤖 Telegram polling запланирован; доступность API проверяется в фоновой задаче.")
        else:
            logger.warning("⚠️ Токен Telegram не найден – режим управления отключён.")

        self.set_market_task(
            background_task_supervisor.spawn(self.market_broker_init(), name="broker-init-and-streams")
        )

