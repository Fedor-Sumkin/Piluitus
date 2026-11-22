"""FastAPI application startup and shutdown orchestration.

This module owns lifecycle sequencing only. Domain services and trading policy
remain injected from the composition root.
"""
from __future__ import annotations

import json
from contextlib import asynccontextmanager
from typing import Any, Callable


class ApplicationLifecycleRuntime:
    def __init__(
        self,
        *,
        logger: Any,
        execution_engine: Any,
        deposit: float,
        db_manager: Any,
        paper_performance_tracker: Any,
        shadow_tracker: Any,
        paper_shadow_labels: list[dict[str, Any]],
        risk_manager: Any,
        paper_bbo_replay_recorder: Any,
        restore_shadow_outcomes: Callable[[], Any],
        restore_paper_performance: Callable[[], Any],
        ai_engine: Any,
        quarantine_unreconciled_paper_positions: Callable[[], Any],
        max_cache_size: int,
        get_verdict_cache: Callable[[], list[dict[str, Any]]],
        set_verdict_cache: Callable[[list[dict[str, Any]]], None],
        worker_factory: Callable[[], Any],
        finam_metadata_refresh_loop: Callable[[], Any],
        start_news_updater: Callable[[], Any],
        start_tables_sync_loop: Callable[[], Any],
        telegram_bot_module: Any,
        broker: Any,
        market_broker_init: Callable[[], Any],
        set_market_task: Callable[[Any], None],
        background_operations: Any,
        trade_flow_maybe_log: Callable[..., Any],
        task_supervisor_factory: Callable[[Any], Any],
    ) -> None:
        self.logger = logger
        self.execution_engine = execution_engine
        self.deposit = float(deposit)
        self.db_manager = db_manager
        self.paper_performance_tracker = paper_performance_tracker
        self.shadow_tracker = shadow_tracker
        self.paper_shadow_labels = paper_shadow_labels
        self.risk_manager = risk_manager
        self.paper_bbo_replay_recorder = paper_bbo_replay_recorder
        self.restore_shadow_outcomes = restore_shadow_outcomes
        self.restore_paper_performance = restore_paper_performance
        self.ai_engine = ai_engine
        self.quarantine_unreconciled_paper_positions = quarantine_unreconciled_paper_positions
        self.max_cache_size = int(max_cache_size)
        self.get_verdict_cache = get_verdict_cache
        self.set_verdict_cache = set_verdict_cache
        self.worker_factory = worker_factory
        self.finam_metadata_refresh_loop = finam_metadata_refresh_loop
        self.start_news_updater = start_news_updater
        self.start_tables_sync_loop = start_tables_sync_loop
        self.telegram_bot_module = telegram_bot_module
        self.broker = broker
        self.market_broker_init = market_broker_init
        self.set_market_task = set_market_task
        self.background_operations = background_operations
        self.trade_flow_maybe_log = trade_flow_maybe_log
        self.task_supervisor_factory = task_supervisor_factory

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

        try:
            await db_manager.connect()
            if getattr(execution_engine, "paper_mode", True):
                try:
                    reset_result = await db_manager.clear_paper_session()
                    self.paper_performance_tracker.clear()
                    self.shadow_tracker.clear()
                    self.paper_shadow_labels.clear()
                    risk_manager = self.risk_manager
                    risk_manager._balance_initialized = False
                    risk_manager.block_trading = False
                    risk_manager._blocked_date = None
                    risk_manager.total_session_pnl = 0.0
                    risk_manager.realized_pnl = 0.0
                    risk_manager.open_pnl = 0.0
                    risk_manager.current_equity = deposit
                    risk_manager.free_margin = deposit
                    risk_manager.high_water_mark = deposit
                    risk_manager.current_drawdown_pct = 0.0
                    risk_manager._drawdown_warning_active = False
                    risk_manager._loss_streak_count = 0
                    risk_manager._loss_pause_signature = None
                    risk_manager._cached_trading_allowed = None
                    risk_manager._last_risk_snapshot_monotonic = 0.0
                    logger.warning(
                        "🧹 [PAPER SESSION START] previous PAPER execution/risk data cleared; "
                        "fresh session_id=%s; storage=%s",
                        getattr(execution_engine, "session_id", "unknown"),
                        reset_result,
                    )
                except Exception:
                    execution_engine._maintenance_active = True
                    execution_engine._maintenance_reason = (
                        "Автоматическая очистка предыдущей PAPER-сессии не завершилась. "
                        "Новые входы заблокированы до устранения причины."
                    )
                    logger.critical(
                        "⛔ [PAPER SESSION START] reset failed; trading remains paused to avoid "
                        "mixing old and new session statistics.",
                        exc_info=True,
                    )
            self.paper_bbo_replay_recorder.start(db_manager)
            logger.info(
                "[PAPER BBO REPLAY] recorder started; selected Top-N + open positions, "
                "queue_limit=%d batch_size=%d storage_limit=%d",
                self.paper_bbo_replay_recorder.queue_limit,
                self.paper_bbo_replay_recorder.batch_size,
                self.paper_bbo_replay_recorder.storage_limit,
            )
            await self.restore_shadow_outcomes()
            if not getattr(execution_engine, "paper_mode", True):
                await self.restore_paper_performance()
            maintenance_state = await db_manager.get_json("piluitus:paper:maintenance")
            if isinstance(maintenance_state, dict) and bool(maintenance_state.get("active")):
                execution_engine._maintenance_active = True
                execution_engine._maintenance_reason = str(maintenance_state.get("reason") or "PAPER-сессия приостановлена для обслуживания.")
                logger.warning("⏸️ Восстановлена сохранённая пауза PAPER-сессии. Нажмите «Возобновить PAPER» на странице обслуживания.")
        except Exception as exc:
            logger.critical(f"❌ Критическая ошибка старта БД в Lifespan: {exc}")

        try:
            from ai.knowledge_parser import TechnicalKnowledgeParser
            await TechnicalKnowledgeParser.warm_up_rag_cache(db_manager)
        except Exception as rag_start_err:
            logger.error("⚠️ Не удалось загрузить базу ai_market_knowledge.json в RAG: %s", rag_start_err, exc_info=True)

        try:
            quarantined = await self.quarantine_unreconciled_paper_positions()
            if quarantined:
                logger.warning("В старом PAPER-журнале найдено позиций для ручной проверки: %d", quarantined)
        except Exception as stale_paper_err:
            logger.error("Не удалось проверить старые PAPER-позиции: %s", stale_paper_err, exc_info=True)

        try:
            redis_inst = getattr(db_manager, "redis", getattr(db_manager, "_redis", None))
            if redis_inst is not None:
                cached_verdicts = await redis_inst.lrange("piluitus:verdicts:history", 0, self.max_cache_size - 1)
                if cached_verdicts:
                    restored_cache = [json.loads(v) for v in cached_verdicts][::-1]
                    self.set_verdict_cache(restored_cache)
                    logger.info(f"📚 Из Redis успешно восстановлено {len(restored_cache)} вердиктов.")
        except Exception as redis_restore_err:
            logger.error(f"⚠️ Не удалось восстановить историю вердиктов из Redis: {redis_restore_err}")

        worker = self.worker_factory()

        async def _run_worker_safely():
            try:
                await worker.start_processing()
            except Exception as e:
                logger.critical(f"❌ КРИТИЧЕСКОЕ ПАДЕНИЕ MarketWorker В ЦИКЛЕ ТОРГОВЛИ: {e}", exc_info=True)

        background_task_supervisor.spawn(_run_worker_safely(), name="market-worker")
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
                await self.paper_bbo_replay_recorder.close()
                logger.info("[PAPER BBO REPLAY] shutdown drain: %s", self.paper_bbo_replay_recorder.status())
            except Exception as replay_close_err:
                logger.error("[PAPER BBO REPLAY] shutdown drain failed: %s", replay_close_err, exc_info=True)

            self.trade_flow_maybe_log(force=True)
            try:
                await self.ai_engine.close()
            except Exception as close_err:
                logger.warning("Ошибка закрытия AIEngine: %s", close_err, exc_info=True)
            logger.info("🛑 Фоновые задачи остановлены; AIEngine закрыт.")
