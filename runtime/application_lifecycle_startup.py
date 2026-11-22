"""Startup persistence, recovery and state restoration phases."""
from __future__ import annotations

import json
from typing import Any


class ApplicationLifecycleStartupMixin:
    """Restore durable state before market and integration tasks are scheduled."""

    async def _restore_startup_state(
        self,
        *,
        logger: Any,
        execution_engine: Any,
        deposit: float,
        db_manager: Any,
    ) -> None:
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
            paper_bbo_replay_recorder = self.paper_bbo_replay_recorder_provider()
            paper_bbo_replay_recorder.start(db_manager)
            logger.info(
                "[PAPER BBO REPLAY] recorder started; selected Top-N + open positions, "
                "queue_limit=%d batch_size=%d storage_limit=%d",
                paper_bbo_replay_recorder.queue_limit,
                paper_bbo_replay_recorder.batch_size,
                paper_bbo_replay_recorder.storage_limit,
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

