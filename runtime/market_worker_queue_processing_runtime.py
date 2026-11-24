"""Priority queue consumption and lifecycle cleanup for MarketWorker candidates."""
from __future__ import annotations

import asyncio
import os


class MarketWorkerQueueProcessingMixin:
    """Own queue acquisition, priority commands, ACK and ticker reservation cleanup."""

    @staticmethod
    def _processing_worker_count() -> int:
        """Return a small bounded consumer count for fresh candidate throughput."""
        try:
            configured = int(os.getenv("PILUITUS_LLM_WORKERS", "2"))
        except (TypeError, ValueError, OverflowError):
            configured = 2
        return min(4, max(1, configured))

    async def _run_queue_consumer(self) -> None:
        while True:
            await self.process_next_item()

    async def start_processing(self) -> None:
        """Run bounded consumers so recent candidates are not starved in FIFO."""
        worker_count = self._processing_worker_count()
        self.logger.info(
            "🤖 ИИ‑конвейер воркеров Piluitus v35.2 запущен: queue_consumers=%d.",
            worker_count,
        )
        async with asyncio.TaskGroup() as task_group:
            for worker_index in range(worker_count):
                task_group.create_task(
                    self._run_queue_consumer(),
                    name=f"piluitus-market-worker-{worker_index + 1}",
                )

    async def process_next_item(self) -> None:
        """Process one item, delegating each market candidate phase in sequence."""
        queue_item_acquired = False
        pending_ticker = ""
        try:
            priority, _sequence_id, _timestamp_id, event_type, event_payload = (
                await self.anomaly_queue.get()
            )
            queue_item_acquired = True
            if event_type == "MARKET_TICK" and isinstance(event_payload, dict):
                pending_ticker = str(event_payload.get("ticker", "")).strip().upper()
                if (
                    getattr(self.execution_engine, "_maintenance_active", False)
                    or event_payload.get("paper_session_generation")
                    != self.session_generation_provider()
                ):
                    return
                created_at = event_payload.get("candidate_created_monotonic")
                if created_at is not None and self.candidate_is_expired(
                    created_at, ttl_sec=self.candidate_ttl_sec
                ):
                    self.trade_flow_increment("candidate_expired")
                    self.logger.info(
                        "[CANDIDATE TTL] %s side=%s expired before LLM; evidence age exceeded %.1fs.",
                        pending_ticker,
                        event_payload.get("candidate_direction", "NONE"),
                        self.candidate_ttl_sec,
                    )
                    return

            if priority == 0:
                self.logger.warning(
                    f"🚨 [ПРИОРИТЕТНЫЙ ПЕРЕХВАТ] Выполнение экстренной команды: {event_type}"
                )
                if event_type == "CMD_CLOSE_ALL":
                    await self.execution_engine.close_all_positions(db=self.db)
                elif event_type == "CMD_SET_RISK_LIMIT":
                    try:
                        self.risk_manager.daily_loss_limit = self.validate_daily_loss_limit(
                            event_payload.get("value", self.settings.DAILY_LOSS_LIMIT_PCT)
                        )
                    except ValueError as exc:
                        self.logger.error("[RISK LIMIT] Некорректное значение в очереди: %s", exc)
                return

            state = await self._prepare_market_candidate_state(event_payload)
            if state is None:
                return
            if not await self._analyze_candidate_and_prepare_entry(state):
                return
            self._build_candidate_verdict(state)
            await self._publish_candidate_verdict(state)

        except Exception as outer_exc:
            self.logger.error(
                "[MarketWorker] КРИТИЧЕСКИЙ СБОЙ КОНВЕЙЕРА: %r",
                outer_exc,
                exc_info=True,
            )

        finally:
            # ACK only items actually retrieved by this worker, exactly once.
            if queue_item_acquired:
                self.anomaly_queue.task_done()
            if pending_ticker:
                self.release_ai_ticker(pending_ticker)
            await asyncio.sleep(0.001)
