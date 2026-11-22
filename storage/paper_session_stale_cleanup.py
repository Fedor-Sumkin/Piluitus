"""Quarantined STALE_REVIEW cleanup followed by verified PAPER resume."""
from __future__ import annotations

from typing import Any

from fastapi import HTTPException


class PaperSessionStaleCleanupMixin:
    """Delete only quarantined stale rows and resume after storage verification."""

    async def cleanup_stale_and_resume(self) -> dict[str, Any]:
        """Delete only quarantined STALE_REVIEW rows, then resume PAPER atomically."""
        self.require_paper_mode()

        async with self.paper_session_lock:
            engine = self.execution_engine
            if engine._opening_tickers or engine._lock_closing:
                raise HTTPException(
                    status_code=409,
                    detail="Есть незавершённая операция исполнения. Очистка и запуск отменены.",
                )

            snapshot_before = await self.db_manager.get_paper_session_snapshot()
            if (
                snapshot_before.get("live_trade_count", 0)
                or snapshot_before.get("live_history_count", 0)
                or snapshot_before.get("live_active_order_count", 0)
            ):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "В хранилище обнаружены LIVE-записи. Автоматический запуск "
                        "PAPER заблокирован; данные не изменены."
                    ),
                )

            engine._maintenance_active = True
            engine._maintenance_reason = (
                "PAPER приостановлен на время удаления только STALE_REVIEW-записей."
            )
            self.advance_session_generation()
            await self._persist_maintenance(
                active=True, reason=engine._maintenance_reason
            )

            # Drop queued market events so old candidates cannot run immediately
            # after resume. Priority-zero emergency commands are retained.
            self._purge_queued_market_events()
            self.ai_candidate_deduplicator.clear()

            try:
                storage_result = (
                    await self.db_manager.clear_unreconciled_paper_positions()
                )
            except RuntimeError as exc:
                raise HTTPException(status_code=409, detail=str(exc)) from exc

            removed_ids = {
                str(order_id) for order_id in storage_result.get("position_ids", [])
            }
            removed_memory_stale = sum(
                1
                for order_id in removed_ids
                if order_id in engine._unreconciled_paper_positions
            )
            for order_id in removed_ids:
                engine._unreconciled_paper_positions.pop(order_id, None)

            snapshot_after = await self.db_manager.get_paper_session_snapshot()
            if snapshot_after.get("paper_stale_count", 0) > 0:
                raise HTTPException(
                    status_code=409,
                    detail=(
                        f"Осталось STALE_REVIEW-позиций: "
                        f"{snapshot_after['paper_stale_count']}. "
                        "PAPER оставлен на паузе; проверьте журнал."
                    ),
                )
            if (
                snapshot_after.get("live_trade_count", 0)
                or snapshot_after.get("live_history_count", 0)
                or snapshot_after.get("live_active_order_count", 0)
            ):
                raise HTTPException(
                    status_code=409,
                    detail=(
                        "После очистки обнаружены LIVE-записи. "
                        "PAPER оставлен на паузе."
                    ),
                )

            # No unresolved PAPER records remain in persistent storage. Clear any
            # matching in-memory quarantine remnants but preserve current open
            # positions, closed history, verdicts and risk state.
            removed_memory_stale += len(engine._unreconciled_paper_positions)
            engine._unreconciled_paper_positions.clear()
            # Persist the resume before lifting the in-memory maintenance lock.
            # If storage fails, this process must remain paused.
            await self._persist_maintenance(active=False, reason="")
            engine._maintenance_active = False
            engine._maintenance_reason = ""

            self.reset_balance_guard_signature()
            await self.broadcast_paper_tables()
            result = {
                "status": "ok",
                "maintenance_active": False,
                "message": (
                    "STALE_REVIEW-записи удалены. PAPER-конвейер автоматически "
                    "возобновлён; закрытая история, вердикты, риск-состояние и "
                    "текущие позиции в памяти сохранены."
                ),
                "storage": storage_result,
                "memory": {"removed_stale_positions": removed_memory_stale},
                "preserved": {
                    "paper_history_rows": snapshot_after.get("paper_history_count", 0),
                    "verdict_history_rows": snapshot_after.get("verdict_history_count", 0),
                    "verdict_execution_keys": snapshot_after.get("verdict_execution_count", 0),
                    "active_memory_positions": len(engine.paper_positions),
                },
            }
            self.logger.warning(
                "[PAPER STALE CLEANUP + RESUME] deleted=%d; memory_stale=%d; PAPER resumed.",
                storage_result.get("deleted_stale_positions", 0),
                removed_memory_stale,
            )
            return result
