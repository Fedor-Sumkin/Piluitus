"""Manual PAPER resume after validating persistent session state."""
from __future__ import annotations

from typing import Any

from fastapi import HTTPException


class PaperSessionResumeMixin:
    """Perform the explicit operator-controlled PAPER resume."""

        async def resume_paper_session(self) -> dict[str, Any]:
            """Manually resume PAPER after verifying persistent state and execution locks."""
            self.require_paper_mode()
            async with self.paper_session_lock:
                engine = self.execution_engine
                if engine._opening_tickers or engine._lock_closing:
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            "Выполняется операция исполнения; PAPER пока нельзя возобновить."
                        ),
                    )
                snapshot = await self.db_manager.get_paper_session_snapshot()
                if snapshot.get("paper_stale_count", 0) > 0:
                    raise HTTPException(
                        status_code=409,
                        detail="Сначала завершите сверку всех STALE_REVIEW-позиций.",
                    )
                if (
                    snapshot.get("live_trade_count", 0)
                    or snapshot.get("live_history_count", 0)
                    or snapshot.get("live_active_order_count", 0)
                ):
                    raise HTTPException(
                        status_code=409,
                        detail=(
                            "В хранилище есть LIVE-записи. Возобновление PAPER "
                            "заблокировано до проверки хранилища."
                        ),
                    )
                engine._maintenance_active = False
                engine._maintenance_reason = ""
                await self._persist_maintenance(active=False, reason="")
                self.logger.warning("▶️ PAPER-сессия вручную возобновлена оператором.")
                await self.broadcast_paper_tables()
                return {
                    "status": "ok",
                    "maintenance_active": False,
                    "message": "PAPER-торговля возобновлена. Новые сигналы будут приниматься.",
                }
