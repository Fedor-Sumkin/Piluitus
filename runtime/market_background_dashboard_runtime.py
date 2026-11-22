"""News and dashboard synchronization tasks for the market background runtime."""
from __future__ import annotations

import asyncio
import json


class MarketBackgroundDashboardMixin:
    async def _start_news_updater(self, ) -> None:
        async def _broadcast(message: str) -> None:
            await self.manager.broadcast(message)
        await self.market_news_parser.update_news_cycle(self.db_manager, broadcast_callback=_broadcast)
    
    async def _start_tables_sync_loop(self):
        """Периодически обновляет позиции и фактическую оценку задействованного капитала."""
        self.logger.info("📊 Воркер синхронизации таблиц и капитала запущен.")
        while True:
            try:
                await asyncio.sleep(1.0)
    
                active_positions = await self.get_dashboard_open_positions()
                balance = await self.refresh_dashboard_balance_snapshot(active_positions)
                raw_closed = getattr(self.execution_engine, "closed_trades", getattr(self.execution_engine, "_closed_trades", []))
    
                payload = {
                    "type": "TABLES_UPDATE",
                    "active_positions": active_positions,
                    "closed_trades": raw_closed[-30:] if isinstance(raw_closed, list) else [],
                    "balance": balance,
                }
                await self.manager.broadcast(json.dumps(payload, ensure_ascii=False))
            except Exception as e:
                self.logger.error("❌ Ошибка воркера синхронизации таблиц/капитала: %s", e, exc_info=True)
