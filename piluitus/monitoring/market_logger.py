# market_logger.py
import os
import json
import logging
import aiofiles  # Асинхронная запись для сохранения HFT-скорости конвейера
from config import PROJECT_ROOT
from datetime import datetime, timezone

logger = logging.getLogger("FinamCore.MarketLogger")
LOG_DIR = str(PROJECT_ROOT / "live_market_logs")

class LiveMarketSnapshotLogger:
    """Система бортового регистратора (Черного Ящика) для живой записи тиков и стаканов"""
    
    @staticmethod
    def _init_dir():
        if not os.path.exists(LOG_DIR):
            os.makedirs(LOG_DIR)

    @staticmethod
    async def log_snapshot(ticker: str, price: float, stats: dict, order_book: dict | None):
        """
        Асинхронно записывает полный срез рынка в момент аномального импульса.
        Формирует идеальную структурированную базу данных для последующего обучения RAG.
        """
        LiveMarketSnapshotLogger._init_dir()
        current_date = datetime.now().strftime("%Y-%m-%d")
        log_file = f"{LOG_DIR}/market_snapshots_{current_date}.jsonl"
        
        # Формируем детальный датасет микроструктуры
        snapshot_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "ticker": ticker,
            "current_price": price,
            "screener_delta_volume": stats.get("current_volume", 0),
            "volume_multiplier": stats.get("volume_spike", "1.0x"),
            "price_deviation_std": stats.get("price_deviation", "0.0000"),
            "trigger_reason": stats.get("reason", ""),
            "order_book_snapshot": {
                "bids": order_book.get("bids", []) if order_book else [],
                "asks": order_book.get("asks", []) if order_book else []
            }
        }
        
        try:
            # Запись в формате JSON Lines (одна строка — один JSON-объект)
            async with aiofiles.open(log_file, mode='a', encoding='utf-8') as f:
                await f.write(json.dumps(snapshot_entry, ensure_ascii=False) + "\n")
        except Exception as e:
            logger.error(f"Не удалось зафиксировать срез рынка в Бортовой Журнал: {e}")
