# Bootstrap: allow running this utility from any working directory.
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# clean_rag.py — Скрипт тотальной HFT-очистки и оптимизации RAG-базы PILUITUS v3.0
import asyncio
import json
import os
import logging
from db_manager import DBManager
from knowledge_parser import KNOWLEDGE_BASE_FILE

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("Piluitus.CleanRAG")

# Эталонный скелет высокоскоростной базы знаний скальпинга
BOEVOY_RAG_CARKAS = [
    {
        "category": "ScalpingRules",
        "source": "Сводный регламент HFT-скальпинга и управления рисками Мосбиржи",
        "rules": "Истинный Импульс (BUY_TRIGGER): Вход в LONG разрешен, если на Ask-стороне стакана происходит лавинообразное исчезновение лимитных заявок («разъедание плотности») на фоне всплеска дельты тикового объема в screener.py более чем в 1.5 раза."
    },
    {
        "category": "ScalpingRules",
        "source": "Сводный регламент HFT-скальпинга и управления рисками Мосбиржи",
        "rules": "Истинный Импульс (SELL_TRIGGER): Вход в SHORT разрешен, если на Bid-стороне стакана происходит лавинообразное исчезновение лимитных заявок на фоне всплеска дельты тикового объема в screener.py более чем в 1.5 раза."
    },
    {
        "category": "ScalpingRules",
        "source": "Сводный регламент HFT-скальпинга и управления рисками Мосбиржи",
        "rules": "Поддержка Спроса: Наличие в топ-3 Bids крупной лимитной заявки (в 5+ раз превышающей средний размер лота по тикеру) является сильным фактором удержания цены. Тактик обязан использовать её основание как опорную точку для безопасного Stop-Loss."
    },
    {
        "category": "ScalpingRules",
        "source": "Сводный регламент HFT-скальпинга и управления рисками Мосбиржи",
        "rules": "Барьер Сопротивления (LOCK_LONG): Если на траектории LONG-входа в пределах 0.5% от текущей цены в Ask-стакане стоит «Плотность» (крупный лимитный ордер, превышающий встречные Bids в 3 и более раз), Критик ОБЯЗАН выставить final_decision = \"HOLD\" или снизить confidence_score минимум на 30 пунктов. Вход в плотность до её явного разъедания запрещен."
    },
    {
        "category": "ScalpingRules",
        "source": "Сводный регламент HFT-скальпинга и управления рисками Мосбиржи",
        "rules": "Барьер Поддержки (LOCK_SHORT): Если на траектории SHORT-входа в пределах 0.5% от текущей цены в Bid-стакане стоит крупный лимитный ордер, Критик ОБЯЗАН заблокировать сделку вердиктом HOLD."
    },
    {
        "category": "ScalpingRules",
        "source": "Сводный регламент HFT-скальпинга и управления рисками Мосбиржи",
        "rules": "Математическое Пенальти по RAG-памяти: Если в блоке ticker_history присутствует недавняя сделка со статусом \"FAILED_MISTAKE\", и текущий спред цены/стакана совпадает с ней более чем на 80%, Критик ОБЯЗАН принудительно выставить confidence_score = 0 и заблокировать вход вердиктом \"HOLD\"."
    },
    {
        "category": "ScalpingRules",
        "source": "Сводный регламент HFT-скальпинга и управления рисками Мосбиржи",
        "rules": "Математический предел риска (RISK_MANAGER): Итоговый размер Stop-Loss не может быть абстрактным. Максимальный убыток на одну сделку жестко ограничен 0.5% от общего торгового депозита. Если Take-Profit меньше, чем Stop-Loss (математическое ожидание сделки < 1:1), Критик ОБЯЗАН полностью аннулировать сигнал до HOLD."
    }
]

async def execute_rag_purge():
    logger.info("⚡ Запуск тотальной очистки RAG-контура проекта PILUITUS v3.0...")
    
    # 1. Очистка и перезапись файла на диске
    try:
        # Делаем бэкап старого файла на всякий случай
        if os.path.exists(KNOWLEDGE_BASE_FILE):
            backup_name = f"backup_{KNOWLEDGE_BASE_FILE}"
            if not os.path.exists(backup_name):
                os.rename(KNOWLEDGE_BASE_FILE, backup_name)
                logger.info(f"📦 Создан бэкап старого JSON: {backup_name}")
            else:
                os.remove(KNOWLEDGE_BASE_FILE)
                
        with open(KNOWLEDGE_BASE_FILE, "w", encoding="utf-8") as f:
            json.dump(BOEVOY_RAG_CARKAS, f, ensure_ascii=False, indent=4)
        logger.info(f"💾 Боевой каркас RAG успешно записан в файл: {KNOWLEDGE_BASE_FILE}")

    except Exception as e:
        logger.error(f"❌ Не удалось перезаписать файл на диске: {e}")
        return

    # 2. Подключение к Redis и синхронизация ОЗУ-кэша
    try:
        db = DBManager() # Инициализируем синглтон-клиент Redis
        
        # Стираем старый забитый массив правил из Redis
        await db.save_rag_rules([])
        logger.info("🗑️ Старый RAG-кэш ОЗУ успешно удален из Redis.")
        
        # Инжектируем новый отфильтрованный каркас в Redis
        await db.save_rag_rules(BOEVOY_RAG_CARKAS)
        logger.info(f"🧠 Оперативная память Redis успешно прогрета! Загружено {len(BOEVOY_RAG_CARKAS)} боевых HFT-правил.")
        
        logger.info("🚀 Сжатие базы знаний завершено. Контур RAG полностью готов к High-Load нагрузкам!")
    except Exception as e:
        logger.error(f"❌ Критическая ошибка при работе с Redis ОЗУ: {e}")

if __name__ == "__main__":
    # Запускаем асинхронную сессию очистки
    asyncio.run(execute_rag_purge())
