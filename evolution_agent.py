# evolution_agent.py - Ночной оффлайн-анализатор ошибок PILUITUS V2.2
import os
import json
import logging
import asyncio
from ollama import AsyncClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Piluitus.Evolution")

JOURNAL_FILE = "trading_journal.json"
RAG_FILE = "ai_market_knowledge.json"
MODEL_CODER = "qwen2.5-coder:14b"  # Тяжелая модель-кодер для ночного анализа

async def run_nightly_evolution():
    """
    Запускается автономно в 23:45. 
    Анализирует сделки FAILED_MISTAKE и корректирует базу ai_market_knowledge.json.
    """
    logger.info("🌙 [ЭВОЛЮЦИЯ] Запуск ночного аудита торговой сессии...")
    
    if not os.path.exists(JOURNAL_FILE) or not os.path.exists(RAG_FILE):
        logger.warning("Файлы журнала или RAG не найдены. Коррекция невозможна.")
        return

    try:
        # 1. Читаем логи сделок за прошедший день
        with open(JOURNAL_FILE, "r", encoding="utf-8") as f:
            journal = json.load(f)
        with open(RAG_FILE, "r", encoding="utf-8") as f:
            rag_database = json.load(f)
            
        if not journal:
            logger.info("Журнал сделок пуст. Нечего анализировать.")
            return

        # Отбираем только неудачные трейды для извлечения уроков
        failed_trades = [t for t in journal if t.get("final_outcome") == "FAILED_MISTAKE"]
        
        if not failed_trades:
            logger.info("🎉 Идеальный день! Убыточных сделок нет. Эволюция не требуется.")
            return

        logger.info(f"🔍 Найдено {len(failed_trades)} неудачных сделок. Запрашиваем инференс {MODEL_CODER}...")

        # 2. Вызываем qwen2.5-coder:14b для генерации контр-правила
        for trade in failed_trades:
            prompt = f"""
            Ты — Старший Алгоритмический Разработчик HFT-систем.
            Изучи убыточную сделку нашего ИИ-агента на Мосбирже и сформируй строгое защитное правило «ЕСЛИ... ТО...», чтобы заблокировать этот ложный вход в будущем.
            
            ДАННЫЕ СБОЯ:
            Инструмент: {trade.get('ticker')}
            Направление: {trade.get('side')}
            Цена входа: {trade.get('entry_price')}₽
            Причина выхода: {trade.get('exit_reason')} (PnL: {trade.get('pnl_percent')}%)
            Мнение Тактика: {trade.get('tactic_opinion')}
            Риски Критика: {trade.get('critic_opinion')}
            
            Сгенерируй ОДНО концентрированное системное правило скальпинга для RAG-базы. 
            Ответ должен содержать ТОЛЬКО текст правила, без комментариев кодера и без markdown.
            Пример: "ЕСЛИ по тикеру идет всплеск объема 7x, но в пределах 0.4% сверху стоит неразъеденный айсберг-заявка, ТО КРИТИК обязан заблокировать LONG вердиктом HOLD".
            """

            resp = await AsyncClient().chat(
                model=MODEL_CODER,
                messages=[{'role': 'user', 'content': prompt}],
                options={'temperature': 0.2, 'num_ctx': 2048}
            )
            
            new_rule_text = resp['message']['content'].strip()
            
            # 3. Имплантируем новое правило в структуру ai_market_knowledge.json
            new_rag_node = {
                "category": "ScalpingRules",
                "source": f"EVOLUTION_AGENT_{trade.get('id')}",
                "rules": new_rule_text,
                "success_count": 0,
                "fail_count": 0
            }
            rag_database.append(new_rag_node)
            logger.info(f"💾 [RAG ОБНОВЛЕН] Записано контр-правило для {trade.get('ticker')}: {new_rule_text}")

        # Сохраняем обновленную базу знаний на диск
        with open(RAG_FILE, "w", encoding="utf-8") as f:
            json.dump(rag_database, f, indent=4, ensure_ascii=False)
            
        logger.info("🏁 [ЭВОЛЮЦИЯ] Ночная самооптимизация базы знаний успешно завершена!")

    except Exception as e:
        logger.error(f"Ошибка в работе ночной эволюции: {e}")

if __name__ == "__main__":
    asyncio.run(run_nightly_evolution())
