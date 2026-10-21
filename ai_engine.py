# ai_engine.py - Ультра-оптимизированная аппаратная сборка PILUITUS V2.2 (Ultra-Fast 7B Консилиум)
import os
import json
import logging
import random
from ollama import AsyncClient
import asyncio


logger = logging.getLogger("FinamCore.AI")
RAG_KNOWLEDGE_FILE = "ai_market_knowledge.json"

# Конфигурация пула моделей под аппаратный VRAM-бюджет RTX 5070 (12GB)
# Обе роли теперь исполняет ультра-быстрая 7B модель, гарантируя 100% нахождение слоев в VRAM!
MODEL_FAST = "qwen2.5:7b-instruct"
MODEL_JUDGE = "qwen2.5:7b-instruct" 

# Снижаем таймаут до безопасных 5.0 секунд. Если 7B модель в VRAM, она обязана ответить за 1-2 сек.
AI_TIMEOUT_SECONDS = 15.0

# Системные промпты Агентов
TACTIC_SYSTEM = "Ты Агент-ТАКТИК HFT-системы. Твоя цель: выдать тезисно аргументы ЗА вход по Price Action и объемам. Ответь кратко, без преамбул."
CRITIC_SYSTEM = "Ты Агент-КРИТИК HFT-системы. Твоя цель: выявить сухие риски угрозы капиталу. Если стакан пуст, выдай пенальти мгновенно одной фразой. Без рассуждений."

# Регламент Судьи
JUDGE_SYSTEM_PROMPT = """Ты — Агент-СУДЬЯ системы PILUITUS на Мосбирже.
Изучи аргументы ТАКТИКА и КРИТИКА и вынеси финальный вердикт.

Твой ответ обязан быть СТРОГО валидным JSON-объектом. Режим размышлений (thought/reasoning) ЗАПРЕЩЕН.
Не пиши никаких вступлений, пояснений и тегов markdown (```json). Только сырой JSON-текст.

МАТЕМАТИЧЕСКИЕ ПЕНАЛЬТИ:
- Плотность против движения в пределах 0.5%: штраф -30 или HOLD.
- Живой стакан пуст при volume_spike < 1.5x: штраф -25 пунктов.
- Сетап напоминает ошибку FAILED_MISTAKE: штраф -25 пунктов или HOLD.
- Матожидание хуже 1:1.5 (тейк к стопу): штраф -20 пунктов.

СТРУКТУРА JSON ДЛЯ ЗАПОЛНЕНИЯ (Выдавать только её):
{{
    "tactic_opinion": "Краткая выжимка Тактика.",
    "critic_opinion": "Выжимка Критика.",
    "final_decision": "BUY", "SELL" или "HOLD",
    "confidence_score": 75,
    "stop_loss": 259.10,
    "take_profit": 263.50
}}""

СТРУКТУРА ВЫХОДНОГО JSON (Заполняй только числовые и строковые значения):
{{
    "tactic_opinion": "Выжимка Тактика.",
    "critic_opinion": "Выжимка Критика и штрафы.",
    "final_decision": "BUY" или "SELL" или "HOLD",
    "confidence_score": 75, 
    "stop_loss": 259.10, 
    "take_profit": 263.50 
}}"""

def get_historical_rag_context(ticker: str) -> str:
    """Извлекает строго сжатый контекст правил для экономии токенов"""
    if not os.path.exists(RAG_KNOWLEDGE_FILE):
        return "Правила отсутствуют."
    try:
        with open(RAG_KNOWLEDGE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        reglement_rules = []
        book_patterns = []
        
        if isinstance(data, list):
            for item in data:
                if "category" in item and item.get("category") == "ScalpingRules":
                    source = item.get("source", "")
                    if "Сводный регламент" in source or "RISK_MANAGER" in item.get("rules", ""):
                        reglement_rules.append(item)
                    else:
                        book_patterns.append(item)

        lines = []
        # Берем строго 1 главное правило риска Мосбиржи
        if reglement_rules:
            lines.append(f"• РЕГЛАМЕНТ: {reglement_rules[0].get('rules', '')[:150]}")
                
        # Ротируем строго 1 случайный свечной паттерн вместо двух
        if book_patterns:
            p = random.choice(book_patterns)
            lines.append(f"• ПАТТЕРН [{p.get('source', '')[:10]}]: {p.get('rules', '')[:150]}")
                
        return "\n".join(lines)
    except Exception:
        return "Ошибка сборки RAG."


async def analyze_stock_signal(
    ticker: str, price: float, volume_spike: str, technical_reason: str, 
    order_book: dict | None = None, ticker_history: list | None = None
) -> dict:
    """ Скоростной ИИ-консилиум V2.2, адаптированный под VRAM 12GB. """
    order_book_text = f"Стакан пуст. Импульс объема: {volume_spike}."
    if order_book and (order_book.get("bids") or order_book.get("asks")):
        bids_text = ", ".join([f"{b['volume']}л по {b['price']}₽" for b in order_book.get("bids", [])[:2]])
        asks_text = ", ".join([f"{a['volume']}л по {a['price']}₽" for a in order_book.get("asks", [])[:2]])
        order_book_text = f"Bids: {bids_text} | Asks: {asks_text}"

    history_text = "История сделок пуста."
    if ticker_history:
        history_text = " | ".join([f"Трейд [{d.get('final_outcome')}]: {d.get('side')} по {d.get('entry_price')}₽" for d in ticker_history[-2:]])

    deep_historical_rag = get_historical_rag_context(ticker)

    try:
        from main import funnel
        current_vwap = funnel.get_ticker_vwap(ticker)
        vwap_text = f"VWAP: {current_vwap}₽" if current_vwap else "VWAP: расчет."
    except Exception:
        vwap_text = "VWAP: буфер."

    # Жестко обрезаем новостной балласт до 250 символов, убирая переполнение окна
    try:
        from main import MarketNewsParser
        raw_news = MarketNewsParser.get_global_sentiment_context()
        news_text = f"Новости: {raw_news[:250]}" 
    except Exception:
        news_text = "Новости: умеренно."

    # Финальный компактный снимок рынка (Строго до 800 символов текста)
    market_snapshot = (
        f"Тикер: {ticker}, Цена: {price}₽, {vwap_text}, {news_text}, Скринер: {volume_spike}, "
        f"Триггер: {technical_reason.strip()[:100]}, Стакан: {order_book_text[:200]}, RAG: {deep_historical_rag}"
    )

    try:
        # 1. АСИНХРОННЫЙ СПОР (7B) — расширяем num_ctx до 3072 для свободного дыхания модели
        tactic_task = AsyncClient().chat(
            model=MODEL_FAST,
            messages=[{'role': 'system', 'content': TACTIC_SYSTEM}, {'role': 'user', 'content': market_snapshot}],
            options={'temperature': 0.1, 'num_ctx': 3072, 'num_predict': 60}
        )
        critic_task = AsyncClient().chat(
            model=MODEL_FAST,
            messages=[{'role': 'system', 'content': CRITIC_SYSTEM}, {'role': 'user', 'content': market_snapshot}],
            options={'temperature': 0.1, 'num_ctx': 3072, 'num_predict': 60}
        )

        logger.info(f"🧠 [TIER ROUTING] Быстрый запуск Тактика и Критика (7B) по {ticker}...")
        tactic_resp, critic_resp = await asyncio.wait_for(asyncio.gather(tactic_task, critic_task), timeout=AI_TIMEOUT_SECONDS)
        
        tactic_opinion = tactic_resp['message']['content'].strip()
        critic_opinion = critic_resp['message']['content'].strip()

        # 2. СУДЕЙСТВО (7B) — также даем модели 3072 токенов контекста под JSON
        judge_prompt = f"АКТИВ: {ticker}, Цена: {price}₽\nТАКТИК: {tactic_opinion}\nКРИТИК: {critic_opinion}"

        logger.info(f"⚖️ [TIER ROUTING] Передача СУДЬЕ (Быстрая 7B модель) по {ticker}...")
        
        judge_resp = await asyncio.wait_for(
            AsyncClient().chat(
                model=MODEL_JUDGE,
                messages=[{'role': 'system', 'content': JUDGE_SYSTEM_PROMPT}, {'role': 'user', 'content': judge_prompt}],
                options={'temperature': 0.1, 'num_ctx': 3072, 'num_predict': 200}
            ),
            timeout=AI_TIMEOUT_SECONDS
        )

        raw_content = judge_resp['message']['content'].strip()
        
        if "<thought>" in raw_content:
            raw_content = raw_content.split("</thought>")[-1].strip()
            
        if "```" in raw_content:
            chunks = raw_content.split("```")
            for chunk in chunks:
                cleaned = chunk.strip()
                if cleaned.startswith("json"): cleaned = cleaned[4:].strip()
                if cleaned.startswith("{") and cleaned.endswith("}"):
                    raw_content = cleaned
                    break

        # Очистка и замена кавычек
        raw_content = raw_content.replace("{{", "{").replace("}}", "}")
        raw_content = raw_content.replace("'", '"')

        # БРОНЕБОЙНЫЙ РЕМОНТ ВИСЯЩИХ ЗАПЯТЫХ (Trailing Commas Fix)
        raw_content = raw_content.strip()
        if raw_content.startswith("{") and not raw_content.endswith("}"):
            # Если строка заканчивается на запятую (модель оборвала генерацию), срезаем её
            if raw_content.endswith(","):
                raw_content = raw_content[:-1].strip()
            raw_content += "\n}"

        # Математический вырез чистых границ JSON-объекта
        start_idx = raw_content.find("{")
        end_idx = raw_content.rfind("}")
        if start_idx != -1 and end_idx != -1:
            raw_content = raw_content[start_idx:end_idx + 1]

        # Финальная проверка: убираем висящую запятую прямо перед закрывающей скобкой, если она там осталась
        raw_content = raw_content.replace(",}", "}")
        raw_content = raw_content.replace(",\n}", "}")

        try:
            ai_data = json.loads(raw_content.strip())
        except json.JSONDecodeError as json_err:
            logger.error(f"⚠️ [JSON RECYCLER V2.2] Не удалось восстановить синтаксис 7B: {json_err}. Текст: {raw_content}")
            return {"final_decision": "HOLD", "confidence_score": 0, "status": "SYNTAX_ERROR"}


        # Локальный Python-Guard для валидации уровней
        decision = ai_data.get("final_decision", "HOLD")
        sl = ai_data.get("stop_loss")
        tp = ai_data.get("take_profit")

        if decision in ["BUY", "SELL"]:
            if not sl or not tp: 
                ai_data["final_decision"] = "HOLD"
            elif decision == "BUY" and (float(sl) >= price or float(tp) <= price): 
                ai_data["final_decision"] = "HOLD"
            elif decision == "SELL" and (float(sl) <= price or float(tp) >= price): 
                ai_data["final_decision"] = "HOLD"

        return ai_data



    except asyncio.TimeoutError:
        logger.error(f"🚨 [TOKEN BUDGET] Превышен таймаут {AI_TIMEOUT_SECONDS} сек. Сигнал по {ticker} аннулирован!")
        return {"final_decision": "HOLD", "confidence_score": 0, "status": "EXPIRED"}
    except Exception as e:
        logger.error(f"❌ Ошибка ИИ-конвейера: {e}")
        return {"final_decision": "HOLD", "confidence_score": 0, "error": str(e)}
