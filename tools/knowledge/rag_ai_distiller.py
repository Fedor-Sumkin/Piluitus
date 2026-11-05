# Bootstrap: allow running this utility from any working directory.
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

"""Извлечение сухих данных по трейдингу из книг"""

import os
import json
import asyncio
from pypdf import PdfReader
from ollama import AsyncClient

# --- НАСТРОЙКИ ПУТЕЙ ---
PDF_DIR = str(PROJECT_ROOT / "data" / "knowledge" / "sources")
OUTPUT_FILE = str(PROJECT_ROOT / "data" / "knowledge" / "purified_book_rules.json")

DISTILLER_SYSTEM_PROMPT = """
Ты — ведущий ИИ-архитектор торговой системы PILUITUS V2. Твоя задача — провести жесткую математическую дистилляцию текста из профессиональной книги по трейдингу.
Перед тобой страница текста, содержащая много "воды", мемуаров, историй из жизни трейдеров Уолл-стрит и общей философии.

ОБЯЗАННОСТИ:
1. Глубоко проанализируй текст и вычлени из него СТРОГО конкретные, применимые на практике правила скальпинга, работы со стаканом (OrderBook), расчета стоп-лоссов/тейк-профитов, управления рисками или паттерны Price Action / волатильности.
2. Полностью проигнорируй: имена авторов, названия компаний (CNBC, Goldman Sachs и т.д.), имена спортсменов, исторические события, истории из жизни, покупки Порше, личные эмоции и рассуждения о сущности денег.
3. Переведи найденную суть на сухой язык жестких алгоритмических триггеров для торговых агентов ТАКТИК и КРИТИК. Форматируй логику в виде: "ЕСЛИ [рыночное условие], ТО [действие Тактика], ИНАЧЕ [действие Критика / Штраф]".
4. Ответ верни СТРОГО в формате валидного JSON. Никакого текста до или после JSON структуры. Маркдаун-разметка (```json) запрещена.

Если на странице ЕСТЬ торговое правило или сетап, структура ответа:
{
    "has_strategy": true,
    "distilled_rules": "Сюда пиши сухой алгоритм (например: ЕСЛИ Ask-плотность разъедается на всплеске тикового объема > 1.5x, ТО Тактик генерирует BUY_TRIGGER...)"
}

Если на странице НЕТ конкретного торгового правила (только истории, биография, введение или философия), верни:
{
    "has_strategy": false,
    "distilled_rules": ""
}
"""

async def distill_text(client, text: str, source_info: str) -> dict | None:
    """Отправляет сырой текст страницы в Ollama для квант-анализа"""
    user_prompt = f"ИСТОЧНИК: {source_info}\n\nТЕКСТ СТРАНИЦЫ ДЛЯ АНАЛИЗА:\n{text}"
    
    try:
        response = await client.chat(
            model='qwen2.5:14b-instruct-q4_K_M',
            messages=[
                {'role': 'system', 'content': DISTILLER_SYSTEM_PROMPT},
                {'role': 'user', 'content': user_prompt}
            ],
            options={
                'temperature': 0.1,  # Минимальная температура для детерминированных расчетов
                'num_ctx': 4096      # Окно контекста
            }
        )
        
        raw_content = response['message']['content'].strip()
        
        # Защита от маркдаун-мусора LLM
        if "```" in raw_content:
            raw_content = raw_content.split("```")[1]
            if raw_content.startswith("json"):
                raw_content = raw_content[4:]
            raw_content = raw_content.strip()

        result = json.loads(raw_content)
        
        if result.get("has_strategy") is True and result.get("distilled_rules"):
            return {
                "category": "ScalpingRules",
                "source": source_info,
                "rules": result["distilled_rules"].strip()
            }
            
    except Exception as e:
        # Тихое логирование ошибок парсинга JSON
        pass
    return None

async def process_pdf_file(client, file_path: str):
    """Постранично читает один PDF и гонит через ИИ-конвейер"""
    file_name = os.path.basename(file_path)
    print(f"📖 Открываем книгу: {file_name}")
    
    try:
        reader = PdfReader(file_path)
        total_pages = len(reader.pages)
        print(f"   Всего страниц в файле: {total_pages}. Запуск дистилляции...")
        
        file_rules = []
        for page_num in range(total_pages):
            page = reader.pages[page_num]
            text = page.extract_text()
            
            if not text or len(text.strip()) < 50:
                continue
                
            source_info = f"Книга: {file_name} | Страница: {page_num + 1}"
            
            # Вызываем Ollama на RTX 5070
            distilled_item = await distill_text(client, text, source_info)
            
            if distilled_item:
                print(f"   🎯 [Стр. {page_num + 1}/{total_pages}] Найдено и дистиллировано торговое правило!")
                file_rules.append(distilled_item)
            else:
                # Мягкий лог фильтрации воды
                if (page_num + 1) % 50 == 0:
                    print(f"   ⏳ Обработано {page_num + 1}/{total_pages} страниц...")
                    
        return file_rules
    except Exception as e:
        print(f"💥 Ошибка при чтении файла {file_name}: {e}")
        return []

async def main():
    print("🚀 Запуск сквозного ИИ-конвейера дистилляции знаний (PDF -> Настоящие Алгоритмы)...")
    
    if not os.path.exists(PDF_DIR):
        print(f"❌ Ошибка: Указанная папка '{PDF_DIR}' не существует. Проверьте путь.")
        return
        
    pdf_files = [os.path.join(PDF_DIR, f) for f in os.listdir(PDF_DIR) if f.lower().endswith('.pdf')]
    
    if not pdf_files:
        print(f"❌ Ошибка: В папке '{PDF_DIR}' не найдено ни одного файла .pdf!")
        return
        
    print(f"Найдено PDF-книг для анализа: {len(pdf_files)}")
    
    client = AsyncClient()
    all_purified_rules = []
    
    for file_path in pdf_files:
        file_rules = await process_pdf_file(client, file_path)
        all_purified_rules.extend(file_rules)
        
    # Сохраняем финальную выжимку
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_purified_rules, f, ensure_ascii=False, indent=4)
        
    print("\n🎯 ГЛУБОКАЯ ИИ-ДИСТИЛЛЯЦИЯ УСПЕШНО ЗАВЕРШЕНА!")
    print(f"💾 Создан эталонный файл: {OUTPUT_FILE}")
    print(f"📉 Сгенерировано чистых, концентрированных HFT-алгоритмов: {len(all_purified_rules)}")

if __name__ == "__main__":
    # Защитная настройка для Windows, предотвращающая RuntimeError Event Loop
    if os.name == 'nt':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    asyncio.run(main())
