# Bootstrap: allow running this utility from any working directory.
import sys
from pathlib import Path
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import json
import os

# --- НАСТРОЙКИ ПУТЕЙ ---
TXT_SOURCE = str(PROJECT_ROOT / "data" / "knowledge" / "sources" / "imported_OrderBook_and_Risk_Rules.txt")
RAG_TARGET = str(PROJECT_ROOT / "ai_market_knowledge.json")

def import_txt_reglement():
    print("🔄 Запуск системного импортера регламентов из TXT...")
    
    if not os.path.exists(TXT_SOURCE):
        print(f"❌ Ошибка: Исходный текстовый файл не найден по пути:\n{TXT_SOURCE}")
        return
        
    if not os.path.exists(RAG_TARGET):
        print(f"❌ Ошибка: Боевая база знаний {RAG_TARGET} не найдена в корне.")
        return

    # 1. Читаем сырой текст регламента с автоматическим определением кодировки (utf-8 / cp1251)
    try:
        with open(TXT_SOURCE, "r", encoding="utf-8") as f:
            raw_text = f.read().strip()
    except UnicodeDecodeError:
        with open(TXT_SOURCE, "r", encoding="cp1251") as f:
            raw_text = f.read().strip()

    if not raw_text:
        print("⚠️ Предупреждение: Файл регламента пуст.")
        return

    # 2. Интеллектуальное разделение текста на логические блоки для RAG-контекста
    # Чтобы Qwen 2.5 14B не путал обязанности агентов, мы разделим текст по ключевым заголовкам
    reglement_blocks = []
    
    # Ищем логические разделы
    sections = raw_text.split("\n\n")
    current_source = "Сводный регламент HFT-скальпинга и управления рисками Мосбиржи"
    
    current_block_text = []
    
    for line in raw_text.split("\n"):
        line_str = line.strip()
        if not line_str:
            continue
            
        # Если встречаем новый раздел, фиксируем предыдущий
        if "ФУНКЦИОНАЛ" in line_str or "ПРАВИЛА И СТРОГИЕ ОГРАНИЧЕНИЯ" in line_str:
            if current_block_text:
                reglement_blocks.append({
                    "category": "ScalpingRules",
                    "source": current_source,
                    "rules": "\n".join(current_block_text).strip()
                })
                current_block_text = []
        current_block_text.append(line_str)
        
    # Дозаписываем финальный блок (Риск-менеджер)
    if current_block_text:
        reglement_blocks.append({
            "category": "ScalpingRules",
            "source": current_source,
            "rules": "\n".join(current_block_text).strip()
        })

    print(f"📦 Текст успешно структурирован на {len(reglement_blocks)} приоритетных ИИ-правила.")

    # 3. Читаем боевой JSON-массив с аномалиями Мосбиржи
    try:
        with open(RAG_TARGET, "r", encoding="utf-8") as f:
            existing_data = json.load(f)
            if not isinstance(existing_data, list):
                existing_data = [existing_data]
    except Exception as e:
        print(f"💥 Ошибка чтения файла базы знаний {RAG_TARGET}: {e}")
        return

    # Избегаем дублирования: убираем старые записи этого регламента, если они уже были в базе
    clean_existing_data = [
        item for item in existing_data 
        if item.get("source") != current_source
    ]

    # 4. Слияние: Регламент Мосбиржи ложится на самый верх JSON (наивысший приоритет выборки)
    final_combined_rag = reglement_blocks + clean_existing_data

    # Записываем обратно на диск
    with open(RAG_TARGET, "w", encoding="utf-8") as f:
        json.dump(final_combined_rag, f, ensure_ascii=False, indent=4)

    print(f"\n✅ УСПЕХ: Файл '{os.path.basename(TXT_SOURCE)}' успешно интегрирован в базу знаний!")
    print(f"📈 Итоговое количество объектов в RAG: {len(final_combined_rag)}")

if __name__ == "__main__":
    import_txt_reglement()
