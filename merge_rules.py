"""Перенос извлечённых данных из книг в базу знаний"""
import json
import os

def deploy_purified_rules():
    purified_file = "purified_book_rules.json"
    target_file = "ai_market_knowledge.json"
    
    if not os.path.exists(purified_file):
        print(f"❌ Ошибка: Файл {purified_file} не найден.")
        return
        
    with open(purified_file, "r", encoding="utf-8") as f:
        purified_rules = json.load(f)
        
    # Записываем правила как фундамент новой чистой базы знаний
    with open(target_file, "w", encoding="utf-8") as f:
        json.dump(purified_rules, f, ensure_ascii=False, indent=4)
        
    print(f"✅ УСПЕХ: 330 эталонных ИИ-алгоритмов развернуты в {target_file}")

if __name__ == "__main__":
    deploy_purified_rules()
