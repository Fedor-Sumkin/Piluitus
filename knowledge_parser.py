# knowledge_parser.py
import os
import json
import logging
from datetime import datetime
from pypdf import PdfReader  # 🟢 Импортируем легковесный парсер PDF

logger = logging.getLogger("FinamCore.KnowledgeParser")
KNOWLEDGE_BASE_FILE = "ai_market_knowledge.json"

class TechnicalKnowledgeParser:
    """Модуль автоматической загрузки, парсинга (TXT/PDF) и инжектирования стратегий в базу ИИ"""

    @staticmethod
    def inject_text_strategy(source_name: str, category: str, content_text: str):
        """Записывает правила торговых систем или чанки книг в JSON базу знаний"""
        knowledge = {}
        if os.path.exists(KNOWLEDGE_BASE_FILE):
            try:
                with open(KNOWLEDGE_BASE_FILE, "r", encoding="utf-8") as f:
                    content = f.read()
                    knowledge = json.loads(content) if content else {}
            except Exception as e:
                logger.error(f"Ошибка чтения базы знаний при инжекте: {e}")

        if category not in knowledge:
            knowledge[category] = []

        # Защита от дублирования одинакового контента
        existing_rules = [item["rules"] for item in knowledge[category]]
        if content_text.strip() in existing_rules:
            return

        knowledge[category].append({
            "source": source_name,
            "injected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "rules": content_text.strip()
        })

        try:
            with open(KNOWLEDGE_BASE_FILE, "w", encoding="utf-8") as f:
                f.write(json.dumps(knowledge, indent=4, ensure_ascii=False))
            logger.info(f"📚 [ПАРСЕР] Успешно загружен блок знаний из '{source_name}' в категорию '{category}'.")
        except Exception as e:
            logger.error(f"Ошибка записи в {KNOWLEDGE_BASE_FILE}: {e}")

    @staticmethod
    def parse_and_inject_pdf(file_path: str, source_name: str, category: str = "ScalpingRules", chunk_size: int = 2000):
        """
        🟢 НОВЫЙ МЕТОД: Постранично читает PDF, режет текст на смысловые чанки 
        и отправляет в базу знаний ai_market_knowledge.json
        """
        if not os.path.exists(file_path):
            logger.error(f"❌ PDF файл не найден: {file_path}")
            return False

        try:
            reader = PdfReader(file_path)
            full_text = ""
            
            logger.info(f"📖 Начало парсинга PDF: {source_name} ({len(reader.pages)} страниц)...")
            
            # Извлекаем текст со всех страниц
            for page_num, page in enumerate(reader.pages, 1):
                text = page.extract_text()
                if text:
                    full_text += f"\n[Страница {page_num}]\n" + text

            if not full_text.strip():
                logger.warning(f"⚠️ Из PDF '{source_name}' не удалось извлечь печатный текст (возможно, это сканы-изображения).")
                return False

            # Нарезаем массивный текст книги на чанки, чтобы ИИ в промпте не захлебнулся
            words = full_text.split()
            current_chunk = []
            current_length = 0
            chunk_index = 1

            for word in words:
                current_chunk.append(word)
                current_length += len(word) + 1
                
                if current_length >= chunk_size:
                    chunk_text = " ".join(current_chunk)
                    TechnicalKnowledgeParser.inject_text_strategy(
                        source_name=f"{source_name} (Часть {chunk_index})",
                        category=category,
                        content_text=chunk_text
                    )
                    chunk_index += 1
                    current_chunk = []
                    current_length = 0

            # Записываем остаток текста
            if current_chunk:
                TechnicalKnowledgeParser.inject_text_strategy(
                    source_name=f"{source_name} (Часть {chunk_index})",
                    category=category,
                    content_text=" ".join(current_chunk)
                )

            return True
        except Exception as e:
            logger.error(f"Критическая ошибка парсинга PDF {file_path}: {e}")
            return False

    @staticmethod
    def get_knowledge_context(category: str) -> str:
        """Извлекает накопленные правила из книг для подмешивания в системный промпт Ollama"""
        if not os.path.exists(KNOWLEDGE_BASE_FILE):
            return "Специфические правила из внешних книг отсутствуют."
        try:
            with open(KNOWLEDGE_BASE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            # ИСПРАВЛЕНО: Безопасная обработка базы, если она является списком (как после history_to_rag)
            if isinstance(data, list):
                rules_list = data
            elif isinstance(data, dict):
                rules_list = data.get(category, [])
            else:
                rules_list = []
                
            if not rules_list:
                return "Внешние правила в этой категории отсутствуют."
            
            # Извлекаем текстовые правила/якоря для подмешивания в промпт
            import random
            selected_rules = random.sample(rules_list, min(2, len(rules_list)))
            
            formatted_rules = []
            for r in selected_rules:
                if isinstance(r, dict):
                    # Поддерживаем как формат книг (rules), так и формат аномалий (rag_anchor)
                    rule_text = r.get("rules") or r.get("rag_anchor") or str(r)
                    source_name = r.get("source") or r.get("ticker", "ANOMALY")
                    formatted_rules.append(f"👉 База знаний [{source_name}]: {rule_text}")
                else:
                    formatted_rules.append(f"👉 Правило: {str(r)}")
                    
            return "\n".join(formatted_rules)
        except Exception as e:
            logger.error(f"Ошибка извлечения контекста знаний: {e}")
            return "Ошибка чтения внешней базы знаний."


    @staticmethod
    def auto_scan_import_dir(directory_path: str = "import_knowledge"):
        """Автоматически сканирует папку на ПК, распознает TXT и PDF, отправляя их в базу"""
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            logger.info(f"📂 Создана папка для импорта книг/стратегий: {directory_path}")
            return

        for filename in os.listdir(directory_path):
            full_path = os.path.join(directory_path, filename)
            
            # Игнорируем уже импортированные файлы
            if filename.startswith("imported_"):
                continue

            source_name = filename.replace(".txt", "").replace(".pdf", "").replace("_", " ")

            # Обработка TXT
            if filename.endswith(".txt"):
                try:
                    with open(full_path, "r", encoding="utf-8") as f:
                        file_content = f.read()
                    if file_content.strip():
                        TechnicalKnowledgeParser.inject_text_strategy(source_name, "ScalpingRules", file_content)
                        os.rename(full_path, os.path.join(directory_path, f"imported_{filename}"))
                except Exception as e:
                    logger.error(f"Ошибка импорта TXT {filename}: {e}")

            # 🟢 Обработка PDF
            elif filename.endswith(".pdf"):
                success = TechnicalKnowledgeParser.parse_and_inject_pdf(
                    file_path=full_path,
                    source_name=source_name,
                    category="ScalpingRules"
                )
                if success:
                    os.rename(full_path, os.path.join(directory_path, f"imported_{filename}"))
