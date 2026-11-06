# knowledge_parser.py — Высокоскоростной ОЗУ RAG-Парсер стратегических знаний PILUITUS V3.0
import os
import json
import math
import logging
import random
from collections.abc import Mapping
from datetime import datetime
from pypdf import PdfReader  # Легковесный парсер PDF
from db_manager import DBManager
from config import PROJECT_ROOT

logger = logging.getLogger("Piluitus.KnowledgeParser")
KNOWLEDGE_BASE_FILE = str(PROJECT_ROOT / "data" / "knowledge" / "ai_market_knowledge.json")

class TechnicalKnowledgeParser:
    """Модуль автоматической загрузки, парсинга (TXT/PDF) и кэширования стратегий в Redis ОЗУ"""

    @staticmethod
    async def inject_text_strategy(source_name: str, category: str, content_text: str, db: DBManager):
        """Записывает правила торговых систем или чанки книг в Redis ОЗУ"""
        try:
            # Загружаем текущую RAG-базу из Redis
            rag_database = await db.load_rag_rules()
            if not rag_database:
                rag_database = []

            # Защита от дублирования контента
            existing_rules = [item.get("rules") for item in rag_database if isinstance(item, dict)]
            if content_text.strip() in existing_rules:
                return

            # Формируем новый чанк знаний под структуру v3.0
            new_node = {
                "category": category,
                "source": source_name,
                "injected_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "rules": content_text.strip(),
                "success_count": 0,
                "fail_count": 0
            }
            rag_database.append(new_node)

            # Перезаписываем в оперативную память Redis
            await db.save_rag_rules(rag_database)
            logger.info(f"📚 [RAG] Добавлен блок знаний из '{source_name}' в ОЗУ Redis.")
        except Exception as e:
            logger.error(f"Ошибка инжекции стратегии в Redis: {e}")

    @staticmethod
    async def parse_and_inject_pdf(file_path: str, source_name: str, db: DBManager, category: str = "ScalpingRules", chunk_size: int = 2000) -> bool:
        """Постранично читает PDF, режет текст на смысловые чанки и отправляет в Redis ОЗУ"""
        if not os.path.exists(file_path):
            logger.error(f"❌ PDF файл не найден: {file_path}")
            return False

        try:
            reader = PdfReader(file_path)
            full_text = ""
            logger.info(f"📖 Начало парсинга PDF: {source_name} ({len(reader.pages)} страниц)...")
            
            for page_num, page in enumerate(reader.pages, 1):
                text = page.extract_text()
                if text:
                    full_text += f"\n[Страница {page_num}]\n" + text

            if not full_text.strip():
                logger.warning(f"⚠️ Из PDF '{source_name}' не удалось извлечь текст (возможно, сканы).")
                return False

            words = full_text.split()
            current_chunk = []
            current_length = 0
            chunk_index = 1

            for word in words:
                current_chunk.append(word)
                current_length += len(word) + 1
                
                if current_length >= chunk_size:
                    chunk_text = " ".join(current_chunk)
                    await TechnicalKnowledgeParser.inject_text_strategy(
                        source_name=f"{source_name} (Часть {chunk_index})",
                        category=category,
                        content_text=chunk_text,
                        db=db
                    )
                    chunk_index += 1
                    current_chunk = []
                    current_length = 0

            if current_chunk:
                await TechnicalKnowledgeParser.inject_text_strategy(
                    source_name=f"{source_name} (Часть {chunk_index})",
                    category=category,
                    content_text=" ".join(current_chunk),
                    db=db
                )
            return True
        except Exception as e:
            logger.error(f"Критическая ошибка парсинга PDF {file_path}: {e}")
            return False

    @staticmethod
    async def warm_up_rag_cache(db: DBManager):
        """Загружает утверждённую JSON-базу как источник истины для ScalpingRules.

        Старые ScalpingRules из Redis намеренно не объединяются с новой базой:
        иначе устаревшие книжные чанки могут снова попадать в каждый промпт.
        Категории, не относящиеся к торговым правилам, сохраняются.
        """
        try:
            existing_raw = await db.load_rag_rules()
            if not isinstance(existing_raw, list):
                existing_raw = []

            static_raw: list[dict] = []
            json_record_count = 0
            rejected_static = 0
            if not os.path.isfile(KNOWLEDGE_BASE_FILE):
                logger.error("❌ Утверждённая RAG-база не найдена: %s", KNOWLEDGE_BASE_FILE)
                return

            with open(KNOWLEDGE_BASE_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if not isinstance(loaded, list):
                raise ValueError("Корневой элемент ai_market_knowledge.json должен быть списком")
            json_record_count = len(loaded)
            seen_static: set[tuple[str, str]] = set()
            for item in loaded:
                if not isinstance(item, dict):
                    rejected_static += 1
                    continue
                category = str(item.get("category", "")).strip()
                rule_text = item.get("rules")
                rule_id = str(item.get("rule_id", "")).strip()
                if category != "ScalpingRules" or not rule_id or not isinstance(rule_text, str) or not rule_text.strip():
                    rejected_static += 1
                    continue
                # Enabled is explicit; disabled knowledge can be kept in the JSON
                # as documentation, but it must never be passed to the LLM.
                prepared = dict(item)
                prepared["enabled"] = bool(item.get("enabled", False))
                fingerprint = (category.casefold(), rule_id.casefold())
                if fingerprint in seen_static:
                    rejected_static += 1
                    continue
                seen_static.add(fingerprint)
                static_raw.append(prepared)

            non_scalping_existing: list[dict] = []
            rejected_redis = 0
            for item in existing_raw:
                if not isinstance(item, dict):
                    rejected_redis += 1
                    continue
                category = str(item.get("category", "")).strip()
                rules = item.get("rules")
                if not category or category.casefold() == "scalpingrules" or not isinstance(rules, str) or not rules.strip():
                    # Drop all legacy ScalpingRules. The curated JSON is authoritative.
                    rejected_redis += 1
                    continue
                non_scalping_existing.append(item)

            merged = static_raw + non_scalping_existing
            await db.save_rag_rules(merged)
            enabled_count = sum(1 for item in static_raw if item.get("enabled"))
            disabled_count = len(static_raw) - enabled_count
            logger.info(
                "🧠 RAG готов: JSON-записей=%d, правил ScalpingRules=%d, активных=%d, отключённых из-за требований к данным=%d, отвергнуто JSON=%d, старых/непригодных записей Redis удалено=%d",
                json_record_count, len(static_raw), enabled_count, disabled_count, rejected_static, rejected_redis,
            )
        except Exception as e:
            logger.error("Критическая ошибка прогрева RAG-кэша: %s", e, exc_info=True)

    @staticmethod
    def _as_finite_float(value: object) -> float | None:
        """Безопасно преобразует JSON-значение в конечное число.

        Pylance корректно сужает тип до int/float/str перед вызовом float();
        None, bool, пустые строки, контейнеры и бесконечности отвергаются.
        """
        if value is None or isinstance(value, bool):
            return None
        if not isinstance(value, (int, float, str)):
            return None
        if isinstance(value, str) and not value.strip():
            return None
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return number if math.isfinite(number) else None

    @staticmethod
    def _conditions_match(conditions: object, market_evidence: Mapping[str, object]) -> bool:
        """Проверяет простые численные условия rule JSON без языковых догадок.

        При отсутствующем/нечисловом значении условие считается неподтверждённым.
        """
        if not isinstance(conditions, list) or not conditions:
            return False
        for condition in conditions:
            if not isinstance(condition, dict):
                return False
            metric_value: object = condition.get("metric")
            op_value: object = condition.get("op")
            metric = metric_value.strip() if isinstance(metric_value, str) else ""
            op = op_value.strip().lower() if isinstance(op_value, str) else ""
            if not metric or metric not in market_evidence:
                return False

            actual_raw: object = market_evidence.get(metric)
            expected_raw: object = condition.get("value")
            actual = TechnicalKnowledgeParser._as_finite_float(actual_raw)
            expected = TechnicalKnowledgeParser._as_finite_float(expected_raw)
            if actual is None or expected is None:
                return False

            if op == "gt" and not actual > expected:
                return False
            if op == "gte" and not actual >= expected:
                return False
            if op == "lt" and not actual < expected:
                return False
            if op == "lte" and not actual <= expected:
                return False
            if op == "eq" and not math.isclose(actual, expected, rel_tol=0.0, abs_tol=1e-9):
                return False
            if op not in {"gt", "gte", "lt", "lte", "eq"}:
                return False
        return True

    @staticmethod
    async def get_knowledge_context(
        db: DBManager,
        category: str,
        market_state: str | None = None,
        market_evidence: Mapping[str, object] | None = None,
    ) -> str:
        """Возвращает только включённые правила, условия которых подтверждены метриками.

        `market_state` оставлен только для совместимости со старым API и не
        используется для выбора правил: положительный volume_spike не означает PUMP.
        """
        try:
            rag_database = await db.load_rag_rules()
            if not isinstance(rag_database, list) or not rag_database:
                return "Нет активных правил. Направленный сигнал не подтверждён; выбрать HOLD."

            evidence: Mapping[str, object]
            if isinstance(market_evidence, Mapping):
                evidence = market_evidence
            else:
                evidence = {}
            valid_rules = [
                r for r in rag_database
                if isinstance(r, dict)
                and str(r.get("category", "")).strip() == category
                and bool(r.get("enabled", False))
                and str(r.get("rule_id", "")).strip()
            ]
            if not valid_rules:
                return "Нет активных измеримых правил. Направленный сигнал не подтверждён; выбрать HOLD."

            matched: list[str] = []
            matched_direction = False
            # Always include risk constraints first. Otherwise several matched
            # pattern rules could fill the context cap and silently drop RISK_MANAGER.
            ordered_rules = sorted(valid_rules, key=lambda item: not bool(item.get("always_include", False)))
            for rule in ordered_rules:
                always_include = bool(rule.get("always_include", False))
                conditions = rule.get("conditions")
                conditions_match = TechnicalKnowledgeParser._conditions_match(conditions, evidence)
                if not always_include and not conditions_match:
                    continue
                direction = str(rule.get("direction", "")).upper()
                if direction in {"BUY", "SELL"}:
                    matched_direction = True
                rule_text = str(rule.get("rules", "")).strip()
                if rule_text:
                    matched.append(f"[{direction or 'RULE'} / {rule.get('rule_id')}]: {rule_text}")
                if len(matched) >= 3:
                    break

            if not matched_direction:
                matched.append("[NO_DIRECTIONAL_RULE]: Ни одно направленное правило не прошло численные условия; решение только HOLD. Не выводить факты из справочного текста.")
            return "\n".join(matched)
        except Exception as e:
            logger.error("Ошибка чтения активных правил RAG: %s", e, exc_info=True)
            return "Ошибка чтения RAG; направленный сигнал не подтверждён, выбрать HOLD."

    @staticmethod
    async def auto_scan_import_dir(db: DBManager, directory_path: str = "data/knowledge/sources"):
        """Автоматически сканирует папку импорта, импортирует TXT/PDF напрямую в ОЗУ Redis"""
        if not os.path.isabs(directory_path):
            directory_path = str(PROJECT_ROOT / directory_path)
        if not os.path.exists(directory_path):
            os.makedirs(directory_path)
            logger.info(f"📂 Создана папка для импорта книг/стратегий: {directory_path}")
            return

        for filename in os.listdir(directory_path):
            full_path = os.path.join(directory_path, filename)
            if filename.startswith("imported_"):
                continue

            source_name = filename.replace(".txt", "").replace(".pdf", "").replace("_", " ")

            if filename.endswith(".txt"):
                try:
                    with open(full_path, "r", encoding="utf-8") as f:
                        file_content = f.read()
                    if file_content.strip():
                        await TechnicalKnowledgeParser.inject_text_strategy(source_name, "ScalpingRules", file_content, db)
                        os.rename(full_path, os.path.join(directory_path, f"imported_{filename}"))
                except Exception as e:
                    logger.error(f"Ошибка импорта TXT {filename}: {e}")

            elif filename.endswith(".pdf"):
                success = await TechnicalKnowledgeParser.parse_and_inject_pdf(
                    file_path=full_path,
                    source_name=source_name,
                    db=db,
                    category="ScalpingRules"
                )
                if success:
                    os.rename(full_path, os.path.join(directory_path, f"imported_{filename}"))
