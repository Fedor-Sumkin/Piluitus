# ----------------------------------------------------------------------
# news_parser.py — Высокоскоростной ОЗУ‑конвейер новостного сентимента
# ----------------------------------------------------------------------
import asyncio
import logging
import json
import httpx
import re
import xml.etree.ElementTree as ET
from datetime import datetime
from db_manager import DBManager
from config import Settings, RSS_FEEDS as CONFIG_RSS_FEEDS
from typing import cast, Any

logger = logging.getLogger("Piluitus.NewsParser")

# ----------------------------------------------------------------------
# Компактный JSON‑промпт для LLM; рассчитан на контекст 2048 токенов
# ----------------------------------------------------------------------
NEWS_SYSTEM_PROMPT = """
Ты фильтруешь короткие официальные новости MOEX. Верни только один JSON-объект вида:
{"news":[{"title":"...","ticker":"SBER","sentiment":"POSITIVE|NEGATIVE|NEUTRAL","impact_reason":"одно короткое предложение"}]}
Если в новостях нет конкретного корпоративного или макроэкономического события, верни {"news":[]}.
Не добавляй <think>, Markdown, пояснения или текст вне JSON. Не придумывай тикеры и факты.
"""

class MarketNewsParser:
    RSS_FEEDS = CONFIG_RSS_FEEDS

    @staticmethod
    def _clean_and_repair_json(raw_text: str) -> list:
        """
        Внутренний глубокий чистильщик и супер-отказоустойчивый Regex-восстановитель.
        Гарантирует сбор объектов даже при тотальном отсутствии структуры массива [.
        """
        # Ответ v34 не должен содержать reasoning-теги. Если сервер всё же добавил
        # закрытый think-блок, удаляем только его, не уничтожая JSON после него.
        cleaned = re.sub(r"<think>.*?</think>", "", raw_text, flags=re.DOTALL | re.IGNORECASE).strip()
        cleaned = re.sub(r"^\s*<think>.*?\n", "", cleaned, count=1, flags=re.DOTALL | re.IGNORECASE)

        # 1. Очистка Markdown-разметки
        if cleaned.startswith("```"):
            parts = cleaned.split("```")
            for part in parts:
                part = part.strip()
                if not part:
                    continue
                if part.startswith("json"):
                    cleaned = part[4:].strip()
                elif part.startswith("["):
                    cleaned = part
                break
        cleaned = cleaned.strip()

        # 3. Тотальная замена инвалидных кавычек
        cleaned = cleaned.replace("'", '"').replace("`", '"').replace("确实е", "истинно")
        
        # 4. Контур исправления незавершенных строк (Unterminated string) перед разделителями полей
        cleaned = re.sub(r'":\s*"([^"\n]*?)(?=\s*[,}\n])', r'": "\1"', cleaned)

        # Сначала разбираем штатный v34-формат {"news": [...]} или старый массив.
        try:
            parsed_json = json.loads(cleaned)
            if isinstance(parsed_json, list):
                return parsed_json
            if isinstance(parsed_json, dict) and isinstance(parsed_json.get("news"), list):
                return parsed_json["news"]
        except json.JSONDecodeError:
            pass

        # Контур №1: Попытка забрать штатный массив [ ... ]
        array_match = re.search(r"(\[.*\])", cleaned, re.DOTALL)
        if array_match:
            try:
                return json.loads(array_match.group(1).strip())
            except json.JSONDecodeError:
                cleaned = array_match.group(1).strip()

        # Контур №2: Аварийный атомарный сбор объектов из сырых фигурных скобок {...}
        # Спасает логику, если LLM выдала объекты без обрамляющих квадратных скобок
        objects = re.findall(r"\{(.*?)\}", cleaned, re.DOTALL)
        if not objects:
            logger.warning("⚠️ В ответе LLM не обнаружено ни одной структуры {} или [].")
            return []

        fallback_list = []
        for obj in objects:
            item = {}
            for key in ["title", "ticker", "sentiment", "impact_reason"]:
                match = re.search(r'"' + re.escape(key) + r'"\s*:\s*["\']?(.*?)["\']?(?=\s*[,}\n])', obj, re.DOTALL)
                if match:
                    val = match.group(1).strip().strip('"').strip("'")
                    item[key] = val
            
            # Защитный фильтр: пропускаем поврежденные фрагменты без ключевых биржевых тегов
            if item.get("ticker") and item.get("sentiment"):
                fallback_list.append(item)

        return fallback_list

    @staticmethod
    def _regex_fallback_parser(raw_xml_text: str) -> list:
        items_extracted = []
        items = re.findall(r'<item>(.*?)</item>', raw_xml_text, re.DOTALL | re.IGNORECASE)

        for item in items[:3]:
            title_match = re.search(r'<title>(.*?)</title>', item, re.DOTALL | re.IGNORECASE)
            desc_match  = re.search(r'<(?:description|summary)>(.*?)</(?:description|summary)>',
                                    item, re.DOTALL | re.IGNORECASE)

            title = title_match.group(1).strip() if title_match else ""
            desc  = desc_match.group(1).strip() if desc_match else ""

            title = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', title, flags=re.DOTALL)
            title = re.sub(r'<[^<]+?>', '', title).strip()
            desc  = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', desc, flags=re.DOTALL)
            desc  = re.sub(r'<[^<]+?>', '', desc).strip()

            if title:
                items_extracted.append((title, desc))
        return items_extracted

    @staticmethod
    async def _fetch_single_feed(client: httpx.AsyncClient, url: str) -> list:
        feed_collected = []
        try:
            response = await client.get(url, timeout=8.0)
            if response.status_code != 200:
                return []

            raw_items = MarketNewsParser._regex_fallback_parser(response.text)

            if not raw_items:
                root = ET.fromstring(response.content)
                items = root.findall(".//item") or root.findall(".//{*}item")
                for item in items[:3]:
                    title_node = item.find("title") or item.find("{*}title")
                    desc_node  = (
                        item.find("description")
                        or item.find("{*}description")
                        or item.find("{*}summary")
                    )
                    title = (title_node.text.strip() if title_node is not None and title_node.text else "")
                    desc  = (desc_node.text.strip() if desc_node is not None and desc_node.text else "")
                    if title:
                        raw_items.append((title, desc))

            for title, desc in raw_items:
                feed_name = url.split('/')[-1].split('?')[0] or "news"
                feed_collected.append(
                    f"📡 Фид: {feed_name} | Заголовок: {title} | Описание: {desc[:1000]}"
                )
        except Exception as exc:
            logger.debug(f"RSS‑feed {url} пропущен: {exc}")
        return feed_collected

    @staticmethod
    async def fetch_raw_rss_http2() -> list:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "application/xml,text/xml,application/xhtml+xml",
        }

        async with httpx.AsyncClient(http2=True, headers=headers, follow_redirects=True) as client:
            tasks = [MarketNewsParser._fetch_single_feed(client, url) for url in MarketNewsParser.RSS_FEEDS]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            collected = []
            for res in results:
                if isinstance(res, list):
                    collected.extend(res)
            return collected
    # ------------------------------------------------------------------
    # 4️⃣  Фильтрация новостей через LLM (Исправленный безопасный контур)
    # ------------------------------------------------------------------
    @staticmethod
    async def filter_news_via_llm(raw_news_list: list) -> list:
        if not raw_news_list:
            return []

        # Short input for the 2048-token vLLM context: no full RSS descriptions.
        truncated_news = [re.sub(r"\s+", " ", str(item)).strip()[:420] for item in raw_news_list[:4]]
        user_prompt = (
            "Отфильтруй новости. Для каждой существенной новости укажи подтверждённый тикер,"
            " sentiment и одну короткую причину. Входные новости:\n"
            + "\n".join(truncated_news)
        )

        endpoint = Settings.VLLM_ENDPOINT.rstrip("/")
        if not endpoint.endswith("/v1/chat/completions") and not endpoint.endswith("/v1/completions"):
            vllm_url = f"{endpoint}/v1/chat/completions"
        else:
            vllm_url = endpoint

        model_name = Settings.VLLM_MODEL
        vllm_timeout = float(getattr(Settings, "VLLM_TIMEOUT", 45.0))

        # Настройка отказоустойчивого контура повторных попыток (Retry Policy)
        max_retries = 3
        retry_delay = 2.0
        result_json = None

        for attempt in range(max_retries):
            try:
                async with httpx.AsyncClient(timeout=vllm_timeout) as client:   
                    resp = await client.post(
                        vllm_url,
                        json={
                            "model": model_name,
                            "messages": [
                                {"role": "system", "content": NEWS_SYSTEM_PROMPT},
                                {"role": "user", "content": user_prompt},
                            ],
                            "response_format": {"type": "json_object"},
                            "temperature": 0.1,
                            "max_tokens": 320,
                        },
                    )
                    if resp.status_code in (400, 422) and "response_format" in resp.request.content.decode("utf-8", errors="ignore"):
                        detail = re.sub(r"\s+", " ", resp.text).strip()[:600]
                        if "response_format" in detail.lower() or "json_object" in detail.lower() or "structured" in detail.lower():
                            retry_body = {
                                "model": model_name,
                                "messages": [
                                    {"role": "system", "content": NEWS_SYSTEM_PROMPT},
                                    {"role": "user", "content": user_prompt},
                                ],
                                "temperature": 0.1,
                                "max_tokens": 320,
                            }
                            resp = await client.post(vllm_url, json=retry_body)
                    if resp.status_code == 200:
                        result_json = resp.json()
                        break

                    logger.error(f"❌ VLLM-news вернул статус {resp.status_code}: {resp.text[:1200]}")
                    return []
                    
            except (httpx.ConnectError, httpx.HTTPError) as exc:
                if attempt < max_retries - 1:
                    logger.warning(f"⏳ [NEWS RETRY] vLLM моно-поток занят скальпингом. Попытка {attempt+1}/{max_retries} через {retry_delay}с...")
                    await asyncio.sleep(retry_delay)
                    retry_delay *= 1.5  # Экспоненциальный рост паузы
                else:
                    logger.error(f"❌ Все {max_retries} попыток подключения к vLLM‑news провалены: {exc!r}")
                    return []

        if not result_json or not isinstance(result_json, dict):
            return []

        choices = result_json.get("choices")
        if not choices or not isinstance(choices, list) or len(choices) == 0:
            logger.error("❌ Choices пуст или имеет неверную структуру.")
            return []

        # Строгое извлечение первого элемента из списка
        first_choice = choices[0]
        raw_content = ""
        if isinstance(first_choice, dict):
            raw_content = first_choice.get("message", {}).get("content", "")
        
        # Не добавляем искусственный <think>: старый очиститель мог удалить весь JSON,
        # если у модели не было закрывающего </think>.

        # Вызов глубокого Regex-очистителя и контура восстановления поврежденных строк JSON
        try:
            filtered_news = MarketNewsParser._clean_and_repair_json(raw_content)
        except Exception as exc:
            logger.error(f"❌ Не удалось распарсить JSON от LLM‑news после всех попыток исправления: {exc}")
            logger.debug(f"Сырой проблемный контент LLM: {raw_content}")
            return []

        valid_news_list = []
        
        if isinstance(filtered_news, dict):
            filtered_news = [filtered_news]

        if isinstance(filtered_news, list):
            for item in filtered_news:
                if isinstance(item, dict):
                    # Принудительное приведение типа через cast для тотального подавления ошибок slice
                    dict_item = cast(dict[str, Any], item)
                    dict_item["time"] = datetime.now().strftime("%H:%M:%S")
                    valid_news_list.append(dict_item)

        return valid_news_list

    @staticmethod
    async def update_news_cycle(db: DBManager, broadcast_callback=None):
        logger.info("📰 Запуск фонового цикла парсинга новостей Мосбиржи...")
        while True:
            try:
                raw_articles = await MarketNewsParser.fetch_raw_rss_http2()
                if raw_articles:
                    filtered = await MarketNewsParser.filter_news_via_llm(raw_articles)
                    
                    redis_inst = getattr(db, "redis", getattr(db, "_redis", None))
                    
                    if filtered:
                        # 1. Если пришли новые валидные новости — пишем их в Redis с TTL 12 часов
                        # Это защищает веб-панель от очистки, если следующий цикл завершится ошибкой LLM
                        if redis_inst is not None:
                            await redis_inst.set(
                                "piluitus:news:latest",
                                json.dumps(filtered, ensure_ascii=False),
                                ex=43200  # 12 часов автономного удержания вместо 5 минут
                            )
                        logger.info("📰 [NEWS] Успешно сохранено и обновлено %d новостей в Redis.", len(filtered))
                        
                        if broadcast_callback:
                            asyncio.create_task(
                                broadcast_callback(
                                    json.dumps({
                                        "type": "NEWS_UPDATE",
                                        "news": filtered,
                                    }, ensure_ascii=False)
                                )
                            )
                    else:
                        # 2. Если ИИ-модель вернула пустой список (шум или сбой парсинга),
                        # мы не затираем кэш, а продлеваем жизнь старым новостям, удерживая интерфейс стабильным
                        logger.warning("⚠️ Новые новости от LLM пусты или сбойнули. Удерживаем старый кэш в веб-панели.")
                        if redis_inst is not None:
                            existing_news = await redis_inst.get("piluitus:news:latest")
                            if existing_news:
                                await redis_inst.expire("piluitus:news:latest", 43200)
                                
            except Exception as exc:  
                logger.error(f"❌ Критическая ошибка в цикле новостей: {exc}")

            await asyncio.sleep(300)
