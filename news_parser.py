import os
import json
import logging
import httpx
import re
import xml.etree.ElementTree as ET
import asyncio
import aiofiles
from datetime import datetime
from ollama import AsyncClient

logger = logging.getLogger("FinamCore.NewsParser")
NEWS_MEMORY_FILE = "ai_market_news.json"

NEWS_SYSTEM_PROMPT = """
Ты — финансовый аналитик Мосбиржи. Твоя задача — отфильтровать экономические новости из официальных СМИ.
Выявляй ТОЛЬКО жесткие фундаментальные и корпоративные события (дивиденды, финансовые отчеты РСБУ/МСФО, крупные слияния, байбэки, кадровые перестановки в руководстве, макроэкономика ЦБ РФ).
Ответ дай СТРОГО в формате JSON-массива объектов, без markdown-разметки и преамбул.

Если новость является рыночным шумом (общие обзоры, прогнозы аналитиков, рассуждения), полностью игнорируй её.

Формат выходного JSON:
[
  {
    "title": "Заголовок официальной новости",
    "ticker": "SBER", 
    "sentiment": "POSITIVE" или "NEGATIVE" или "NEUTRAL",
    "impact_reason": "Краткое техническое объяснение влияния на цену акции в 1 предложение."
  }
]
"""

class MarketNewsParser:
    # 🟢 ИСПРАВЛЕНО: Заменены сырые домены на прямые канонические эндпоинты XML-лент RSS
    RSS_FEEDS = [
        "https://bcs-express.ru",
        "https://www.bfm.ru/news.rss?rubric=28",
        "https://moex.com/export/news.aspx?cat=100",
        "https://moex.com/export/news.aspx?cat=101",
        "https://www.moex.com/export/news.aspx?cat=104",
        "https://bfm.ru",
        "https://moex.com",
        "https://www.moex.com/export/news.aspx?cat=122",
        "https://www.finam.ru/analysis/forecasts/rsspoint/"
    ]

    @staticmethod
    def _regex_fallback_parser(raw_xml_text: str) -> list:
        """Резервный парсер сырого XML текста через регулярные выражения"""
        items_extracted = []
        items = re.findall(r'<item>(.*?)</item>', raw_xml_text, re.DOTALL | re.IGNORECASE)
        
        for item in items[:3]: 
            title_match = re.search(r'<title>(.*?)</title>', item, re.DOTALL | re.IGNORECASE)
            desc_match = re.search(r'<(?:description|summary)>(.*?)</(?:description|summary)>', item, re.DOTALL | re.IGNORECASE)
            
            title = title_match.group(1).strip() if title_match else ""
            desc = desc_match.group(1).strip() if desc_match else ""
            
            title = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', title, flags=re.DOTALL)
            title = re.sub(r'<[^<]+?>', '', title).strip()
            
            desc = re.sub(r'<!\[CDATA\[(.*?)\]\]>', r'\1', desc, flags=re.DOTALL)
            desc = re.sub(r'<[^<]+?>', '', desc).strip()
            
            if title:
                items_extracted.append((title, desc))
        return items_extracted

    @staticmethod
    async def fetch_raw_rss_http2() -> list:
        """Скачивает XML-ленты из открытого РФ-контура"""
        collected = []
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
            "Accept": "application/xml,text/xml,application/xhtml+xml,text/html"
        }
        
        async with httpx.AsyncClient(http2=True, headers=headers, timeout=15.0, follow_redirects=True) as client:
            for url in MarketNewsParser.RSS_FEEDS:
                try:
                    response = await client.get(url)
                    if response.status_code != 200: continue
                    
                    raw_items = MarketNewsParser._regex_fallback_parser(response.text)
                    
                    if not raw_items:
                        root = ET.fromstring(response.content)
                        items = root.findall(".//item") or root.findall(".//{*}item")
                        for item in items[:3]:
                            title_node = item.find("title") or item.find("{*}title")
                            desc_node = item.find("description") or item.find("{*}description") or item.find("{*}summary")
                            
                            title = title_node.text.strip() if title_node is not None and title_node.text else ""
                            desc = desc_node.text.strip() if desc_node is not None and desc_node.text else ""
                            if title: raw_items.append((title, desc))

                    for title, desc in raw_items:
                        feed_name = url.split('/')[-1].split('?')[0]
                        collected.append(f"📡 Фид: {feed_name} | Заголовок: {title} | Описание: {desc[:150]}")
                        
                except Exception:
                    continue
        return collected

    @staticmethod
    async def filter_news_via_llm(raw_news_list: list) -> list:
        if not raw_news_list: return []

        truncated_news = raw_news_list[:5]
        user_prompt = "Проанализируй и отфильтруй от рыночного шума следующие официальные новости Мосбиржи:\n" + "\n\n".join(truncated_news)
        raw_content = ""

        try:
            response = await AsyncClient().chat(
                model='qwen2.5:14b-instruct-q4_K_M',
                messages=[
                    {'role': 'system', 'content': NEWS_SYSTEM_PROMPT},
                    {'role': 'user', 'content': user_prompt}
                ],
                options={'temperature': 0.1, 'num_predict': 1024, 'num_ctx': 4096}
            )
            
            raw_content = response['message']['content'].strip()
            
            if "```" in raw_content:
                raw_content = raw_content.split("```")[1]
                if raw_content.startswith("json"): raw_content = raw_content[4:]
            
            raw_content = raw_content.strip()
            if raw_content.startswith("[") and not raw_content.endswith("]"): raw_content += "]"

            filtered_news = json.loads(raw_content)
            if isinstance(filtered_news, dict): filtered_news = [filtered_news]
                
            for item in filtered_news:
                item["time"] = datetime.now().strftime("%H:%M:%S")
                
            return filtered_news
        except Exception as e:
            logger.error(f"❌ Ошибка ИИ-новости: {e}")
            return []

    @staticmethod
    def get_global_sentiment_context() -> str:
        if not os.path.exists(NEWS_MEMORY_FILE): return "Новостной фон нейтральный."
        try:
            with open(NEWS_MEMORY_FILE, "r", encoding="utf-8") as f:
                news = json.load(f)
            if not news or not isinstance(news, list): return "Новостной фон нейтральный."
            return "\n".join([f"• [{n.get('ticker', 'GLOBAL')}] {n.get('title')} ({n.get('sentiment')})" for n in news[-3:]])
        except Exception:
            return "Новостной фон нейтральный."

    @staticmethod
    async def update_news_cycle(broadcast_callback):
        if not os.path.exists(NEWS_MEMORY_FILE):
            async with aiofiles.open(NEWS_MEMORY_FILE, mode='w', encoding='utf-8') as f:
                await f.write(json.dumps([], ensure_ascii=False))

        while True:
            try:
                raw_articles = await MarketNewsParser.fetch_raw_rss_http2()
                if raw_articles:
                    filtered = await MarketNewsParser.filter_news_via_llm(raw_articles)
                    if filtered:
                        async with aiofiles.open(NEWS_MEMORY_FILE, mode='w', encoding='utf-8') as f:
                            await f.write(json.dumps(filtered, indent=4, ensure_ascii=False))
                        
                        await broadcast_callback(json.dumps({"type": "NEWS_UPDATE", "news": filtered}, ensure_ascii=False))
                        logger.info(f"📰 [ИИ-СЕНТИМЕНТ] Обновлено {len(filtered)} событий макро-фона Мосбиржи.")
            except Exception as e:
                logger.error(f"Ошибка в цикле новостей: {e}")
            await asyncio.sleep(300)
