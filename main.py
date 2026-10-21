import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request
from fastapi.responses import HTMLResponse
from dotenv import load_dotenv
from datetime import datetime

# Нативные модули экосистемы PILUITUS V2
from news_parser import MarketNewsParser
from ai_engine import analyze_stock_signal, get_historical_rag_context
from finam_client import FinamBrokerClient
from screener import MarketScreener
from execution_layer import BaseExecutionEngine
from risk_manager import RiskManager
import tg_bot
from screener_funnel import VolumeVolatilityFunnel

load_dotenv()

FINAM_TOKEN = os.getenv("FINAM_TOKEN", "default_demo_token")
CLIENT_ID = os.getenv("FINAM_CLIENT_ID", "UNKNOWN_ACCOUNT")
DEPOSIT = float(os.getenv("TRADING_DEPOSIT", 100000.0))

# --- СИСТЕМНОЕ КУПИРОВАНИЕ СЕТЕВОГО ШУМА (ОПТИМИЗАЦИЯ ЛОГОВ) ---
logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("FinamCore").setLevel(logging.WARNING)
logging.getLogger("finam").setLevel(logging.WARNING)

# Оставляем активными только промышленные логи PILUITUS
logger = logging.getLogger("FinamCore.Main")
logger.setLevel(logging.INFO)
logging.getLogger("FinamCore.AI").setLevel(logging.INFO)
logging.getLogger("FinamCore.ExecutionLayer").setLevel(logging.INFO)
logging.getLogger("FinamCore.Telegram").setLevel(logging.INFO)

# Глобальные кэш-структуры
last_ai_verdicts_cache = []
tick_counter = 0
spinner_frames = ["|", "/", "-", "\\"]
AI_SEMAPHORE = asyncio.Semaphore(1)

TICKERS_TO_TRADE = [
    "SBER", "SBERP", "GAZP", "LKOH", "NVTK", "YDEX", "ROSN", "VTBR", "AFLT", "MAGN",
    "CHMF", "NLMK", "MOEX", "ALRS", "MTSS", "MGNT", "TATN", "SNGS", "SNGSP", "PLZL",
    "HYDR", "FEES", "CBOM", "IRAO", "PHOR", "PIKK", "TRNFP", "TATNP", "AFKS", "RUAL",
    "GCHE", "LSRG", "SMLT", "MSNG", "BANEP", "BSPB", "RENI", "UPRO", "SOFL", "POSI",
    "RNFT", "UWGN", "VKCO", "HEAD", "OZON", "APRI", "MVID", "LENT", "OKEY", "AGRO",
    "MDMG", "WUSH", "CARM", "LEAS", "DELI", "SVCB", "SFIN", "MTLR", "MTLRP", "RASP",
    "FESH", "NMTP", "FLOT", "GECO", "ABIO", "BLNG", "DVEC", "ELFV", "ENPG", "GTRK",
    "IRKT", "KAZT", "KAZTP", "KBSB", "KROT", "MAGE", "MAGEP", "MGTS", "MGTSP", "NKHP",
    "NKNC", "NKNCP", "TGKA", "TGKB", "TGKBP", "TTLK", "VJGZ", "VJGZP"
]

class ConnectionManager:
    """Асинхронный диспетчер WebSocket-соединений фронтенд-терминала"""
    def __init__(self):
        self.active_connections: list[WebSocket] = []
    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
    async def broadcast(self, message: str):
        for connection in self.active_connections:
            try: 
                await connection.send_text(message)
            except Exception: 
                pass

manager = ConnectionManager()
broker = FinamBrokerClient(token=FINAM_TOKEN, is_demo=True)
screener = MarketScreener(window_size=30)
execution_engine = BaseExecutionEngine(finam_broker_client=broker)
risk_manager = RiskManager(max_risk_per_trade=0.005, daily_loss_limit=0.015)
funnel = VolumeVolatilityFunnel(target_count=15)
last_funnel_update = 0.0
active_trading_locks = {ticker: False for ticker in TICKERS_TO_TRADE}

async def process_market_event(event):
    global tick_counter, last_funnel_update
    try:
        ticker = getattr(event, 'security_code', 'SBER')
        price = float(getattr(event, 'price', 0.0))
        volume = int(getattr(event, 'volume', 0))

        if price == 0.0: return

        tick_counter += 1
        current_frame = spinner_frames[tick_counter % len(spinner_frames)]
        print(f"\r⚡ [ПОТОК ФИНАМ] {current_frame} Обработано тиков: {tick_counter:,} | Последний: {ticker} ({price}₽)   ", end="", flush=True)

        funnel.update_ticker_price(ticker, price)
        execution_engine.monitor_paper_positions(ticker, price)
        
# main.py — Модификация HFT-пакета FUNNEL_UPDATE для передачи графических метрик

        # ОПТИМИЗАЦИЯ HFT: Считаем воронку и шлем на фронт строго раз в 2 секунды
        current_time = time.time()
        if current_time - last_funnel_update >= 2.0:
            active_top_15 = funnel.select_top_volatile_tickers()
            last_funnel_update = current_time
            
            # Собираем обогащенный массив данных по Топ-15 для графиков фронтенда
            enriched_top_15_data = []
            for t in active_top_15:
                # Извлекаем VWAP и ATR налету из ОЗУ без нагрузки на процессор
                vwap_val = funnel.get_ticker_vwap(t) or price
                
                # Расчет ATR по скользящему окну воронки
                t_queue = funnel.prices_history.get(t)
                atr_val = (max(t_queue) - min(t_queue)) if t_queue and len(t_queue) > 1 else (price * 0.002)
                if atr_val <= 0: atr_val = price * 0.002

                enriched_top_15_data.append({
                    "ticker": t,
                    "price": float(funnel.prices_history[t][-1]) if funnel.prices_history.get(t) else price,
                    "vwap": vwap_val,
                    "atr": round(atr_val, 2)
                })

            asyncio.create_task(manager.broadcast(json.dumps({
                "type": "FUNNEL_UPDATE", 
                "active_tickers_data": enriched_top_15_data
            }, ensure_ascii=False)))

        else:
            active_top_15 = funnel.last_cached_top_15 

        if not active_top_15 or ticker not in active_top_15: return

        is_anomaly, stats = screener.update_and_check(ticker, price, volume)
        
        if is_anomaly and stats:
            if active_trading_locks.get(ticker, False): return
            active_trading_locks[ticker] = True
            
            print("\n" + "-"*80)
            asyncio.create_task(execute_ai_and_paper_flow(ticker, price, stats))
    except Exception as e:
        print(f"\n💥 Критическая ошибка в обработчике тиков: {e}")
# main.py — ЧАСТЬ 2 из 2. Главный управляющий HFT/ИИ-контур PILUITUS V2.2

async def execute_ai_and_paper_flow(ticker: str, price: float, stats: dict):
    """ Параллельный ИИ-конвейер PILUITUS V2.2 с жестким Bias Gate. """
    global last_ai_verdicts_cache
    
    if AI_SEMAPHORE.locked():
        return

    async with AI_SEMAPHORE:
        try:
            if not risk_manager.is_trading_allowed(): 
                return

            # 1. АКТИВАЦИЯ ПАТТЕРНА BIAS-CORRECTION
            dynamic_confidence_gate = await execution_engine.bias_tracker.get_dynamic_threshold()

            hybrid_rag_context = get_historical_rag_context(ticker)
            news_context = MarketNewsParser.get_global_sentiment_context()
            
            order_book_task = asyncio.create_task(broker.get_ticker_order_book(ticker))
            portfolio_state = await execution_engine.fetch_portfolio(CLIENT_ID)
            ticker_history = [d for d in portfolio_state.get("journal", []) if d.get("ticker") == ticker][-2:]

            order_book = await order_book_task

            # 2. ИНФЕРЕНС МУЛЬТИ-АГЕНТСКОГО КОНСИЛИУМА (7B -> 7B)
            ai_analysis = await analyze_stock_signal(
                ticker=ticker,
                price=price,
                volume_spike=stats["volume_spike"],
                technical_reason=stats["reason"] + f"\n[МЕДИА-СЕНТИМЕНТ]: {news_context}",
                order_book=order_book,
                ticker_history=ticker_history
            )

            decision = ai_analysis.get("final_decision", "HOLD")
            confidence = float(ai_analysis.get("confidence_score", 0))

            # 3. ЖЕСТКАЯ ФИЛЬТРАЦИЯ ПО ДИНАМИЧЕСКОМУ ПОРОГУ BIAS TRACKER
            if decision in ["BUY", "SELL"]:
                if confidence >= dynamic_confidence_gate:
                    logger.warning(f"🎯 [BIAS GATE PASSED] Сигнал {ticker} утвержден! {confidence}% >= {dynamic_confidence_gate:.1f}%")
                    
                    await execution_engine.execute_order(
                        ticker=ticker,
                        lots=1,  # Внутри пересчитается под риск 0.5% по ATR
                        side=decision,
                        price=price,
                        client_id=CLIENT_ID,
                        ai_analysis=ai_analysis
                    )
                    asyncio.create_task(tg_bot.send_ai_signal_to_tg(ticker, price, stats["volume_spike"], stats["reason"], ai_analysis))
                else:
                    logger.info(f"🛡️ [BIAS GATE BLOCKED] Сигнал {ticker} отклонен. Confidence: {confidence}% < Порог: {dynamic_confidence_gate:.1f}%")

            # Обновление локального циклического кэша вердиктов (15 записей)
            verdict_data = {
                "timestamp": datetime.now().strftime("%H:%M:%S"),
                "ticker": ticker, 
                "price": price, 
                "decision": decision, 
                "confidence_score": confidence,
                "tactic_opinion": ai_analysis.get("tactic_opinion", ""),
                "critic_opinion": ai_analysis.get("critic_opinion", "")
            }
            
            last_ai_verdicts_cache.append(verdict_data)
            last_ai_verdicts_cache = last_ai_verdicts_cache[-15:] 
            
            # Вещание на фронтенд-терминал
            await manager.broadcast(json.dumps({
                "type": "MARKET_UPDATE", 
                "ticker": ticker, 
                "price": price, 
                "decision": decision, 
                "stats": stats, 
                "ai": ai_analysis, 
                "portfolio": await execution_engine.fetch_portfolio(CLIENT_ID)
            }, ensure_ascii=False))

        except Exception as e:
            logger.error(f"Сбой ИИ-конвейера V2.2 по {ticker}: {e}")
        finally:
            active_trading_locks[ticker] = False

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Управление фоновыми сервисами"""
    tg_bot.global_broker_ref = broker 
    bg_news_task = asyncio.create_task(MarketNewsParser.update_news_cycle(manager.broadcast))
    
    async def safe_telegram_polling():
        if not tg_bot.bot or not tg_bot.dp:
            logger.error("🛑 [ТГ-БОТ] Ошибка инициализации bot/dp. Запуск отменен.")
            return
            
        logger.info("🔌 [ТГ-БОТ] Активация промышленного контура Telegram V2.2...")
        
        # Запускаем бесконечный цикл авто-восстановления сессии при таймаутах
        while True:
            try:
                logger.info("🧹 [ТГ-БОТ] Сброс накопленной ночной очереди спама...")
                await tg_bot.bot.delete_webhook(drop_pending_updates=True)
                
                logger.info("🚀 [ТГ-БОТ] Сеть очищена. Бот успешно переведен в режим живого HFT-мониторинга.")
                # Запускаем поллинг (skip_updates защищает от лавины старых сообщений)
                await tg_bot.dp.start_polling(tg_bot.bot, skip_updates=True)
                
            except Exception as telegram_error:
                # Паттерн Orallexa: перехватываем Request timeout error, не давая упасть главному процессу
                logger.error(
                    f"💥 [ТГ-БОТ ВНИМАНИЕ] Сетевой сбой или таймаут HTTP Client: {telegram_error}. "
                    f"Автоматический перезапуск сессии через 5 секунд..."
                )
                # Даем Event Loop передохнуть и очистить сетевые сокеты
                await asyncio.sleep(5.0)

    bg_tg_task = asyncio.create_task(safe_telegram_polling())
    
    async def background_broker_init():
        asyncio.create_task(broker.connect())
        global bg_market_task
        bg_market_task = asyncio.create_task(broker.start_market_data_stream(TICKERS_TO_TRADE, process_market_event))

    broker_init_task = asyncio.create_task(background_broker_init())
    logger.info("🚀 [LIFESPAN] Контур PILUITUS запущен.")
    yield
    bg_news_task.cancel()
    if bg_tg_task: bg_tg_task.cancel()
    broker_init_task.cancel()
    if 'bg_market_task' in globals(): bg_market_task.cancel()

app = FastAPI(title="Piluitus AI Trading Core", lifespan=lifespan)

@app.get("/", response_class=HTMLResponse)
async def get_index():
    with open("templates/index.html", "r", encoding="utf-8") as f: 
        return f.read()

@app.websocket("/ws/frontend")
async def websocket_frontend_endpoint(websocket: WebSocket):
    """ ИСПРАВЛЕННЫЙ ЭНДПОИНТ: Сначала пушит ВСЕ исторические данные, затем слушает команды """
    global last_ai_verdicts_cache
    await manager.connect(websocket)
    try:
        news_context = MarketNewsParser.get_global_sentiment_context()
        portfolio_state = await execution_engine.fetch_portfolio(CLIENT_ID)
        
        # 1. МГНОВЕННЫЙ ЕДИНЫЙ ПУШ ИСТОРИИ И СОСТОЯНИЯ (Решает проблемы №1, №4, №5)
        await websocket.send_json({
            "type": "INIT_DATA",
            "news": news_context,
            "portfolio": portfolio_state,
            "verdicts_history": last_ai_verdicts_cache
        })
        
        # 2. АСИНХРОННЫЙ ЦИКЛ ОЖИДАНИЯ СИСТЕМНЫХ КОМАНД (Кнопка паники)
        while True:
            data = await websocket.receive_text()
            message = json.loads(data)
            if message.get("command") == "PANIC_BUTTON":
                if execution_engine.paper_mode:
                    execution_engine.active_virtual_positions.clear()
                else:
                    await broker.market_close_all_positions(client_id=CLIENT_ID)
                await manager.broadcast(json.dumps({"type": "PANIC_STATUS", "status": "CLOSED"}))
    except WebSocketDisconnect:
        manager.disconnect(websocket)
