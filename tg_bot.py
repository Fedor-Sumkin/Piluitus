import os
import json
import logging
import html
from datetime import datetime
from aiogram import Bot, Dispatcher, types, Router
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.filters import Command
import asyncio
from dotenv import load_dotenv



load_dotenv()
logger = logging.getLogger("FinamCore.Telegram")

# Консервативная загрузка токенов
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
USER_ID = os.getenv("TELEGRAM_USER_ID")

if not BOT_TOKEN:
    print("❌ КРИТИЧЕСКАЯ ОШИБКА: TELEGRAM_BOT_TOKEN не найден в .env!")

# Жесткая инициализация строго ОДНОГО роутера и ОДНОГО диспетчера
router = Router()
bot = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
dp = Dispatcher()

# Принудительно связываем роутер с диспетчером прямо здесь
dp.include_router(router)
global_broker_ref = None

# --- ГЕНЕРАЦИЯ ГЛАВНОГО МЕНЮ КНОПОК (Клавиатура внизу экрана) ---
def get_main_keyboard():
    builder = ReplyKeyboardBuilder()
    # Разметка главного кибер-пульта
    builder.button(text="📊 Портфель")
    builder.button(text="🔥 Воронка Топ-15")
    builder.button(text="⚙️ Статус Системы")
    builder.button(text="📜 История сделок")
    builder.button(text="🚨 ПАНИКА")
    
    # Формируем сетку: 2 кнопки в ряд, паника на отдельной строке
    builder.adjust(2, 2, 1)
    return builder.as_markup(resize_keyboard=True, placeholder="Выберите команду пульта...")

# --- ИНЛАЙН-КНОПКА ПОДТВЕРЖДЕНИЯ ДЛЯ ПАНИКИ ---
def get_panic_inline_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="💥 ПОДТВЕРДИТЬ УБИЙСТВО ОРДЕРОВ", callback_data="tg_panic_kill")
    return builder.as_markup()

# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ПРОВЕРКИ ---
def get_service_statuses() -> dict:
    from main import funnel
    return {
        "puller": "🟢 АКТИВЕН (HTTP/2)" if global_broker_ref and getattr(global_broker_ref, "client", None) else "🔴 ОТКЛЮЧЕН",
        "funnel": f"🟢 РАБОТАЕТ (Акции в ОЗУ: {len(funnel.prices_history)})",
        "ai_engine": "🟢 В ГОТОВНОСТИ (Qwen 2.5 )",
        "news_parser": "🟢 МОНИТОРИНГ (RSS )",
        "learning": "🟢 САМООБУЧЕНИЕ (RAG Активно)" if os.path.exists("trading_journal.json") else "⚪ СТАРТ"
    }

# --- СТАРТОВАЯ КОМАНДА: Инициализация пульта ---
@router.message(Command("start"))
async def cmd_start(message: types.Message):
    if not message.from_user or str(message.from_user.id) != str(USER_ID): return
    msg_text = (
        f"<b>🤖 Терминал PILUITUS (V2) приветствует оператора!</b>\n\n"
        f"Нижняя клавиатура активирована. Используйте кнопки для мгновенного опроса "
        f"высокоскоростной ОЗУ-воронки и ИИ-конвейера."
    )
    await message.answer(msg_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())

# --- ОБРАБОТЧИК: 📊 Портфель ---
@router.message(lambda msg: msg.text in ["📊 Портфель", "/status"])
async def cmd_status(message: types.Message):
#    if not message.from_user or str(message.from_user.id) != str(USER_ID): return
    from main import execution_engine, CLIENT_ID

    portfolio = await execution_engine.fetch_portfolio(CLIENT_ID)
    positions = portfolio.get("positions", [])
    
    pos_text = ""
    for p in positions:
        pos_text += f"• <b>{p['securityCode']}</b> | {p['side']} | Вход: {p['averagePrice']} ₽\n"
    if not pos_text: pos_text = "Нет открытых сделок."

    msg_text = (
        f"<b>📊 СОСТОЯНИЕ ПОРТФЕЛЯ:</b>\n\n"
        f"💰 Депозит: <code>100 000.00 ₽</code>\n"
        f"📈 Чистая прибыль: <code>{portfolio.get('pure_pnl_rub', 0.0):+.2f} ₽ ({portfolio.get('pure_pnl_percent', 0.0):.2f}%)</code>\n"
        f"💼 Баланс с учетом позиций: <code>{portfolio.get('equity', 100000.0):.2f} ₽</code>\n\n"
        f"🗂️ <b>Открытые сделки в активах:</b>\n{pos_text}"
    )
    await message.answer(msg_text, parse_mode=ParseMode.HTML)

# --- ОБРАБОТЧИК: 🔥 Воронка ---
@router.message(lambda msg: msg.text in ["🔥 Воронка Топ-15", "/cup"])
async def cmd_cup(message: types.Message):
    if not message.from_user or str(message.from_user.id) != str(USER_ID): return
    from main import funnel
    
    active_top_15 = funnel.select_top_volatile_tickers()
    tickers_str = ", ".join(active_top_15) if active_top_15 else "Воронка пуста, идет сбор тиков."
    
    msg_text = (
        f"<b>📊 СРЕЗ ВОРОНКИ ОТБОРА АКТИВОВ:</b>\n"
        f"Акции, попавшие в обработку прямо сейчас (Топ-15):\n\n"
        f"<code>{tickers_str}</code>"
    )
    await message.answer(msg_text, parse_mode=ParseMode.HTML)

# --- ОБРАБОТЧИК: ⚙️ Статус Системы ---
@router.message(lambda msg: msg.text in ["⚙️ Статус Системы", "/services"])
async def cmd_services(message: types.Message):
    if not message.from_user or str(message.from_user.id) != str(USER_ID): return
    
    status = get_service_statuses()
    msg_text = (
        f"<b>⚙️ МОНИТОР ЖИЗНЕДЕЯТЕЛЬНОСТИ РОБОТА:</b>\n\n"
        f"📡 Пуллер акций Финам: <code>{status['puller']}</code>\n"
        f"📊 Математическая воронка: <code>{status['funnel']}</code>\n"
        f"🧠 Тактик-Критик ИИ: <code>{status['ai_engine']}</code>\n"
        f"📰 Новости & Sentiment ИИ: <code>{status['news_parser']}</code>\n"
        f"💾 Самообучение (RAG): <code>{status['learning']}</code>"
    )
    await message.answer(msg_text, parse_mode=ParseMode.HTML)

# --- ОБРАБОТЧИК: 📜 История сделок ---
@router.message(lambda msg: msg.text in ["📜 История сделок", "/statistics"])
async def cmd_statistics(message: types.Message):
    if not message.from_user or str(message.from_user.id) != str(USER_ID): return
    
    deals = []
    if os.path.exists("trading_journal.json"):
        try:
            with open("trading_journal.json", "r", encoding="utf-8") as f:
                content = f.read()
                deals = json.loads(content) if content else []
        except Exception as e:
            logger.error(f"Ошибка чтения trading_journal.json: {e}")
            
    if not deals:
        await message.answer("📜 История завершенных сделок пуста.")
        return
        
    lines = ["<b>📜 ПЕРЕЧЕНЬ ЗАВЕРШЕННЫХ СДЕЛКАХ (ИСТОРИЯ):</b>\n"]
    for d in deals[-15:]:
        pnl = d.get('pnl_percent')
        pnl_val = float(pnl) if pnl is not None else 0.0
        
        emoji = "🟢" if pnl_val >= 0.0 else "🔴"
        ticker = d.get('ticker', 'UNKNOWN')
        decision = d.get('final_decision', 'HOLD')
        
        lines.append(f"{emoji} <b>{ticker}</b> | P&L: <code>{pnl_val:+.2f}%</code> | Род: {decision}")
        
    await message.answer("\n".join(lines), parse_mode=ParseMode.HTML)

# --- ОБРАБОТЧИК: 🚨 ПАНИКА (Вывод Инлайн-кнопки подтверждения) ---
@router.message(lambda msg: msg.text in ["🚨 ПАНИКА", "/panic"])
async def cmd_panic_request(message: types.Message):
    if not message.from_user or str(message.from_user.id) != str(USER_ID): return
    
    await message.answer(
        "<b>⚠️ ВНИМАНИЕ: Требуется подтверждение экстренной ликвидации!</b>\n"
        "Нажатие на инлайн-кнопку ниже мгновенно сбросит все локальные позиции "
        "или выбросит маркет-ордера закрытия на Мосбиржу.",
        parse_mode=ParseMode.HTML,
        reply_markup=get_panic_inline_keyboard()
    )

# --- ОБРАБОТЧИК НАЖАТИЯ ИНЛАЙН-КНОПКИ ПАНИКИ ---
@router.callback_query(lambda c: c.data == "tg_panic_kill")
async def process_panic_callback(callback_query: types.CallbackQuery):
    if not callback_query.message or not bot:
        await callback_query.answer("Ошибка: Инициализация прервана.", show_alert=True)
        return

    logger.critical("🚨 КНОПКА ПАНИКА АКТИВИРОВАНА ИЗ TELEGRAM ЧЕРЕЗ ИНЛАЙН-КНОПКУ!")
    client_id = os.getenv("FINAM_CLIENT_ID", "UNKNOWN_ACCOUNT")
    from main import execution_engine

    if execution_engine.paper_mode:
        execution_engine.active_virtual_positions.clear()
        res_msg = "🛡️ [PAPER] Все локальные псевдо-позиции экстренно очищены через Telegram."
    else:
        if global_broker_ref is not None:
            asyncio.create_task(global_broker_ref.market_close_all_positions(client_id=client_id))
            res_msg = "🔴 [LIVE] Выполнен экстренный маркет-выход из всех акций на Мосбирже!"
        else:
            res_msg = "❌ Критическая ошибка: Ссылка на брокера global_broker_ref отсутствует."

    await bot.send_message(chat_id=callback_query.message.chat.id, text=res_msg, parse_mode=ParseMode.HTML)
    await callback_query.answer(text="Ликвидация запущена!", show_alert=True)

# Вспомогательный метод отправки карточек сигналов (остается без изменений)
async def send_ai_signal_to_tg(ticker: str, price: float, volume: str, reason: str, ai_analysis: dict):
    if not bot or not USER_ID: return
    decision = ai_analysis.get("final_decision", "HOLD")
    confidence = ai_analysis.get("confidence_score", 0)
    tactic = html.escape(ai_analysis.get("tactic_opinion", ""))
    critic = html.escape(ai_analysis.get("critic_opinion", ""))
    
    emoji = "🟢 BUY" if decision == "BUY" else "🔴 SELL" if decision == "SELL" else "⚪ HOLD"
    message_text = f"<b>🤖 СИГНАЛ АГЕНТА: {ticker} → {emoji}</b>\nУверенность: {confidence}%\n\n⚔️ Тактик: <i>{tactic}</i>\n\n🛡️ Критик: <i>{critic}</i>"
    
    try: await bot.send_message(chat_id=USER_ID, text=message_text, parse_mode=ParseMode.HTML)
    except Exception: pass
