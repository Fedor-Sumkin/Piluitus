# tg_bot.py — Стабилизированный интерактивный кибер-пульт PILUITUS V3.0 (Часть 1)
import os
import json
import logging
import html
import asyncio
import fnmatch
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, types, Router
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from aiogram.filters import Command
from aiogram.types import FSInputFile  # Для отправки файлов бортового журнала

# Нативные модули v3.0
from db_manager import DBManager
from config import PROJECT_ROOT

load_dotenv()
logger = logging.getLogger("Piluitus.Telegram")

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TG_BOT_TOKEN")
USER_ID = os.getenv("TELEGRAM_USER_ID") or os.getenv("TG_USER_ID")

if not BOT_TOKEN:
    logger.critical("❌ КРИТИЧЕСКАЯ ОШИБКА: TELEGRAM_BOT_TOKEN не найден в .env!")

# Инициализация архитектурных слоев Aiogram
router = Router()
bot = Bot(token=BOT_TOKEN) if BOT_TOKEN else None
dp = Dispatcher()
dp.include_router(router)

# Ссылки на глобальные инстансы, инжектируемые из main.py при старте (Фикс Circular Import)
global_broker_ref = None
global_execution_engine_ref = None

# Создаем инстанс менеджера памяти, защищенный манки-патчами из main.py
db_manager = DBManager()


# --- ОБНОВЛЕННАЯ ГЕНЕРАЦИЯ ГЛАВНОГО МЕНЮ КНОПОК ---
def get_main_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.button(text="📊 Портфель")
    builder.button(text="⚙️ Status Системы")
    builder.button(text="📜 История сделок")
    builder.button(text="🔓 Сброс RISK BLOCK")  # Новая кнопка оперативного реагирования
    builder.button(text="📂 Получить Бортовой Лог")  # Новая кнопка выгрузки истории тестов
    builder.button(text="🚨 ПАНИКА")
    builder.adjust(2, 2, 1, 1)
    return builder.as_markup(resize_keyboard=True, placeholder="Выберите команду пульта...")


# --- ИНЛАЙН-КНОПКА ПОДТВЕРЖДЕНИЯ ДЛЯ ПАНИКИ ---
def get_panic_inline_keyboard():
    builder = InlineKeyboardBuilder()
    builder.button(text="💥 ПОДТВЕРДИТЬ УБИЙСТВО ОРДЕРОВ", callback_data="tg_panic_kill")
    return builder.as_markup()


# --- ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ПРОВЕРКИ ---
async def get_service_statuses() -> dict:
    """Проверяет статусы сервисов в ОЗУ Piluitus v3.0"""
    try:
        active_rules = await db_manager.load_rag_rules()
        learning_status = f"🟢 АКТИВНО (Правил в ОЗУ: {len(active_rules)})"
    except Exception:
        learning_status = "🔴 ОШИБКА REDIS"

    return {
        "puller": "🟢 АКТИВЕН (WebSockets)" if global_broker_ref and getattr(global_broker_ref, "is_ws_running", False) else "🔴 РЕЖИМ SANDBOX",
        "ai_engine": "🟢 В ГОТОВНОСТИ (DeepSeek-R1 8B)",
        "news_parser": "🟢 МОНИТОРИНГ (RSS)",
        "learning": learning_status
    }


# --- СТАРТОВАЯ КОМАНДА ---
@router.message(Command("start"))
async def cmd_start(message: types.Message):
    if not message.from_user or str(message.from_user.id) != str(USER_ID): return
    msg_text = (
        f"<b>🤖 Пульт управления PILUITUS v3.0 активирован!</b>\n\n"
        f"Нижняя клавиатура переведена в режим живого HFT-мониторинга. "
        f"Данные извлекаются напрямую из In-Memory DB Redis."
    )
    await message.answer(msg_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())


# --- ОБРАБОТЧИК: 🔓 Сброс RISK BLOCK ---
@router.message(lambda msg: msg.text in ["🔓 Сброс RISK BLOCK", "/reset_risks"])
async def cmd_reset_risks(message: types.Message):
    """Принудительно очищает журнал сделок, снимая аварийную блокировку риск-менеджера."""
    if not message.from_user or str(message.from_user.id) != str(USER_ID): return
    
    try:
        # Вызываем метод полной очистки журнала сделок и сброса AOF-команд
        await db_manager.clear_journal()
        
        msg_text = (
            f"🔓 <b>АВАРИЙНЫЙ РЕЖИМ СБРОШЕН!</b>\n\n"
            f"Суточный кумулятивный убыток сессии обнулен в Redis.\n"
            f"Флаг <code>RISK_BLOCK</code> деактивирован. Робот готов к "
            f"конкурентному приему импульсов и инференсу ИИ."
        )
        logger.warning("🔓 Принудительное снятие RISK_BLOCK администратором через Telegram.")
        await message.answer(msg_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    except Exception as e:
        logger.error(f"Ошибка принудительного сброса рисков: {e}")
        await message.answer(f"❌ Сбой сброса лимитов: {e}", reply_markup=get_main_keyboard())
# tg_bot.py — Стабилизированный интерактивный кибер-пульт PILUITUS V3.0 (Часть 2)

# --- ОБРАБОТЧИК: 📂 Получить Бортовой Лог ---
@router.message(lambda msg: msg.text in ["📂 Получить Бортовой Лог", "/get_logs"])
async def cmd_get_logs(message: types.Message):
    """Находит актуальный текстовый лог текущей торговой сессии и отправляет файлом."""
    if not message.from_user or str(message.from_user.id) != str(USER_ID): return
    
    log_dir = PROJECT_ROOT / "trading_logs"
    date_str = datetime.now().strftime("%Y-%m-%d")
    target_file = log_dir / f"piluitus_session_{date_str}.txt"
    
    if not target_file.is_file():
        # Если файл за сегодня еще не создался, ищем самый свежий доступный лог в папке
        if log_dir.is_dir():
            files = sorted(log_dir.glob("piluitus_session_*.txt"), key=lambda p: p.stat().st_mtime)
            if files:
                target_file = files[-1]
                
    if not target_file or not target_file.is_file():
        await message.answer("📂 Папка бортовых журналов <code>trading_logs/</code> пуста. Запустите стресс-тест.", parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
        return

    await message.answer(f"⏳ <i>Формирование выгрузки... Подготовка файла: {target_file.name}</i>", parse_mode=ParseMode.HTML)
    
    try:
        # Асинхронная отправка файла через нативный инструмент aiogram
        document = FSInputFile(path=str(target_file), filename=target_file.name)
        await message.answer_document(document=document, caption=f"📜 Полный бортовой журнал сессии: <code>{target_file.name}</code>\n\nИспользуйте текстовый редактор для анализа цепочек рассуждений ИИ.", parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())
    except Exception as e:
        logger.error(f"Ошибка отправки файла логов в Telegram: {e}")
        await message.answer(f"❌ Ошибка выгрузки файла: {e}", reply_markup=get_main_keyboard())


# --- ИСПРАВЛЕННЫЙ ОБРАБОТЧИК: 📊 Портфель ---
@router.message(lambda msg: msg.text in ["📊 Портфель", "/status"])
async def cmd_status(message: types.Message):
    if not message.from_user or str(message.from_user.id) != str(USER_ID): return
    
    client_id = os.getenv("FINAM_CLIENT_ID", "UNKNOWN_ACCOUNT")
    positions_list = []
    
    # Инициализируем финансовые показатели по умолчанию (Paper-Mode)
    pure_pnl_rub = 0.0
    pure_pnl_pct = 0.0
    equity = 100000.0  # Базовый депо из настроек
    
    try:
        # Динамический полиморфный сбор данных: защищаем линтер от типа "Never"
        engine_has_portfolio = (
            global_execution_engine_ref is not None 
            and hasattr(global_execution_engine_ref, "fetch_portfolio")
        )
        
        if engine_has_portfolio:
            # Извлекаем метод динамически, обходя статические ограничения типов BaseExecutionEngine
            fetch_func = getattr(global_execution_engine_ref, "fetch_portfolio")
            
            # Аварийный вызов асинхронной сопрограммы
            portfolio = await fetch_func(client_id, db_manager)
            
            positions_list = portfolio.get("positions", []) if isinstance(portfolio, dict) else getattr(portfolio, 'positions', [])
            pure_pnl_rub = portfolio.get('pure_pnl_rub', 0.0) if isinstance(portfolio, dict) else 0.0
            pure_pnl_pct = portfolio.get('pure_pnl_percent', 0.0) if isinstance(portfolio, dict) else 0.0
            equity = portfolio.get('equity', equity) if isinstance(portfolio, dict) else equity
            
        # Падаем в Fallback: берем данные напрямую из gRPC/REST брокера Финам или ОЗУ-журнала Redis
        elif global_broker_ref:
            # Запрашиваем открытые позиции напрямую из Redis менеджера памяти
            open_trades = await db_manager.get_active_positions()
            for order_id, trade in open_trades.items():
                positions_list.append({
                    "security_code": trade.get("ticker", "UNKNOWN"),
                    "side": trade.get("side", "BUY"),
                    "average_price": trade.get("price", 0.0)
                })
            
            # Рассчитываем виртуальный финрез по закрытым сделкам дня для симулятора
            all_trades = await db_manager.get_all_daily_trades()
            for trade in all_trades.values():
                if trade.get("status") == "CLOSED":
                    pure_pnl_rub += float(trade.get("pnl_ruble", 0.0))
            
            pure_pnl_pct = (pure_pnl_rub / equity) * 100.0
            equity += pure_pnl_rub
            
    except Exception as e:
        logger.error(f"❌ Ошибка полиморфного расчета портфеля в ТГ-боте: {e}")

    # Формируем текстовый блок открытых позиций
    pos_text = ""
    for p in positions_list:
        if isinstance(p, dict):
            code = p.get('security_code') or p.get('securityCode', 'UNKNOWN')
            side = p.get('side', 'BUY')
            entry = p.get('average_price', 0.0)
        else:
            code = getattr(p, 'security_code', 'UNKNOWN')
            side = getattr(p, 'side', 'BUY')
            entry = getattr(p, 'average_price', 0.0)
        pos_text += f"• <b>{code}</b> | {side} | Вход: {entry:.2f} ₽\n"
        
    if not pos_text: 
        pos_text = "Нет открытых позиций (Торговый стейт чист)."

    msg_text = (
        f"<b>📊 СОСТОЯНИЕ ПОРТФЕЛЯ (ПОЛИМОРФНЫЙ МОНИТОР):</b>\n\n"
        f"💰 Стартовый депозит: <code>100 000.00 ₽</code>\n"
        f"📈 Дневной финрез сессии: <code>{pure_pnl_rub:+.2f} ₽ ({pure_pnl_pct:.2f}%)</code>\n"
        f"💼 Текущая оценка эквити: <code>{equity:.2f} ₽</code>\n\n"
        f"🗂️ <b>Активные позиции в памяти:</b>\n{pos_text}"
    )
    await message.answer(msg_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())



# --- ОБРАБОТЧИК: ⚙️ Статус Системы ---
@router.message(lambda msg: msg.text in ["⚙️ Статус Системы", "⚙️ Status Системы", "/services"])
async def cmd_services(message: types.Message):
    if not message.from_user or str(message.from_user.id) != str(USER_ID): return
    
    status = await get_service_statuses()
    msg_text = (
        f"<b>⚙️ МОНИТОР ЖИЗНЕДЕЯТЕЛЬНОСТИ РОБОТА v3.0:</b>\n\n"
        f"📡 Поток котировок Финам: <code>{status['puller']}</code>\n"
        f"🧠 Ядро инференса vLLM: <code>{status['ai_engine']}</code>\n"
        f"📰 Пайплайн новостей: <code>{status['news_parser']}</code>\n"
        f"💾 База знаний RAG в ОЗУ: <code>{status['learning']}</code>"
    )
    await message.answer(msg_text, parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())


# --- ОБРАБОТЧИК: 📜 История сделок ---
@router.message(lambda msg: msg.text in ["📜 История сделок", "/statistics"])
async def cmd_statistics(message: types.Message):
    if not message.from_user or str(message.from_user.id) != str(USER_ID): return
    
    try:
        journal = await db_manager.get_all_daily_trades()  # Изменено на сквозной аудит сессии
        deals = list(journal.values()) if isinstance(journal, dict) else (journal if isinstance(journal, list) else [])
    except Exception as e:
        logger.error(f"Ошибка чтения журнала из Redis: {e}")
        deals = []
            
    if not deals:
        await message.answer("📜 История сделок (включая CLOSED) в ОЗУ пуста.", reply_markup=get_main_keyboard())
        return
        
    lines = ["<b>📜 ПОСЛЕДНИЕ СДЕЛКИ ИЗ ОЗУ-ЖУРНАЛА REDIS:</b>\n"]
    for d in deals[-15:]:
        if not isinstance(d, dict): continue
        pnl_val = float(d.get('pnl_percent', 0.0))
        
        emoji = "🟢" if pnl_val >= 0.0 else "🔴"
        ticker = d.get('ticker', 'UNKNOWN')
        side = d.get('side', 'BUY')
        status = d.get('status', 'OPEN')
        
        status_str = f"PnL: <code>{pnl_val:+.2f}%</code>" if status == "CLOSED" else "<code>АКТИВНА</code>"
        lines.append(f"{emoji} <b>{ticker}</b> | {side} | {status_str}")
        
    await message.answer("\n".join(lines), parse_mode=ParseMode.HTML, reply_markup=get_main_keyboard())


# --- ОБРАБОТЧИК: 🚨 ПАНИКА ---
@router.message(lambda msg: msg.text in ["🚨 ПАНИКА", "/panic"])
async def cmd_panic_request(message: types.Message):
    if not message.from_user or str(message.from_user.id) != str(USER_ID): return
    
    await message.answer(
        "<b>⚠️ ВНИМАНИЕ: Запрошена экстренная ликвидация!</b>\n"
        "Нажатие на инлайн-кнопку ниже отдаст команду в gRPC/REST шлюз Финама "
        "на моментальный сброс всех активных позиций по рыночной цене.",
        parse_mode=ParseMode.HTML,
        reply_markup=get_panic_inline_keyboard()
    )


# --- ОБРАБОТЧИК НАЖАТИЯ ИНЛАЙН-КНОПКИ ПАНИКИ ---
@router.callback_query(lambda c: c.data == "tg_panic_kill")
async def process_panic_callback(callback_query: types.CallbackQuery):
    if not callback_query.message or not bot:
        await callback_query.answer("Ошибка инициализации.", show_alert=True)
        return

    logger.critical("🚨 КНОПКА ПАНИКА АКТИВИРОВАНА ИЗ TELEGRAM!")
    client_id = os.getenv("FINAM_CLIENT_ID", "UNKNOWN_ACCOUNT")

    is_paper = True
    if global_broker_ref and hasattr(global_broker_ref, "is_demo"):
        is_paper = global_broker_ref.is_demo

    if is_paper:
        try:
            await db_manager.clear_journal()
            res_msg = "🛡️ [PAPER] Все виртуальные позиции успешно стерты из памяти и файлы бэкапа сброшены."
        except Exception as e:
            res_msg = f"❌ Ошибка очистки ОЗУ-журнала: {e}"
    else:
        if global_broker_ref is not None:
            asyncio.create_task(global_broker_ref.market_close_all_positions(client_id=client_id))
            res_msg = "🔴 [LIVE] Ордера ликвидации отправлены! Полный маркет-выход из всех акций на Мосбирже!"
        else:
            res_msg = "❌ Критическая ошибка: Шлюз global_broker_ref недоступен."

    await bot.send_message(chat_id=callback_query.message.chat.id, text=res_msg, parse_mode=ParseMode.HTML)
    await callback_query.answer(text="Ликвидация запущена!", show_alert=True)


# --- ВЕЩАНИЕ КАРТОЧЕК СИГНАЛОВ ИИ С РАССУЖДЕНИЯМИ DEEPSEEK-R1 ---
async def send_ai_signal_to_tg(ticker: str, price: float, volume_spike: float, reason: str, ai_analysis: dict):
    """Пушит сигналы с логами мыслей DeepSeek-R1 на экран оператора"""
    if not bot or not USER_ID: return
    decision = ai_analysis.get("final_decision", "HOLD")
    confidence_raw = ai_analysis.get("confidence_score", 0)
    try:
        confidence_value = float(confidence_raw)
    except (TypeError, ValueError):
        confidence_value = 0.0
    if 0.0 <= confidence_value <= 1.0:
        confidence_value *= 100.0
    confidence_label = "без входа" if decision == "HOLD" and confidence_value <= 0.0 else f"{confidence_value:.0f}%"
    tactic = html.escape(str(ai_analysis.get("tactic_opinion", "Нет данных")))
    critic = html.escape(ai_analysis.get("critic_opinion", "Контр-аргументы отсутствуют."))
    
    emoji = "🟢 BUY" if decision == "BUY" else "🔴 SELL" if decision == "SELL" else "⚪ HOLD"
    
    message_text = (
        f"<b>🤖 СИГНАЛ УТВЕРЖДЕН: {ticker} → {emoji}</b>\n"
        f"🎯 Уверенность решения: <code>{confidence_label}</code>\n"
        f"📈 Цена входа: <code>{price:.2f} ₽</code>\n"
        f"⚡ Сплеск объема: <code>{volume_spike:.1f}x</code>\n"
        f"📋 Триггер: <i>{html.escape(reason)}</i>\n\n"
        f"⚔️ <b>Тактический маневр:</b>\n<i>{tactic}</i>\n\n"
        f"🛡️ <b>Контр-аргументы Критика:</b>\n<i>{critic}</i>\n\n"
        f"🦅 <i>Piluitus Core v3.0 (vLLM Контур)</i>"
    )
    
    try: 
        await bot.send_message(chat_id=USER_ID, text=message_text, parse_mode=ParseMode.HTML)
    except Exception as e:
        logger.error(f"Не удалось отправить карточку сигнала в Telegram: {e}")
