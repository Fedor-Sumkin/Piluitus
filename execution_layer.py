import os
import json
import logging
import asyncio
import aiofiles  
from datetime import datetime, timezone
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from finam_client import FinamBrokerClient

logger = logging.getLogger("FinamCore.ExecutionLayer")
JOURNAL_FILE = "trading_journal.json"

# Жесткая спецификация лотности акций Мосбиржи для точного рублевого P&L
LOT_SIZES = {
    "SBER": 10, "SBERP": 10, "VTBR": 10000, "GAZP": 10, "LKOH": 1,
    "ROSN": 10, "AFLT": 10, "MOEX": 10, "YDEX": 1
}

class BiasTracker:
    """
    Модуль коррекции смещения ИИ (Bias Self-Correction) из практик Orallexa.
    Отслеживает серийные ошибки Qwen и динамически регулирует жесткость фильтра входа.
    """
    def __init__(self, base_threshold: float = 70.0):
        self.base_threshold = base_threshold
        self.history_window = 6  # Окно анализа последних трейдов

    async def get_dynamic_threshold(self) -> float:
        """ Calculates a dynamic confidence threshold based on recent performance to prevent overconfidence. """
        if not os.path.exists(JOURNAL_FILE):
            return self.base_threshold
            
        try:
            # Асинхронно поглощаем журнал сделок для HFT-безопасности
            async with aiofiles.open(JOURNAL_FILE, mode="r", encoding="utf-8") as f:
                content = await f.read()
                deals = json.loads(content) if content else []
                
            if not deals:
                return self.base_threshold
                
            # Вырезаем скользящее окно последних завершенных сессионных сделок
            recent_deals = deals[-self.history_window:]
            
            # Считаем количество сделок, помеченных вашим системным статусом "FAILED_MISTAKE"
            failed_count = sum(1 for d in recent_deals if d.get("final_outcome") == "FAILED_MISTAKE")
            total_recent = len(recent_deals)
            
            if total_recent >= 2 and failed_count >= (total_recent / 2):
                # Рассчитываем динамическое пенальти за сверхверенность ИИ
                # Например, если 4 из 6 сделок убыточны: (4/6) * 20 = +13.3% к строгости ворот входа
                error_ratio = failed_count / total_recent
                penalty = error_ratio * 20.0
                
                dynamic_gate = min(self.base_threshold + penalty, 90.0)
                logger.warning(
                    f"⚠️ [BIAS TRACKER] Обнаружена серия рыночных осечек ИИ ({failed_count}/{total_recent}). "
                    f"Порог confidence_score принудительно завышен: {dynamic_gate:.1f}% (Базовый: {self.base_threshold}%)"
                )
                return dynamic_gate
                
        except Exception as e:
            logger.error(f"Ошибка вычисления Bias Self-Correction трекера: {e}")
            
        return self.base_threshold

class BaseExecutionEngine:
    def __init__(self, finam_broker_client = None):
        self.broker = finam_broker_client
        self.paper_mode = os.getenv("PAPER_TRADING", "True").upper() == "TRUE"
        self.active_virtual_positions = {}
        self.file_lock = asyncio.Lock()
        
        # Жесткая имплантация трекера смещения в ядро экзекутора
        self.bias_tracker = BiasTracker(base_threshold=70.0)
        
        if self.paper_mode:
            logger.info("🛡️ РЕЖИМ АГЕНТА: [PAPER TRADING] — Симуляция ордеров с Bias-коррекцией.")
        else:
            logger.warning("🔥 РЕЖИМ АГЕНТА: [LIVE TRADING] — Работа на реальном счете!")

# execution_layer.py - Интеграция математического модуля ATR на основе ОЗУ-воронки

    def calculate_atr_stop(self, ticker: str, current_price: float, side: str) -> tuple[float, float]:
        """
        Паттерн Orallexa: Расчет стоп-уровней на основе True Range (волатильности ОЗУ-воронки).
        Защищает от галлюцинаций LLM и шумовых выбиваний крупными игроками Мосбиржи.
        """
        from main import funnel # Импортируем инстанцию живой воронки из главного скрипта
        
        # Извлекаем скользящую тиковую историю цен из воронки
        ticker_queue = funnel.prices_history.get(ticker)
        
        if not ticker_queue or len(ticker_queue) < 5:
            # Защитный скальперский фикс (0.4% стоп, 1.0% тейк с матожиданием ~1:2.5),
            # если воронка только запустилась и не успела накопить тики
            if side.upper() == "BUY":
                return round(current_price * 0.996, 2), round(current_price * 1.01, 2)
            return round(current_price * 1.004, 2), round(current_price * 0.99, 2)

        prices = list(ticker_queue)
        high_p = max(prices)
        low_p = min(prices)
        
        # Размах тиковой волатильности (True Range окна воронки)
        atr_range = high_p - low_p
        
        # Защитный фильтр, если акция замерла в узком спреде
        if atr_range <= 0:
            atr_range = current_price * 0.002 # Выставляем минимальный базовый шум в 0.2%

        # Скальперский коэффициент: 1.5x ATR на Stop-Loss, Take-Profit с матожиданием 1:2 (3.0x ATR)
        multiplier = 1.5
        if side.upper() == "BUY":
            sl = current_price - (atr_range * multiplier)
            tp = current_price + (atr_range * multiplier * 2.0)
        else:
            sl = current_price + (atr_range * multiplier)
            tp = current_price - (atr_range * multiplier * 2.0)

        # Геометрический enforcer: стоп не может превышать 1.5% по вашему Регламенту рисков
        max_sl_dist = current_price * 0.015
        if abs(current_price - sl) > max_sl_dist:
            sl = current_price * 0.985 if side.upper() == "BUY" else current_price * 1.015

        return round(sl, 2), round(tp, 2)


    async def execute_order(self, ticker: str, lots: int, side: str, price: float, client_id: str, ai_analysis: dict) -> dict:
        """Единая точка входа для отправки ордеров с предварительным риск-аудитом"""
        if self.paper_mode and ticker in self.active_virtual_positions:
            logger.warning(f"⏭️ Позиция по {ticker} уже открыта в ОЗУ. Игнорируем дублирующий сигнал ИИ.")
            return {"status": "SKIPPED", "message": "Position already exists"}

        # ПЕРЕХВАТ И КОРРЕКЦИЯ УРОВНЕЙ ИИ:
        # Рассчитываем точные математические стопы по волатильности ОЗУ-воронки Piluitus
        stop_loss, take_profit = self.calculate_atr_stop(ticker, price, side)
        
        # Перезаписываем уровни ИИ в словаре анализа, чтобы симулятор и Telegram получили точные ATR-уровни
        ai_analysis["stop_loss"] = stop_loss
        ai_analysis["take_profit"] = take_profit
        
        logger.info(f"📊 [ATR ENFORCER] Уровни для {ticker} скорректированы по волатильности: SL: {stop_loss}₽ | TP: {take_profit}₽")

        # ПРИНУДИТЕЛЬНЫЙ PYTHON-GUARD: Проверка на блокировку HOLD
        if ai_analysis.get("final_decision", "HOLD") == "HOLD":
            return {"status": "SKIPPED", "message": "AI Decision is HOLD"}

        if self.paper_mode:
            return await self._simulate_order_placement(ticker, side, price, ai_analysis, lots)
        else:
            if self.broker is None:
                logger.error("🛑 Критическая ошибка: Нативный брокер Финам не подключен.")
                return {"status": "FAILED", "message": "Broker client is None"}
                
            res = await self.broker.place_order(ticker, lots, side, price, client_id)
            return res if isinstance(res, dict) else {"status": "SUCCESS", "raw_data": str(res)}

    async def fetch_portfolio(self, client_id: str) -> dict:
        """🟢 ОТКРЫТЫЙ МЕТОД: Рассчитывает точный баланс и чистый P&L с учетом множителей лотов"""
        journal_deals = []
        pure_pnl_rub = 0.0
        
        if os.path.exists(JOURNAL_FILE):
            try:
                async with aiofiles.open(JOURNAL_FILE, mode="r", encoding="utf-8") as f:
                    content = await f.read()
                    journal_deals = json.loads(content) if content else []
            except Exception as e:
                logger.error(f"Ошибка чтения trading_journal.json в экзекуторе: {e}")

        for deal in journal_deals:
            entry = float(deal.get("entry_price", 0.0))
            exit_p = float(deal.get("exit_price", 0.0))
            lots = int(deal.get("lots", 1))
            ticker = deal.get("ticker", "GLOBAL")
            
            lot_multiplier = LOT_SIZES.get(ticker, 1)
            
            if deal.get("side") == "BUY":
                pure_pnl_rub += (exit_p - entry) * lots * lot_multiplier
            elif deal.get("side") == "SELL":
                pure_pnl_rub += (entry - exit_p) * lots * lot_multiplier

        start_deposit = float(os.getenv("TRADING_DEPOSIT", 100000.0))
        pure_pnl_percent = (pure_pnl_rub / start_deposit) * 100

        if self.paper_mode:
            virtual_positions = []
            for ticker_key in list(self.active_virtual_positions.keys()):
                pos = self.active_virtual_positions.get(ticker_key)
                if not pos: continue
                virtual_positions.append({
                    "securityCode": pos["ticker"],
                    "balance": pos["lots"],
                    "averagePrice": pos["entry_price"],
                    "side": pos["side"],
                    "sl": pos["stop_loss"],
                    "tp": pos["take_profit"]
                })
            
            return {
                "account_type": "LOCAL_PAPER_TRADING",
                "equity": round(start_deposit + pure_pnl_rub, 2),
                "pure_pnl_rub": round(pure_pnl_rub, 2),
                "pure_pnl_percent": round(pure_pnl_percent, 2),
                "positions": virtual_positions,
                "journal": journal_deals
            }
        else:
            if self.broker is None: 
                return {"equity": start_deposit, "pure_pnl_rub": 0.0, "pure_pnl_percent": 0.0, "positions": [], "journal": []}
            res = await self.broker.get_portfolio(client_id)
            if isinstance(res, dict):
                res["pure_pnl_rub"] = round(pure_pnl_rub, 2)
                res["pure_pnl_percent"] = round(pure_pnl_percent, 2)
                res["journal"] = journal_deals
                return res
            return {"equity": start_deposit, "pure_pnl_rub": pure_pnl_rub, "pure_pnl_percent": pure_pnl_percent, "positions": [], "journal": journal_deals}
    def monitor_paper_positions(self, ticker: str, current_price: float):
        """Сканирует тиковые изменения цен и закрывает позиции по правилам SL/TP"""
        if not self.paper_mode or ticker not in self.active_virtual_positions:
            return

        pos = self.active_virtual_positions[ticker]
        is_closed = False
        reason = ""
        pnl = 0.0

        if pos["side"] == "BUY":
            if current_price >= pos["take_profit"]:
                is_closed, reason = True, "TAKE_PROFIT"
            elif current_price <= pos["stop_loss"]:
                is_closed, reason = True, "STOP_LOSS"
            pnl = ((current_price - pos["entry_price"]) / pos["entry_price"]) * 100
        elif pos["side"] == "SELL":
            if current_price <= pos["take_profit"]:
                is_closed, reason = True, "TAKE_PROFIT"
            elif current_price >= pos["stop_loss"]:
                is_closed, reason = True, "STOP_LOSS"
            pnl = ((pos["entry_price"] - current_price) / pos["entry_price"]) * 100

        if is_closed:
            del self.active_virtual_positions[ticker]
            # Безопасный запуск корутины в текущем Event Loop без блокировок планировщика
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._log_to_journal(pos, current_price, reason, pnl))
            except RuntimeError:
                asyncio.create_task(self._log_to_journal(pos, current_price, reason, pnl))

    async def _log_to_journal(self, pos: dict, exit_price: float, reason: str, pnl_percent: float):
        """Асинхронная модификация статуса сделки, запись в архив и пуш-уведомление в Telegram"""
        async with self.file_lock:
            try:
                deals = []
                if os.path.exists(JOURNAL_FILE):
                    async with aiofiles.open(JOURNAL_FILE, mode="r", encoding="utf-8") as f:
                        content = await f.read()
                        deals = json.loads(content) if content else []

                # 1. Фиксация сквозного статуса для Next.js терминала
                pos["status"] = "CLOSED"           
                pos["exit_time"] = datetime.now(timezone.utc).isoformat()
                pos["exit_price"] = round(exit_price, 2)
                pos["exit_reason"] = reason
                pos["pnl_percent"] = round(pnl_percent, 2)
                pos["final_outcome"] = "SUCCESSFUL_EXPERIENCE" if pnl_percent > 0 else "FAILED_MISTAKE"

                deals.append(pos)
                
                async with aiofiles.open(JOURNAL_FILE, mode="w", encoding="utf-8") as f:
                    await f.write(json.dumps(deals, indent=4, ensure_ascii=False))

                # 2. МАТЕМАТИЧЕСКИЙ РАСЧЕТ РУБЛЕВОГО ФИНРЕЗА ДЛЯ ТЕЛЕГРАМА
                entry_p = float(pos.get("entry_price", 0.0))
                lots = int(pos.get("lots", 1))
                ticker = pos.get("ticker", "GLOBAL")
                lot_multiplier = LOT_SIZES.get(ticker, 1)
                
                pnl_rub = 0.0
                if pos["side"] == "BUY":
                    pnl_rub = (exit_price - entry_p) * lots * lot_multiplier
                elif pos["side"] == "SELL":
                    pnl_rub = (entry_p - exit_price) * lots * lot_multiplier

                # 3. ФОРМИРОВАНИЕ БОЕВОГО ШАБЛОНА ДЛЯ СМАРТФОНА
                if pnl_percent > 0:
                    res_emoji, res_text = "🟢 ПРОФИТ", f"+{round(pnl_rub, 2)} ₽"
                else:
                    res_emoji, res_text = "🔴 УБЫТОК", f"{round(pnl_rub, 2)} ₽"
                    
                logger.info(f"💾 [RAG ЖУРНАЛ] Сделка {pos['id']} ({ticker}) закрыта. Финрез: {res_emoji} ({pnl_percent:+.2f}%)")

                # 4. АСИНХРОННЫЙ ПУШ В ТЕЛЕГРАМчерез tg_bot
                import tg_bot
                if hasattr(tg_bot, "bot") and tg_bot.bot:
                    # Извлекаем ID администратора из .env
                    chat_id = os.getenv("TELEGRAM_ADMIN_ID")
                    if chat_id:
                        tg_message = (
                            f"🏁 **ПОЗИЦИЯ ЗАКРЫТА: {ticker} ({pos['side']})**\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"🆔 ID сделки: `{pos['id']}`\n"
                            f"🚪 Причина: `{reason}`\n"
                            f"📈 Вход: `{entry_p} ₽` | Выход: `{round(exit_price, 2)} ₽`\n"
                            f"📦 Объем: `{lots} лотов` (x{lot_multiplier} акций)\n"
                            f"────────────────────\n"
                            f"📊 Результат: **{res_emoji}**\n"
                            f"💰 Рублевый PnL: `{res_text}`\n"
                            f"📉 Процент: `{pnl_percent:+.2f}%`\n"
                            f"━━━━━━━━━━━━━━━━━━━━\n"
                            f"🧠 Вердикт консилиума зафиксирован в RAG-памяти системы."
                        )
                        # Запускаем отправку как независимую фоновую задачу, чтобы не блокировать HFT-контур
                        asyncio.create_task(tg_bot.bot.send_message(
                            chat_id=chat_id, 
                            text=tg_message, 
                            parse_mode="Markdown"
                        ))

            except Exception as e:
                logger.error(f"Ошибка сохранения сделки и отправки в ТГ: {e}")


    async def _simulate_order_placement(self, ticker: str, side: str, price: float, ai_analysis: dict, lots: int) -> dict:
        """Локальный риск-ориентированный симулятор ордеров на основе Регламента Мосбиржи"""
        raw_sl = ai_analysis.get("stop_loss")
        raw_tp = ai_analysis.get("take_profit")
        
        # 1. Математическая калибровка SL/TP (Защита от некорректных данных LLM)
        if side.upper() == "BUY":
            stop_loss = float(raw_sl) if (raw_sl and float(raw_sl) < price) else price * 0.993
            take_profit = float(raw_tp) if (raw_tp and float(raw_tp) > price) else price * 1.015
        else: # SHORT / SELL
            stop_loss = float(raw_sl) if (raw_sl and float(raw_sl) > price) else price * 1.007
            take_profit = float(raw_tp) if (raw_tp and float(raw_tp) < price) else price * 0.985

        # 2. Автоматический расчет рублевого риска по регламенту Мосбиржи (0.5% от депозита)
        start_deposit = float(os.getenv("TRADING_DEPOSIT", 100000.0))
        max_allowed_risk_rub = start_deposit * 0.005
        
        lot_multiplier = LOT_SIZES.get(ticker, 1)
        price_risk_per_stock = abs(price - stop_loss)
        
        if price_risk_per_stock > 0:
            calculated_lots = int(max_allowed_risk_rub / (price_risk_per_stock * lot_multiplier))
            lots = max(1, calculated_lots)

        # Генерируем уникальный ID для сквозной идентификации строки в Next.js
        trade_id = f"TRADE_{int(datetime.now(timezone.utc).timestamp() * 1000)}"

        position = {
            "id": trade_id,
            "status": "IN_GAME",
            "ticker": ticker,
            "entry_time": datetime.now(timezone.utc).isoformat(),
            "entry_price": price,
            "exit_time": None,
            "exit_price": None,
            "exit_reason": "OPEN",
            "side": side.upper(),
            "lots": lots, 
            "stop_loss": round(stop_loss, 2),
            "take_profit": round(take_profit, 2),
            "pnl_percent": 0.0,
            "final_outcome": "PENDING",
            "volume_spike": ai_analysis.get("volume_spike", "1.0x"),
            "tactic_opinion": ai_analysis.get("tactic_opinion", ""),
            "critic_opinion": ai_analysis.get("critic_opinion", ""),
            "confidence_score": ai_analysis.get("confidence_score", 0)
        }
        
        self.active_virtual_positions[ticker] = position
        logger.info(f"📊 [PAPER_GUARD] В игре: {ticker} ({side}) | Лотов: {lots} | Вход: {price}₽ | Статус: IN_GAME")
        return {"status": "SUCCESS", "orderId": trade_id}


