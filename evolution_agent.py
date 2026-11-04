import logging
import os
import numpy as np
from datetime import datetime
from db_manager import DBManager
import asyncio

logger = logging.getLogger("Piluitus.ExecutionLayer")

# Жесткая спецификация лотности акций Мосбиржи для точного рублевого P&L
LOT_SIZES = {
    "SBER": 10, "SBERP": 10, "VTBR": 10000, "GAZP": 10, "LKOH": 1,
    "ROSN": 10, "AFLT": 10, "MOEX": 10, "YDEX": 1
}

class BaseExecutionEngine:
    """
    Основной исполнительный движок.

    Добавлен параметр `screener` – объект :class:`HFTMemoryScreener`,
    который хранит исторические цены и объёмы.  Это позволяет избавиться
    от циклического импорта `from main import screener`.
    """
    def __init__(self, finam_broker_client=None, screener=None):
        self.broker   = finam_broker_client
        self.screener = screener            # <-- сохраняем скринер
        self.paper_mode = os.getenv("PAPER_TRADING", "True").upper() == "TRUE"

        if self.paper_mode:
            logger.info("🛡️ РЕЖИМ АГЕНТА: [PAPER TRADING] — Симуляция ордеров через Redis ОЗУ.")
        else:
            logger.warning("🔥 РЕЖИМ АГЕНТА: [LIVE TRADING] — Работа на реальном счёте Finam!")

    # ------------------------------------------------------------------
    # 🚨 ИСПРАВЛЕНИЕ: Метод экстренного параллельного закрытия (Паника)
    # ------------------------------------------------------------------
    async def market_close_all_positions_safe(self, risk_manager, db: DBManager) -> bool:
        """
        Экстренное параллельное закрытие всех открытых позиций по рынку (Кнопка 'Паника').
        Мгновенно изолирует торговый контур от фоновых процессов через RiskManager.
        Умеет корректно закрывать позиции как в LIVE-режиме, так и очищать в PAPER-режиме.
        """
        # 1. Мгновенная блокировка торгового контура
        risk_manager.block_trading = True
        logger.warning("!!! ПАНИКА !!! Торговый контур принудительно заблокирован RiskManager.")

        try:
            # Получаем все открытые позиции из Redis журнала
            active_positions = await db.get_active_positions()
            if not active_positions:
                logger.info("Активных открытых позиций в Redis журнале не обнаружено. Контур заблокирован.")
                return False

            if self.paper_mode:
                # --- РЕЖИМ PAPER TRADING ---
                # Согласно бизнес-правилам, в режиме "paper" кнопка паники очищает Redis-журнал сделок
                logger.warning("🧹 [ПАНИКА: PAPER] Очистка активных позиций из Redis...")
                for order_id, trade in active_positions.items():
                    if isinstance(trade, dict):
                        trade["status"] = "CLOSED"
                        trade["exit_time"] = datetime.now().isoformat()
                        trade["exit_reason"] = "PANIC_CLEAR"
                        trade["final_outcome"] = "FAILED_MISTAKE"
                        await db.log_trade(order_id, trade)
                logger.warning("✅ [ПАНИКА: PAPER] Журнал активных позиций в Redis успешно переведен в статус CLOSED.")
                return True

            else:
                # --- РЕЖИМ LIVE TRADING ---
                # В режиме "live" посылает массовый рыночный маркет-клоуз всех позиций в Finam
                if self.broker is None:
                    logger.error("🛑 Критическая ошибка паники: Нативный брокер Finam не подключён.")
                    return False

                tasks = []
                for order_id, trade in active_positions.items():
                    if not isinstance(trade, dict) or trade.get("status") != "OPEN":
                        continue
                    
                    ticker = trade.get("ticker")
                    lots = int(trade.get("lots", 0))
                    side = trade.get("side", "BUY")
                    
                    # Переворачиваем сделку для закрытия по рынку
                    close_side = "SELL" if side == "BUY" else "BUY"
                    
                    if lots <= 0 or not ticker:
                        continue

                    logger.warning(f"💥 [ПАНИКА: LIVE] Готовим экстренный рыночный ордер: {close_side} {lots} лотов по {ticker}")
                    
                    # Создаем асинхронную задачу отправки ордера через ваш Finam клиент
                    task = self.broker.place_order(ticker, lots, close_side, 0.0, f"PANIC_{order_id}")
                    tasks.append((order_id, trade, task))

                if not tasks:
                    logger.info("Нет валидных ордеров для отправки на биржу.")
                    return False

                # Стреляем всеми ордерами параллельно в Finam API
                coroutines = [t[2] for t in tasks]
                results = await asyncio.gather(*coroutines, return_exceptions=True)

                # Анализируем ответы биржи и обновляем статусы в Redis
                for idx, (order_id, trade, _) in enumerate(tasks):
                    res = results[idx]
                    if isinstance(res, Exception):
                        logger.error(f"❌ Сбой ордера паники для сделки {order_id}: {res}")
                    else:
                        logger.warning(f"✅ Ордер паники для сделки {order_id} успешно ушел на биржу. Ответ: {res}")
                        trade["status"] = "CLOSED"
                        trade["exit_time"] = datetime.now().isoformat()
                        trade["exit_reason"] = "PANIC_CLOSE"
                        await db.log_trade(order_id, trade)

                return True

        except Exception as e:
            logger.critical(f"❗ Критическая ошибка внутри движка паники ExecutionEngine: {e}")
            return False
    # ------------------------------------------------------------------
    # 2️⃣  ATR‑стоп
    # ------------------------------------------------------------------
    def calculate_atr_stop(self, ticker: str, current_price: float, side: str) -> tuple[float, float]:
        """
        Расчёт стоп‑/тейк‑уровней по ATR.
        Использует исторические данные, хранящиеся в HFTMemoryScreener.
        """
        if not self.screener:
            logger.debug("Screener not provided – using fallback ATR calculation.")
            if side.upper() == "BUY":
                return round(current_price * 0.996, 2), round(current_price * 1.01, 2)
            return round(current_price * 1.004, 2), round(current_price * 0.99, 2)

        ticker_data = self.screener.history.get(ticker)

        if not ticker_data or ticker_data["count"] < 5:
            if side.upper() == "BUY":
                return round(current_price * 0.996, 2), round(current_price * 1.01, 2)
            return round(current_price * 1.004, 2), round(current_price * 0.99, 2)

        prices = ticker_data["prices"][: ticker_data["count"]]
        std_dev = float(prices.std()) if hasattr(prices, "std") else float(np.std(prices))

        atr_range = max(prices) - min(prices)
        if atr_range <= 0:
            atr_range = current_price * 0.002  # базовый шум 0.2 %

        multiplier = 1.5
        if side.upper() == "BUY":
            sl = current_price - (atr_range * multiplier)
            tp = current_price + (atr_range * multiplier * 2.0)
        else:
            sl = current_price + (atr_range * multiplier)
            tp = current_price - (atr_range * multiplier * 2.0)

        max_sl_dist = current_price * 0.015
        if abs(current_price - sl) > max_sl_dist:
            sl = current_price * 0.985 if side.upper() == "BUY" else current_price * 1.015

        return round(sl, 2), round(tp, 2)

    # ------------------------------------------------------------------
    # 3️⃣  execute_order
    # ------------------------------------------------------------------
    async def execute_order(self, ticker: str, lots: int, side: str,
                            price: float, client_id: str,
                            db: DBManager, ai_analysis: dict) -> dict:
        """Единая точка входа для отправки ордеров с предварительным риск‑аудитом."""
        try:
            stop_loss, take_profit = self.calculate_atr_stop(ticker, price, side)
            ai_analysis["stop_loss"] = stop_loss
            ai_analysis["take_profit"] = take_profit

            logger.info(
                f"📊 [ATR ENFORCER] Уровни скорректированы: SL={stop_loss}₽ TP={take_profit}₽"
            )

            if ai_analysis.get("final_decision", "HOLD") == "HOLD":
                return {"status": "SKIPPED", "message": "AI Decision is HOLD"}

            trade_resp = None
            if not self.paper_mode:
                if self.broker is None:
                    logger.error("🛑 Нативный брокер Finam не подключён.")
                    return {"status": "FAILED", "message": "Broker client is None"}
                trade_resp = await self.broker.place_order(
                    ticker, lots, side, 0.0, client_id
                )

            order_id = (
                f"TRADE_{int(datetime.now().timestamp() * 1000)}"
                if not trade_resp
                else str(trade_resp.get("orderId"))
            )

            trade_log = {
                "id": order_id,
                "status": "OPEN",
                "ticker": ticker,
                "entry_time": datetime.now().isoformat(),
                "entry_price": price,
                "side": side.upper(),
                "lots": lots,
                "stop_loss": stop_loss,
                "take_profit": take_profit,
                "pnl_percent": 0.0,
                "final_outcome": "PENDING",
                "tactic_opinion": ai_analysis.get("tactic_opinion", ""),
                "critic_opinion": ai_analysis.get("critic_opinion", ""),
                "confidence_score": ai_analysis.get("confidence_score", 100),
            }

            await db.log_trade(order_id, trade_log)
            logger.info(
                f"✅ [EXECUTION] Позиция {order_id} по {ticker} зарегистрирована в Redis."
            )
            return {"status": "SUCCESS", "orderId": order_id}

        except Exception as e:                     # noqa: BLE001
            logger.error(f"❗ Критический сбой исполнения ордера по {ticker}: {e}")
            return {"status": "FAILED", "message": str(e)}

    # ------------------------------------------------------------------
    # 4️⃣  monitor_paper_positions
    # ------------------------------------------------------------------
    async def monitor_paper_positions(self, ticker: str, current_price: float, db: DBManager):
        """Сканирует открытые позиции и закрывает их, когда достигнут SL/TP."""
        try:
            active_positions = await db.get_active_positions()
            if not active_positions:
                return

            for order_id, trade in active_positions.items():
                if not isinstance(trade, dict) or trade.get("status") != "OPEN":
                    continue
                if trade.get("ticker") != ticker:
                    continue

                entry_price = float(trade.get("entry_price", 0.0))
                side        = trade.get("side", "BUY")
                stop_loss   = float(trade.get("stop_loss", entry_price * 0.995))
                take_profit = float(trade.get("take_profit", entry_price * 1.015))

                pnl_percent = 0.0
                should_close = False
                exit_reason  = "MANUAL"

                if side == "BUY":
                    pnl_percent = ((current_price - entry_price) / entry_price) * 100.0
                    if current_price <= stop_loss:
                        should_close = True
                        exit_reason = "STOP_LOSS"
                    elif current_price >= take_profit:
                        should_close = True
                        exit_reason = "TAKE_PROFIT"
                else:   # SHORT
                    pnl_percent = ((entry_price - current_price) / entry_price) * 100.0
                    if current_price >= stop_loss:
                        should_close = True
                        exit_reason = "STOP_LOSS"
                    elif current_price <= take_profit:
                        should_close = True
                        exit_reason = "TAKE_PROFIT"

                if should_close:
                    trade["status"]      = "CLOSED"
                    trade["exit_time"]   = datetime.now().isoformat()
                    trade["exit_price"]  = current_price
                    trade["exit_reason"] = exit_reason
                    trade["pnl_percent"] = round(pnl_percent, 4)

                    trade["final_outcome"] = (
                        "FAILED_MISTAKE" if exit_reason == "STOP_LOSS"
                        else "SUCCESSFUL_EXPERIENCE"
                    )

                    await db.log_trade(order_id, trade)

                    lots          = int(trade.get("lots", 1))
                    lot_multiplier = LOT_SIZES.get(ticker, 1)
                    pnl_rub = (
                        (current_price - entry_price) * lots * lot_multiplier
                        if side == "BUY"
                        else (entry_price - current_price) * lots * lot_multiplier
                    )

                    logger.warning(
                        f"💾 [RAG ЖУРНАЛ] Сделка {order_id} ({ticker}) закрыта. "
                        f"Pnl {pnl_rub:.2f}₽ ({pnl_percent:+.2f}%)."
                    )

                    import tg_bot
                    if hasattr(tg_bot, "bot") and tg_bot.bot:
                        chat_id = os.getenv("TELEGRAM_USER_ID")
                        if chat_id:
                            msg = (
                                f"🏁 **ПОЗИЦИЯ ЗАКРЫТА:** {ticker} ({side})\n"
                                f"🔹 Цена входа: `{entry_price:.2f}₽`\n"
                                f"🔹 Цена выхода: `{current_price:.2f}₽`\n"
                                f"💰 PnL: `{pnl_rub:+.2f} ₽` ({pnl_percent:+.2f}%)\n"
                                f"📝 Причина: `{exit_reason}`"
                            )
                            asyncio.create_task(
                                tg_bot.bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
                            )
        except Exception as e:                     # noqa: BLE001
            logger.error(f"❗ Ошибка мониторинга позиций в ОЗУ по {ticker}: {e}")
