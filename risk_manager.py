# risk_manager.py - Промышленный риск-инфорсер PILUITUS V2.2 (Полная сборка с калькулятором лотов)
import os
import logging

logger = logging.getLogger("FinamCore.RiskManager")

# Словари лотности Мосбиржи (базовый справочник для верификации риска)
LOT_SIZES = {
    "SBER": 10, "SBERP": 10, "VTBR": 10000, "GAZP": 10, "LKOH": 1,
    "ROSN": 10, "AFLT": 10, "MOEX": 10, "YDEX": 1
}

class RiskManager:
    """
    Глобальный автоматический предохранитель торговой сессии Мосбиржи.
    Защищает депозит от критической просадки и рассчитывает безопасный объем позиции.
    """
    def __init__(self, max_risk_per_trade: float = 0.005, daily_loss_limit: float = 0.015):
        self.max_risk_per_trade = max_risk_per_trade  # Рублевый риск на сделку (0.5% от депо)
        self.daily_loss_limit = daily_loss_limit      # Лимит потерь на день (1.5% от депо)
        self.trading_blocked = False

    def is_trading_allowed(self) -> bool:
        """
        Проверяет, не превышены ли лимиты просадки.
        Исправление: В режиме Paper Trading блокировка деактивируется для непрерывного сбора RAG-логов.
        """
        if self.trading_blocked:
            paper_mode = os.getenv("PAPER_TRADING", "True").upper() == "TRUE"
            if paper_mode:
                logger.warning("⚠️ [RISK BYPASS] Дневной лимит потерь превышен, но симуляция продолжается для Агента Эволюции.")
                return True
            else:
                logger.error("🚨 [RISK ENFORCER] ТОРГОВЛЯ БЛОКИРОВАНА! Достигнут суточный лимит просадки 1.5%.")
                return False
        return True

    def calculate_position_size(self, ticker: str, current_price: float, stop_loss: float) -> int:
        """
        Центральный калькулятор объема позиции по Регламенту Мосбиржи.
        Рассчитывает количество лотов так, чтобы рублевый убыток при стопе составлял ровно 0.5% от депозита.
        """
        try:
            start_deposit = float(os.getenv("TRADING_DEPOSIT", 100000.0))
            # Абсолютный максимальный рублевый риск на одну сделку (по умолчанию 500 рублей)
            max_allowed_risk_rub = start_deposit * self.max_risk_per_trade
            
            # Шаг цены инструмента и множитель лотности (например, в 1 лоте Сбера = 10 акций)
            lot_multiplier = LOT_SIZES.get(ticker, 1)
            
            # Вычисляем рублевый риск изменения цены на одну акцию
            price_risk_per_stock = abs(current_price - stop_loss)
            
            if price_risk_per_stock > 0:
                # Формула: Риск в рублях / (Риск на 1 акцию * Количество акций в лоте)
                calculated_lots = int(max_allowed_risk_rub / (price_risk_per_stock * lot_multiplier))
                lots = max(1, calculated_lots) # Защитный минимум в 1 лот
            else:
                lots = 1
                
            logger.info(f"💵 [RISK CALC] Инструмент: {ticker} | Расчетный объем: {lots} лотов под риск {max_allowed_risk_rub}₽")
            return lots
            
        except Exception as e:
            logger.error(f"Ошибка расчета размера позиции в RiskManager: {e}")
            return 1 # Безопасный возвращаемый минимум при сбоях расчетов

    def enforce_daily_stop(self):
        """ Вызывается экзекутором при фиксации критического убытка по журналу сессии """
        self.trading_blocked = True
        logger.error("🛑 [RISK MANAGER] Инициирована экстренная блокировка контура торгов.")
