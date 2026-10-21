import logging
from collections import deque

logger = logging.getLogger("FinamCore.ScreenerFunnel")

class VolumeVolatilityFunnel:
    def __init__(self, target_count: int = 15):
        self.target_count = target_count
        self.prices_history = {}
        self.last_cached_top_15 = [] 

    def update_ticker_price(self, ticker: str, price: float):
        """Обновляет тиковую историю для расчета волатильности (вызывается на каждый входящий тик)"""
        if price <= 0: return # Защита от некорректных брокерских цен
        
        if ticker not in self.prices_history:
            self.prices_history[ticker] = deque(maxlen=20)  # Окно в 20 тиков для оценки микро-трендов
        self.prices_history[ticker].append(price)

    def select_top_volatile_tickers(self) -> list[str]:
        """Ранжирует все накопленные тикеры по размаху спреда и вытесняет неликвид"""
        metrics = []
        
        for ticker, queue in self.prices_history.items():
            if len(queue) < 5:  # Пропускаем, пока не накопится минимальная база для расчета
                continue
                
            prices = list(queue)
            high_p = max(prices)
            low_p = min(prices)
            
            # Жесткая защита от деления на ноль
            if low_p <= 0: 
                continue
            
            # Математический коэффициент размаха: (High - Low) / Low
            volatility_range = (high_p - low_p) / low_p
            metrics.append((ticker, volatility_range))

        # Сортируем акции по убыванию волатильности (самые "горячие" — наверх)
        metrics.sort(key=lambda x: x[1], reverse=True)
        
        # Срезаем строго до целевого количества (Топ-15 воронки Piluitus)
        top_selected = [item[0] for item in metrics[:self.target_count]]
        
        if len(top_selected) > 0 and len(metrics) >= 10:
            logger.info(f"📊 [ВОРОНКА] Буфер: {len(metrics)} акций. Выделен Топ-{len(top_selected)} волатильных активов: {top_selected}")
            
        # Кэшируем результат перед выходом для неблокирующего чтения в main.py
        self.last_cached_top_15 = top_selected
        return top_selected

# screener_funnel.py - Добавление метода расчета VWAP (Внутри класса VolumeVolatilityFunnel)
    
    def get_ticker_vwap(self, ticker: str) -> float | None:
        """
        Математический расчет VWAP (Volume-Weighted Average Price) из ОЗУ-тиков.
        Паттерн Orallexa: Используется КРИТИКОМ как LOCK-фильтр против покупок на вершинах.
        """
        queue = self.prices_history.get(ticker)
        if not queue or len(queue) < 5:
            return None
            
        prices = list(queue)
        
        # Импортируем NumPy локально внутри метода, чтобы не перегружать глобальные зависимости
        import numpy as np
        
        # Генерируем веса объемов по тиковой плотности распределения (линейный вес к последним тикам)
        volumes = np.linspace(1, 2, len(prices)) 
        
        # Формула VWAP: Сумма(Цена * Объем) / Сумма(Объемов)
        vwap = np.dot(prices, volumes) / np.sum(volumes)
        
        return round(float(vwap), 2)
