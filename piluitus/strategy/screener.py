# screener.py - Полная сборка V2.3 с интеграцией всего каталога паттернов CScalp
import logging
from collections import deque

logger = logging.getLogger("FinamCore.Screener")

class MarketScreener:
    """
    Высокоскоростной ОЗУ-сканер паттернов технического анализа PILUITUS V2.3.
    Математически вычисляет фигуры разворота, продолжения тренда и гармонические модели.
    """
    def __init__(self, window_size: int = 40, spike_threshold: float = 7.0):
        # Расширяем окно до 40 точек, чтобы уместить 5-точечные гармонические паттерны
        self.window_size = window_size
        self.spike_threshold = spike_threshold
        self.volume_history = {}
        self.price_history = {}

    def update_and_check(self, ticker: str, price: float, volume: int) -> tuple[bool, dict | None]:
        if ticker not in self.volume_history:
            self.volume_history[ticker] = deque(maxlen=self.window_size)
            self.price_history[ticker] = deque(maxlen=self.window_size)

        self.volume_history[ticker].append(volume)
        self.price_history[ticker].append(price)

        # Для анализа сложных геометрических фигур нам нужен буфер минимум из 15 тиков
        if len(self.volume_history[ticker]) < 15:
            return False, None

        historical_volumes = list(self.volume_history[ticker])[:-1]
        mean_volume = sum(historical_volumes) / len(historical_volumes)
        if mean_volume <= 0: mean_volume = 1.0

        volume_spike = volume / mean_volume

        # Фильтр аномалий: сканируем паттерны только при наличии институционального объема
        if volume_spike >= self.spike_threshold:
            prices = list(self.price_history[ticker])
            p = prices  # Сокращение для удобства формул
            
            detected_pattern = "Импульсный объем (Вне паттернов)"
            
            try:
                # =================================================================
                # ГРУППА 1: ПАТТЕРНЫ РАЗВОРOТА ТРЕНДА
                # =================================================================
                # 1.1 Двойная вершина / Двойное дно
                if abs(p[-1] - max(p[:-5])) / max(p[:-5]) < 0.0006 and abs(p[-1] - p[-5]) / p[-5] > 0.001:
                    detected_pattern = "ФИГУРА: ДВОЙНАЯ ВЕРШИНА (Разворот вниз)"
                elif abs(p[-1] - min(p[:-5])) / min(p[:-5]) < 0.0006 and abs(p[-1] - p[-5]) / p[-5] > 0.001:
                    detected_pattern = "ФИГУРА: ДВОЙНОЕ ДНО (Разворот вверх)"
                
                # 1.2 Голова и Плечи (3 локальных экстремума, средний — выше/ниже)
                elif p[-1] < p[-3] and p[-5] > p[-3] and p[-3] > p[-7] and max(p[-10:-5]) < p[-5]:
                    detected_pattern = "ФИГУРА: ГОЛОВА И ПЛЕЧИ (Локальный пик, разворот)"
                
                # 1.3 Трубовидное дно / Вершина (Бычье/Медвежье поглощение)
                elif abs(p[-1] - p[-2]) > (sum([abs(p[i] - p[i-1]) for i in range(-5, -2)]) / 3) * 2.5:
                    detected_pattern = "СВЕЧНАЯ МОДЕЛЬ: ТРУБОВИДНЫЙ РАЗВОРOТ (Поглощение)"
                    
                # 1.4 Три черные вороны (Устойчивое последовательное падение/рост на объемах)
                elif p[-1] < p[-2] < p[-3] < p[-4]:
                    detected_pattern = "СВЕЧНАЯ МОДЕЛЬ: ТРИ ВОРОНЫ (Подтверждение шорта)"

                # =================================================================
                # ГРУППА 2: ПАТТЕРНЫ ПРОДОЛЖЕНИЯ ТРЕНДА
                # =================================================================
                # 2.1 Флаг / Вымпел (Резкий импульс и сжатие волатильности последних 4 тиков)
                elif (max(p[-4:]) - min(p[-4:])) < (max(p[:-4]) - min(p[:-4])) * 0.22:
                    detected_pattern = "ФИГУРА: ФЛАГ/ВЫМПЕЛ (Продолжение сильного тренда)"
                
                # 2.2 Прямоугольник (Консолидация в горизонтальном коридоре)
                elif abs(max(p[-8:]) - min(p[-8:])) < p[-1] * 0.0015:
                    detected_pattern = "ФИГУРА: ПРЯМОУГОЛЬНИК (Накопление крупного игрока)"
                
                # 2.3 Золотой куб (4 свечи равного размера, сжатый флэт)
                elif abs(p[-1] - p[-4]) < p[-1] * 0.0004 and abs(max(p[-4:]) - min(p[-4:])) < p[-1] * 0.001:
                    detected_pattern = "ФИГУРА: ЗОЛОТОЙ КУБ (Флэт перед выстрелом)"

                # =================================================================
                # ГРУППА 3: ДВУСТОРОННИЕ (НЕОПРЕДЕЛЕННЫЕ) ФИГУРЫ
                # =================================================================
                # 3.1 Симметричный треугольник / Клин (Минимумы растут, максимумы падают)
                elif p[-1] < p[-4] and p[-2] > p[-5] and (max(p[-3:]) - min(p[-3:])) < (max(p[-8:-4]) - min(p[-8:-4])):
                    detected_pattern = "ФИГУРА: СИММЕТРИЧНЫЙ КЛИН (Неопределенность, ждем пробой)"

                # =================================================================
                # ГРУППА 4: СЛОЖНЫЕ ГАРМОНИЧЕСКИЕ МОДЕЛИ (XABCD)
                # =================================================================
                # Математическая оценка 5 ключевых экстремумов для вычисления Бабочки Гартли и Летучей мыши
                elif len(p) >= 15:
                    x, a, b, c, d = p[-15], p[-11], p[-8], p[-4], p[-1]
                    xa = abs(a - x) or 1
                    ab = abs(b - a) or 1
                    bc = abs(c - b) or 1
                    
                    # Пропорция корреляции Бабочки Гартли (AB=CD, XB около 0.61)
                    if 0.55 < (ab / xa) < 0.68 and 0.65 < (bc / ab) < 0.85:
                        detected_pattern = "ГАРМОНИКА: БАБОЧКА ГАРТЛИ (Глубокий разворот Order Flow)"
                    # Пропорция Летучей мыши (XD близко к 0.886)
                    elif 0.30 < (ab / xa) < 0.52 and 0.82 < (abs(d - x) / xa) < 0.92:
                        detected_pattern = "ГАРМОНИКА: ЛЕТУЧАЯ МЫШЬ (Точка разворота D найдена)"

            except Exception as math_err:
                logger.debug(f"Пропуск расчета геометрии из-за нехватки экстремумов: {math_err}")

            # Формируем тренд-вектор
            price_change = p[-1] - p[-5]
            trend_vector = "ИМПУЛЬС КВЕРХУ (PUMP)" if price_change >= 0 else "ИМПУЛЬС КНИЗУ (DUMP)"

            stats = {
                "volume_spike": f"{volume_spike:.1f}x",
                "reason": f"[{detected_pattern}] {trend_vector} | Объем: {volume} лотов",
                "raw_spike": volume_spike
            }
            return True, stats

        return False, None
