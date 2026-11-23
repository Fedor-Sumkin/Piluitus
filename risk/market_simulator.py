"""Synthetic market stream simulator used for controlled stress tests."""
from __future__ import annotations

from risk.risk_shared import *  # noqa: F401,F403

class RealMarketSimulator:
    def __init__(self, uri: str = "ws://127.0.0.1:8080/ws/frontend", batch_size: int = 10):
        self.uri = uri
        self.batch_size = batch_size  
        self.tickers = ["SBER", "GAZP"]
        
        # Математика процессов: Текущая цена, Справедливая средняя цена (Mean) и Скорость возврата (Theta)
        self.current_prices = {"SBER": 250.0, "GAZP": 130.0}
        self.mean_prices = {"SBER": 251.5, "GAZP": 130.8}
        self.theta = {"SBER": 0.05, "GAZP": 0.04}  # Коэффициент жесткости возврата к среднему
        
        self.is_running = False
        self.step_counter = 0

    def _generate_single_tick(self) -> dict:
        """
        Математическая модель Орнштейна-Уленбека с волнообразной внутридневной активностью.
        Генерирует реалистичные микро-тренды и флэт Мосбиржи.
        """
        self.step_counter += 1
        ticker = random.choice(self.tickers)
        
        # 1. Моделирование времени суток через синусоиду (Фазы: Утренний Гэп -> Обед -> Вечерний Импульс)
        market_activity_wave = abs(math.sin(self.step_counter / 500.0))
        
        # Реалистичная частота аномалий (0.5% вместо экстремальных 8%)
        anomaly_threshold = 0.005 * (1 + market_activity_wave * 2)
        is_anomaly = random.random() < anomaly_threshold

        current_price = self.current_prices[ticker]
        mean_price = self.mean_prices[ticker]
        theta = self.theta[ticker]

        if is_anomaly:
            # 💥 Импульс: Крупный институциональный Айсберг-ордер пробивает стакан
            direction = random.choice([-1, 1])
            price_change = direction * random.uniform(0.4, 1.2)  # Реалистичный скачок в %
            trade_volume = random.randint(8000, 25000)
            
            # Аномалия временно смещает справедливое среднее актива (смена тренда)
            self.mean_prices[ticker] += direction * (price_change * 0.3)
        else:
            # ⚪ Процесс возврата к среднему (Mean Reversion) + Гауссов шум
            drift = theta * (mean_price - current_price)
            noise = random.normalvariate(0, 0.05) if hasattr(random, "normalvariative") else random.gauss(0, 0.05)
            price_change = drift + noise
            trade_volume = random.randint(10, 300)

        # Пересчет и округление цены до шага цены Мосбиржи
        self.current_prices[ticker] += price_change
        final_price = round(self.current_prices[ticker], 4)
        if final_price <= 0: final_price = 0.01

        # Формируем индикаторы (Знак объема объема жестко привязан к импульсу)
        volume_spike_ratio = round(random.uniform(4.5, 8.5), 2) if is_anomaly else round(random.uniform(0.3, 1.1), 2)
        if is_anomaly and direction == -1:
            volume_spike_ratio = -abs(volume_spike_ratio) # Эмуляция истинного DUMP

        std_dev_val = round(random.uniform(1.5, 3.2), 2) if is_anomaly else round(random.uniform(0.05, 0.4), 2)
        atr_val = round(final_price * random.uniform(0.008, 0.015), 4)

        return {
            "event": "MARKET_UPDATE",
            "ticker": ticker,
            "security_code": ticker,
            "timestamp": datetime.now().isoformat(),
            "price": final_price,
            "last_price": final_price,
            "volume": trade_volume,
            "qty": trade_volume,
            "is_anomaly": is_anomaly,
            "order_book": {
                "bids": [[round(final_price * (1 - i*0.0001), 2), random.randint(100, 1500)] for i in range(1, 6)],
                "asks": [[round(final_price * (1 + i*0.0001), 2), random.randint(100, 1500)] for i in range(1, 6)]
            },
            "metrics": {
                "volume_spike": volume_spike_ratio,
                "std_dev": std_dev_val,
                "vwap": round(final_price * (1 + random.uniform(-0.0005, 0.0005)), 4),
                "atr": atr_val,
                "volatility": round(std_dev_val * 0.8, 2),
                "mean_price": round(mean_price, 4)
            }
        }

    def _serialize_batch(self, batch_data: list) -> str:
        return json.dumps(batch_data, ensure_ascii=False)

    async def generate_hft_stream(self):
        self.is_running = True
        logger.info(f"🔌 Подключение к реалистичному WebSocket-шлюзу: {self.uri}")
        
        try:
            async with websockets.connect(self.uri) as ws:
                logger.info("🟢 Симулятор запущен. Генерация волнового внутридневного потока Мосбиржи...")
                
                tick_counter = 0
                loop = asyncio.get_running_loop()
                
                while self.is_running:
                    batch = [self._generate_single_tick() for _ in range(self.batch_size)]
                    serialized_data = await loop.run_in_executor(None, self._serialize_batch, batch)
                    
                    await ws.send(serialized_data)
                    tick_counter += self.batch_size

                    if tick_counter % 500 == 0:
                        logger.info(f"📊 [РЕАЛ-СИМУЛЯТОР] Транслировано {tick_counter} тиков. Поток стабилизирован.")

                    await asyncio.sleep(0.35)

        except Exception as e:
            logger.error(f"❌ Ошибка в конвейере симулятора: {e}")
        finally:
            self.is_running = False
# ======================================================================
# 2️⃣ ВЫСОКОСКОРОСТНОЙ ПЕРСИСТЕНТНЫЙ РИСК-МЕНЕДЖЕР (RISK_MANAGER)
# ======================================================================
# LOT_SIZES остаётся статическим справочником MOEX; новые заявки используют FINAM_TRADE_LOT_SIZES.
