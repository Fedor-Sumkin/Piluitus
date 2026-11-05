import asyncio
import json
import random
import logging
import math
import os
from datetime import datetime, date
from collections.abc import Callable
import websockets
import httpx
from db_manager import DBManager
from config import LOT_SIZES

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("Piluitus.RealMarketSim")

# ======================================================================
# 1️⃣ РЕАЛИСТИЧНЫЙ СИМУЛЯТОР РЫНКА (HFT_STRESS_TESTER)
# ======================================================================
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
# Спецификация лотности Мосбиржи для точного перевода штук в лоты
LOT_SIZES = {}

class RiskManager:
    def __init__(self, max_risk_per_trade=0.005, daily_loss_limit=0.10, initial_deposit=100000.0):
        self.initial_deposit = initial_deposit
        
        # Контур ОЗУ-метрик, которые теперь восстанавливаются из Redis
        self.current_equity = initial_deposit
        self.free_margin = initial_deposit
        self.total_session_pnl = 0.0
        self.realized_pnl = 0.0
        self.open_pnl = 0.0
        
        self.max_risk_per_trade = max_risk_per_trade
        self.daily_loss_limit = daily_loss_limit
        self.block_trading = False
        
        # Флаг, указывающий, прогрет ли баланс историческими данными из Redis
        self._balance_initialized = False
        self._blocked_date: date | None = None
        # Optional in-process unrealized PnL provider (wired to PAPER positions by main.py).
        self.open_pnl_provider: Callable[[], float] | None = None
        self.open_assets_cost_provider: Callable[[], float] | None = None

    @classmethod
    async def update_finam_lot_sizes(cls, finam_broker_client=None):
        """Проверяет готовность единого справочника LOT_SIZES из config.py."""
        risk_logger = logging.getLogger("Piluitus.RiskManager")
        if not LOT_SIZES:
            risk_logger.error("❌ Справочник LOT_SIZES в config.py пуст; расчёт размера позиции будет ограничен.")
            return
        risk_logger.info(
            "📦 [STATIC CONTOUR] Лотность загружена из config.py: %d инструментов.",
            len(LOT_SIZES),
        )



    async def _load_balance_from_broker(self, finam_broker_client) -> float:
        """
        🚀 ЗАГЛУШКА НА БУДУЩЕЕ: Прямой запрос баланса (Equity) у брокера Финам.
        Раскомментировать и настроить, когда перейдем на реальный счет.
        """
        try:
            # response = await finam_broker_client.get_portfolio()
            # return float(response.get("equity") or response.get("balance", self.initial_deposit))
            return self.initial_deposit
        except Exception:
            return self.initial_deposit

    async def is_trading_allowed(self, db: DBManager) -> bool:
        """
        Проверяет текущую дневную просадку и обеспечивает ПЕРСИСТЕНТНОСТЬ баланса.
        Данные сохраняются в Redis и выдерживают перезагрузку ПК.
        """
        risk_logger = logging.getLogger("Piluitus.RiskManager")
        today = datetime.now().date()
        if self._blocked_date is not None and self._blocked_date != today:
            self.block_trading = False
            self._blocked_date = None
            risk_logger.warning("🌅 Новая календарная дата: дневная блокировка риска снята; расчёт PnL начат заново.")
        if self.block_trading:
            return False

        try:
            # Извлекаем защищенный инстанс Redis
            redis_inst = getattr(db, "redis", getattr(db, "_redis", None))
            if redis_inst is None:
                risk_logger.critical("🛑 Хранилище риска недоступно: новые сделки заблокированы (fail-closed).")
                self.block_trading = True
                self._blocked_date = today
                return False

            # 1. ПЕРСИСТЕНТНАЯ ИНИЦИАЛИЗАЦИЯ: Восстановление баланса при старте сессии
            if not self._balance_initialized:
                saved_balance = await redis_inst.get("piluitus:risk:persistent_balance")
                if saved_balance is not None:
                    self.initial_deposit = float(saved_balance)
                    risk_logger.info(f"💾 [PERSISTENCE] Баланс восстановлен из Redis после перезагрузки: {self.initial_deposit:.2f} ₽")
                else:
                    # Если запускаемся впервые в истории — берем дефолтный депозит и фиксируем его в базу
                    await redis_inst.set("piluitus:risk:persistent_balance", str(self.initial_deposit))
                    risk_logger.info(f"🆕 [PERSISTENCE] Зафиксирован стартовый депозит сессии: {self.initial_deposit:.2f} ₽")
                
                self._balance_initialized = True

            # 2. Считаем PnL только за сегодняшнюю календарную дату, а не за всю
            # историю из Redis/AOF. Это важно для корректного дневного стоп-лимита.
            journal = await db.get_active_positions()
            history = await redis_inst.lrange("piluitus:trade_history", 0, -1)
            total_daily_pnl_rub = 0.0
            total_open_pnl_rub = 0.0
            total_realized_pnl_rub = 0.0

            def _trade_date(trade: dict) -> date | None:
                for field in ("exit_timestamp", "entry_timestamp", "timestamp", "closed_at", "created_at"):
                    raw = trade.get(field)
                    if not raw:
                        continue
                    try:
                        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                        return parsed.date()
                    except (TypeError, ValueError):
                        continue
                return None

            # PAPER: in-memory monitor is authoritative for floating PnL. Redis
            # OPEN rows describe the same positions and must not be added again.
            # Other modes retain the journal-based open-PnL path.
            if self.open_pnl_provider is None and journal and isinstance(journal, dict):
                for trade in journal.values():
                    if not isinstance(trade, dict):
                        continue
                    trade_date = _trade_date(trade)
                    try:
                        position_pnl = float(trade.get("pnl", 0.0) or 0.0)
                    except (TypeError, ValueError):
                        position_pnl = 0.0
                    total_open_pnl_rub += position_pnl
                    if trade_date is None or trade_date == today:
                        total_daily_pnl_rub += position_pnl

            if self.open_pnl_provider is not None:
                try:
                    open_pnl = float(self.open_pnl_provider())
                    if math.isfinite(open_pnl):
                        total_open_pnl_rub += open_pnl
                        total_daily_pnl_rub += open_pnl
                except Exception as provider_exc:
                    risk_logger.warning("Не удалось получить текущий PnL открытых PAPER-позиций: %s", provider_exc)

            # PnL истории участвует в суммарной оценке капитала на всём доступном
            # участке журнала, но в daily-loss cutoff входит только сегодняшняя дата.
            if history:
                for raw_trade in history:
                    try:
                        trade = json.loads(raw_trade) if isinstance(raw_trade, str) else raw_trade
                        if not isinstance(trade, dict):
                            continue
                        trade_pnl = float(trade.get("pnl", 0.0) or 0.0)
                        total_realized_pnl_rub += trade_pnl
                        if _trade_date(trade) == today:
                            total_daily_pnl_rub += trade_pnl
                    except (TypeError, ValueError, json.JSONDecodeError):
                        continue

            # 3. Разделяем реализованный и плавающий PnL.
            # Свободные средства = стартовый депозит + реализованный PnL
            # минус себестоимость открытых позиций. Плавающий PnL входит
            # в equity/общую доходность, но не изменяет денежный остаток
            # до фактического закрытия позиции.
            self.realized_pnl = round(total_realized_pnl_rub, 2)
            self.open_pnl = round(total_open_pnl_rub, 2)
            total_session_pnl_rub = total_realized_pnl_rub + total_open_pnl_rub
            self.total_session_pnl = round(total_session_pnl_rub, 2)
            self.current_equity = round(self.initial_deposit + total_session_pnl_rub, 2)
            if self.open_assets_cost_provider is not None:
                try:
                    assets_cost = float(self.open_assets_cost_provider())
                    if math.isfinite(assets_cost) and assets_cost >= 0.0:
                        self.free_margin = round(
                            self.initial_deposit + self.realized_pnl - assets_cost, 2
                        )
                    else:
                        self.free_margin = 0.0
                except Exception as provider_exc:
                    risk_logger.warning("Не удалось получить себестоимость открытых PAPER-позиций: %s", provider_exc)
                    self.free_margin = 0.0
            else:
                self.free_margin = self.current_equity

            # Каждую миллисекунду перезаписываем стейт баланса в Redis (защита от выключения ПК)
            # Этот ключ main.py также считывает при INIT_DATA фронтенда
            balance_state_payload = {
                "equity": self.current_equity,
                "free_margin": self.free_margin,
                "pnl": self.total_session_pnl,
                "realized_pnl": self.realized_pnl,
                "open_pnl": self.open_pnl
            }
            await redis_inst.set("piluitus:risk:balance_state", json.dumps(balance_state_payload))

            # Предельный лимит потерь в рублях от стартовой точки
            max_allowed_loss_money = self.initial_deposit * self.daily_loss_limit

            if total_daily_pnl_rub < 0 and abs(total_daily_pnl_rub) >= max_allowed_loss_money:
                risk_logger.critical(
                    f"🛑 [РИСК МЕНЕДЖЕР] ДОСТИГНУТ ЛИМИТ ДНЕВНОЙ ПРОСАДКИ! "
                    f"Чистый убыток сессии: {total_daily_pnl_rub:.2f}₽ >= Лимит: -{max_allowed_loss_money:.2f}₽. БЛОКИРОВКА ТОРГОВ."
                )
                self.block_trading = True
                self._blocked_date = today
                return False

            return True

        except Exception as e:
            risk_logger.critical("Ошибка проверки риска/хранилища: %s. Новые сделки блокируются.", e, exc_info=True)
            self.block_trading = True
            self._blocked_date = datetime.now().date()
            return False

    def calculate_lot_size(self, ticker: str, current_price: float, atr_stop: float) -> int:
        """Return affordable lots under the risk cap; return 0 if even one lot exceeds it."""
        if current_price <= 0 or atr_stop <= 0:
            return 0
        risk_per_share_rub = abs(current_price - atr_stop)
        if risk_per_share_rub <= 0:
            return 0
        allowed_risk_money = max(0.0, self.initial_deposit * self.max_risk_per_trade)
        lot_size = max(int(LOT_SIZES.get(ticker, 1)), 1)
        risk_per_lot_rub = risk_per_share_rub * lot_size
        notional_per_lot = float(current_price) * lot_size
        if risk_per_lot_rub <= 0 or notional_per_lot <= 0 or allowed_risk_money < risk_per_lot_rub:
            return 0
        lots_by_risk = int(allowed_risk_money // risk_per_lot_rub)
        # Pre-limit is intentionally capped at 5% per instrument. ExecutionLayer
        # applies the final 15% aggregate / 5% single-position guard atomically.
        try:
            max_single_pct = float(os.getenv("PILUITUS_MAX_SINGLE_POSITION_PCT", "0.05"))
        except (TypeError, ValueError, OverflowError):
            max_single_pct = 0.05
        if not math.isfinite(max_single_pct) or max_single_pct <= 0:
            max_single_pct = 0.05
        max_single_pct = min(max_single_pct, 0.05)
        lots_by_notional = int((self.initial_deposit * max_single_pct) // notional_per_lot)
        return max(0, min(lots_by_risk, lots_by_notional))

if __name__ == "__main__":
    emulator = RealMarketSimulator(uri="ws://127.0.0.1:8080/ws/frontend", batch_size=10)
    try:
        asyncio.run(emulator.generate_hft_stream())
    except KeyboardInterrupt:
        logger.info("🛑 Симулятор торгов успешно остановлен.")
