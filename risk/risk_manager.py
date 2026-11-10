import asyncio
import json
import random
import logging
import math
import os
import time
from datetime import datetime, date, timedelta, timezone
from collections.abc import Callable
import websockets
import httpx
from db_manager import DBManager
from config import ASSET_METADATA, FINAM_TRADE_LOT_SIZES, LOT_SIZES, TICKERS_TO_TRADE

_MOSCOW_TZ = timezone(timedelta(hours=3), name="MSK")

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
# LOT_SIZES остаётся статическим справочником MOEX; новые заявки используют FINAM_TRADE_LOT_SIZES.

class RiskManager:
    def __init__(
        self,
        max_risk_per_trade=0.005,
        daily_loss_limit=0.015,
        initial_deposit=100000.0,
        drawdown_warning_pct=0.03,
        max_drawdown_pct=0.05,
        loss_streak_3_pause_sec=600,
        loss_streak_5_pause_sec=1800,
        loss_streak_hard_stop=5,
    ):
        self.initial_deposit = initial_deposit
        self.current_equity = initial_deposit
        self.free_margin = initial_deposit
        self.total_session_pnl = 0.0
        self.realized_pnl = 0.0
        self.open_pnl = 0.0
        self.high_water_mark = max(0.0, float(initial_deposit))
        self.current_drawdown_pct = 0.0
        self.max_risk_per_trade = max_risk_per_trade
        self.daily_loss_limit = daily_loss_limit
        self.drawdown_warning_pct = max(0.0, min(float(drawdown_warning_pct), 0.50))
        self.max_drawdown_pct = max(
            self.drawdown_warning_pct, min(float(max_drawdown_pct), 0.50)
        )
        self.loss_streak_3_pause_sec = max(0, int(loss_streak_3_pause_sec))
        self.loss_streak_5_pause_sec = max(
            self.loss_streak_3_pause_sec, int(loss_streak_5_pause_sec)
        )
        self.loss_streak_hard_stop = max(3, int(loss_streak_hard_stop))
        self.block_trading = False
        self._drawdown_warning_active = False
        self._loss_streak_count = 0
        self._loss_pause_signature: str | None = None
        self._last_loss_streak_diagnostic_signature: str | None = None
        self._last_risk_snapshot_monotonic = 0.0
        self._cached_trading_allowed: bool | None = None
        self._balance_initialized = False
        self._blocked_date: date | None = None
        self.open_pnl_provider: Callable[[], float] | None = None
        self.open_assets_cost_provider: Callable[[], float] | None = None

    @classmethod
    async def update_finam_lot_sizes(cls, finam_broker_client=None):
        """Refresh trading lot sizes from Finam; retain config values only as fallback."""
        risk_logger = logging.getLogger("Piluitus.RiskManager")
        if not LOT_SIZES and not TICKERS_TO_TRADE:
            risk_logger.error("❌ Список инструментов пуст: Finam metadata refresh невозможен.")
            return
        if finam_broker_client is None or not callable(getattr(finam_broker_client, "get_asset_params", None)):
            risk_logger.error(
                "Finam GetAssetParams unavailable: account-specific trade_lot_size cannot be confirmed; "
                "new orders remain blocked. Static LOT_SIZES is not used for new entries."
            )
            return
        account_id = str(os.getenv("FINAM_CLIENT_ID", "")).strip()
        asset_method = getattr(finam_broker_client, "get_asset", None)
        params_method = getattr(finam_broker_client, "get_asset_params", None)
        if not callable(asset_method):
            risk_logger.warning("📐 [ASSET METADATA] Finam GetAsset unavailable; static tick-size metadata retained.")
        if not callable(params_method):
            risk_logger.error(
                "Finam GetAssetParams unavailable; no account-specific lot values will authorize new orders."
            )

        # Finam documents a 200 REST requests/minute limit. Bound the two
        # per-instrument lookups to avoid an uncontrolled startup burst.
        semaphore = asyncio.Semaphore(6)
        request_lock = asyncio.Lock()
        try:
            request_interval_sec = float(os.getenv("FINAM_METADATA_REQUEST_INTERVAL_SEC", "0.36"))
        except (TypeError, ValueError, OverflowError):
            request_interval_sec = 0.36
        if not math.isfinite(request_interval_sec) or request_interval_sec < 0.0:
            request_interval_sec = 0.36
        next_request_at = 0.0
        updated_lots = failed_lots = updated_metadata = failed_metadata = 0

        async def call_limited(method, ticker: str):
            nonlocal next_request_at
            async with request_lock:
                wait_sec = max(0.0, next_request_at - time.monotonic())
                if wait_sec > 0.0:
                    await asyncio.sleep(wait_sec)
                next_request_at = time.monotonic() + request_interval_sec
            return await method(ticker, account_id=account_id)

        async def refresh_one(ticker: str) -> None:
            nonlocal updated_lots, failed_lots, updated_metadata, failed_metadata
            normalized = str(ticker or "").strip().upper().split("@", 1)[0]
            if not normalized:
                return
            async with semaphore:
                asset_payload = None
                params_payload = None
                if callable(asset_method):
                    try:
                        asset_payload = await call_limited(asset_method, normalized)
                    except Exception:
                        asset_payload = None
                if callable(params_method):
                    try:
                        params_payload = await call_limited(params_method, normalized)
                    except Exception:
                        params_payload = None

            if isinstance(asset_payload, dict):
                try:
                    decimals = int(asset_payload.get("decimals", -1))
                    min_step = int(float(asset_payload.get("min_step", 0)))
                    lot_raw = asset_payload.get("lot_size")
                    exchange_lot = float(lot_raw.get("value")) if isinstance(lot_raw, dict) else float(lot_raw or 0)
                    mic = str(asset_payload.get("mic") or "").strip().upper()
                    if decimals >= 0 and min_step > 0 and mic in {"", "MISX"}:
                        ASSET_METADATA[normalized] = {
                            "symbol": str(asset_payload.get("symbol") or f"{normalized}@MISX"),
                            "decimals": decimals,
                            "min_step": min_step,
                            "price_tick": min_step / (10 ** decimals),
                            "lot_size": exchange_lot,
                            "quote_currency": str(asset_payload.get("quote_currency") or ""),
                            "metadata_source": "FINAM_GET_ASSET",
                        }
                        updated_metadata += 1
                    else:
                        failed_metadata += 1
                except (TypeError, ValueError, OverflowError):
                    failed_metadata += 1
            elif callable(asset_method):
                failed_metadata += 1

            if isinstance(params_payload, dict):
                raw = params_payload.get("trade_lot_size")
                try:
                    value_float = float(raw)
                    value = int(value_float)
                    if not math.isfinite(value_float) or value_float != value:
                        value = 0
                except (TypeError, ValueError, OverflowError):
                    value = 0
                if value > 0:
                    FINAM_TRADE_LOT_SIZES[normalized] = value
                    updated_lots += 1
                else:
                    # Finam documents zero/missing trade_lot_size as unavailable.
                    # Do not silently substitute the static exchange-lot reference.
                    FINAM_TRADE_LOT_SIZES.pop(normalized, None)
                    failed_lots += 1
            elif callable(params_method):
                failed_lots += 1

        symbols_to_refresh = list(dict.fromkeys(list(LOT_SIZES) + list(TICKERS_TO_TRADE)))
        await asyncio.gather(*(refresh_one(ticker) for ticker in symbols_to_refresh))
        risk_logger.info(
            "📦 [FINAM METADATA] account trade_lot_size confirmed=%d/%d; unavailable=%d (new entries blocked); "
            "tick-size=%d/%d; metadata unavailable=%d (static tick-size retained).",
            updated_lots, len(symbols_to_refresh), failed_lots,
            updated_metadata, len(symbols_to_refresh), failed_metadata,
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

    async def _sync_configured_deposit(self, redis_inst) -> None:
        """Keep persisted start capital aligned with configured account capital.

        TRADING_DEPOSIT is authoritative for this run. Only the deposit metadata key
        is synchronized; trades, positions, verdicts, and risk history are untouched.
        """
        risk_logger = logging.getLogger("Piluitus.RiskManager")
        key = "piluitus:risk:persistent_balance"
        try:
            configured = float(self.initial_deposit)
        except (TypeError, ValueError, OverflowError) as exc:
            raise RuntimeError("Некорректный стартовый депозит в конфигурации.") from exc
        if not math.isfinite(configured) or configured < 0.0:
            raise RuntimeError("Стартовый депозит должен быть конечным неотрицательным числом.")

        saved_raw = await redis_inst.get(key)
        if saved_raw is None:
            await redis_inst.set(key, f"{configured:.2f}")
            risk_logger.info(
                "🆕 [PERSISTENCE] Зафиксирован стартовый депозит из конфигурации: %.2f ₽",
                configured,
            )
            return

        try:
            saved = float(saved_raw)
        except (TypeError, ValueError, OverflowError):
            saved = float("nan")

        if not math.isfinite(saved) or saved < 0.0 or not math.isclose(
            saved, configured, rel_tol=0.0, abs_tol=0.01
        ):
            await redis_inst.set(key, f"{configured:.2f}")
            risk_logger.warning(
                "⚠️ [PERSISTENCE] Сохранённый депозит (%r ₽) отличается от конфигурации "
                "(%.2f ₽); используем TRADING_DEPOSIT и синхронизируем только ключ %s. "
                "Журнал сделок и остальные данные Redis сохранены.",
                saved_raw,
                configured,
                key,
            )
            return

        risk_logger.info(
            "💾 [PERSISTENCE] Депозит Redis согласован с конфигурацией: %.2f ₽",
            configured,
        )

    async def is_trading_allowed(
        self, db: DBManager, *, force_refresh: bool = False
    ) -> bool:
        """Apply daily loss, high-water drawdown and global loss-streak safeguards.

        force_refresh is used at the final execution boundary so a candidate
        approved before a new loss-streak lock cannot slip through a cached
        pre-LLM risk decision.
        """
        risk_logger = logging.getLogger("Piluitus.RiskManager")
        now = datetime.now(_MOSCOW_TZ).replace(tzinfo=None)
        today = now.date()
        now_monotonic = time.monotonic()

        # Full journal scans are cached briefly to avoid one SCAN per market tick.
        if (
            not force_refresh
            and self._cached_trading_allowed is not None
            and now_monotonic - self._last_risk_snapshot_monotonic < 1.0
            and self._blocked_date in (None, today)
        ):
            return self._cached_trading_allowed

        def _cache_result(allowed: bool) -> bool:
            self._cached_trading_allowed = allowed
            self._last_risk_snapshot_monotonic = time.monotonic()
            return allowed

        if self._blocked_date is not None and self._blocked_date != today:
            self.block_trading = False
            self._blocked_date = None
            self._loss_pause_signature = None
            self._last_loss_streak_diagnostic_signature = None
            risk_logger.warning(
                "🌅 Новая торговая дата MSK: дневная блокировка и серия убытков текущей сессии "
                "сбрасываются; долгосрочный HWM и накопленная просадка сохраняются."
            )
        if self.block_trading:
            return _cache_result(False)

        def _parse_trade_datetime(trade: dict) -> datetime | None:
            for field in (
                "exit_timestamp", "closed_at", "timestamp",
                "entry_timestamp", "created_at",
            ):
                raw = trade.get(field)
                if not raw:
                    continue
                try:
                    parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                    if parsed.tzinfo is not None:
                        parsed = parsed.astimezone(_MOSCOW_TZ).replace(tzinfo=None)
                    # Naive legacy timestamps were produced by the trading process;
                    # interpret them in Moscow exchange time, not the host PC timezone.
                    return parsed
                except (TypeError, ValueError, OverflowError):
                    continue
            return None

        try:
            redis_inst = getattr(db, "redis", getattr(db, "_redis", None))
            if redis_inst is None:
                raise RuntimeError("Redis/Memurai недоступен при проверке риска.")

            if not self._balance_initialized:
                await self._sync_configured_deposit(redis_inst)
                self._balance_initialized = True

            get_all_trades = getattr(db, "get_all_daily_trades", None)
            if not callable(get_all_trades):
                raise RuntimeError("DBManager не предоставляет полный индекс сделок.")
            all_trade_rows = await get_all_trades()
            if not isinstance(all_trade_rows, dict):
                raise RuntimeError("Полный индекс сделок имеет некорректный формат.")

            expected_mode = (
                "PAPER"
                if os.getenv("PAPER_TRADING", "True").strip().lower() in {"1", "true", "yes", "on"}
                else "LIVE"
            )
            trades_by_id: dict[str, dict] = {}
            for order_id, trade in all_trade_rows.items():
                if not isinstance(trade, dict):
                    continue
                mode = str(trade.get("mode", "")).upper()
                if mode and mode != expected_mode:
                    continue
                identity = str(trade.get("id") or order_id).strip()
                if identity:
                    trades_by_id[identity] = dict(trade)

            # Merge bounded history only for legacy records absent from the per-order ledger.
            history = await redis_inst.lrange("piluitus:trade_history", 0, -1)
            for index, raw_trade in enumerate(history or []):
                try:
                    trade = json.loads(raw_trade) if isinstance(raw_trade, (str, bytes, bytearray)) else raw_trade
                except (TypeError, ValueError, json.JSONDecodeError):
                    risk_logger.warning("Пропущена некорректная строка trade_history[%d].", index)
                    continue
                if not isinstance(trade, dict):
                    continue
                mode = str(trade.get("mode", "")).upper()
                if mode and mode != expected_mode:
                    continue
                identity = str(trade.get("id") or trade.get("order_id") or "").strip()
                if not identity:
                    identity = "legacy:" + "|".join(
                        str(trade.get(field, ""))
                        for field in ("exit_timestamp", "ticker", "side", "entry_price", "exit_price", "pnl")
                    )
                trades_by_id.setdefault(identity, dict(trade))

            closed_trades: list[tuple[datetime, dict]] = []
            total_daily_pnl_rub = 0.0
            total_realized_pnl_rub = 0.0
            for trade in trades_by_id.values():
                if str(trade.get("status", "")).upper() != "CLOSED":
                    continue
                try:
                    trade_pnl = float(trade.get("pnl", 0.0) or 0.0)
                except (TypeError, ValueError, OverflowError) as pnl_exc:
                    raise RuntimeError("В закрытой сделке обнаружен некорректный PnL.") from pnl_exc
                if not math.isfinite(trade_pnl):
                    raise RuntimeError("В закрытой сделке обнаружен нечисловой PnL.")
                total_realized_pnl_rub += trade_pnl
                closed_at = _parse_trade_datetime(trade)
                if closed_at is not None:
                    closed_trades.append((closed_at, trade))
                    if closed_at.date() == today:
                        total_daily_pnl_rub += trade_pnl

            journal = await db.get_active_positions()
            total_open_pnl_rub = 0.0
            if self.open_pnl_provider is None and isinstance(journal, dict):
                for trade in journal.values():
                    if not isinstance(trade, dict):
                        continue
                    position_pnl = float(trade.get("pnl", 0.0) or 0.0)
                    if not math.isfinite(position_pnl):
                        raise RuntimeError("Нечисловой плавающий PnL открытой позиции.")
                    total_open_pnl_rub += position_pnl
                    opened_at = _parse_trade_datetime(trade)
                    if opened_at is None or opened_at.date() == today:
                        total_daily_pnl_rub += position_pnl

            if self.open_pnl_provider is not None:
                try:
                    open_pnl = float(self.open_pnl_provider())
                    if not math.isfinite(open_pnl):
                        raise ValueError("open PnL is not finite")
                    total_open_pnl_rub += open_pnl
                    total_daily_pnl_rub += open_pnl
                except Exception as provider_exc:
                    raise RuntimeError(f"Не удалось получить плавающий PnL: {provider_exc}") from provider_exc

            self.realized_pnl = round(total_realized_pnl_rub, 2)
            self.open_pnl = round(total_open_pnl_rub, 2)
            self.total_session_pnl = round(total_realized_pnl_rub + total_open_pnl_rub, 2)
            self.current_equity = round(self.initial_deposit + total_realized_pnl_rub + total_open_pnl_rub, 2)

            if self.open_assets_cost_provider is not None:
                try:
                    assets_cost = float(self.open_assets_cost_provider())
                    self.free_margin = (
                        round(self.initial_deposit + self.realized_pnl - assets_cost, 2)
                        if math.isfinite(assets_cost) and assets_cost >= 0.0 else 0.0
                    )
                except Exception as provider_exc:
                    risk_logger.warning("Не удалось получить себестоимость открытых PAPER-позиций: %s", provider_exc)
                    self.free_margin = 0.0
            else:
                self.free_margin = self.current_equity

            await redis_inst.set(
                "piluitus:risk:balance_state",
                json.dumps({
                    "equity": self.current_equity,
                    "free_margin": self.free_margin,
                    "pnl": self.total_session_pnl,
                    "realized_pnl": self.realized_pnl,
                    "open_pnl": self.open_pnl,
                }),
            )

            hwm_key = f"piluitus:risk:high_water_mark:{expected_mode}"
            raw_hwm = await redis_inst.get(hwm_key)
            if raw_hwm is None:
                # On first deployment, rebuild the realized equity peak from the
                # full close sequence instead of treating current equity as the peak.
                reconstructed_equity = float(self.initial_deposit)
                reconstructed_high_water_mark = reconstructed_equity
                for closed_at, closed_trade in sorted(closed_trades, key=lambda row: row[0]):
                    historical_pnl = float(closed_trade.get("pnl", 0.0) or 0.0)
                    reconstructed_equity += historical_pnl
                    reconstructed_high_water_mark = max(
                        reconstructed_high_water_mark, reconstructed_equity
                    )
                high_water_mark = max(
                    reconstructed_high_water_mark, self.current_equity
                )
                risk_logger.info(
                    "[EQUITY DRAWDOWN] HWM впервые восстановлен из полного журнала: %.2f ₽.",
                    high_water_mark,
                )
            else:
                high_water_mark = float(raw_hwm)
                if not math.isfinite(high_water_mark) or high_water_mark <= 0.0:
                    raise RuntimeError("Некорректный equity high-water mark.")
            high_water_mark = max(high_water_mark, self.current_equity)
            self.high_water_mark = round(high_water_mark, 2)
            self.current_drawdown_pct = max(0.0, (high_water_mark - self.current_equity) / high_water_mark)
            await redis_inst.set(hwm_key, f"{self.high_water_mark:.2f}")

            if self.current_drawdown_pct >= self.drawdown_warning_pct:
                if not self._drawdown_warning_active:
                    risk_logger.warning(
                        "⚠️ [EQUITY DRAWDOWN] Просадка от HWM %.2f ₽ достигла %.2f%% "
                        "(equity=%.2f ₽, порог=%.2f%%).",
                        self.high_water_mark, self.current_drawdown_pct * 100.0,
                        self.current_equity, self.drawdown_warning_pct * 100.0,
                    )
                self._drawdown_warning_active = True
            else:
                self._drawdown_warning_active = False

            if self.current_drawdown_pct >= self.max_drawdown_pct:
                risk_logger.critical(
                    "🛑 [EQUITY DRAWDOWN GUARD] Просадка %.2f%% от HWM достигла лимита %.2f%%; "
                    "новые входы заблокированы, открытые позиции продолжают сопровождаться.",
                    self.current_drawdown_pct * 100.0, self.max_drawdown_pct * 100.0,
                )
                return _cache_result(False)

            closed_trades.sort(key=lambda row: row[0])
            # Consecutive-loss pauses are scoped to the current Moscow trading
            # date. Older losses remain in realized PnL and HWM drawdown, but a
            # Friday streak must not automatically pause Monday's first entry.
            session_closed_trades = [
                row for row in closed_trades if row[0].date() == today
            ]
            loss_streak = 0
            latest_loss_at: datetime | None = None
            recent_loss_rows: list[tuple[datetime, dict]] = []
            for closed_at, trade in reversed(session_closed_trades):
                pnl = float(trade.get("pnl", 0.0) or 0.0)
                if pnl >= 0.0:
                    break
                loss_streak += 1
                recent_loss_rows.append((closed_at, trade))
                if latest_loss_at is None:
                    latest_loss_at = closed_at
            self._loss_streak_count = loss_streak

            # Keep the pause durations unchanged, but scope the consecutive-loss
            # trigger to this Moscow trading date and audit the exact source trades.
            diagnostic_signature = (
                f"{today.isoformat()}:{expected_mode}:{loss_streak}:"
                f"{latest_loss_at.isoformat() if latest_loss_at is not None else 'none'}"
            )
            if diagnostic_signature != self._last_loss_streak_diagnostic_signature:
                recent_details = []
                for closed_at, trade in recent_loss_rows[:7]:
                    recent_details.append(
                        f"{str(trade.get('ticker') or '?').upper()}/"
                        f"{str(trade.get('side') or '?').upper()}:"
                        f"{float(trade.get('pnl', 0.0) or 0.0):+.2f}₽@"
                        f"{closed_at.isoformat(timespec='seconds')}"
                        f"[mode={str(trade.get('mode') or 'LEGACY_UNTAGGED').upper()},"
                        f"id={str(trade.get('id') or trade.get('order_id') or '?')[:16]}]"
                    )
                risk_logger.info(
                    "[SESSION LOSS STREAK AUDIT] session_date=%s mode=%s; consecutive_losses=%d; "
                    "latest_loss=%s; recent_losses_newest_first=%s; "
                    "historical_drawdown_uses_full_closed_journal=true",
                    today.isoformat(), expected_mode, loss_streak,
                    latest_loss_at.isoformat(timespec="seconds") if latest_loss_at else "нет",
                    " | ".join(recent_details) if recent_details else "нет",
                )
                self._last_loss_streak_diagnostic_signature = diagnostic_signature

            if loss_streak >= self.loss_streak_hard_stop:
                signature = f"{today.isoformat()}:{expected_mode}:{loss_streak}:{latest_loss_at.isoformat() if latest_loss_at else 'none'}"
                if signature != self._loss_pause_signature:
                    risk_logger.critical(
                        "🛑 [SESSION LOSS HARD STOP] session_date=%s mode=%s; "
                        "%d последовательных убыточных сделок (порог=%d). "
                        "Новые входы заблокированы до следующей торговой даты MSK.",
                        today.isoformat(), expected_mode, loss_streak,
                        self.loss_streak_hard_stop,
                    )
                    self._loss_pause_signature = signature
                self.block_trading = True
                self._blocked_date = today
                return _cache_result(False)

            pause_sec = (
                self.loss_streak_5_pause_sec if loss_streak >= 5
                else self.loss_streak_3_pause_sec if loss_streak >= 3
                else 0
            )
            if pause_sec > 0 and latest_loss_at is not None:
                pause_until = latest_loss_at + timedelta(seconds=pause_sec)
                if now < pause_until:
                    signature = f"{today.isoformat()}:{loss_streak}:{latest_loss_at.isoformat()}:{pause_sec}"
                    if signature != self._loss_pause_signature:
                        risk_logger.critical(
                            "🧊 [SESSION LOSS STREAK] session_date=%s mode=%s; %d убыточных сделок подряд "
                            "в текущей торговой сессии; последний убыток=%s; "
                            "новые входы на паузе ещё %.0f сек.",
                            today.isoformat(), expected_mode, loss_streak,
                            latest_loss_at.isoformat(timespec="seconds"),
                            max(0.0, (pause_until - now).total_seconds()),
                        )
                        self._loss_pause_signature = signature
                    return _cache_result(False)
            self._loss_pause_signature = None

            max_allowed_loss_money = self.initial_deposit * self.daily_loss_limit
            if total_daily_pnl_rub < 0.0 and abs(total_daily_pnl_rub) >= max_allowed_loss_money:
                risk_logger.critical(
                    "🛑 [РИСК МЕНЕДЖЕР] Достигнут дневной лимит убытка: PnL=%.2f ₽, "
                    "лимит=-%.2f ₽. Новые входы заблокированы до следующей даты.",
                    total_daily_pnl_rub, max_allowed_loss_money,
                )
                self.block_trading = True
                self._blocked_date = today
                return _cache_result(False)

            risk_logger.debug(
                "[MONEY MANAGEMENT] equity=%.2f ₽ HWM=%.2f ₽ drawdown=%.2f%% "
                "realized=%.2f ₽ open=%.2f ₽ daily=%.2f ₽ loss_streak=%d.",
                self.current_equity, self.high_water_mark,
                self.current_drawdown_pct * 100.0, self.realized_pnl,
                self.open_pnl, total_daily_pnl_rub, self._loss_streak_count,
            )
            return _cache_result(True)

        except Exception as exc:
            risk_logger.critical(
                "Ошибка проверки Money Management/полного журнала: %s. "
                "Новые входы заблокированы (fail-closed).",
                exc, exc_info=True,
            )
            self.block_trading = True
            self._blocked_date = today
            return _cache_result(False)

    def calculate_lot_size(self, ticker: str, current_price: float, atr_stop: float) -> int:
        """Return affordable lots under the risk cap; return 0 if even one lot exceeds it."""
        if current_price <= 0 or atr_stop <= 0:
            return 0
        risk_per_share_rub = abs(current_price - atr_stop)
        if risk_per_share_rub <= 0:
            return 0
        allowed_risk_money = max(0.0, self.initial_deposit * self.max_risk_per_trade)
        normalized_ticker = str(ticker or "").strip().upper().split("@", 1)[0]
        raw_lot_size = FINAM_TRADE_LOT_SIZES.get(normalized_ticker)
        try:
            lot_size = int(raw_lot_size)
        except (TypeError, ValueError, OverflowError):
            risk_logger = logging.getLogger("Piluitus.RiskManager")
            risk_logger.error(
                "Размер позиции заблокирован: Finam trade_lot_size для %s не подтверждён; "
                "статический LOT_SIZES не используется для нового ордера.",
                normalized_ticker,
            )
            return 0
        if lot_size <= 0:
            return 0
        risk_per_lot_rub = risk_per_share_rub * lot_size
        notional_per_lot = float(current_price) * lot_size
        if risk_per_lot_rub <= 0 or notional_per_lot <= 0 or allowed_risk_money < risk_per_lot_rub:
            return 0
        lots_by_risk = int(allowed_risk_money // risk_per_lot_rub)
        # Pre-limit is intentionally capped at 3% per instrument. ExecutionLayer
        # applies the final aggregate / single-position guard atomically.
        try:
            max_single_pct = float(os.getenv("PILUITUS_MAX_SINGLE_POSITION_PCT", "0.03"))
        except (TypeError, ValueError, OverflowError):
            max_single_pct = 0.03
        if not math.isfinite(max_single_pct) or max_single_pct <= 0:
            max_single_pct = 0.03
        max_single_pct = min(max_single_pct, 0.03)
        lots_by_notional = int((self.initial_deposit * max_single_pct) // notional_per_lot)
        return max(0, min(lots_by_risk, lots_by_notional))

if __name__ == "__main__":
    emulator = RealMarketSimulator(uri="ws://127.0.0.1:8080/ws/frontend", batch_size=10)
    try:
        asyncio.run(emulator.generate_hft_stream())
    except KeyboardInterrupt:
        logger.info("🛑 Симулятор торгов успешно остановлен.")
