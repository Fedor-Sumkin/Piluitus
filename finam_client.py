import asyncio
import logging
from datetime import datetime, timezone
from typing import Any
from finam_trade_api.client import Client, TokenManager
import httpx

logger = logging.getLogger("FinamCore.NativeBroker")
JOURNAL_FILE = "trading_journal.json"

class FinamBrokerClient:
    def __init__(self, token: str, is_demo: bool = True):
        self.token = token
        # Единый промышленный хост REST v2 шлюзов Финам
        self.host = "api.finam.ru"
        self.base_url = f"https://{self.host}"
        self.jwt_token = ""
        self.headers: dict[str, str] = {}
        self.is_demo = is_demo
        self.client: Any = None

    async def connect(self):
        """Инициализация клиента Финам с защитой от блокировки главного потока ядра // PILUITUS V2"""
        token_manager = TokenManager(token=self.token)
        self.client = Client(token_manger=token_manager, auto_refresh_tokens=True)
        
        self.headers = {
            "X-Api-Key": self.token,
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
            "Content-Type": "application/json"
        }
        
        logger.info("Установка сетевого соединения с серверами Финам...")
        
        # Инкапсулируем тяжелую авторизацию SDK во внутреннюю функцию
        async def run_auth():
            if hasattr(self.client, 'access_tokens'):
                access_module = getattr(self.client, 'access_tokens')
                if hasattr(access_module, '_client') and getattr(access_module, '_client'):
                    http_client = getattr(access_module, '_client')
                    if hasattr(http_client, 'timeout'):
                        setattr(http_client, 'timeout', httpx.Timeout(5.0))
                
                # Попытка нативного вызова gRPC/REST авторизации Финама
                await access_module.set_jwt_token()

        while True:
            try:
                # Ограничиваем жестким таймаутом в 4 секунды сам процесс авторизации сокета
                await asyncio.wait_for(run_auth(), timeout=4.0)
                logger.info("✅ Успешная авторизация в Finam Trade API. REST-интерфейсы SDK готовы.")
                break
            except Exception as e:
                logger.warning(f"⚠️ Сетевое ожидание авторизации Финам: {e}. Принудительно пробиваем шлюз...")
                # Принудительно выходим из цикла — резервные HTTP2 каналы пуллинга 
                # и ордеров в finam_client запустятся сами, не вешая ядро робота
                break

    async def _fetch_single_ticker_hft(self, market_service, ticker: str, callback_function):
        """Параллельный воркер: опрашивает одну акцию через нативный одиночный метод SDK Finam"""
        try:
            class MarketTickEvent:
                def __init__(self, security_code, price, volume):
                    self.security_code = security_code
                    self.price = price        
                    self.volume = volume      

            # Вызываем гарантированно существующий одиночный метод вашей версии SDK
            single_call = market_service.get_last_quote(f"{ticker}@MISX")
            resp = await single_call if asyncio.iscoroutine(single_call) else single_call
            
            if not resp:
                return

            quote_obj = getattr(resp, 'quote', resp if isinstance(resp, dict) else None)
            if not quote_obj:
                return

            def get_numeric_value(struct) -> float:
                if struct is None: return 0.0
                if isinstance(struct, dict): return float(struct.get('value', struct))
                if hasattr(struct, 'value'): return float(getattr(struct, 'value', 0.0))
                return float(struct)

            # Вытаскиваем числовое значение цены тика
            price_struct = quote_obj.get('price', None) if isinstance(quote_obj, dict) else getattr(quote_obj, 'price', None)
            price = get_numeric_value(price_struct)
            
            # Резервный поиск цены по смежным полям (last/close)
            if price == 0.0:
                for alt in ['last', 'close', 'last_price', 'lastPrice']:
                    alt_struct = quote_obj.get(alt) if isinstance(quote_obj, dict) else getattr(quote_obj, alt, None)
                    if alt_struct:
                        price = get_numeric_value(alt_struct)
                        if price > 0.0: break
                        
            # Вытаскиваем тиковый объем лотов
            vol_struct = quote_obj.get('volume', None) if isinstance(quote_obj, dict) else getattr(quote_obj, 'volume', None)
            volume = int(get_numeric_value(vol_struct)) if vol_struct else 10
            
            if price > 0.0:
                event = MarketTickEvent(security_code=ticker, price=price, volume=volume)
                await callback_function(event)
                
        except Exception:
            pass  # Микро-сбои гасим, чтобы не тормозить Event Loop во время ралли

    async def start_market_data_stream(self, tickers: list, callback_function):
        """Высокопроизводительный асинхронный веерный пуллинг цен (Конкурентный HFT Fan-Out)"""
        logger.info("🚀 ЗАПУСК ВЕЕРНОГО REST-КОНВЕЙЕРА ЦЕН PILUITUS V2...")
        
        market_service = getattr(self.client, 'instruments', None)
        if market_service is None:
            logger.error("🛑 Нативный сервис инструментов SDK Финам недоступен.")
            return

        # Проверяем наличие базового одиночного метода опроса в ядре вашего SDK
        if not hasattr(market_service, 'get_last_quote') and not hasattr(market_service, 'get_quote'):
            logger.error("🛑 В вашем SDK Финам полностью отсутствуют методы запроса котировок.")
            return

        logger.info(f"✅ Веерный HFT-контур успешно захватил метод одиночного опроса. Запуск...")

        while True:
            try:
                # Опрашиваем ВСЕ 87 акций Мосбиржи одновременно в параллельных задачах!
                tasks = [self._fetch_single_ticker_hft(market_service, ticker, callback_function) for ticker in tickers]
                await asyncio.gather(*tasks)

            except asyncio.CancelledError:
                raise
            except Exception as e:
                logger.error(f"Ошибка в цикле веерного пуллинга: {e}")

            # Защитная HFT пауза между кругами веерного опроса Мосбиржи (350 миллисекунд)
            await asyncio.sleep(0.35)
    async def get_ticker_order_book(self, ticker: str) -> dict | None:
        """3. Получение текущего стакана по инструменту через нативный SDK Финама (gRPC/REST)"""
        try:
            md_service = getattr(self.client, 'market_data', None) or getattr(self.client, 'instruments', None)
            if md_service and hasattr(md_service, 'get_order_book'):
                res_call = md_service.get_order_book(security_board="TQBR", security_code=ticker, order_book_depth=3)
                response = await res_call if asyncio.iscoroutine(res_call) else res_call
                
                if response:
                    bids = getattr(response, 'bids', [])
                    asks = getattr(response, 'asks', [])
                    return {
                        "bids": [{"price": float(getattr(b, 'price', b)), "volume": int(getattr(b, 'quantity', b))} for b in bids],
                        "asks": [{"price": float(getattr(a, 'price', a)), "volume": int(getattr(a, 'quantity', a))} for a in asks]
                    }
        except Exception as e:
            logger.debug(f"Резервный переход на HTTP-запрос стакана из-за: {e}")
            
        # Защитный HTTP-канал, если gRPC-модуль стакана не собран в данной версии SDK
        try:
            url = f"{self.base_url}/v1/marketdata/orderbook"
            params = {"securityBoard": "TQBR", "securityCode": ticker}
            async with httpx.AsyncClient(timeout=5.0, http2=True) as client:
                response = await client.get(url, headers=self.headers, params=params)
                if response.status_code == 200:
                    data = response.json()
                    ob = data.get("data", data.get("orderBook", data))
                    return {
                        "bids": [{"price": float(b.get("price", 0.0)), "volume": int(b.get("quantity", 0))} for b in ob.get("bids", [])[:3]],
                        "asks": [{"price": float(a.get("price", 0.0)), "volume": int(a.get("quantity", 0))} for a in ob.get("asks", [])[:3]]
                    }
        except Exception as http_err:
            logger.error(f"Не удалось получить стакан по тикеру {ticker}: {http_err}")
        return {"bids": [], "asks": []}

    async def place_order(self, ticker: str, lots: int, side: str, price: float, client_id: str):
        """4. Выставление биржевой заявки через оригинальный SDK Финам (OrdersService) с HTTP-резервом"""
        try:
            orders_service = getattr(self.client, 'orders', None)
            if not orders_service:
                logger.error("❌ В SDK Финам не найден нативный orders_service.")
                return None

            order_side = "BUY" if side.upper() == "BUY" else "SELL"
            
            order_params = {
                "account_id": client_id,
                "security_board": "TQBR",
                "security_code": ticker,
                "buy_sell": order_side,
                "quantity": int(lots),
                "use_credit": True
            }
            
            if price > 0.0:
                order_params["price"] = float(price)
                if hasattr(orders_service, 'place_limit_order'):
                    res_call = orders_service.place_limit_order(**order_params)
                else:
                    order_params["type"] = "LIMIT"
                    res_call = orders_service.place_order(**order_params)
            else:
                if hasattr(orders_service, 'place_market_order'):
                    res_call = orders_service.place_market_order(**order_params)
                else:
                    order_params["type"] = "MARKET"
                    res_call = orders_service.place_order(**order_params)

            logger.info(f"🚀 ТРЕНД-КОНВЕЙЕР: Отправка нативного ордера SDK {side} {lots} лотов {ticker}...")
            response = await res_call if asyncio.iscoroutine(res_call) else res_call
            
            if response:
                logger.info("✅ Нативный ордер успешно принят шлюзом Финама через SDK.")
                return response
                
        except Exception as sdk_err:
            logger.error(f"❌ Сбой отправки ордера через SDK: {sdk_err}. Пробуем канонический HTTP-резерв...")
            
        # Защитный HTTP-канал: Строгий канонический URL со слешем на конце во избежание 308 редиректа!
        try:
            url = f"{self.base_url}/v1/orders/"
            
            payload = {
                "clientCode": client_id,
                "securityBoard": "TQBR",
                "securityCode": ticker,
                "buySell": "BUY" if side.upper() == "BUY" else "SELL",
                "quantity": int(lots),
                "useCredit": True,
                "property": "PUT_IN_QUEUE"
            }
            if price > 0.0:
                payload["price"] = float(price)
                payload["type"] = "LIMIT"
            else:
                payload["type"] = "MARKET"
            
            async with httpx.AsyncClient(timeout=10.0, http2=True) as client:
                response = await client.post(url, headers=self.headers, json=payload)
                
                # Прямое логическое сравнение, защищенное от сбоев разметки
                if response.status_code == 200 or response.status_code == 201:
                    logger.info("✅ Нативный ордер успешно принят шлюзом Финама по резервному HTTP.")
                    return response.json()
                else:
                    logger.error(f"❌ Финам отклонил резервный HTTP ордер: {response.status_code} | {response.text}")

        except Exception as http_err:
            logger.error(f"Критический сбой всех каналов отправки ордера: {http_err}")
        return None

    async def get_portfolio(self, client_id: str):
        """5. Получение информации по портфелю аккаунта через оригинальный SDK"""
        try:
            portfolio_service = getattr(self.client, 'portfolio', None) or getattr(self.client, 'accounts', None)
            if portfolio_service and hasattr(portfolio_service, 'get_portfolio'):
                res_call = portfolio_service.get_portfolio(client_code=client_id)
                response = await res_call if asyncio.iscoroutine(res_call) else res_call
                if response:
                    return response
        except Exception as e:
            logger.debug(f"Резервный переход на HTTP-запрос портфеля из-за: {e}")
            
        try:
            url = f"{self.base_url}/public/api/v1/portfolio"
            params = {"clientCode": client_id}
            async with httpx.AsyncClient(timeout=10.0, http2=True) as client:
                response = await client.get(url, headers=self.headers, params=params)
                if response.status_code == 200:
                    return response.json()
        except Exception as http_err:
            logger.error(f"Ошибка получения нативного портфеля: {http_err}")
        return None

    async def market_close_all_positions(self, client_id: str):
        """🚨 Полная нативная ликвидация портфеля встречными маркет-ордерами."""
        logger.warning(f"🚨 ИНИЦИИРОВАНА НА ТИВНАЯ ЭКСТРЕННАЯ ЛИКВИДАЦИЯ ПОРТФЕЛЯ {client_id}!")
        portfolio_data = await self.get_portfolio(client_id=client_id)
        if not portfolio_data:
            return

        positions = getattr(portfolio_data, 'positions', []) if not isinstance(portfolio_data, dict) else portfolio_data.get("positions", [])
        for pos in positions:
            ticker = getattr(pos, 'security_code', '') if not isinstance(pos, dict) else pos.get("securityCode", "")
            balance = int(float(getattr(pos, 'balance', 0) if not isinstance(pos, dict) else pos.get("balance", 0)))
            if balance == 0 or not ticker:
                continue

            side = "SELL" if balance > 0 else "BUY"
            lots_to_close = abs(balance)
            await self.place_order(ticker=ticker, lots=lots_to_close, side=side, price=0.0, client_id=client_id)
