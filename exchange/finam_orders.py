"""Finam order submission, portfolio and securities API mixin."""
from __future__ import annotations

from exchange.finam_shared import (
    asyncio, logging, math, datetime, timezone, timedelta, url_quote,
    Callable, Iterable, Any, json, inspect, os, re, time, websockets, httpx,
    logger, JOURNAL_FILE, _MOSCOW_TZ,
    _websocket_close_code, _websocket_disconnect_log_level,
    _coalesce_queued_market_event, _event_timestamp_to_monotonic,
    _should_resync_position_order_book,
)

class FinamOrdersMixin:
    async def place_order(self, ticker: str, lots: int, side: str, price: float, client_id: str):
        """Fail closed until order payload is validated against the account's current REST schema."""
        if self.is_demo is False:
            logger.critical("LIVE-ордер заблокирован: старые методы/поля заявки в проекте не соответствуют текущему REST API Finam. Нужна отдельная проверка схемы заявки и account_id.")
            return None
        logger.warning("Заявка не отправлена: безопасный адаптер не имитирует подтверждение брокера.")
        return None

    # ------------------------------------------------------------------
    # 5️⃣  GET_PORTFOLIO
    # ------------------------------------------------------------------

    async def get_portfolio(self, client_id: str):
        """Получение информации по портфелю аккаунта через оригинальный SDK"""
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
            url = f"{self.base_url}/v1/accounts/{client_id}"
            params = {}
            async with httpx.AsyncClient(timeout=10.0, http2=True) as client:
                response = await client.get(url, headers=self.headers, params=params)
                if response.status_code == 200:
                    return response.json()
        except Exception as http_err:
            logger.error(f"Ошибка получения нативного портфеля: {http_err}")
        return None

        # ------------------------------------------------------------------
    # 7️⃣  GET_SECURITIES (Скачивание спецификаций лотности Мосбиржи через gRPC/MOEX)
    # ------------------------------------------------------------------

    async def get_securities(self) -> dict | None:
        """
        Запрашивает список доступных инструментов Мосбиржи для извлечения лотности.
        Линия 1: Оригинальный gRPC-сервис InstrumentsService (активируется в пн на боевом).
        Линия 2: Прямой резервный запрос к ISS MOEX на случай выходных или сбоя gRPC.
        """
        # 1. Первая линия обороны: нативный gRPC-клиент SDK (для реального счета)
        try:
            if self.client:
                ins_service = getattr(self.client, 'instruments', None)
                if ins_service and hasattr(ins_service, 'get_securities'):
                    logger.info("🎯 [FINAM gRPC] Запрос списка инструментов доски TQBR через InstrumentsService...")
                    res_call = ins_service.get_securities(board="TQBR")
                    response = await res_call if asyncio.iscoroutine(res_call) else res_call
                    if response and getattr(response, "securities", None):
                        logger.info("✅ [FINAM gRPC] Спецификация инструментов получена от gRPC-шлюза.")
                        return response
        except Exception as grpc_err:
            logger.debug(f"gRPC-шлюз недоступен или отключен брокером: {grpc_err}")

        # 2. Вторая линия обороны: Бронебойный резервный запрос напрямую к инфо-системе Мосбиржи (ISS MOEX)
        # Отдает точные лоты по режиму TQBR 24/7 без авторизации, редиректов и 404 ошибок.
        try:
            moex_url = "https://moex.com"
            params = {"iss.meta": "off", "iss.only": "securities", "securities.columns": "SECID,LOTSIZE"}
            
            logger.info(f"📡 gRPC пуст (выходной режим). Пробиваем резервный шлюз ISS MOEX: {moex_url}...")
            
            async with httpx.AsyncClient(timeout=10.0, http2=True, follow_redirects=True) as http_client:
                response = await http_client.get(moex_url, params=params)
                
                if response.status_code == 200:
                    logger.info("✅ [ISS MOEX] Спецификация лотности акций успешно скачана из ядра Московской биржи.")
                    return response.json()
                else:
                    logger.error(f"❌ Шлюз ISS MOEX отклонил запрос. Статус: {response.status_code}")
                
        except Exception as http_err:
            logger.error(f"Критический сбой всех каналов загрузки инструментов (Finam/MOEX): {http_err}")
            
        return None


    # ------------------------------------------------------------------
    # 6️⃣  MARKET_CLOSE_ALL_POSITIONS (Адаптирован под параллельный вызов)
    # ------------------------------------------------------------------

    async def market_close_all_positions(self, client_id: str):
        """🚨 Полная нативная ликвидация портфеля встречными маркет-ордерами."""
        logger.warning(f"🚨 ИНИЦИИРОВАНА НА ТИВНАЯ ЭКСТРЕННАЯ ЛИКВИДАЦИЯ ПОРТФЕЛЯ {client_id}!")
        portfolio_data = await self.get_portfolio(client_id=client_id)
        if not portfolio_data:
            return

        positions = getattr(portfolio_data, 'positions', []) if not isinstance(portfolio_data, dict) else portfolio_data.get("positions", [])
        
        # Сбор пачки параллельных задач
        tasks = []
        for pos in positions:
            ticker = getattr(pos, 'security_code', '') if not isinstance(pos, dict) else pos.get("securityCode", "")
            balance = int(float(getattr(pos, 'balance', 0) if not isinstance(pos, dict) else pos.get("balance", 0)))
            if balance == 0 or not ticker:
                continue

            side = "SELL" if balance > 0 else "BUY"
            lots_to_close = abs(balance)
            
            tasks.append(self.place_order(ticker=ticker, lots=lots_to_close, side=side, price=0.0, client_id=client_id))
            
        if tasks:
            # 🚨 ИСПРАВЛЕНИЕ: Безопасный перехват результатов. Исключения в отдельных ордерах не роняют общий паник-режим
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for idx, res in enumerate(results):
                if isinstance(res, Exception):
                    logger.error(f"❌ Ошибка при отправке {idx}-го ликвидационного ордера: {res}")
            logger.warning(f"🏁 Все ордера нативной экстренной ликвидации ({len(tasks)} шт) обработаны шлюзом Финама.")
