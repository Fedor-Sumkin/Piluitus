"""Finam authentication, market-stream bridge and exchange-lot refresh tasks."""
from __future__ import annotations

import asyncio
import os
from typing import Any

from risk.risk_manager import RiskManager


class MarketBackgroundFinamMixin:
    async def background_broker_init(self):
        """
        Поддерживает Finam market-data stream независимо от дня недели.
        Торговые интервалы каждого инструмента проверяются отдельно через Finam schedule API;
        это также позволяет получать данные в предусмотренные MOEX сессии выходного дня.
        """
        self.logger.info("🔌 Проверка брокерского соединения Finam; календарь сессий будет проверяться по каждому инструменту...")
        try:
            await self.broker.connect()
            if bool(getattr(self.execution_engine, "paper_mode", True)):
                self.logger.info(
                    "🧪 [FINAM ACCOUNT] PAPER работает на внутреннем псевдосчёте; "
                    "Finam используется только для авторизации и рыночных данных. Account discovery не требуется."
                )
            else:
                selected_account_id = await self.broker.discover_accounts()
                if selected_account_id:
                    self.logger.info(
                        "🏦 [FINAM ACCOUNT] account_id успешно определён из текущей сессии; "
                        "source=%s; account_id=%s.",
                        getattr(self.broker, "account_id_source", "session"),
                        self.broker._mask_account_id(selected_account_id),
                    )
                else:
                    self.logger.error(
                        "⛔ [FINAM ACCOUNT] LIVE-счёт не определён; LIVE-входы заблокированы."
                    )
    
            # Verify both the authenticated endpoint and the schedule parser before
            # the first candidate reaches the executor. SBER is used only as a probe;
            # every actual order is checked against its own instrument schedule.
            try:
                schedule_probe = await self.execution_engine._market_session_status("SBER")
                self.logger.info(
                    "📅 [SESSION CALENDAR PROBE] SBER@MISX: known=%s, open=%s, entry_allowed=%s, "
                    "reason=%s, next_session=%s; actual entries are checked per instrument.",
                    schedule_probe.get("known"),
                    schedule_probe.get("is_open"),
                    schedule_probe.get("entry_allowed"),
                    schedule_probe.get("reason"),
                    schedule_probe.get("next_session_start", "-"),
                )
                if not schedule_probe.get("known"):
                    self.logger.warning(
                        "⛔ [SESSION CALENDAR] расписание Finam не подтверждено; "
                        "новые входы останутся заблокированы до успешной проверки."
                    )
            except Exception as schedule_probe_exc:
                self.logger.error(
                    "❌ [SESSION CALENDAR PROBE] ошибка проверки расписания Finam: %s",
                    type(schedule_probe_exc).__name__,
                    exc_info=True,
                )
    
            bridge_queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=50000)
            async def _run_market_stream():
                await self.broker.start_market_data_stream(
                    tickers=self.tickers,
                    queue=bridge_queue,
                    screener=self.screener,
                )
    
            async def _bridge_worker():
                while True:
                    try:
                        raw_event = await bridge_queue.get()
                        try:
                            await self.process_market_event(raw_event)
                        finally:
                            bridge_queue.task_done()
                    except Exception as bridge_err:
                        self.logger.error("❌ Ошибка моста WebSocket-котировок: %s", bridge_err, exc_info=True)
                        await asyncio.sleep(0.1)
    
            # TaskGroup ties both stream tasks to this parent task.
            async with asyncio.TaskGroup() as task_group:
                task_group.create_task(_run_market_stream(), name="finam-market-stream")
                task_group.create_task(_bridge_worker(), name="finam-bridge-worker")
                self.logger.info("⏳ Задача Finam WebSocket запущена; ожидаем HANDSHAKE_SUCCESS.")
        except Exception as exc:
            self.logger.error("❌ Ошибка инициализации API Finam: %s", exc, exc_info=True)
    
        while True:
            await asyncio.sleep(3600)
    
    async def _finam_metadata_refresh_loop(self, ) -> None:
        """Refresh exchange lots for PAPER or account-specific lots for LIVE."""
        from risk.risk_manager import RiskManager
    
        try:
            refresh_sec = max(300.0, float(os.getenv("FINAM_METADATA_REFRESH_SEC", "900")))
        except (TypeError, ValueError, OverflowError):
            refresh_sec = 900.0
        try:
            account_retry_sec = min(
                300.0,
                max(15.0, float(os.getenv("FINAM_ACCOUNT_DISCOVERY_RETRY_SEC", "60"))),
            )
        except (TypeError, ValueError, OverflowError):
            account_retry_sec = 60.0
        while True:
            # background_broker_init owns authentication. Do not send REST requests
            # until a JWT has been obtained successfully.
            if not str(getattr(self.broker, "jwt_token", "") or "").strip():
                await asyncio.sleep(1.0)
                continue
            paper_mode = bool(getattr(self.execution_engine, "paper_mode", True))
            account_id = str(getattr(self.broker, "account_id", "") or "").strip()
            metadata_account_id = str(
                getattr(self.broker, "metadata_account_id", "") or ""
            ).strip()
            if paper_mode:
                self.logger.info(
                    "[FINAM METADATA WORKER] PAPER LOTSIZE source=LOT_SIZES; "
                    "GetAsset MINSTEP/DECIMALS uses a separate session-authorized metadata context."
                )
            elif not account_id:
                discover = getattr(self.broker, "discover_accounts", None)
                if callable(discover):
                    try:
                        account_id = str(
                            await discover() or getattr(self.broker, "account_id", "") or ""
                        ).strip()
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        self.logger.warning(
                            "[FINAM METADATA WORKER] account discovery failed: %s",
                            type(exc).__name__,
                        )
            if not paper_mode and (not account_id or account_id.upper() == "UNKNOWN_ACCOUNT"):
                self.logger.warning(
                    "⛔ [FINAM METADATA WORKER] LIVE account_id не получен; LIVE-входы остаются заблокированы."
                )
                await asyncio.sleep(account_retry_sec)
                continue
            try:
                await RiskManager.update_finam_lot_sizes(
                    finam_broker_client=self.broker, paper_mode=paper_mode
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self.logger.error(
                    "❌ [FINAM METADATA WORKER] refresh failed: %s",
                    type(exc).__name__, exc_info=True,
                )
            confirmed = sum(
                1 for ticker in self.tickers
                if int(self.finam_trade_lot_sizes.get(str(ticker).strip().upper(), 0) or 0) > 0
            )
            # Retry account-context discovery sooner than the full metadata
            # refresh interval if the session did not provide a read-only ID.
            retry_delay = refresh_sec
            if paper_mode:
                metadata_account_id = str(
                    getattr(self.broker, "metadata_account_id", "") or ""
                ).strip()
                if not metadata_account_id:
                    retry_delay = account_retry_sec
                self.logger.info(
                    "📦 [FINAM METADATA WORKER] static LOTSIZE available=%d/%d; "
                    "Finam MINSTEP refresh attempted; metadata_context=%s; next refresh in %.0fs.",
                    confirmed, len(self.tickers), bool(metadata_account_id), retry_delay,
                )
            elif confirmed:
                self.logger.info(
                    "📦 [FINAM METADATA WORKER] confirmed account lots=%d/%d; next refresh in %.0fs.",
                    confirmed, len(self.tickers), refresh_sec,
                )
            await asyncio.sleep(retry_delay)
