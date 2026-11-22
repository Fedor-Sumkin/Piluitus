"""HTTP and WebSocket routes for the live dashboard and operator controls."""
from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any, Callable

from fastapi import APIRouter, Body, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse, HTMLResponse


def create_dashboard_router(
    *,
    anomaly_queue: Any,
    priority_queue_item_factory: Any,
    validate_daily_loss_limit: Callable[[Any], float],
    default_daily_loss_limit: float,
    template_dir: Any,
    manager: Any,
    logger: Any,
    get_dashboard_open_positions: Callable[[], Any],
    refresh_dashboard_balance_snapshot: Callable[[list[dict] | None], Any],
    db_manager: Any,
    execution_engine: Any,
    last_ai_verdicts_cache: list[dict[str, Any]],
    process_market_event: Callable[[Any], Any],
) -> APIRouter:
    """Bind dashboard endpoints to application services without owning their state."""
    router = APIRouter()

    @router.post("/api/v1/control/close_all")
    async def http_emergency_close_all():
        """Queue an emergency close command with the highest priority."""
        await anomaly_queue.put(priority_queue_item_factory.create(0, "CMD_CLOSE_ALL", {}))
        return {"status": "queued", "priority": "high_emergency"}

    @router.post("/api/v1/control/risk_limit")
    async def http_update_risk_limit(payload: dict = Body(...)):
        """Queue a validated daily-loss-limit update."""
        try:
            new_limit = validate_daily_loss_limit(
                payload.get("value", default_daily_loss_limit)
            )
        except ValueError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        await anomaly_queue.put(
            priority_queue_item_factory.create(
                0, "CMD_SET_RISK_LIMIT", {"value": new_limit}
            )
        )
        return {
            "status": "queued",
            "priority": "high_emergency",
            "new_target_limit": new_limit,
        }

    @router.get("/maintenance", response_class=HTMLResponse)
    async def read_maintenance_page():
        page_path = template_dir / "maintenance.html"
        if not page_path.is_file():
            return HTMLResponse("<h1>maintenance.html не найден</h1>", status_code=404)
        return FileResponse(page_path)

    @router.get("/", response_class=HTMLResponse)
    async def read_root():
        index_path = template_dir / "index.html"
        if not index_path.is_file():
            return HTMLResponse(
                content="<h1>404 – index.html не найден</h1><p>Поместите файл в папку interface/templates.</p>",
                status_code=404,
            )
        return FileResponse(index_path)

    @router.websocket("/ws/frontend")
    async def websocket_frontend_endpoint(websocket: WebSocket):
        await manager.connect(websocket)
        try:
            logger.info("🔌 К WS‑шлюзу подключился веб-интерфейс. Отправка стартового пакета...")
            initial_positions = await get_dashboard_open_positions()
            initial_balance = await refresh_dashboard_balance_snapshot(initial_positions)

            cached_news = []
            try:
                redis_inst = getattr(db_manager, "redis", getattr(db_manager, "_redis", None))
                if redis_inst is not None:
                    news_raw = await redis_inst.get("piluitus:news:latest")
                    if news_raw:
                        cached_news = json.loads(news_raw)
            except Exception:
                pass

            raw_closed = getattr(
                execution_engine,
                "closed_trades",
                getattr(execution_engine, "_closed_trades", []),
            )
            verdicts_for_client = [
                dict(item) for item in last_ai_verdicts_cache if isinstance(item, dict)
            ]
            try:
                redis_inst = getattr(db_manager, "redis", getattr(db_manager, "_redis", None))
                if redis_inst is not None:
                    for verdict in verdicts_for_client:
                        verdict_id = verdict.get("verdict_id")
                        if not verdict_id:
                            continue
                        execution_raw = await redis_inst.get(
                            f"piluitus:verdict:execution:{verdict_id}"
                        )
                        if execution_raw:
                            execution_update = json.loads(execution_raw)
                            if isinstance(execution_update, dict):
                                verdict.update(execution_update)
            except Exception as history_exc:
                logger.debug(
                    "Не удалось обогатить историю статусами исполнения: %s",
                    history_exc,
                )

            await websocket.send_json({
                "type": "INIT_DATA",
                "portfolio": {},
                "balance": initial_balance,
                "news": cached_news,
                "verdicts_history": verdicts_for_client,
                "active_positions": initial_positions,
                "closed_trades": raw_closed[-30:] if isinstance(raw_closed, list) else [],
            })

            while True:
                data = await websocket.receive_text()
                message = json.loads(data)

                if isinstance(message, list):
                    for single_message in message:
                        if isinstance(single_message, dict):
                            target_ticker = (
                                single_message.get("security_code")
                                or single_message.get("ticker")
                            )
                            price_val = (
                                single_message.get("price")
                                or single_message.get("last_price")
                            )
                            if target_ticker and price_val is not None:
                                mock_event = SimpleNamespace(
                                    security_code=str(target_ticker),
                                    price=float(price_val),
                                    volume=int(single_message.get("volume", 1000)),
                                )
                                asyncio.create_task(process_market_event(mock_event))
                    await asyncio.sleep(0.0001)

                elif isinstance(message, dict):
                    msg_type = message.get("type")
                    if msg_type == "USER_COMMAND_CLOSE_ALL":
                        await anomaly_queue.put(
                            priority_queue_item_factory.create(0, "CMD_CLOSE_ALL", {})
                        )
                        continue
                    elif msg_type == "USER_COMMAND_SET_RISK":
                        await anomaly_queue.put(
                            priority_queue_item_factory.create(
                                0,
                                "CMD_SET_RISK_LIMIT",
                                {"value": message.get("value", 0.10)},
                            )
                        )
                        continue

                    target_ticker = message.get("security_code") or message.get("ticker")
                    price_val = message.get("price") or message.get("last_price")
                    if target_ticker and price_val is not None:
                        mock_event = SimpleNamespace(
                            security_code=str(target_ticker),
                            price=float(price_val),
                            volume=int(message.get("volume", 1000)),
                        )
                        asyncio.create_task(process_market_event(mock_event))
                else:
                    logger.error(
                        "❌ Получено вебсокет-сообщение неизвестного типа: %s",
                        type(message),
                    )

        except WebSocketDisconnect:
            manager.disconnect(websocket)
        except Exception as ws_err:
            logger.error("⚠️ Ошибка WebSocket: %s", ws_err)

    return router
