"""Finam WebSocket market-data stream lifecycle and event dispatch mixin."""
from __future__ import annotations

from exchange.finam_stream_session import wait_for_finam_handshake, send_quote_subscription_batches
from exchange.finam_stream_setup import (
    build_quote_batches, build_quote_subscription_plan, resolve_websocket_header_argument,
)
from exchange.finam_stream_support import (
    FinamOrderBookSymbolDiagnostics, FinamPositionSnapshotRefresher, FinamQueueOverflowTracker,
)
from exchange.finam_shared import (
    asyncio, logging, math, datetime, timezone, timedelta, url_quote,
    Callable, Iterable, Any, json, inspect, os, re, time, websockets, httpx,
    logger, JOURNAL_FILE, _MOSCOW_TZ,
    _websocket_close_code, _websocket_disconnect_log_level,
    _coalesce_queued_market_event, _event_timestamp_to_monotonic,
    _should_resync_position_order_book,
)


class FinamMarketDataMixin:
    async def start_market_data_stream(self, tickers: list, queue: asyncio.Queue, screener=None):
        """Подключиться к Finam WS и подписать инструменты небольшими группами.

        Размер одного запроса задаётся переменной FINAM_WS_SYMBOL_BATCH_SIZE
        (по умолчанию 10). Одно WebSocket-соединение может обслуживать несколько
        подписок, поэтому большой список не отправляется одним запросом.
        """
        if not self.jwt_token:
            raise RuntimeError("Сначала вызовите broker.connect() для получения JWT.")

        header_argument = resolve_websocket_header_argument(websockets.connect)
        symbols, batch_size, batches = build_quote_subscription_plan(tickers)
        logger.info(
            "Подготовлено %d уникальных инструментов Finam; подписка будет отправлена "
            "%d запросами по не более %d символов.",
            len(symbols), len(batches), batch_size,
        )

        self.is_ws_running = True
        self._market_stream_connected = False
        backoff = 1
        refresh_token_before_retry = False

        while self.is_ws_running:
            try:
                if refresh_token_before_retry:
                    await self.connect()
                    refresh_token_before_retry = False

                # `websockets.connect` имеет разные типы заголовков в разных версиях:
                # extra_headers (v12) и additional_headers (новые версии).
                # Объявляем вызываемый объект как Any: динамический **kwargs иначе
                # ошибочно сопоставляется Pylance с create_protocol в старых stubs.
                header_kwargs: dict[str, Any] = {
                    header_argument: {"Authorization": self.jwt_token}
                }
                connect_fn: Any = websockets.connect
                self._order_book_state.clear()
                async with connect_fn(
                    self.ws_endpoint,
                    **header_kwargs,
                    ping_interval=20,
                    ping_timeout=10,
                    open_timeout=10,
                ) as ws:
                    logger.info(
                        "TCP/TLS WebSocket Finam установлен; ожидаем HANDSHAKE_SUCCESS (websockets %s).",
                        getattr(websockets, "__version__", "unknown"),
                    )

                    await wait_for_finam_handshake(ws, logger)
                    self._market_stream_connected = True

                    # Отправляем несколько SUBSCRIBE на одном сокете.
                    # Документация Finam допускает несколько подписок на одном соединении.
                    await send_quote_subscription_batches(
                        ws, self.jwt_token, batches, logger
                    )

                    logger.info(
                        "Все %d запросов подписки отправлены; ожидаем поток котировок.",
                        len(batches),
                    )
                    backoff = 1

                    # Enable strict trade-data mode before the first QUOTES event.
                    # Actual INSTRUMENT_TRADES subscriptions follow live Top-15 plus
                    # open positions, avoiding 88 unnecessary high-rate streams.
                    if screener is not None:
                        screener.enable_trade_data_mode()
                    logger.info("📈 Finam INSTRUMENT_TRADES: dynamic Top-15/position subscriptions enabled; QUOTES.lastSize excluded from trade volume.")
                    backoff = 1

                    # Диагностические счётчики: отправка SUBSCRIBE сама по себе не
                    # означает, что сервер уже прислал полезные данные.
                    data_messages = 0
                    enqueued_quotes = 0
                    enqueued_order_book_events = 0
                    enqueued_bbo_side_updates = 0
                    first_data_logged = False
                    queue_overflow_tracker = FinamQueueOverflowTracker()
                    first_order_book_logged = False
                    first_trade_logged = False
                    instrument_trade_messages = 0
                    instrument_trade_events = 0
                    instrument_trade_queue_dropped = 0
                    instrument_trade_queue_backpressure = 0
                    order_book_events_coalesced = 0
                    quote_events_coalesced = 0
                    last_trade_diag = asyncio.get_running_loop().time()
                    order_book_decode_warned = False
                    order_book_messages = 0
                    order_book_decode_failures = 0
                    order_book_valid_no_bbo_delta = 0
                    order_book_invalid_states = 0
                    order_book_crossed_episodes = 0
                    order_book_invalid_reasons: dict[str, int] = {}
                    order_book_invalid_by_ticker: dict[str, int] = {}
                    order_book_missing_envelope_timestamp = 0
                    order_book_queue_dropped = 0
                    order_book_out_of_order = 0
                    order_book_out_of_order_age_deltas_ms: list[float] = []
                    order_book_subscribed: set[str] = set()
                    order_book_subscribed_since: dict[str, float] = {}
                    trade_subscribed: set[str] = set()
                    trade_subscribed_since: dict[str, float] = {}
                    last_trade_subscription_sync = 0.0
                    try:
                        trade_subscription_sync_sec = max(2.0, float(os.getenv("FINAM_INSTRUMENT_TRADES_REFRESH_SEC", "5")))
                        trade_subscription_top_n = max(1, min(15, int(os.getenv("FINAM_INSTRUMENT_TRADES_TOP_N", "15"))))
                        trade_subscription_min_hold_sec = max(0.0, min(120.0, float(os.getenv("FINAM_INSTRUMENT_TRADES_MIN_HOLD_SEC", "60"))))
                        trade_subscription_max_overbook = max(0, min(10, int(os.getenv("FINAM_INSTRUMENT_TRADES_MAX_OVERBOOK", "5"))))
                    except (TypeError, ValueError, OverflowError):
                        trade_subscription_sync_sec, trade_subscription_top_n = 5.0, 15
                        trade_subscription_min_hold_sec, trade_subscription_max_overbook = 60.0, 5
                    order_book_symbol_diagnostics = FinamOrderBookSymbolDiagnostics()
                    last_open_position_symbols: set[str] = set()
                    order_book_unattributed_messages = 0
                    order_book_unattributed_sample = ""
                    order_book_parser_sample = ""

                    last_order_book_sync = 0.0
                    last_order_book_diag = asyncio.get_running_loop().time()
                    last_ws_data_at = last_order_book_diag
                    position_snapshot_inflight: set[str] = set()
                    position_snapshot_last_at: dict[str, float] = {}
                    try:
                        crossed_resubscribe_after_sec = max(2.0, float(os.getenv("FINAM_CROSSED_BBO_RESUBSCRIBE_AFTER_SEC", "5")))
                        crossed_resubscribe_cooldown_sec = max(10.0, float(os.getenv("FINAM_CROSSED_BBO_RESUBSCRIBE_COOLDOWN_SEC", "30")))
                        position_rest_refresh_interval_sec = max(3.0, float(os.getenv("FINAM_POSITION_REST_BBO_REFRESH_SEC", "15")))
                        position_rest_refresh_trigger_age_sec = max(0.5, float(os.getenv("FINAM_POSITION_REST_BBO_TRIGGER_AGE_SEC", "1.5")))
                        position_ws_idle_resync_sec = max(1.0, float(os.getenv("FINAM_POSITION_WS_IDLE_RESYNC_SEC", "3")))
                    except (TypeError, ValueError):
                        crossed_resubscribe_after_sec = 5.0
                        crossed_resubscribe_cooldown_sec = 30.0
                        position_rest_refresh_interval_sec = 15.0
                        position_rest_refresh_trigger_age_sec = 1.5
                        position_ws_idle_resync_sec = 3.0

                    logger.info(
                        "[ORDER_BOOK POSITION POLICY] stale_trigger=%.2fs ws_idle_resync=%.2fs "
                        "resync_cooldown=%.2fs rest_control_interval=%.2fs",
                        position_rest_refresh_trigger_age_sec,
                        position_ws_idle_resync_sec,
                        crossed_resubscribe_cooldown_sec,
                        position_rest_refresh_interval_sec,
                    )

                    position_snapshot_refresher = FinamPositionSnapshotRefresher(
                        self, queue, position_snapshot_inflight
                    )
                    last_no_data_warning_at = 0.0
                    try:
                        order_book_refresh_sec = max(1.0, float(os.getenv("FINAM_ORDER_BOOK_REFRESH_SEC", "1")))
                        order_book_limit = max(1, min(15, int(os.getenv("FINAM_ORDER_BOOK_TOP_N", "15"))))
                        order_book_min_hold_sec = max(0.0, min(120.0, float(os.getenv("FINAM_ORDER_BOOK_MIN_HOLD_SEC", "20"))))
                        order_book_max_overbook = max(0, min(15, int(os.getenv("FINAM_ORDER_BOOK_MAX_OVERBOOK", "5"))))
                    except (TypeError, ValueError):
                        order_book_refresh_sec, order_book_limit = 10.0, 15
                        order_book_min_hold_sec, order_book_max_overbook = 20.0, 5

                    while self.is_ws_running:
                        # Keep ORDER_BOOK subscriptions aligned with the current top-N.
                        # QUOTES for the full universe continue independently.
                        now_loop = asyncio.get_running_loop().time()
                        if screener is not None:
                            try:
                                funnel_status = screener.get_status()
                                # Keep currently eligible members subscribed during
                                # short coverage dips; screener readiness still gates LLM.
                                selected = funnel_status.get("selected_tickers", [])
                                # Recalculate the desired set on every receive-loop
                                # turn. A Top-15 rotation should not wait for the
                                # periodic refresh timer before its BBO subscription
                                # is requested (the timer is only a safety resync).
                                # Top-N remains the only screener/LLM candidate set.
                                # ORDER_BOOK additionally retains every open position for exit monitoring.
                                normalized_selected: list[str] = []
                                seen_selected: set[str] = set()
                                for raw_ticker in selected:
                                    ticker = str(raw_ticker).strip().upper().split("@")[0]
                                    if not ticker or ticker in seen_selected:
                                        continue
                                    seen_selected.add(ticker)
                                    normalized_selected.append(f"{ticker}@MISX")
                                    if len(normalized_selected) >= order_book_limit:
                                        break
                                # The screener/LLM universe remains strictly Top-N.
                                # Position monitoring has a separate technical requirement:
                                # every open position must keep receiving BBO after it leaves Top-N.
                                wanted = set(normalized_selected)
                                position_symbols: set[str] = set()
                                if self.open_positions_provider is not None:
                                    try:
                                        raw_positions = self.open_positions_provider()
                                        seen_positions: set[str] = set()
                                        for raw_ticker in raw_positions:
                                            ticker = str(raw_ticker).strip().upper().split("@")[0]
                                            if not ticker or ticker in seen_positions:
                                                continue
                                            seen_positions.add(ticker)
                                            position_symbols.add(f"{ticker}@MISX")
                                    except Exception as position_exc:
                                        logger.warning(
                                            "Finam ORDER_BOOK: не удалось получить тикеры открытых позиций; "
                                            "подписки Top-%d сохраняются: %s",
                                            order_book_limit, position_exc,
                                        )
                                # Position diagnostics use bare tickers because parser statistics
                                # are keyed by normalized ticker (for example, "LSRG", not "LSRG@MISX").
                                last_open_position_symbols = {
                                    symbol.split("@", 1)[0].strip().upper()
                                    for symbol in position_symbols
                                }
                                position_only_symbols = position_symbols - wanted
                                wanted.update(position_symbols)

                                # Trade flow is required only for active Top-15 and open
                                # positions. Keep a bounded overlap while symbols rotate.
                                trade_wanted = set(normalized_selected[:trade_subscription_top_n]) | position_symbols
                                effective_trade_wanted, protected_trade = retain_subscription_overlap(
                                    trade_subscribed, trade_wanted, trade_subscribed_since, now_loop,
                                    trade_subscription_min_hold_sec, trade_subscription_max_overbook,
                                )
                                trade_sync_due = now_loop - last_trade_subscription_sync >= trade_subscription_sync_sec
                                if effective_trade_wanted != trade_subscribed or trade_sync_due:
                                    removed_trade = trade_subscribed - effective_trade_wanted
                                    added_trade = effective_trade_wanted - trade_subscribed
                                    for trade_symbol in sorted(removed_trade):
                                        await ws.send(json.dumps({
                                            "action": "UNSUBSCRIBE", "type": "INSTRUMENT_TRADES",
                                            "data": {"symbol": trade_symbol}, "token": self.jwt_token,
                                        }, ensure_ascii=False))
                                        if screener is not None:
                                            reset_subscription = getattr(screener, "reset_trade_subscription", None)
                                            if callable(reset_subscription):
                                                reset_subscription(trade_symbol)
                                            else:
                                                screener.reset_trade_history(trade_symbol)
                                        trade_subscribed.discard(trade_symbol)
                                        trade_subscribed_since.pop(trade_symbol, None)
                                    for trade_symbol in sorted(added_trade):
                                        if screener is not None:
                                            reset_subscription = getattr(screener, "reset_trade_subscription", None)
                                            if callable(reset_subscription):
                                                reset_subscription(trade_symbol)
                                            else:
                                                screener.reset_trade_history(trade_symbol)
                                        await ws.send(json.dumps({
                                            "action": "SUBSCRIBE", "type": "INSTRUMENT_TRADES",
                                            "data": {"symbol": trade_symbol}, "token": self.jwt_token,
                                        }, ensure_ascii=False))
                                        trade_subscribed.add(trade_symbol)
                                        trade_subscribed_since[trade_symbol] = now_loop
                                    last_trade_subscription_sync = now_loop
                                    if added_trade or removed_trade:
                                        logger.info(
                                            "📈 Finam INSTRUMENT_TRADES subscriptions: Top-%d=%d, positions=%d, active=%d, added=%d, removed=%d, held=%d.",
                                            trade_subscription_top_n, len(normalized_selected[:trade_subscription_top_n]),
                                            len(position_symbols), len(trade_subscribed), len(added_trade),
                                            len(removed_trade), len(protected_trade),
                                        )

                                # Positions need continuously executable BBO even outside Top-15.
                                # Refresh stale/missing sides with a coherent control snapshot.
                                for position_symbol in sorted(position_symbols):
                                    position_ticker = position_symbol.split("@", 1)[0].strip().upper()
                                    if (
                                        not position_ticker
                                        or position_ticker in position_snapshot_inflight
                                        or now_loop - position_snapshot_last_at.get(position_ticker, 0.0)
                                        < position_rest_refresh_interval_sec
                                    ):
                                        continue
                                    state = self._order_book_state.get(position_ticker, {})
                                    bid_stamp = state.get("bid_bbo_timestamp")
                                    ask_stamp = state.get("ask_bbo_timestamp")
                                    bid_seen = _event_timestamp_to_monotonic(bid_stamp, now_loop)
                                    ask_seen = _event_timestamp_to_monotonic(ask_stamp, now_loop)
                                    bid_age = now_loop - bid_seen if bid_stamp else math.inf
                                    ask_age = now_loop - ask_seen if ask_stamp else math.inf
                                    book_event_seen = float(state.get("last_event_monotonic", 0.0) or 0.0)
                                    book_event_age = (
                                        now_loop - book_event_seen if book_event_seen > 0.0 else math.inf
                                    )
                                    should_resync = _should_resync_position_order_book(
                                        bid_age,
                                        ask_age,
                                        book_event_age,
                                        stale_after_sec=position_rest_refresh_trigger_age_sec,
                                        stream_idle_sec=position_ws_idle_resync_sec,
                                    )

                                    # REST is diagnostic-only. A stale best-level change timestamp
                                    # is not sufficient to reset an otherwise active delta-chain:
                                    # resubscribe only after both the executable BBO age threshold
                                    # and a genuine ORDER_BOOK stream-idle interval are exceeded.
                                    last_resubscribe = self._crossed_bbo_resubscribe_at.get(
                                        position_ticker, 0.0
                                    )
                                    if (
                                        should_resync
                                        and position_symbol in order_book_subscribed
                                        and now_loop - last_resubscribe >= crossed_resubscribe_cooldown_sec
                                    ):
                                        self._order_book_snapshot_required.add(position_ticker)
                                        self._last_order_book_snapshot_wait_tickers.discard(position_ticker)
                                        self._order_book_state.pop(position_ticker, None)
                                        self._crossed_bbo_resubscribe_at[position_ticker] = now_loop
                                        try:
                                            await ws.send(json.dumps({
                                                "action": "UNSUBSCRIBE", "type": "ORDER_BOOK",
                                                "data": {"symbol": position_symbol}, "token": self.jwt_token,
                                            }))
                                            await asyncio.sleep(0.05)
                                            await ws.send(json.dumps({
                                                "action": "SUBSCRIBE", "type": "ORDER_BOOK",
                                                "data": {"symbol": position_symbol}, "token": self.jwt_token,
                                            }))
                                            order_book_subscribed_since[position_symbol] = now_loop
                                            logger.warning(
                                                "🔄 [ORDER_BOOK POSITION RESYNC] %s: BBO устарел "
                                                "(Bid age=%.2fs, Ask age=%.2fs); WS subscription restarted, "
                                                "ожидается полный provider snapshot; REST остаётся диагностическим.",
                                                position_ticker, bid_age, ask_age,
                                            )
                                        except Exception as resync_exc:
                                            # Let the regular subscription synchronizer retry
                                            # on the next loop turn if either WS command failed.
                                            order_book_subscribed.discard(position_symbol)
                                            order_book_subscribed_since.pop(position_symbol, None)
                                            logger.warning(
                                                "Не удалось перезапустить ORDER_BOOK для позиции %s: %s; "
                                                "подписка будет повторена синхронизатором.",
                                                position_ticker, type(resync_exc).__name__,
                                            )

                                    # Fresh BBO needs no REST control request. If the BBO is
                                    # stale but WS events are still flowing, keep the subscription
                                    # and use only the throttled diagnostic snapshot below.
                                    if max(bid_age, ask_age) < position_rest_refresh_trigger_age_sec:
                                        continue

                                    if (
                                        position_ticker not in position_snapshot_inflight
                                        and now_loop - position_snapshot_last_at.get(position_ticker, 0.0)
                                        >= position_rest_refresh_interval_sec
                                    ):
                                        position_snapshot_inflight.add(position_ticker)
                                        position_snapshot_last_at[position_ticker] = now_loop
                                        asyncio.create_task(position_snapshot_refresher.refresh(position_symbol))

                                # Keep a small subscription overlap during Top-N rotation.
                                # It avoids tearing down a recently warmed book because of a
                                # marginal score change while still bounding provider load.
                                effective_wanted, protected = retain_subscription_overlap(
                                    order_book_subscribed, wanted, order_book_subscribed_since, now_loop,
                                    order_book_min_hold_sec, order_book_max_overbook,
                                )
                                sync_due = now_loop - last_order_book_sync >= order_book_refresh_sec
                                selection_changed = effective_wanted != order_book_subscribed
                                if selection_changed or sync_due:
                                    removed = order_book_subscribed - effective_wanted
                                    added = effective_wanted - order_book_subscribed
                                    for symbol in sorted(removed):
                                        await ws.send(json.dumps({
                                            "action": "UNSUBSCRIBE", "type": "ORDER_BOOK",
                                            "data": {"symbol": symbol}, "token": self.jwt_token,
                                        }))
                                        self._order_book_state.pop(symbol.split("@")[0].upper(), None)
                                        order_book_subscribed.discard(symbol)
                                        order_book_subscribed_since.pop(symbol, None)
                                    for symbol in sorted(added):
                                        self._order_book_state.pop(symbol.split("@")[0].upper(), None)
                                        await ws.send(json.dumps({
                                            "action": "SUBSCRIBE", "type": "ORDER_BOOK",
                                            "data": {"symbol": symbol}, "token": self.jwt_token,
                                        }))
                                        order_book_subscribed.add(symbol)
                                        order_book_subscribed_since[symbol] = now_loop
                                    last_order_book_sync = now_loop
                                    if added or removed:
                                        position_symbols_text = ",".join(sorted(
                                            symbol.split("@")[0] for symbol in position_symbols
                                        )) or "-"
                                        position_subscriptions_text = ",".join(sorted(
                                            symbol.split("@")[0]
                                            for symbol in position_symbols
                                            if symbol in order_book_subscribed
                                        )) or "-"
                                        logger.info(
                                            "📚 Finam ORDER_BOOK подписки синхронизированы: Top-%d=%d, "
                                            "позиций_вне_Top=%d, всего=%d, добавлено=%d, снято=%d, "
                                            "временно_удержано=%d; "
                                            "активные_позиции=%s; запрошенные_подписки_позиций=%s.",
                                            order_book_limit, len(normalized_selected),
                                            len(position_only_symbols), len(order_book_subscribed),
                                            len(added), len(removed), len(protected),
                                            position_symbols_text, position_subscriptions_text,
                                        )
                            except Exception as sub_exc:
                                last_order_book_sync = now_loop
                                logger.warning(
                                    "Finam ORDER_BOOK: не удалось синхронизировать подписки, "
                                    "QUOTES продолжается: %s", sub_exc,
                                )

                        if now_loop - last_trade_diag >= 60.0:
                            logger.info(
                                "📈 Finam INSTRUMENT_TRADES 60s: messages=%d, trades=%d, queue_dropped=%d, backpressure_waits=%d.",
                                instrument_trade_messages, instrument_trade_events,
                                instrument_trade_queue_dropped, instrument_trade_queue_backpressure,
                            )
                            instrument_trade_messages = 0
                            instrument_trade_events = 0
                            instrument_trade_queue_dropped = 0
                            instrument_trade_queue_backpressure = 0
                            last_trade_diag = now_loop

                        if now_loop - last_order_book_diag >= 60.0:
                            invalid_reason_summary = ", ".join(
                                f"{reason}:{count}"
                                for reason, count in sorted(
                                    order_book_invalid_reasons.items(),
                                    key=lambda item: item[1],
                                    reverse=True,
                                )[:8]
                            ) or "-"
                            invalid_ticker_summary = ", ".join(
                                f"{ticker}:{count}"
                                for ticker, count in sorted(
                                    order_book_invalid_by_ticker.items(),
                                    key=lambda item: item[1],
                                    reverse=True,
                                )[:12]
                            ) or "-"

                            position_diag_parts: list[str] = []
                            position_gaps: list[str] = []
                            for ticker in sorted(last_open_position_symbols):
                                symbol = f"{ticker}@MISX"
                                stats = order_book_symbol_diagnostics.for_ticker(ticker)
                                subscribe_requested = symbol in order_book_subscribed
                                subscribed_since = order_book_subscribed_since.get(symbol, 0.0)
                                last_wire = float(stats.get("last_wire_monotonic", 0.0) or 0.0)
                                last_parsed = float(stats.get("last_parsed_monotonic", 0.0) or 0.0)
                                last_queued = float(stats.get("last_queued_monotonic", 0.0) or 0.0)
                                wire_age = now_loop - last_wire if last_wire > 0.0 else None
                                sub_age = now_loop - subscribed_since if subscribed_since > 0.0 else None
                                parsed_age = now_loop - last_parsed if last_parsed > 0.0 else None
                                queued_age = now_loop - last_queued if last_queued > 0.0 else None

                                flags: list[str] = []
                                if not subscribe_requested:
                                    flags.append("NO_SUBSCRIBE_REQUEST")
                                    position_gaps.append(f"{ticker}:NO_SUBSCRIBE_REQUEST")
                                elif wire_age is None:
                                    flags.append("NO_WIRE_DATA")
                                    position_gaps.append(f"{ticker}:NO_WIRE_DATA")
                                elif wire_age >= 60.0:
                                    flags.append("WIRE_STALE")
                                    position_gaps.append(f"{ticker}:WIRE_STALE={wire_age:.1f}s")
                                if stats["window_snapshot_wait"] > 0:
                                    flags.append("WAIT_SNAPSHOT")
                                    position_gaps.append(f"{ticker}:WAIT_SNAPSHOT={stats['window_snapshot_wait']}")
                                if stats["window_wire"] > 0 and stats["window_parsed"] == 0:
                                    flags.append("PARSE_EMPTY")
                                    position_gaps.append(f"{ticker}:PARSE_EMPTY")
                                if stats["window_dropped"] > 0:
                                    flags.append("QUEUE_DROP")
                                    position_gaps.append(f"{ticker}:QUEUE_DROP={stats['window_dropped']}")

                                sub_age_text = f"{sub_age:.1f}s" if sub_age is not None else "нет"
                                wire_age_text = f"{wire_age:.1f}s" if wire_age is not None else "нет"
                                parsed_age_text = f"{parsed_age:.1f}s" if parsed_age is not None else "нет"
                                queued_age_text = f"{queued_age:.1f}s" if queued_age is not None else "нет"
                                position_diag_parts.append(
                                    f"{ticker}(sub_req_local={'Y' if subscribe_requested else 'N'},"
                                    f"sub_req_age={sub_age_text},wire={stats['window_wire']},"
                                    f"wire_age={wire_age_text},parsed={stats['window_parsed']},"
                                    f"parse_age={parsed_age_text},queued={stats['window_queued']},"
                                    f"queue_age={queued_age_text},"
                                    f"no_update={stats['window_parse_no_update']},"
                                    f"parse_ooo={stats['window_parse_out_of_order']},"
                                    f"unparsed={stats['window_parse_unrecognized']},"
                                    f"dropped={stats['window_dropped']},"
                                    f"flags={','.join(flags) or 'OK'})"
                                )

                            position_diag_summary = " | ".join(position_diag_parts) or "-"
                            logger.info(
                                "📚 [ORDER_BOOK 60s] подписок=%d, сообщений=%d, "
                                "событий_в_очередь=%d, затронуто_сторон_BBO=%d, "
                                "heartbeat_без_движения_BBO=%d, некорректных_состояний=%d "
                                "(причины=%s; тикеры=%s), CROSSED_BBO_эпизодов=%d, "
                                "без_timestamp_envelope=%d, out_of_order=%d, потерь_очереди=%d, "
                                "coalesced_ORDER_BOOK=%d, coalesced_QUOTES=%d, "
                                "ошибок_разбора=%d; позиции=[%s]; сообщений_без_тикера=%d.",
                                len(order_book_subscribed), order_book_messages,
                                enqueued_order_book_events, enqueued_bbo_side_updates,
                                order_book_valid_no_bbo_delta, order_book_invalid_states,
                                invalid_reason_summary, invalid_ticker_summary,
                                order_book_crossed_episodes, order_book_missing_envelope_timestamp,
                                order_book_out_of_order, order_book_queue_dropped,
                                order_book_events_coalesced, quote_events_coalesced,
                                order_book_decode_failures,
                                position_diag_summary, order_book_unattributed_messages,
                            )
                            if position_gaps:
                                logger.warning(
                                    "📚 [ORDER_BOOK POSITION GAP] %s",
                                    " | ".join(sorted(set(position_gaps))),
                                )
                            if order_book_parser_sample:
                                logger.warning(
                                    "📚 [ORDER_BOOK PARSER SAMPLE] %s",
                                    order_book_parser_sample,
                                )
                            if order_book_unattributed_sample:
                                logger.warning(
                                    "📚 [ORDER_BOOK UNATTRIBUTED SAMPLE] %s",
                                    order_book_unattributed_sample,
                                )

                            if order_book_out_of_order_age_deltas_ms:
                                ordered_ooo_ages = sorted(order_book_out_of_order_age_deltas_ms)
                                logger.info(
                                    "📚 [ORDER_BOOK OUT_OF_ORDER 60s] events=%d age_delta_ms_min=%.1f "
                                    "avg=%.1f max=%.1f",
                                    len(ordered_ooo_ages), ordered_ooo_ages[0],
                                    sum(ordered_ooo_ages) / len(ordered_ooo_ages), ordered_ooo_ages[-1],
                                )
                            else:
                                logger.info("[ORDER_BOOK OUT_OF_ORDER 60s] events=0")
                            order_book_out_of_order_age_deltas_ms.clear()
                            order_book_messages = 0
                            order_book_decode_failures = 0
                            order_book_valid_no_bbo_delta = 0
                            order_book_invalid_states = 0
                            order_book_crossed_episodes = 0
                            order_book_invalid_reasons.clear()
                            order_book_invalid_by_ticker.clear()
                            order_book_missing_envelope_timestamp = 0
                            order_book_out_of_order = 0
                            order_book_queue_dropped = 0
                            order_book_events_coalesced = 0
                            quote_events_coalesced = 0
                            enqueued_order_book_events = 0
                            enqueued_bbo_side_updates = 0
                            order_book_unattributed_messages = 0
                            order_book_unattributed_sample = ""
                            order_book_parser_sample = ""
                            # Keep every symbol's window counters on the same 60-second
                            # boundary, including symbols that become positions later.
                            for symbol_stats in order_book_symbol_diagnostics.values():
                                for key in (
                                    "window_wire", "window_parse_no_update",
                                    "window_parse_out_of_order", "window_parse_unrecognized",
                                    "window_snapshot_wait", "window_parsed", "window_queued", "window_dropped",
                                ):
                                    symbol_stats[key] = 0
                            last_order_book_diag = now_loop

                        try:
                            # Wake periodically even during a quiet market so a newly
                            # selected Top-15 can be subscribed without waiting for
                            # the next unrelated market-data packet.
                            raw_message = await asyncio.wait_for(ws.recv(), timeout=1.0)
                        except asyncio.TimeoutError:
                            quiet_for = asyncio.get_running_loop().time() - last_ws_data_at
                            now_warning = asyncio.get_running_loop().time()
                            if quiet_for >= 30.0 and now_warning - last_no_data_warning_at >= 30.0:
                                logger.warning(
                                    "Finam WS: %.0f секунд без входящих сообщений; "
                                    "DATA=%d, котировок в очереди=%d, ORDER_BOOK сообщений=%d, "
                                    "ORDER_BOOK событий в очереди=%d, потерь очереди=%d, подписок стакана=%d. "
                                    "Соединение оставляем открытым.",
                                    quiet_for, data_messages, enqueued_quotes,
                                    order_book_messages, enqueued_order_book_events,
                                    order_book_queue_dropped, len(order_book_subscribed),
                                )
                                last_no_data_warning_at = now_warning
                            continue

                        try:
                            envelope = json.loads(raw_message)
                        except (TypeError, json.JSONDecodeError):
                            logger.warning("Пропущено некорректное JSON-сообщение Finam WS.")
                            continue
                        if not isinstance(envelope, dict):
                            logger.warning(
                                "Finam WS прислал JSON не-объектного типа: %s",
                                type(envelope).__name__,
                            )
                            continue
                        # Preserve the socket-ingress time so queue backlog cannot
                        # turn an old message without a provider timestamp into a
                        # fresh BBO confirmation when the worker finally handles it.
                        envelope["_received_monotonic"] = time.monotonic()

                        message_type = str(envelope.get("type", "")).upper()
                        if message_type == "ERROR" or envelope.get("error_info"):
                            error_info = envelope.get("error_info", envelope)
                            logger.error("Ошибка подписки/потока Finam: %s", error_info)
                            if "maximum number of symbols exceeded" in str(error_info).lower():
                                raise RuntimeError(
                                    f"Finam отклонил размер пакета символов: {error_info}"
                                )
                            continue

                        if message_type == "EVENT":
                            event_info = envelope.get("event_info", {})
                            logger.info("Событие Finam WS: %s", event_info)
                            if (
                                isinstance(event_info, dict)
                                and str(event_info.get("event", "")).upper() == "CONNECTION_CLOSED"
                            ):
                                raise RuntimeError(f"Finam закрыл WebSocket: {event_info}")
                            continue

                        if message_type != "DATA":
                            logger.debug("Неизвестное сообщение Finam WS: type=%r", message_type)
                            continue

                        data_messages += 1
                        last_ws_data_at = asyncio.get_running_loop().time()
                        subscription_type = str(envelope.get("subscription_type", "")).upper()
                        raw_payload = envelope.get("payload", {})
                        payload = self._decode_payload(raw_payload)

                        if not first_data_logged:
                            payload_keys = list(payload.keys()) if isinstance(payload, dict) else []
                            logger.info(
                                "Получено первое DATA от Finam: subscription_type=%s, "
                                "payload_type=%s, payload_keys=%s",
                                subscription_type or "<не указан>",
                                type(payload).__name__,
                                payload_keys,
                            )
                            first_data_logged = True

                        if subscription_type in {"ORDER_BOOK", "ORDERBOOK"}:
                            order_book_messages += 1
                            message_received_monotonic = float(
                                envelope.get("_received_monotonic", time.monotonic())
                            )
                            symbol_hints = self._extract_order_book_symbol_hints(payload)
                            book_updates = self._extract_order_book_updates(envelope)
                            for ooo_detail in self._last_order_book_out_of_order_details.values():
                                try:
                                    age_delta_ms = float(ooo_detail.get("age_delta_ms", 0.0))
                                except (TypeError, ValueError, OverflowError):
                                    continue
                                if math.isfinite(age_delta_ms) and age_delta_ms >= 0.0:
                                    order_book_out_of_order_age_deltas_ms.append(age_delta_ms)
                            # A rejected out-of-order delta breaks confidence in the local
                            # delta chain. Resubscribe under a per-symbol cooldown and wait
                            # for a provider snapshot before accepting another book update.
                            for stale_ticker in sorted(self._last_order_book_out_of_order_tickers):
                                stale_symbol = f"{stale_ticker}@MISX"
                                last_resubscribe = self._crossed_bbo_resubscribe_at.get(stale_ticker, 0.0)
                                if stale_symbol not in order_book_subscribed or message_received_monotonic - last_resubscribe < crossed_resubscribe_cooldown_sec:
                                    continue
                                self._order_book_snapshot_required.add(stale_ticker)
                                self._order_book_state.pop(stale_ticker, None)
                                self._crossed_bbo_resubscribe_at[stale_ticker] = message_received_monotonic
                                try:
                                    await ws.send(json.dumps({"action":"UNSUBSCRIBE","type":"ORDER_BOOK","data":{"symbol":stale_symbol},"token":self.jwt_token}))
                                    await asyncio.sleep(0.02)
                                    await ws.send(json.dumps({"action":"SUBSCRIBE","type":"ORDER_BOOK","data":{"symbol":stale_symbol},"token":self.jwt_token}))
                                    order_book_subscribed_since[stale_symbol] = message_received_monotonic
                                    detail = self._last_order_book_out_of_order_details.get(stale_ticker, {})
                                    logger.warning(
                                        "[ORDER_BOOK OUT_OF_ORDER RESYNC] %s: delta chain invalidated; "
                                        "age_delta_ms=%.1f event_ts=%s prior_event_monotonic=%.6f; waiting for full snapshot.",
                                        stale_ticker,
                                        float(detail.get("age_delta_ms", 0.0) or 0.0),
                                        str(detail.get("event_timestamp") or "-")[:48],
                                        float(detail.get("prior_monotonic", 0.0) or 0.0),
                                    )
                                except Exception as resync_exc:
                                    order_book_subscribed.discard(stale_symbol)
                                    order_book_subscribed_since.pop(stale_symbol, None)
                                    logger.warning("[ORDER_BOOK OUT_OF_ORDER RESYNC FAILED] %s: %s; subscription will retry.", stale_ticker, type(resync_exc).__name__)
                            update_tickers = {
                                str(item.get("ticker") or "").strip().upper().split("@")[0]
                                for item in book_updates
                                if isinstance(item, dict) and item.get("ticker")
                            }
                            observed_tickers = symbol_hints | update_tickers
                            parse_missing_tickers = symbol_hints - update_tickers
                            if not self._last_order_book_payload_valid:
                                self._last_order_book_unrecognized_tickers.update(
                                    parse_missing_tickers
                                    - self._last_order_book_out_of_order_tickers
                                    - self._last_order_book_snapshot_wait_tickers
                                )
                            if not observed_tickers:
                                order_book_unattributed_messages += 1
                                if not order_book_unattributed_sample:
                                    safe_payload = self._redact_for_log(payload)
                                    order_book_unattributed_sample = json.dumps(
                                        safe_payload, ensure_ascii=False, default=str
                                    )[:1200]
                            elif parse_missing_tickers and not order_book_parser_sample:
                                safe_payload = self._redact_for_log(payload)
                                out_of_order_symbols = parse_missing_tickers & self._last_order_book_out_of_order_tickers
                                unrecognized_symbols = parse_missing_tickers & self._last_order_book_unrecognized_tickers
                                snapshot_wait_symbols = parse_missing_tickers & self._last_order_book_snapshot_wait_tickers
                                classifications = []
                                if out_of_order_symbols:
                                    classifications.append("OUT_OF_ORDER=" + ",".join(sorted(out_of_order_symbols)))
                                if unrecognized_symbols:
                                    classifications.append("UNRECOGNIZED=" + ",".join(sorted(unrecognized_symbols)))
                                if snapshot_wait_symbols:
                                    classifications.append("WAIT_SNAPSHOT=" + ",".join(sorted(snapshot_wait_symbols)))
                                if not classifications:
                                    classifications.append("NO_UPDATE=unknown")
                                detail = {
                                    symbol: self._last_order_book_out_of_order_details[symbol]
                                    for symbol in sorted(out_of_order_symbols)
                                    if symbol in self._last_order_book_out_of_order_details
                                }
                                order_book_parser_sample = (
                                    f"classification={'/'.join(classifications)}; "
                                    f"out_of_order_detail={json.dumps(detail, ensure_ascii=False, default=str)}; "
                                    f"symbols={','.join(sorted(parse_missing_tickers))}; payload="
                                    + json.dumps(safe_payload, ensure_ascii=False, default=str)[:1000]
                                )
                            for observed_ticker in observed_tickers:
                                symbol_stats = order_book_symbol_diagnostics.for_ticker(observed_ticker)
                                symbol_stats["window_wire"] += 1
                                symbol_stats["last_wire_monotonic"] = message_received_monotonic
                                if observed_ticker not in update_tickers:
                                    symbol_stats["window_parse_no_update"] += 1
                                    if observed_ticker in self._last_order_book_snapshot_wait_tickers:
                                        symbol_stats["window_snapshot_wait"] += 1
                                    elif observed_ticker in self._last_order_book_out_of_order_tickers:
                                        symbol_stats["window_parse_out_of_order"] += 1
                                    elif (
                                        observed_ticker in self._last_order_book_unrecognized_tickers
                                        or not self._last_order_book_payload_valid
                                    ):
                                        symbol_stats["window_parse_unrecognized"] += 1
                            # Count per-symbol drops even when another symbol in the
                            # same envelope produced a valid update.
                            order_book_out_of_order += len(self._last_order_book_out_of_order_tickers)
                            order_book_decode_failures += len(self._last_order_book_unrecognized_tickers)
                            if (
                                not book_updates
                                and not self._last_order_book_payload_valid
                                and not self._last_order_book_snapshot_wait_tickers
                            ):
                                if not order_book_decode_warned:
                                    safe_envelope = self._redact_for_log(envelope)
                                    logger.warning(
                                        "Finam ORDER_BOOK: не удалось разобрать структуру стакана: "
                                        "payload_keys=%s, sample=%s",
                                        list(payload.keys()) if isinstance(payload, dict) else [],
                                        json.dumps(safe_envelope, ensure_ascii=False, default=str)[:1800],
                                    )
                                    order_book_decode_warned = True
                            if book_updates and not first_order_book_logged:
                                sample = next(
                                    (item for item in book_updates if item.get("book_valid", True)),
                                    None,
                                )
                                if sample is not None:
                                    logger.info(
                                        "✅ Finam ORDER_BOOK активен: %s Bid=%s×%s Ask=%s×%s; "
                                        "timestamp_source=%s.",
                                        sample["ticker"], sample["bid"], sample["bid_size"],
                                        sample["ask"], sample["ask_size"],
                                        sample.get("timestamp_source", "UNKNOWN"),
                                    )
                                    first_order_book_logged = True
                            for book_update in book_updates:
                                book_ticker = str(book_update.get("ticker") or "").strip().upper().split("@")[0]
                                symbol_stats = order_book_symbol_diagnostics.for_ticker(book_ticker) if book_ticker else None
                                if symbol_stats is not None:
                                    symbol_stats["window_parsed"] += 1
                                    symbol_stats["last_parsed_monotonic"] = message_received_monotonic
                                has_bbo_side_update = bool(
                                    book_update.get("bid_present") or book_update.get("ask_present")
                                )
                                if book_update.get("timestamp_source") == "RECEIVE_FALLBACK":
                                    order_book_missing_envelope_timestamp += 1
                                if not book_update.get("book_valid", True):
                                    order_book_invalid_states += 1
                                    invalid_reason = str(book_update.get("book_reason") or "UNKNOWN").upper()
                                    invalid_ticker = str(book_update.get("ticker") or "UNKNOWN").upper()
                                    invalid_ticker_reason = f"{invalid_ticker}/{invalid_reason}"
                                    order_book_invalid_reasons[invalid_reason] = (
                                        order_book_invalid_reasons.get(invalid_reason, 0) + 1
                                    )
                                    order_book_invalid_by_ticker[invalid_ticker_reason] = (
                                        order_book_invalid_by_ticker.get(invalid_ticker_reason, 0) + 1
                                    )
                                    if invalid_reason == "CROSSED_BBO":
                                        if book_update.get("crossed_started"):
                                            order_book_crossed_episodes += 1
                                        now_monotonic = time.monotonic()
                                        last_detail_log = self._crossed_bbo_log_last_at.get(invalid_ticker, 0.0)
                                        if now_monotonic - last_detail_log >= 15.0:
                                            self._crossed_bbo_log_last_at[invalid_ticker] = now_monotonic
                                            bid_price = float(book_update.get("bid") or 0.0)
                                            ask_price = float(book_update.get("ask") or 0.0)
                                            midpoint = (bid_price + ask_price) / 2.0
                                            crossed_bps = (
                                                (bid_price - ask_price) / midpoint * 10000.0
                                                if midpoint > 0.0 else 0.0
                                            )
                                            bid_depth = book_update.get("bid_levels")
                                            ask_depth = book_update.get("ask_levels")
                                            bid_top = bid_depth[:3] if isinstance(bid_depth, list) else []
                                            ask_top = ask_depth[:3] if isinstance(ask_depth, list) else []
                                            logger.warning(
                                                "[ORDER_BOOK CROSSED_BBO] %s: Bid=%.6f x %s, Ask=%.6f x %s; "
                                                "cross=%.2f bps; previous Bid/Ask=%s/%s; "
                                                "side-touched Bid/Ask=%s/%s; best-level-touched=%s/%s; "
                                                "delta-levels Bid=%s Ask=%s; top3 Bid=%s Ask=%s; "
                                                "event_timestamp=%s; timestamp_source=%s; crossed_age=%.3fs.",
                                                invalid_ticker,
                                                bid_price,
                                                book_update.get("bid_size"),
                                                ask_price,
                                                book_update.get("ask_size"),
                                                crossed_bps,
                                                book_update.get("previous_bid"),
                                                book_update.get("previous_ask"),
                                                book_update.get("bid_side_touched"),
                                                book_update.get("ask_side_touched"),
                                                book_update.get("bid_best_level_touched"),
                                                book_update.get("ask_best_level_touched"),
                                                book_update.get("bid_touched_levels"),
                                                book_update.get("ask_touched_levels"),
                                                bid_top,
                                                ask_top,
                                                book_update.get("order_book_timestamp"),
                                                book_update.get("timestamp_source", "UNKNOWN"),
                                                float(book_update.get("crossed_duration_sec") or 0.0),
                                            )
                                        crossed_age_sec = float(book_update.get("crossed_duration_sec") or 0.0)
                                        last_resubscribe = self._crossed_bbo_resubscribe_at.get(invalid_ticker, 0.0)
                                        symbol = f"{invalid_ticker}@MISX"
                                        if (
                                            crossed_age_sec >= crossed_resubscribe_after_sec
                                            and symbol in order_book_subscribed
                                            and now_monotonic - last_resubscribe >= crossed_resubscribe_cooldown_sec
                                        ):
                                            # A long-lived crossed state can indicate a poisoned delta chain.
                                            # Restart the subscription and wait for a full provider snapshot.
                                            self._order_book_snapshot_required.add(invalid_ticker)
                                            self._last_order_book_snapshot_wait_tickers.discard(invalid_ticker)
                                            self._order_book_state.pop(invalid_ticker, None)
                                            self._crossed_bbo_since.pop(invalid_ticker, None)
                                            self._crossed_bbo_resubscribe_at[invalid_ticker] = now_monotonic
                                            try:
                                                await ws.send(json.dumps({
                                                    "action": "UNSUBSCRIBE", "type": "ORDER_BOOK",
                                                    "data": {"symbol": symbol}, "token": self.jwt_token,
                                                }))
                                                await asyncio.sleep(0.05)
                                                await ws.send(json.dumps({
                                                    "action": "SUBSCRIBE", "type": "ORDER_BOOK",
                                                    "data": {"symbol": symbol}, "token": self.jwt_token,
                                                }))
                                                order_book_subscribed_since[symbol] = now_loop
                                                logger.warning(
                                                    "🔄 [ORDER_BOOK RESYNC] %s: persistent CROSSED_BBO %.1fs; "
                                                    "subscription restarted, waiting for full snapshot.",
                                                    invalid_ticker, crossed_age_sec,
                                                )
                                            except Exception as resubscribe_exc:
                                                logger.error(
                                                    "[ORDER_BOOK RESYNC FAILED] %s: %s; BBO remains invalid.",
                                                    invalid_ticker, resubscribe_exc,
                                                )
                                else:
                                    if book_update.get("crossed_recovered"):
                                        logger.warning(
                                            "[ORDER_BOOK CROSSED_RECOVERY] %s: согласованный BBO восстановлен "
                                            "через %.3fs; Bid=%.6f Ask=%.6f.",
                                            book_ticker,
                                            float(book_update.get("crossed_recovered_after_sec") or 0.0),
                                            float(book_update.get("bid") or 0.0),
                                            float(book_update.get("ask") or 0.0),
                                        )
                                if book_update.get("book_valid", True) and not has_bbo_side_update:
                                    order_book_valid_no_bbo_delta += 1
                                try:
                                    queue.put_nowait(book_update)
                                    enqueued_order_book_events += 1
                                    if symbol_stats is not None:
                                        symbol_stats["window_queued"] += 1
                                        symbol_stats["last_queued_monotonic"] = message_received_monotonic
                                    if book_update.get("bid_present"):
                                        enqueued_bbo_side_updates += 1
                                    if book_update.get("ask_present"):
                                        enqueued_bbo_side_updates += 1
                                except asyncio.QueueFull:
                                    if _coalesce_queued_market_event(queue, book_update):
                                        order_book_events_coalesced += 1
                                        if symbol_stats is not None:
                                            symbol_stats["window_coalesced"] = symbol_stats.get("window_coalesced", 0) + 1
                                    else:
                                        order_book_queue_dropped += 1
                                        if symbol_stats is not None:
                                            symbol_stats["window_dropped"] += 1
                                        queue_overflow_tracker.record("ORDER_BOOK", book_update.get("ticker"))
                            continue

                        if subscription_type in {"INSTRUMENT_TRADES", "INSTRUMENT_TRADE"}:
                            instrument_trade_messages += 1
                            trade_events = self._extract_instrument_trades(envelope)
                            instrument_trade_events += len(trade_events)
                            for trade_event in trade_events:
                                try:
                                    queue.put_nowait(trade_event)
                                except asyncio.QueueFull:
                                    # Individual exchange prints are not replaceable snapshots.
                                    # Apply bounded backpressure instead of silently losing
                                    # trade-flow evidence when the consumer briefly falls behind.
                                    instrument_trade_queue_backpressure += 1
                                    await queue.put(trade_event)
                            if trade_events and not first_trade_logged:
                                logger.info(
                                    "📈 Первые сделки Finam: распознано=%d, тикер=%s, side=%s.",
                                    len(trade_events), trade_events[0].get("ticker"), trade_events[0].get("side"),
                                )
                                first_trade_logged = True
                            continue

                        if subscription_type and subscription_type != "QUOTES":
                            continue

                        ticks = self._extract_quotes(envelope)
                        if not ticks:
                            # The documented format is payload.quote[], but the log so far
                            # shows an empty payload. Emit a short, credential-redacted sample
                            # of the actual server envelope so the next diagnosis is based on
                            # wire data, not guessed field names.
                            if data_messages <= 3:
                                candidate = None
                                if isinstance(payload, dict):
                                    candidate = payload.get("quote") or payload.get("quotes")
                                    if isinstance(candidate, list) and candidate:
                                        candidate = candidate[0]
                                safe_envelope = self._redact_for_log(envelope)
                                raw_sample = json.dumps(
                                    safe_envelope, ensure_ascii=False, default=str
                                )[:2500]
                                logger.warning(
                                    "Finam DATA без распознанной котировки: subscription_type=%s, "
                                    "envelope_keys=%s, payload_type=%s, payload_keys=%s, "
                                    "first_quote_keys=%s, sample=%s",
                                    subscription_type or "<не указан>",
                                    list(envelope.keys()),
                                    type(payload).__name__,
                                    list(payload.keys()) if isinstance(payload, dict) else [],
                                    list(candidate.keys()) if isinstance(candidate, dict) else [],
                                    raw_sample,
                                )
                            continue

                        for tick in ticks:
                            try:
                                queue.put_nowait(tick)
                            except asyncio.QueueFull:
                                if _coalesce_queued_market_event(queue, tick):
                                    quote_events_coalesced += 1
                                else:
                                    queue_overflow_tracker.record("QUOTES", tick.get("ticker"))
                                continue

                            enqueued_quotes += 1
                            if enqueued_quotes == 1:
                                logger.info(
                                    "✅ Первая котировка Finam передана в очередь: "
                                    "%s price=%s volume=%s bid=%s ask=%s bid_size=%s ask_size=%s "
                                    "presence=(%s,%s,%s,%s) timestamp=%s",
                                    tick.get("ticker"), tick.get("price"), tick.get("volume"),
                                    tick.get("bid"), tick.get("ask"), tick.get("bid_size"), tick.get("ask_size"),
                                    tick.get("bid_present"), tick.get("ask_present"),
                                    tick.get("bid_size_present"), tick.get("ask_size_present"),
                                    tick.get("timestamp"),
                                )
                            elif enqueued_quotes % 100 == 0:
                                logger.info(
                                    "Поток Finam активен: DATA=%d, котировок в очередь=%d; "
                                    "последний тик %s=%s",
                                    data_messages,
                                    enqueued_quotes,
                                    tick.get("ticker"),
                                    tick.get("price"),
                                )

                    if self.is_ws_running:
                        raise RuntimeError("Finam WebSocket завершил поток без команды остановки.")

            except asyncio.CancelledError:
                self._market_stream_connected = False
                raise
            except Exception as exc:
                self._market_stream_connected = False
                if not self.is_ws_running:
                    break

                if "maximum number of symbols exceeded" in str(exc).lower():
                    if batch_size > 1:
                        previous_size = batch_size
                        batch_size = max(1, batch_size // 2)
                        batches = build_quote_batches(symbols, batch_size)
                        backoff = 1
                        logger.warning(
                            "Finam отклонил размер пакета %d. Уменьшаем до %d и повторяем подписку.",
                            previous_size,
                            batch_size,
                        )
                        refresh_token_before_retry = True
                        await asyncio.sleep(0.5)
                        continue

                    logger.critical(
                        "Finam отклоняет даже пакет из одного символа. Вероятно, достигнут "
                        "лимит подписок аккаунта/тарифа или есть ограничение на соединение. "
                        "Автоповторы остановлены; проверьте лимиты Finam и список инструментов."
                    )
                    self.is_ws_running = False
                    break

                close_code = _websocket_close_code(exc)
                if _websocket_disconnect_log_level(exc) == logging.INFO:
                    logger.info(
                        "Finam WebSocket штатно закрыл соединение (code=%s): %s. "
                        "Переподключение через %d сек.",
                        close_code if close_code is not None else "unknown",
                        exc,
                        backoff,
                    )
                else:
                    logger.error(
                        "Finam WebSocket отключён/не подключён (close_code=%s): %s. "
                        "Повтор через %d сек.",
                        close_code if close_code is not None else "unknown",
                        exc,
                        backoff,
                    )
                refresh_token_before_retry = True
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    async def stop_market_data_stream(self):
        self.is_ws_running = False
        self._market_stream_connected = False
