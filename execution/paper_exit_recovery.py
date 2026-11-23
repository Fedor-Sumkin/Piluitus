"""PAPER position recovery, quarantine and bulk-close mixin."""
from __future__ import annotations

from execution.paper_exit_shared import (
    asyncio, logging, math, os, time, Mapping, datetime, timedelta, timezone, Any, cast,
    LOT_SIZES, DBManager, PAPER_CLOSED_TRADES_STORAGE_KEY, PAPER_CLOSED_TRADES_STORAGE_LIMIT,
    paper_performance_tracker, build_exit_execution_diagnostics,
    BBOExitQuote, TradeRecord, _as_string_mapping, _finite_or_default, logger,
)

class PaperExitRecoveryMixin:
    async def quarantine_unreconciled_paper_positions(self, db: DBManager) -> int:
        """На старте помечает старые PAPER OPEN как требующие проверки, не рисуя фиктивное закрытие."""
        if not self.paper_mode:
            return 0

        # A complete journal is mandatory: get_active_positions() omits
        # STALE_REVIEW records and therefore cannot prove that a prior quarantine
        # has been resolved. Any read/shape failure must retain the entry lock.
        def _lock_unavailable_journal(reason: str) -> None:
            self._unreconciled_paper_positions = {
                "__PAPER_JOURNAL_UNAVAILABLE__": {
                    "status": "STALE_REVIEW",
                    "reconciliation_status": "JOURNAL_UNAVAILABLE",
                    "stale_reason": reason,
                }
            }

        getter = getattr(db, "get_all_daily_trades", None)
        if not callable(getter):
            reason = "DBManager не предоставляет полный журнал PAPER-сделок."
            _lock_unavailable_journal(reason)
            raise RuntimeError(reason)
        try:
            all_positions = await getter()
        except Exception as exc:
            reason = f"Не удалось прочитать полный журнал PAPER-сделок: {exc}"
            _lock_unavailable_journal(reason)
            logger.critical("[PAPER RECONCILIATION LOCK] %s; новые входы заблокированы.", reason)
            raise RuntimeError(reason) from exc
        if not isinstance(all_positions, Mapping):
            reason = "Полный журнал PAPER-сделок вернул некорректный формат."
            _lock_unavailable_journal(reason)
            logger.critical("[PAPER RECONCILIATION LOCK] %s; новые входы заблокированы.", reason)
            raise RuntimeError(reason)

        quarantined = 0
        unresolved: dict[str, TradeRecord] = {}
        self._unreconciled_paper_positions = unresolved
        now = datetime.now().isoformat(timespec="seconds")
        for order_id, raw_trade in all_positions.items():
            if not isinstance(raw_trade, dict):
                # The key is part of the trade journal, but its mode/status can
                # no longer be established. Do not assume it was a harmless
                # non-PAPER record and release the PAPER entry gate.
                reason = f"Запись сделки {order_id} имеет некорректный формат; режим/статус неизвестен."
                unresolved[str(order_id)] = {
                    "status": "STALE_REVIEW",
                    "reconciliation_status": "JOURNAL_RECORD_INVALID",
                    "stale_reason": reason,
                }
                logger.critical("[PAPER RECONCILIATION LOCK] %s; новые входы заблокированы.", reason)
                continue
            trade = dict(raw_trade)
            # TRADE_ — legacy ID used by old PAPER builds. In PAPER mode these
            # records are simulated trades, not a broker's inventory.
            paper_id = str(order_id).startswith(("PAPER_", "TRADE_"))
            explicitly_paper = str(trade.get("mode", "")).upper() == "PAPER"
            if not (paper_id or explicitly_paper):
                continue

            status = str(trade.get("status", "")).upper()
            if status == "OPEN":
                trade["status"] = "STALE_REVIEW"
                trade["reconciliation_status"] = "REVIEW_REQUIRED"
                trade["stale_detected_at"] = now
                trade["stale_reason"] = (
                    "PAPER-позиция осталась в постоянном хранилище после перезапуска. "
                    "Симулятор не восстановил её в ОЗУ; цена выхода и PnL не определялись. "
                    "До ручной сверки новые сделки заблокированы."
                )
                # Publish the in-memory lock before persistence. If the write
                # fails, startup may continue for diagnostics, but new entries
                # must remain blocked in this process.
                unresolved[str(order_id)] = trade
                self._unreconciled_paper_positions = unresolved
                await db.set_json(f"piluitus:trade:{order_id}", trade)
                quarantined += 1
            elif status == "STALE_REVIEW" and str(trade.get("reconciliation_status", "")).upper() == "REVIEW_REQUIRED":
                unresolved[str(order_id)] = trade

        self._unreconciled_paper_positions = unresolved
        if unresolved:
            for stale_id, stale_trade in sorted(unresolved.items()):
                side = str(stale_trade.get("side") or "").upper()
                entry = _finite_or_default(stale_trade.get("entry_price"), 0.0)
                stop = _finite_or_default(stale_trade.get("stop_loss"), 0.0)
                target = _finite_or_default(stale_trade.get("take_profit"), 0.0)
                levels_valid = (
                    entry > 0.0 and stop > 0.0 and target > 0.0
                    and (
                        (side == "BUY" and stop < entry < target)
                        or (side == "SELL" and target < entry < stop)
                    )
                )
                logger.warning(
                    "🔎 [PAPER STALE POSITION DETAIL] id=%s ticker=%s side=%s status=%s "
                    "shares=%s lots=%s entry=%.6f SL=%.6f TP=%.6f SLTP=%s "
                    "pending_exit=%s entry_timestamp=%s; позиция сохранена, "
                    "автоматическое закрытие/удаление не выполнялось.",
                    stale_id,
                    str(stale_trade.get("ticker") or stale_id).upper(),
                    side or "UNKNOWN",
                    str(stale_trade.get("status") or "UNKNOWN"),
                    stale_trade.get("volume", "N/A"),
                    stale_trade.get("lots", "N/A"),
                    entry, stop, target,
                    "VALID" if levels_valid else "INVALID_OR_MISSING",
                    stale_trade.get("pending_exit_reason") or "нет",
                    stale_trade.get("entry_timestamp") or "нет",
                )
            from portfolio.portfolio_metrics import calculate_assets_used
            unresolved_cost = calculate_assets_used(unresolved.values(), lot_sizes=LOT_SIZES)
            logger.critical(
                "🛑 [PAPER RECONCILIATION GUARD] Найдено %d не сверенных PAPER-позиций (себестоимость %.2f ₽). "
                "Новые входы заблокированы, пока позиции не будут проверены оператором.",
                len(unresolved), unresolved_cost,
            )
        if quarantined:
            logger.warning(
                "⚠️ Переведено в STALE_REVIEW старых PAPER-позиций: %d. "
                "Они не закрыты автоматически и не добавлены в realized PnL.",
                quarantined,
            )
        return quarantined

    async def close_all_positions(self, db: DBManager) -> dict[str, Any]:
        """Close active PAPER positions while atomically blocking concurrent entries.

        The maintenance latch and PAPER entry commit use the same portfolio lock:
        an entry either becomes fully journaled before this snapshot, or is rejected
        before it can become an untracked open position.
        """
        if not self.paper_mode:
            logger.error("CMD_CLOSE_ALL в этом менеджере поддерживает только активные PAPER-позиции.")
            return {"closed": 0, "partial": 0, "blocked": 0, "message": "PAPER-позиции не активны."}

        emergency_reason = "Экстренное закрытие PAPER-позиций."
        async with self._portfolio_lock:
            if self._maintenance_active:
                existing_count = (
                    len(self.paper_positions)
                    if isinstance(self.paper_positions, dict) else 0
                )
                reason = self._maintenance_reason or "Другое обслуживание уже выполняется."
                return {
                    "closed": 0,
                    "partial": 0,
                    "blocked": existing_count,
                    "message": f"Экстренное закрытие не запущено: {reason}",
                }

            raw_positions_obj: object = self.paper_positions
            if not isinstance(raw_positions_obj, dict) or not raw_positions_obj:
                logger.info("📭 Нет активных позиций для экстренного закрытия.")
                return {
                    "closed": 0, "partial": 0, "blocked": 0,
                    "message": "Активных PAPER-позиций нет.",
                }

            raw_positions = dict(cast(dict[object, object], raw_positions_obj))
            self._maintenance_active = True
            self._maintenance_reason = emergency_reason

        logger.warning(
            "🚨 Запущено экстренное PAPER-закрытие %s позиций; новые входы заблокированы.",
            len(raw_positions),
        )
        closed_count = 0
        blocked_count = 0
        partial_count = 0
        try:
            for raw_ticker, pos_obj in raw_positions.items():
                ticker = str(raw_ticker).strip().upper().split("@")[0]
                if not ticker or not isinstance(pos_obj, dict):
                    blocked_count += 1
                    logger.error("Пропуск некорректной позиции %s при CMD_CLOSE_ALL.", raw_ticker)
                    continue

                pos = cast(TradeRecord, pos_obj)
                if str(pos.get("status", "OPEN")).strip().upper() != "OPEN":
                    continue
                side = str(pos.get("side", "BUY")).strip().upper()
                order_id = str(pos.get("id", "")).strip()
                if side not in {"BUY", "SELL"} or not order_id:
                    blocked_count += 1
                    logger.error("Пропуск позиции %s: некорректная сторона или ID.", ticker)
                    continue
                if order_id in self._lock_closing:
                    blocked_count += 1
                    logger.warning(
                        "CMD_CLOSE_ALL: позиция %s уже находится в процессе закрытия.", ticker
                    )
                    continue

                try:
                    bbo = self._latest_fresh_exit_quote(ticker, side)
                except Exception as exc:
                    bbo = None
                    logger.error(
                        "CMD_CLOSE_ALL: ошибка получения исполнимого BBO для %s: %s",
                        ticker, exc, exc_info=True,
                    )
                if bbo is None:
                    blocked_count += 1
                    logger.error(
                        "CMD_CLOSE_ALL: %s не закрыта — нет свежего исполнимого %s (порог %.2fs).",
                        ticker, "Bid" if side == "BUY" else "Ask",
                        _finite_or_default(os.getenv("PILUITUS_BBO_EXIT_MAX_AGE_SEC", "2.0"), 2.0),
                    )
                    continue

                partial_count_before = int(
                    _finite_or_default(pos.get("exit_partial_fill_count"), 0.0)
                )
                self._lock_closing.add(order_id)
                try:
                    last_price = self._latest_price(ticker, pos)
                    close_result = await self._finalize_paper_close(
                        ticker, pos, db, bbo,
                        exit_reason="MANUAL_CLOSE",
                        observed_last_price=last_price,
                    )
                    if str(close_result.get("status", "")).strip().upper() == "CLOSED":
                        closed_count += 1
                    elif int(
                        _finite_or_default(close_result.get("exit_partial_fill_count"), 0.0)
                    ) > partial_count_before:
                        partial_count += 1
                    else:
                        blocked_count += 1
                except Exception as exc:
                    blocked_count += 1
                    logger.error(
                        "CMD_CLOSE_ALL: не удалось закрыть %s: %s", ticker, exc, exc_info=True
                    )
                finally:
                    self._lock_closing.discard(order_id)

            result = {
                "closed": closed_count,
                "partial": partial_count,
                "blocked": blocked_count,
                "message": (
                    f"Экстренное PAPER-закрытие: полностью закрыто {closed_count}, "
                    f"частично закрыто {partial_count}, ожидает исполнения/заблокировано {blocked_count}."
                ),
            }
            logger.warning("🚨 [PAPER CLOSE ALL] %s", result["message"])
            return result
        finally:
            async with self._portfolio_lock:
                if self._maintenance_reason == emergency_reason:
                    self._maintenance_active = False
                    self._maintenance_reason = ""
                    logger.info(
                        "[PAPER MAINTENANCE] Экстренное закрытие завершено; "
                        "новые входы снова разрешены при выполнении всех риск-условий."
                    )
