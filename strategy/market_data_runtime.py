"""Runtime helpers for M1 history warmup and deferred BBO candidates."""
from __future__ import annotations

import asyncio
import os
import threading
import time
from typing import Any, Callable

from strategy.candidate_recovery import coalesce_pending_bbo_entry
from strategy.signal_guard import validate_atr_readiness


class M1HistoryWarmup:
    """Bounded, non-blocking warmup of completed M1 bars for selected tickers."""

    def __init__(
        self,
        broker: Any,
        screener: Any,
        logger: Any,
        *,
        retry_sec: float = 30.0,
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Any] = asyncio.sleep,
        request_interval_env: str = "FINAM_METADATA_REQUEST_INTERVAL_SEC",
    ) -> None:
        self.broker = broker
        self.screener = screener
        self.logger = logger
        self.retry_sec = max(0.0, float(retry_sec))
        self.monotonic = monotonic
        self.sleep = sleep
        self.request_interval_env = request_interval_env
        self.pending: set[str] = set()
        self.warmed: set[str] = set()
        self.last_attempt: dict[str, float] = {}
        self.lock = threading.Lock()
        self.task: asyncio.Task | None = None

    async def warmup_selected_m1_history(
        self, tickers: tuple[str, ...] | list[str]
    ) -> None:
        """Warm selected symbols from completed Finam M1 bars without blocking quotes."""
        now = self.monotonic()
        due: list[str] = []
        with self.lock:
            for raw in tickers:
                ticker = str(raw or "").strip().upper().split("@")[0]
                if not ticker or ticker in self.pending:
                    continue
                if ticker in self.warmed:
                    engine = self.screener.minute_bar_patterns
                    snapshot = engine.get_atr_snapshot(ticker)
                    state = engine._states.get(ticker, {})
                    last_closed = float(state.get("last_closed_bar_monotonic", 0.0) or 0.0)
                    age = max(0.0, now - last_closed) if last_closed > 0.0 else -1.0
                    still_ready, _ = validate_atr_readiness({
                        "bar_atr": snapshot.get("atr", 0.0),
                        "bar_atr_periods": snapshot.get("periods", 0.0),
                        "bar_atr_age_sec": age,
                    })
                    if still_ready:
                        continue
                    self.warmed.discard(ticker)
                if now - self.last_attempt.get(ticker, 0.0) < self.retry_sec:
                    continue
                self.pending.add(ticker)
                self.last_attempt[ticker] = now
                due.append(ticker)

        for ticker in due:
            try:
                bars = await self.broker.get_historical_m1_bars(
                    ticker, lookback_minutes=120
                )
                imported = self.screener.minute_bar_patterns.load_closed_bars(
                    ticker, bars
                )
                if imported:
                    engine = self.screener.minute_bar_patterns
                    snapshot = engine.get_atr_snapshot(ticker)
                    state = engine._states.get(ticker, {})
                    last_closed = float(
                        state.get("last_closed_bar_monotonic", 0.0) or 0.0
                    )
                    atr_age = (
                        max(0.0, self.monotonic() - last_closed)
                        if last_closed > 0.0 else -1.0
                    )
                    atr_ready, atr_reason = validate_atr_readiness({
                        "bar_atr": snapshot.get("atr", 0.0),
                        "bar_atr_periods": snapshot.get("periods", 0.0),
                        "bar_atr_age_sec": atr_age,
                    })
                    if atr_ready:
                        with self.lock:
                            self.warmed.add(ticker)
                    self.logger.info(
                        "[M1 HISTORY WARMUP] %s: получено=%d, загружено_закрытых=%d, "
                        "ATR_ranges=%.0f, age=%.1fs, ready=%s; %s",
                        ticker, len(bars), imported,
                        float(snapshot.get("periods", 0.0)),
                        atr_age, atr_ready, atr_reason,
                    )
                else:
                    self.logger.warning(
                        "[M1 HISTORY WARMUP] %s: закрытые исторические бары не получены; "
                        "ATR будет прогреваться по live QUOTES.",
                        ticker,
                    )
            except Exception as exc:
                self.logger.warning(
                    "[M1 HISTORY WARMUP] %s failed (%s); live-built ATR remains active.",
                    ticker, type(exc).__name__,
                )
            finally:
                with self.lock:
                    self.pending.discard(ticker)
            try:
                request_interval = max(
                    0.1,
                    float(os.getenv(self.request_interval_env, "0.36")),
                )
            except (TypeError, ValueError, OverflowError):
                request_interval = 0.36
            await self.sleep(request_interval)

    def schedule(self, tickers: tuple[str, ...] | list[str]) -> None:
        """Keep at most one warmup coroutine scheduled while quotes continue."""
        if self.task is None or self.task.done():
            self.task = asyncio.create_task(
                self.warmup_selected_m1_history(tuple(tickers))
            )


class PendingBBOCandidateStore:
    """Thread-safe TTL store for candidates waiting for a coherent fresh BBO."""

    def __init__(
        self,
        ttl_sec: float,
        trade_flow_increment: Callable[[str, int], None],
        logger: Any,
        *,
        candidates: dict[str, dict[str, Any]] | None = None,
        lock: Any | None = None,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.ttl_sec = max(0.0, float(ttl_sec))
        self.trade_flow_increment = trade_flow_increment
        self.logger = logger
        self.candidates = candidates if candidates is not None else {}
        self.lock = lock if lock is not None else threading.Lock()
        self.monotonic = monotonic


    def get(self, ticker: str) -> dict[str, Any] | None:
        key = str(ticker or "").strip().upper().split("@")[0]
        with self.lock:
            return self.candidates.get(key)

    def drop(
        self, ticker: str, expected: dict[str, Any] | None = None
    ) -> bool:
        key = str(ticker or "").strip().upper()
        with self.lock:
            current = self.candidates.get(key)
            if current is not None and (expected is None or current is expected):
                self.candidates.pop(key, None)
                return True
        return False

    def cancel(
        self, ticker: str, expected: dict[str, Any] | None = None
    ) -> bool:
        removed = self.drop(ticker, expected)
        if removed:
            self.trade_flow_increment("bbo_cancelled", 1)
        return removed

    def expire(self, now: float | None = None) -> None:
        current_time = self.monotonic() if now is None else now
        expired: list[str] = []
        with self.lock:
            for ticker, item in list(self.candidates.items()):
                created_at = float(item.get("created_at", 0.0) or 0.0)
                if (
                    created_at <= 0.0
                    or current_time - created_at > self.ttl_sec
                ):
                    self.candidates.pop(ticker, None)
                    expired.append(ticker)
        for ticker in expired:
            self.trade_flow_increment("bbo_expired", 1)
            self.logger.debug(
                "⌛ Отложенный кандидат %s истёк: свежий BBO не поступил за %.2f сек.",
                ticker, self.ttl_sec,
            )

    def store(
        self,
        ticker: str,
        payload: dict[str, Any],
        received_monotonic: object | None = None,
    ) -> None:
        # TTL starts only when the candidate is stored. Coalescing preserves the
        # original waiter object and does not restart its deadline.
        now = self.monotonic()
        key = str(ticker).strip().upper().split("@")[0]
        with self.lock:
            previous = self.candidates.get(key)
            entry, status = coalesce_pending_bbo_entry(
                previous, payload, now=now, ttl_sec=self.ttl_sec
            )
            if status == "coalesced" and previous is not None:
                previous["payload"] = entry["payload"]
                self.candidates[key] = previous
            else:
                self.candidates[key] = entry
        if status == "expired":
            self.trade_flow_increment("bbo_expired", 1)
        elif status == "coalesced":
            self.trade_flow_increment("bbo_coalesced", 1)
            self.logger.debug(
                "♻️ [PRE-LLM COALESCE] %s: повторный сигнал обновил существующее ожидание.",
                key,
            )
            return
        elif status == "superseded":
            self.trade_flow_increment("bbo_superseded", 1)
        self.trade_flow_increment("bbo_deferred", 1)
        self.logger.info(
            "⏳ [PRE-LLM WAIT] %s: кандидат ожидает целостный свежий ORDER_BOOK "
            "(TTL=%.2fs; status=%s).",
            key, self.ttl_sec, status,
        )

    def is_current(
        self,
        ticker: str,
        pending: dict[str, Any],
        payload: dict[str, Any],
    ) -> bool:
        key = str(ticker or "").strip().upper().split("@")[0]
        with self.lock:
            return (
                self.candidates.get(key) is pending
                and pending.get("payload") is payload
            )

    def clear(self) -> None:
        with self.lock:
            self.candidates.clear()

    def count(self) -> int:
        with self.lock:
            return len(self.candidates)



class PendingBBOCandidateRuntime:
    """Coordinate deferred-candidate storage, revalidation, and AI queue admission."""

    def __init__(
        self,
        *,
        store: PendingBBOCandidateStore,
        screener: Any,
        execution_engine: Any,
        anomaly_queue: Any,
        queue_item_factory: Any,
        refresh_bbo_metrics: Callable[..., tuple[bool, str]],
        infer_candidate_direction: Callable[[dict[str, Any]], str | None],
        paper_mode_provider: Callable[[], bool],
        validate_entry_evidence: Callable[..., tuple[bool, str]],
        validate_atr_readiness: Callable[[dict[str, Any]], tuple[bool, str]],
        queue_flow_conflict_reason: Callable[[str, dict[str, Any]], str],
        reserve_ai_ticker: Callable[[str, str, dict[str, Any]], bool],
        release_ai_ticker: Callable[[str], None],
        mark_shadow_llm_not_called: Callable[[object, str], None],
        log_shadow_candidate: Callable[[str, str, dict[str, Any]], dict[str, Any]],
        trade_flow_increment: Callable[[str], None],
        guard_reason_bucket: Callable[[str], str],
        trade_flow_guard_block: Callable[[str], None],
        log_prefilter_blocked: Callable[..., None],
        update_numpy_impulse_status: Callable[[str], None],
        session_generation_provider: Callable[[], int],
        logger: Any,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.store = store
        self.screener = screener
        self.execution_engine = execution_engine
        self.anomaly_queue = anomaly_queue
        self.queue_item_factory = queue_item_factory
        self.refresh_bbo_metrics = refresh_bbo_metrics
        self.infer_candidate_direction = infer_candidate_direction
        self.paper_mode_provider = paper_mode_provider
        self.validate_entry_evidence = validate_entry_evidence
        self.validate_atr_readiness = validate_atr_readiness
        self.queue_flow_conflict_reason = queue_flow_conflict_reason
        self.reserve_ai_ticker = reserve_ai_ticker
        self.release_ai_ticker = release_ai_ticker
        self.mark_shadow_llm_not_called = mark_shadow_llm_not_called
        self.log_shadow_candidate = log_shadow_candidate
        self.trade_flow_increment = trade_flow_increment
        self.guard_reason_bucket = guard_reason_bucket
        self.trade_flow_guard_block = trade_flow_guard_block
        self.log_prefilter_blocked = log_prefilter_blocked
        self.update_numpy_impulse_status = update_numpy_impulse_status
        self.session_generation_provider = session_generation_provider
        self.logger = logger
        self.monotonic = monotonic

    def drop(
        self, ticker: str, expected: dict[str, Any] | None = None
    ) -> bool:
        return self.store.drop(ticker, expected)

    def cancel(
        self, ticker: str, expected: dict[str, Any] | None = None
    ) -> bool:
        return self.store.cancel(ticker, expected)

    def expire(self, now: float | None = None) -> None:
        self.store.expire(now)

    def store_candidate(
        self,
        ticker: str,
        payload: dict[str, Any],
        received_monotonic: object | None = None,
    ) -> None:
        self.store.store(ticker, payload, received_monotonic)

    def has_pending(self, ticker: str) -> bool:
        return self.store.get(ticker) is not None

    async def resume(
        self,
        ticker: str,
        *,
        trigger: str = "ORDER_BOOK",
    ) -> None:
        """Revalidate a short-lived candidate after a coherent book or fresh quote."""
        key = str(ticker or "").strip().upper().split("@")[0]
        pending = self.store.get(key)
        if pending is None:
            return

        normalized_trigger = str(trigger or "ORDER_BOOK").strip().upper()
        self.trade_flow_increment("bbo_resume_attempted")
        self.trade_flow_increment(
            "bbo_resume_from_quotes"
            if normalized_trigger == "QUOTES"
            else "bbo_resume_from_book"
        )

        now = self.monotonic()
        created_at = float(pending.get("created_at", 0.0) or 0.0)
        if created_at <= 0.0 or now - created_at > self.store.ttl_sec:
            self.drop(key, pending)
            self.trade_flow_increment("bbo_expired")
            self.trade_flow_increment("bbo_resume_expired")
            return

        payload = pending.get("payload")
        if not isinstance(payload, dict):
            self.trade_flow_increment("bbo_resume_invalid_payload")
            self.cancel(key, pending)
            return
        if (
            getattr(self.execution_engine, "_maintenance_active", False)
            or payload.get("paper_session_generation")
            != self.session_generation_provider()
        ):
            self.mark_shadow_llm_not_called(
                payload.get("shadow_candidate_id"), "PAPER_SESSION_CHANGED"
            )
            self.trade_flow_increment("bbo_resume_session_cancelled")
            self.cancel(key, pending)
            return

        funnel_status = self.screener.get_status()
        if not funnel_status.get("ready"):
            self.mark_shadow_llm_not_called(
                payload.get("shadow_candidate_id"), "FUNNEL_NOT_READY"
            )
            self.trade_flow_increment("bbo_resume_funnel_not_ready")
            self.cancel(key, pending)
            return
        if key not in set(funnel_status.get("selected_tickers", [])):
            self.mark_shadow_llm_not_called(
                payload.get("shadow_candidate_id"), "TOP15_DESELECTED"
            )
            self.trade_flow_increment("bbo_resume_not_selected")
            self.cancel(key, pending)
            return

        active_tickers = self.execution_engine.get_open_position_tickers()
        if key in active_tickers or key in getattr(
            self.execution_engine, "_opening_tickers", set()
        ):
            self.mark_shadow_llm_not_called(
                payload.get("shadow_candidate_id"), "POSITION_ALREADY_OPEN"
            )
            self.trade_flow_increment("bbo_resume_active_position")
            self.cancel(key, pending)
            self.trade_flow_increment("active_position_skipped")
            return

        metrics = payload.get("metrics")
        if not isinstance(metrics, dict):
            self.mark_shadow_llm_not_called(
                payload.get("shadow_candidate_id"), "INVALID_METRICS_PAYLOAD"
            )
            self.trade_flow_increment("bbo_resume_invalid_payload")
            self.cancel(key, pending)
            return

        book_ready, book_reason = self.refresh_bbo_metrics(
            self.screener, key, metrics, now=now
        )
        if not book_ready:
            self.trade_flow_increment(
                "bbo_resume_" + self.guard_reason_bucket(book_reason)
            )
            self.logger.debug(
                "[PRE-LLM BBO RESUME WAIT] %s trigger=%s: %s",
                key,
                normalized_trigger,
                book_reason,
            )
            return

        direction = self.infer_candidate_direction(metrics)
        original_direction = str(payload.get("candidate_direction", "")).upper()
        if direction not in {"BUY", "SELL"} or direction != original_direction:
            self.mark_shadow_llm_not_called(
                payload.get("shadow_candidate_id"), "DIRECTION_CHANGED"
            )
            self.trade_flow_increment("bbo_resume_direction_changed")
            self.cancel(key, pending)
            self.trade_flow_increment("direction_prefilter_skipped")
            return

        if not payload.get("shadow_candidate_id"):
            shadow_result = self.log_shadow_candidate(key, direction, metrics)
            payload["shadow_candidate_id"] = shadow_result.get("candidate_id")

        queue_flow_reason = self.queue_flow_conflict_reason(direction, metrics)
        if queue_flow_reason:
            self.mark_shadow_llm_not_called(
                payload.get("shadow_candidate_id"), "QUEUE_TRADE_FLOW_CONFLICT"
            )
            self.trade_flow_increment("bbo_resume_evidence_blocked")
            self.trade_flow_guard_block(queue_flow_reason)
            self.log_prefilter_blocked(key, queue_flow_reason)
            self.cancel(key, pending)
            return

        evidence_ok, evidence_reason = self.validate_entry_evidence(
            direction,
            metrics,
            paper_mode=self.paper_mode_provider(),
        )
        if not evidence_ok:
            if self.guard_reason_bucket(evidence_reason) == "quotes":
                self.trade_flow_increment("bbo_resume_evidence_quotes")
                return
            self.mark_shadow_llm_not_called(
                payload.get("shadow_candidate_id"), "SIGNAL_EVIDENCE_BLOCKED"
            )
            self.trade_flow_increment("bbo_resume_evidence_blocked")
            self.cancel(key, pending)
            self.trade_flow_increment("prefilter_blocked")
            self.trade_flow_guard_block(evidence_reason)
            self.log_prefilter_blocked(key, evidence_reason)
            return

        atr_ready, atr_reason = self.validate_atr_readiness(metrics)
        if not atr_ready:
            self.mark_shadow_llm_not_called(
                payload.get("shadow_candidate_id"), "ATR_NOT_READY"
            )
            self.trade_flow_increment("bbo_resume_atr_blocked")
            self.trade_flow_increment("atr_unready_candidates")
            self.cancel(key, pending)
            self.log_prefilter_blocked(key, atr_reason, interval_sec=60.0)
            return

        # Never enqueue stale evidence or remove a newer same-ticker candidate.
        if not self.store.is_current(key, pending, payload):
            self.mark_shadow_llm_not_called(
                payload.get("shadow_candidate_id"), "PAYLOAD_SUPERSEDED"
            )
            self.trade_flow_increment("bbo_resume_payload_changed")
            return

        if not self.reserve_ai_ticker(
            key, payload.get("candidate_direction"), metrics
        ):
            self.mark_shadow_llm_not_called(
                payload.get("shadow_candidate_id"), "AI_DEDUPLICATED"
            )
            self.trade_flow_increment("bbo_resume_deduplicated")
            self.cancel(key, pending)
            self.trade_flow_increment("candidate_deduplicated")
            return

        payload["bid"] = metrics.get("bid", 0.0)
        payload["ask"] = metrics.get("ask", 0.0)
        payload["bid_size"] = metrics.get("bid_size", 0.0)
        payload["ask_size"] = metrics.get("ask_size", 0.0)
        try:
            self.anomaly_queue.put_nowait(
                self.queue_item_factory.create(1, "MARKET_TICK", payload)
            )
            self.drop(key, pending)
            self.trade_flow_increment("bbo_resumed")
            self.trade_flow_increment("anomalies_queued")
            self.trade_flow_increment(
                "prefilter_pass_buy" if direction == "BUY" else "prefilter_pass_sell"
            )
            self.logger.info(
                "✅ [PRE-LLM RESUME] %s: свежий BBO и ORDER_BOOK подтверждены; "
                "кандидат %s передан в LLM-очередь (trigger=%s).",
                key,
                direction,
                normalized_trigger,
            )
            self.update_numpy_impulse_status(key)
        except asyncio.QueueFull:
            self.mark_shadow_llm_not_called(
                payload.get("shadow_candidate_id"), "AI_QUEUE_FULL"
            )
            self.release_ai_ticker(key)
            self.drop(key, pending)
            self.trade_flow_increment("bbo_resume_queue_full")
            self.trade_flow_increment("queue_dropped")
            self.logger.error(
                "Очередь AI-сигналов заполнена; восстановленный кандидат %s отброшен.",
                key,
            )


def paper_queue_flow_conflict_reason(
    direction: str,
    metrics: dict[str, Any],
    *,
    paper_mode: bool,
    enabled: object,
    conflict_detector: Callable[[str, dict[str, Any]], bool],
    finite_float: Callable[[object], float | None],
) -> str:
    """Return a block reason only for enabled PAPER queue/flow contradictions."""
    if not paper_mode:
        return ""
    guard_enabled = str(enabled if enabled is not None else "1").strip().lower()
    if guard_enabled in {"0", "false", "no", "off", "disabled"}:
        return ""
    if not conflict_detector(direction, metrics):
        return ""
    queue = finite_float(metrics.get("queue_imbalance_5s"))
    flow = finite_float(metrics.get("trade_flow_imbalance_30s"))
    return (
        "Устойчивый дисбаланс очереди 5s противоречит агрессивному потоку сделок 30s "
        f"(queue={queue:+.3f}, flow={flow:+.3f}); PAPER-вход заблокирован."
        if queue is not None and flow is not None
        else "Устойчивый дисбаланс очереди противоречит потоку сделок; PAPER-вход заблокирован."
    )
