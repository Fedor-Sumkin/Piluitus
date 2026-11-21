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
