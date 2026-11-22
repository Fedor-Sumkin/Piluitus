"""Behavioral contracts for market observability composition."""
from __future__ import annotations

from monitoring.market_observability_runtime import MarketObservabilityRuntime


class _Logger:
    def __init__(self) -> None:
        self.info_messages: list[tuple[object, ...]] = []
        self.warning_messages: list[tuple[object, ...]] = []

    def info(self, *args: object, **kwargs: object) -> None:
        self.info_messages.append(args)

    def warning(self, *args: object, **kwargs: object) -> None:
        self.warning_messages.append(args)


def _build_runtime() -> MarketObservabilityRuntime:
    return MarketObservabilityRuntime(
        logger=_Logger(),  # type: ignore[arg-type]
        shadow_tracker=object(),
        execution_engine=object(),
        screener=object(),
        parse_finite_float=lambda value: None,
        guard_reason_bucket=lambda reason: "other",
        evaluate_candidate=lambda direction, metrics: {},
        pending_bbo_count_provider=lambda: 0,
        monotonic=lambda: 100.0,
    )


def test_guard_blocks_keep_the_existing_telemetry_categories() -> None:
    runtime = _build_runtime()

    runtime.guard_block("Сокращение объёма лучшей заявки ниже порога")

    counts = runtime.trade_flow_counters.snapshot_if_due(
        now=160.0, interval_sec=60.0, force=True
    )
    assert counts == {"guard_blocked": 1, "guard_size": 1}


def test_prefilter_warnings_keep_the_ticker_reason_cooldown() -> None:
    runtime = _build_runtime()

    runtime.log_prefilter_blocked("SBER", "Сокращение объёма лучшей заявки")
    runtime.log_prefilter_blocked("SBER", "Сокращение размера лучшей заявки")

    assert len(runtime.logger.warning_messages) == 1
