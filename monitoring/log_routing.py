"""Deterministic routing of PILUITUS log records into thematic run files."""
from __future__ import annotations

import logging


DIAGNOSTIC_MARKERS = (
    # Strategy quality diagnostics retained in diagnostic_*.log.
    "[CANDIDATE TTL]",
    "[POSITION SIZE]",
    "[SIGNAL GUARD]",
    "[RISK FINAL GATE]",
    "[M1 HISTORY WARMUP]",
    "[M1 PATTERN]",
    "[NUMPY ИМПУЛЬС]",
    "[DIAGNOSTIC LOGGER READY]",
)


class DiagnosticLogFilter(logging.Filter):
    """Preserve the established diagnostic stream without mixing it with themes."""

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.ERROR or record.name == "Piluitus.Verdicts":
            return False
        try:
            message = record.getMessage()
        except Exception:
            return False
        return any(marker in message for marker in DIAGNOSTIC_MARKERS)


_THEMATIC_RULES = {
    "shadow": {
        "prefixes": ("Piluitus.StrategyShadow", "Piluitus.Shadow"),
        "markers": (
            "[STRATEGY SHADOW", "[SHADOW OUTCOME", "[SHADOW VALIDATION",
            "[SHADOW BE ARMED]", "[SHADOW BE TRIGGERED]",
        ),
    },
    "vllm": {
        "prefixes": ("Piluitus.AIEngine", "Piluitus.VLLM",
                     "Piluitus.EvolutionThoughts", "ai.vllm_client"),
        "markers": (
            "[vLLM", "[AIEngine", "[LLM REQUEST]", "[LLM RESPONSE]",
            "[LLM DECISION]", "[LLM REASON MISMATCH]", "[LLM CONTRACT",
            "[PRE-LLM FILTER]", "[PRE-LLM WAIT]",
        ),
    },
    "market": {
        "prefixes": ("Piluitus.FinamGateway", "Piluitus.Screener",
                     "Piluitus.Market", "FinamCore.MarketLogger"),
        "markers": (
            "[FINAM", "[ORDER_BOOK", "[TRADE FLOW", "[OPEN POSITION BBO",
            "[MARKET DATA", "[BBO ", "[QUOTE ", "[WS ПОТОК ФИНАМ]",
            "Поток Finam активен:", "Finam ORDER_BOOK подписки синхронизированы:",
        ),
    },
    "execution": {
        "prefixes": ("Piluitus.ExecutionLayer", "Piluitus.RiskManager"),
        "markers": (
            "[EXECUTION", "[PAPER", "[POSITION", "[PORTFOLIO",
            "[STOP COOLDOWN", "[ENTRY SESSION GUARD]",
        ),
    },
}


class ThematicLogFilter(logging.Filter):
    """Select one subsystem while leaving diagnostic-only records in diagnostics."""

    def __init__(self, kind: str,
                 diagnostic_filter: DiagnosticLogFilter | None = None) -> None:
        super().__init__()
        if kind not in _THEMATIC_RULES:
            raise ValueError(f"Unknown thematic log kind: {kind}")
        self.kind = kind
        self.diagnostic_filter = diagnostic_filter
        self.prefixes = _THEMATIC_RULES[kind]["prefixes"]
        self.markers = _THEMATIC_RULES[kind]["markers"]

    def filter(self, record: logging.LogRecord) -> bool:
        if record.name == "Piluitus.Verdicts":
            return False
        if self.diagnostic_filter and self.diagnostic_filter.filter(record):
            return False
        try:
            message = record.getMessage()
        except Exception:
            return False
        # A shadow break-even event may originate in ExecutionLayer, but belongs
        # exclusively to shadow_*.log rather than execution_*.log.
        if self.kind == "execution" and (
            "[SHADOW BE ARMED]" in message or "[SHADOW BE TRIGGERED]" in message
        ):
            return False
        if record.name.startswith(self.prefixes):
            return True
        return any(marker in message for marker in self.markers)


class SessionLogFilter(logging.Filter):
    """Keep only general run lifecycle and messages not owned by another stream."""

    def __init__(self, diagnostic_filter: DiagnosticLogFilter,
                 thematic_filters: tuple[ThematicLogFilter, ...]) -> None:
        super().__init__()
        self.diagnostic_filter = diagnostic_filter
        self.thematic_filters = thematic_filters

    def filter(self, record: logging.LogRecord) -> bool:
        if record.levelno >= logging.ERROR or record.name == "Piluitus.Verdicts":
            return False
        if self.diagnostic_filter.filter(record):
            return False
        return not any(item.filter(record) for item in self.thematic_filters)


class ErrorLogFilter(logging.Filter):
    """Collect all errors and critical events independently of thematic routing."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.levelno >= logging.ERROR


class VerdictLogFilter(logging.Filter):
    """Keep the original human-readable verdict blocks in their own file."""

    def filter(self, record: logging.LogRecord) -> bool:
        return record.name == "Piluitus.Verdicts"
