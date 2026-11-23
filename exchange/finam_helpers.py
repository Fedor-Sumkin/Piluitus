"""Shared parsing and safe-normalization helpers for Finam client mixins."""
from __future__ import annotations

from exchange.finam_shared import (
    asyncio, logging, math, datetime, timezone, timedelta, url_quote,
    Callable, Iterable, Any, json, inspect, os, re, time, websockets, httpx,
    logger, JOURNAL_FILE, _MOSCOW_TZ,
    _websocket_close_code, _websocket_disconnect_log_level,
    _coalesce_queued_market_event, _event_timestamp_to_monotonic,
    _should_resync_position_order_book,
)

class FinamHelpersMixin:
    @staticmethod
    def _decimal(value: Any, default: float = 0.0) -> float:
        """Convert Finam decimal fields and reject NaN/Infinity."""
        if isinstance(value, dict):
            value = value.get("value", default)
        try:
            number = float(value)
        except (TypeError, ValueError, OverflowError):
            return default
        return number if math.isfinite(number) else default

    @staticmethod
    def _redact_for_log(value: Any) -> Any:
        """Recursively redact credentials before logging a server message."""
        secret_words = ("token", "secret", "authorization", "password", "api_key", "access_key")
        if isinstance(value, dict):
            redacted = {}
            for key, item in value.items():
                key_text = str(key).lower()
                redacted[key] = "<redacted>" if any(word in key_text for word in secret_words) else FinamBrokerClient._redact_for_log(item)
            return redacted
        if isinstance(value, list):
            return [FinamBrokerClient._redact_for_log(item) for item in value[:20]]
        return value

    @staticmethod
    def _safe_response_hint(response: httpx.Response, max_chars: int = 400) -> str:
        """Return a bounded provider error body with credentials redacted."""
        try:
            payload = response.json()
        except (ValueError, TypeError):
            payload = response.text or ""
        if isinstance(payload, (dict, list)):
            rendered = json.dumps(
                FinamBrokerClient._redact_for_log(payload),
                ensure_ascii=False,
                separators=(",", ":"),
            )
        else:
            rendered = str(payload)
        rendered = re.sub(
            r"(?i)(bearer\s+)[A-Za-z0-9._~+/=-]+",
            r"\1<redacted>",
            rendered,
        )
        rendered = re.sub(
            r'(?i)((?:access[_-]?token|refresh[_-]?token|token|authorization|password|secret|api[_-]?key)\s*[:=]\s*)("[^"]*"|[^,\s}]+)',
            r"\1<redacted>",
            rendered,
        )
        rendered = re.sub(r"\s+", " ", rendered).strip()
        return rendered[:max(0, int(max_chars))] or "<empty>"

    @staticmethod
    def _extract_order_book_symbol_hints(value: Any) -> set[str]:
        """Find instrument symbols in a decoded ORDER_BOOK envelope for diagnostics."""
        found: set[str] = set()
        symbol_keys = {
            "symbol", "symbols", "ticker", "securitycode", "symbolcode",
        }

        def add_symbol(candidate: Any) -> None:
            if isinstance(candidate, str):
                ticker = candidate.strip().upper().split("@", 1)[0]
                if ticker and len(ticker) <= 32 and not any(ch.isspace() for ch in ticker):
                    found.add(ticker)
            elif isinstance(candidate, dict):
                for nested_key, nested_value in candidate.items():
                    normalized = str(nested_key).replace("_", "").lower()
                    if normalized in symbol_keys or normalized == "code":
                        add_symbol(nested_value)
            elif isinstance(candidate, list):
                for item in candidate[:100]:
                    add_symbol(item)

        def visit(node: Any, depth: int = 0) -> None:
            if depth > 7:
                return
            if isinstance(node, dict):
                for key, child in node.items():
                    normalized = str(key).replace("_", "").lower()
                    if normalized in symbol_keys or normalized == "security":
                        add_symbol(child)
                    elif isinstance(child, (dict, list)):
                        visit(child, depth + 1)
            elif isinstance(node, list):
                for item in node[:100]:
                    visit(item, depth + 1)

        visit(value)
        return found

    @staticmethod
    def _decode_payload(payload: Any) -> Any:
        """Decode Finam's payload: it can be a dict or a JSON-encoded string."""
        if isinstance(payload, str):
            payload = payload.strip()
            if not payload:
                return {}
            try:
                return json.loads(payload)
            except json.JSONDecodeError:
                return {}
        return payload
