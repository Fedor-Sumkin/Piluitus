"""Finam QUOTES payload normalization."""
from __future__ import annotations
import time
from typing import Any

class FinamQuoteParserMixin:
    @classmethod
    def _extract_quotes(cls, envelope: dict) -> list[dict]:
        """Normalize Finam QUOTES envelopes, including JSON-string payloads.

        Current Finam messages can look like:
        {"type":"DATA", "subscription_type":"QUOTES",
         "payload":"{\"quote\":[{\"symbol\":...,\"last\":{\"value\":...}}]}"}

        In that format payload itself is a JSON string (not an object), and
        decimal-like fields such as last/lastSize/volume are objects containing
        a `value` member.
        """
        payload = cls._decode_payload(envelope.get("payload"))
        if not payload:
            payload = cls._decode_payload(envelope.get("data"))
        if not payload:
            payload = {}

        if isinstance(payload, list):
            candidates: Any = payload
        elif isinstance(payload, dict):
            candidates = (
                payload.get("quote")
                or payload.get("quotes")
                or payload.get("trades")
                or payload.get("trade")
            )
            if candidates is None and any(
                key in payload for key in ("symbol", "ticker", "security_code", "last", "price")
            ):
                candidates = [payload]
        else:
            candidates = None

        # Compatibility with envelopes that place the quote outside payload.
        if candidates is None:
            top_quote = cls._decode_payload(envelope.get("quote") or envelope.get("quotes"))
            if top_quote not in (None, {}, []):
                candidates = top_quote

        if isinstance(candidates, dict):
            candidates = [candidates]
        if not isinstance(candidates, list):
            return []

        envelope_timestamp = (
            envelope.get("timestamp") or envelope.get("datetime") or envelope.get("time")
            or (payload.get("timestamp") if isinstance(payload, dict) else None)
        )
        try:
            received_monotonic = float(envelope.get("_received_monotonic", time.monotonic()))
        except (TypeError, ValueError, OverflowError):
            received_monotonic = time.monotonic()

        result: list[dict] = []
        for item in candidates:
            if not isinstance(item, dict):
                continue

            symbol = str(
                item.get("symbol")
                or item.get("ticker")
                or item.get("security_code")
                or ""
            ).strip()
            if not symbol:
                continue

            # Finam decimal fields can be plain values or {"value": "..."}.
            last_raw = item.get("last")
            if last_raw is None:
                last_raw = item.get("price")
            if last_raw is None:
                last_raw = item.get("last_price")
            if last_raw is None:
                last_raw = item.get("close")
            price = cls._decimal(last_raw)
            if price <= 0:
                continue

            # The screener's per-event `volume` must be the size of this
            # trade, never cumulative session volume. Falling back to `volume`
            # fabricates a trade-size spike when lastSize is absent and can
            # distort both anomaly detection and VWAP weighting.
            last_size_raw = item.get("lastSize")
            if last_size_raw is None:
                last_size_raw = item.get("last_size")
            if last_size_raw is None:
                last_size_raw = item.get("size")
            session_volume_raw = item.get("volume")
            last_size = cls._decimal(last_size_raw)
            session_volume = cls._decimal(session_volume_raw)

            ticker = symbol.split("@")[0]
            bid_raw = item.get("bid")
            ask_raw = item.get("ask")
            # Prefer documented snake_case keys; fall back to legacy camelCase.
            bid_size_raw = item.get("bid_size")
            if bid_size_raw is None:
                bid_size_raw = item.get("bidSize")
            ask_size_raw = item.get("ask_size")
            if ask_size_raw is None:
                ask_size_raw = item.get("askSize")

            # Presence flags distinguish a partial update (field omitted) from an
            # explicit zero quote (Finam documents bid=0 when no active bid).
            bid_present = "bid" in item and bid_raw is not None and bid_raw != ""
            ask_present = "ask" in item and ask_raw is not None and ask_raw != ""
            bid_size_present = (
                ("bid_size" in item or "bidSize" in item)
                and bid_size_raw is not None and bid_size_raw != ""
            )
            ask_size_present = (
                ("ask_size" in item or "askSize" in item)
                and ask_size_raw is not None and ask_size_raw != ""
            )

            result.append({
                "ticker": ticker,
                "security_code": ticker,
                "symbol": symbol,
                "price": price,
                "volume": max(0.0, last_size),
                "last_size": max(0.0, last_size),
                "day_volume": max(0.0, session_volume),
                "bid": cls._decimal(bid_raw),
                "ask": cls._decimal(ask_raw),
                "bid_size": max(0.0, cls._decimal(bid_size_raw)),
                "ask_size": max(0.0, cls._decimal(ask_size_raw)),
                "bid_present": bool(bid_present),
                "ask_present": bool(ask_present),
                "bid_size_present": bool(bid_size_present),
                "ask_size_present": bool(ask_size_present),
                # An explicit null/empty item timestamp must not mask the
                # envelope event timestamp. Both feed paths use the same clock.
                "timestamp": item.get("timestamp") or envelope_timestamp,
                "received_monotonic": received_monotonic,
                "is_snapshot": bool(item.get("isDataSnapshot", item.get("is_snapshot", False))),
            })
        return result
