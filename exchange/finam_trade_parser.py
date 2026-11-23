"""Finam INSTRUMENT_TRADES payload normalization."""
from __future__ import annotations
import time
from typing import Any

class FinamTradeParserMixin:
    @classmethod
    def _extract_instrument_trades(cls, envelope: dict) -> list[dict[str, Any]]:
        """Normalize public Finam INSTRUMENT_TRADES events; never infer a trade from QUOTES."""
        payload = cls._decode_payload(envelope.get("payload"))
        if not payload:
            payload = cls._decode_payload(envelope.get("data"))
        if isinstance(payload, list):
            candidates: Any = payload
            parent_symbol = str(envelope.get("symbol") or envelope.get("ticker") or "").strip()
        elif isinstance(payload, dict):
            candidates = (
                payload.get("trades")
                or payload.get("trade")
                or payload.get("instrument_trades")
                or payload.get("instrumentTrades")
            )
            parent_symbol = str(
                payload.get("symbol")
                or payload.get("ticker")
                or payload.get("securityCode")
                or envelope.get("symbol")
                or envelope.get("ticker")
                or ""
            ).strip()
            if candidates is None and any(
                key in payload
                for key in (
                    "trade_id", "tradeId", "price", "tradePrice",
                    "size", "tradeSize",
                )
            ):
                candidates = [payload]
        else:
            return []
        if isinstance(candidates, dict):
            candidates = [candidates]
        if not isinstance(candidates, list):
            return []
        envelope_timestamp = envelope.get("timestamp") or envelope.get("datetime") or envelope.get("time")
        received = envelope.get("_received_monotonic", time.monotonic())
        try:
            received = float(received)
        except (TypeError, ValueError, OverflowError):
            received = time.monotonic()
        result: list[dict[str, Any]] = []
        for item in candidates:
            if not isinstance(item, dict):
                continue
            symbol = str(
                item.get("symbol")
                or item.get("ticker")
                or item.get("securityCode")
                or parent_symbol
            ).strip()
            if not symbol:
                continue
            raw_price = item.get("price")
            if raw_price is None:
                raw_price = item.get("tradePrice")
            raw_size = (
                item.get("size")
                or item.get("quantity")
                or item.get("volume")
                or item.get("tradeSize")
            )
            price = cls._decimal(raw_price)
            size = cls._decimal(raw_size)
            if price <= 0.0 or size <= 0.0:
                continue
            result.append({
                "event_type": "INSTRUMENT_TRADE",
                "ticker": symbol.split("@", 1)[0].upper(),
                "symbol": symbol if "@" in symbol else f"{symbol}@MISX",
                "trade_id": item.get("trade_id") or item.get("tradeId") or item.get("id"),
                "price": price,
                "size": size,
                "side": item.get("side") or item.get("tradeSide"),
                "timestamp": (
                    item.get("timestamp")
                    or item.get("tradeTimestamp")
                    or item.get("tradeTime")
                    or envelope_timestamp
                ),
                "received_monotonic": received,
            })
        return result
