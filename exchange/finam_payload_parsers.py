"""Compatibility facade combining the focused Finam payload parser mixins."""
from __future__ import annotations

from exchange.finam_quote_parser import FinamQuoteParserMixin
from exchange.finam_trade_parser import FinamTradeParserMixin
from exchange.finam_order_book_parser import FinamOrderBookParserMixin


class FinamPayloadParserMixin(
    FinamQuoteParserMixin,
    FinamTradeParserMixin,
    FinamOrderBookParserMixin,
):
    """Retain the historical parser import path and combined API."""
