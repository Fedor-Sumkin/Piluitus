"""Executable quote freshness and market-session validation mixin."""
from __future__ import annotations

from execution.execution_shared import *  # noqa: F401,F403


class ExecutionMarketSessionMixin:
    def _latest_fresh_exit_quote(self, ticker: str, position_side: str) -> BBOExitQuote | None:
        """Return the fresh executable side needed to close this position.

        A long position exits by selling at Bid; a short position exits by buying
        at Ask. Requiring the *opposite* side to be fresh can unnecessarily defer
        a valid stop/target check, so freshness is evaluated independently for
        the executable side only.
        """
        if self.screener is None:
            return None
        try:
            quote = self.screener.quotes.get(ticker)
            if not isinstance(quote, Mapping):
                return None
            side = str(position_side or "").strip().upper()
            if side not in {"BUY", "SELL"}:
                return None
            bid = _finite_or_default(quote.get("bid", 0.0), 0.0)
            ask = _finite_or_default(quote.get("ask", 0.0), 0.0)
            bid_seen = _finite_or_default(
                quote.get("last_bid_confirmed_seen", quote.get("last_bid_seen", 0.0)), 0.0
            )
            ask_seen = _finite_or_default(
                quote.get("last_ask_confirmed_seen", quote.get("last_ask_seen", 0.0)), 0.0
            )
            now = time.monotonic()
            bid_age = now - bid_seen if bid_seen > 0.0 else math.inf
            ask_age = now - ask_seen if ask_seen > 0.0 else math.inf
            # A crossed book is not a valid executable market. Do not open/close
            # a simulated position against an internally inconsistent BBO.
            if bid > 0.0 and ask > 0.0 and ask < bid:
                return None
            # The screener TTL is the single source of truth for candidate
            # validation and PAPER execution; do not let two env settings drift.
            configured_age = getattr(self.screener, "bbo_max_age_sec", None)
            if configured_age is None:
                configured_age = os.getenv(
                    "PILUITUS_BBO_MAX_AGE_SEC",
                    os.getenv("PILUITUS_BBO_EXIT_MAX_AGE_SEC", "2.0"),
                )
            max_age = _finite_or_default(configured_age, 2.0)
            if not math.isfinite(max_age) or max_age <= 0.0:
                max_age = 2.0
            max_age = min(5.0, max(0.25, max_age))

            executable_price = bid if side == "BUY" else ask
            executable_age = bid_age if side == "BUY" else ask_age
            if (
                not math.isfinite(executable_price) or executable_price <= 0.0
                or executable_age < 0.0 or executable_age > max_age
            ):
                return None

            two_sided_valid = (
                math.isfinite(bid) and math.isfinite(ask)
                and bid > 0.0 and ask >= bid
                and bid_age >= 0.0 and ask_age >= 0.0
                and bid_age <= max_age and ask_age <= max_age
            )
            book_seen = _finite_or_default(quote.get("last_order_book_seen", 0.0), 0.0)
            book_age = now - book_seen if book_seen > 0.0 else math.inf
            book_valid = bool(quote.get("order_book_valid", False))
            mid = (bid + ask) / 2.0 if two_sided_valid else 0.0
            spread_pct = ((ask - bid) / mid * 100.0) if mid > 0.0 else 0.0
            bid_levels_obj = quote.get("bid_levels", [])
            ask_levels_obj = quote.get("ask_levels", [])
            bid_levels = bid_levels_obj if isinstance(bid_levels_obj, list) else []
            ask_levels = ask_levels_obj if isinstance(ask_levels_obj, list) else []
            return {
                "bid": bid,
                "ask": ask,
                "bid_size": _finite_or_default(quote.get("bid_size", 0.0), 0.0),
                "ask_size": _finite_or_default(quote.get("ask_size", 0.0), 0.0),
                "bid_levels": cast(list[list[float]], bid_levels),
                "ask_levels": cast(list[list[float]], ask_levels),
                "bid_age_sec": bid_age,
                "ask_age_sec": ask_age,
                "age_sec": executable_age,
                "executable_price": executable_price,
                "executable_side": "BID" if side == "BUY" else "ASK",
                "spread_pct": spread_pct,
                "max_age_sec": max_age,
                "order_book_valid": book_valid,
                "order_book_age_sec": book_age,
            }
        except (TypeError, ValueError, AttributeError, OverflowError):
            return None

    def _latest_fresh_entry_quote(self, ticker: str, entry_side: str) -> BBOExitQuote | None:
        """Require a fresh two-sided BBO and select Ask for BUY / Bid for SELL."""
        side = str(entry_side or "").strip().upper()
        if side not in {"BUY", "SELL"}:
            return None
        # Existing exit helper already applies the same per-side max-age policy.
        bid_quote = self._latest_fresh_exit_quote(ticker, "BUY")
        ask_quote = self._latest_fresh_exit_quote(ticker, "SELL")
        if bid_quote is None or ask_quote is None:
            return None
        if (
            float(bid_quote["bid_age_sec"]) > float(bid_quote["max_age_sec"])
            or float(ask_quote["ask_age_sec"]) > float(ask_quote["max_age_sec"])
        ):
            return None
        chosen = ask_quote if side == "BUY" else bid_quote
        max_spread_pct = _finite_or_default(
            os.getenv("PILUITUS_MAX_ENTRY_SPREAD_PCT", "0.50"), 0.50
        )
        if not math.isfinite(max_spread_pct) or max_spread_pct <= 0.0:
            max_spread_pct = 0.50
        if (
            not math.isfinite(float(chosen.get("spread_pct", math.inf)))
            or float(chosen.get("spread_pct", math.inf)) > max_spread_pct
        ):
            return None
        return {
            **chosen,
            "executable_price": float(chosen["ask"] if side == "BUY" else chosen["bid"]),
            "executable_side": "ASK" if side == "BUY" else "BID",
            "age_sec": float(chosen["ask_age_sec"] if side == "BUY" else chosen["bid_age_sec"]),
        }

    @staticmethod
    def _entry_hard_cutoff_msk(now: datetime) -> datetime | None:
        """Return the configured hard cutoff for new entries on this Moscow date."""
        raw = str(os.getenv("PILUITUS_ENTRY_HARD_CUTOFF_MSK", getattr(
            Settings, "ENTRY_HARD_CUTOFF_MSK", "22:00"
        try:
            result = getter(ticker)
            payload = await result if inspect.isawaitable(result) else result
        except Exception as exc:
            return {
                "known": False, "is_open": False, "entry_allowed": False,
                "reason": f"schedule_request_failed:{type(exc).__name__}",
            }
        if not isinstance(payload, Mapping) or not isinstance(payload.get("sessions"), list):
            return {"known": False, "is_open": False, "entry_allowed": False, "reason": "schedule_missing_or_invalid"}

        now = datetime.now(_MOSCOW_TZ)
        parsed_windows: list[tuple[datetime, datetime, str]] = []
        for raw_session in payload.get("sessions", []):
            if not isinstance(raw_session, Mapping):
                continue
            quote = self.screener.quotes.get(ticker)
            if not isinstance(quote, Mapping):
                return "quote=отсутствует"
            now = time.monotonic()
            bid = _finite_or_default(quote.get("bid"), 0.0)
            ask = _finite_or_default(quote.get("ask"), 0.0)
            bid_seen = _finite_or_default(
                quote.get("last_bid_confirmed_seen", quote.get("last_bid_seen", 0.0)), 0.0
            )
            ask_seen = _finite_or_default(
                quote.get("last_ask_confirmed_seen", quote.get("last_ask_seen", 0.0)), 0.0
            )
            bid_age = now - bid_seen if bid_seen > 0.0 else math.inf
            ask_age = now - ask_seen if ask_seen > 0.0 else math.inf
            max_age = _finite_or_default(getattr(self.screener, "bbo_max_age_sec", 2.0), 2.0)
            if not math.isfinite(max_age) or max_age <= 0.0:
                max_age = 2.0
            max_age = min(5.0, max(0.25, max_age))
            valid = bool(quote.get("order_book_valid", False))
            reason = str(quote.get("order_book_last_reason", "UNKNOWN") or "UNKNOWN")
            bid_label = f"{bid_age:.3f}s" if math.isfinite(bid_age) else "нет"
            ask_label = f"{ask_age:.3f}s" if math.isfinite(ask_age) else "нет"
            mid = (bid + ask) / 2.0 if bid > 0.0 and ask >= bid else 0.0
            spread_pct = ((ask - bid) / mid * 100.0) if mid > 0.0 else math.inf
            spread_limit = _finite_or_default(
                os.getenv("PILUITUS_MAX_ENTRY_SPREAD_PCT", "0.50"), 0.50
            )
            return (
                f"bid={bid:.6f} age={bid_label}; ask={ask:.6f} age={ask_label}; "
                f"spread={spread_pct:.3f}%/limit={spread_limit:.3f}%; "
                f"limit={max_age:.3f}s; book_valid={valid}; book_reason={reason}"
            )
        except Exception as exc:
            return f"diagnostic_error={type(exc).__name__}: {exc}"
