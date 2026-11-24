"""Finam-confirmed lot metadata refresh and normalization mixin."""
from __future__ import annotations

from risk.risk_shared import *  # noqa: F401,F403


class RiskLotMetadataMixin:
    @classmethod
    async def update_finam_lot_sizes(cls, finam_broker_client=None, paper_mode: bool | None = None):
        """Load PAPER lot multipliers from LOT_SIZES and refresh Finam price-step metadata."""
        risk_logger = logging.getLogger("Piluitus.RiskManager")
        is_paper = PAPER_TRADING if paper_mode is None else bool(paper_mode)
        if not LOT_SIZES and not TICKERS_TO_TRADE:
            risk_logger.error("❌ Список инструментов пуст: Finam metadata refresh невозможен.")
            return
        asset_method = getattr(finam_broker_client, "get_asset", None) if finam_broker_client is not None else None
        if finam_broker_client is None or not callable(asset_method):
            risk_logger.error(
                "Finam GetAsset metadata source unavailable; confirmed tick rules are preserved "
                "and entries without valid price-step metadata remain blocked."
            )
            return

        params_method = getattr(finam_broker_client, "get_asset_params", None)
        account_id = str(getattr(finam_broker_client, "account_id", "") or "").strip()
        account_id_source = str(
            getattr(finam_broker_client, "account_id_source", "") or "authenticated_session"
        )
        # Finam REST GetAsset requires account_id even for read-only instrument metadata.
        # PAPER resolves a token-authorized metadata context, never the execution account.
        metadata_account_id = str(
            getattr(finam_broker_client, "metadata_account_id", "") or ""
        ).strip()
        metadata_account_id_source = str(
            getattr(finam_broker_client, "metadata_account_id_source", "") or "unresolved"
        )
        if is_paper and not metadata_account_id:
            discover_metadata = getattr(finam_broker_client, "discover_metadata_account_id", None)
            if callable(discover_metadata):
                try:
                    resolved = await discover_metadata()
                    metadata_account_id = str(
                        getattr(finam_broker_client, "metadata_account_id", "") or resolved or ""
                    ).strip()
                    metadata_account_id_source = str(
                        getattr(finam_broker_client, "metadata_account_id_source", "")
                        or ("session_details" if metadata_account_id else "unresolved")
                    )
                except Exception as exc:
                    risk_logger.warning(
                        "[FINAM METADATA] PAPER read-only account discovery failed: %s",
                        type(exc).__name__,
                    )
        if is_paper and (
            not metadata_account_id or metadata_account_id.upper() == "UNKNOWN_ACCOUNT"
        ):
            metadata_account_id = ""
            risk_logger.warning(
                "[FINAM METADATA] PAPER GetAsset skipped: Finam REST requires account_id, "
                "but the authenticated token has no resolved account_ids. Static LOT_SIZES "
                "remain active; MINSTEP refresh is unavailable until a token-authorized "
                "account is selected in Finam."
            )
        if not is_paper and not account_id:
            discover = getattr(finam_broker_client, "discover_accounts", None)
            if callable(discover):
                try:
                    resolved = await discover()
                    account_id = str(
                        getattr(finam_broker_client, "account_id", "") or resolved or ""
                    ).strip()
                    account_id_source = str(
                        getattr(finam_broker_client, "account_id_source", "")
                        or "authenticated_session"
                    )
                except Exception as exc:
                    risk_logger.warning(
                        "[FINAM METADATA] account discovery failed: %s",
                        type(exc).__name__,
                    )
        if not is_paper and (not account_id or account_id.upper() == "UNKNOWN_ACCOUNT"):
            account_id = ""
            risk_logger.error(
                "⛔ [FINAM METADATA] account_id was not resolved from the authenticated "
                "Finam session; LIVE GetAssetParams is skipped and LIVE entries remain blocked."
            )

        # LOTSIZE in PAPER remains sourced exclusively from LOT_SIZES.
        # The metadata request context is independent from order routing.
        metadata_request_account_id = metadata_account_id if is_paper else account_id
        metadata_source_label = "FINAM_GET_ASSET"
        if not is_paper and not callable(params_method):
            risk_logger.error(
                "Finam GetAssetParams unavailable; no account-specific lot values will authorize LIVE orders."
            )

        # Pace external metadata calls and avoid an uncontrolled startup burst.
        # Finam documents a 200 requests/minute REST limit; the default
        # 0.36-second spacing stays below that ceiling for the 88-symbol universe.
        semaphore = asyncio.Semaphore(6)
        request_lock = asyncio.Lock()
        interval_env = "FINAM_METADATA_REQUEST_INTERVAL_SEC"
        interval_default = "0.36"
        try:
            request_interval_sec = float(os.getenv(interval_env, interval_default))
        except (TypeError, ValueError, OverflowError):
            request_interval_sec = float(interval_default)
        if not math.isfinite(request_interval_sec) or request_interval_sec < 0.0:
            request_interval_sec = float(interval_default)
        next_request_at = 0.0
        updated_lots = failed_lots = updated_metadata = failed_metadata = 0
        blocked_not_tradable = 0
        paper_lot_failure_examples: list[str] = []

        async def call_limited(method, ticker: str):
            nonlocal next_request_at
            async with request_lock:
                wait_sec = max(0.0, next_request_at - time.monotonic())
                if wait_sec > 0.0:
                    await asyncio.sleep(wait_sec)
                next_request_at = time.monotonic() + request_interval_sec
            # Finam GetAsset requires an explicit, token-authorized account_id.
            # PAPER uses metadata_account_id only; LIVE uses its execution account_id.
            if not metadata_request_account_id:
                return None
            return await method(ticker, account_id=metadata_request_account_id)

        async def refresh_one(ticker: str) -> None:
            nonlocal updated_lots, failed_lots, updated_metadata, failed_metadata, blocked_not_tradable
            normalized = str(ticker or "").strip().upper().split("@", 1)[0]
            if not normalized:
                return
            async with semaphore:
                asset_payload = None
                params_payload = None
                exchange_lot = 0.0
                asset_market_ok = False
                asset_mic = ""
                # Never issue GetAsset without the required Finam account context.
                if callable(asset_method) and metadata_request_account_id:
                    try:
                        asset_payload = await call_limited(asset_method, normalized)
                    except Exception as exc:
                        risk_logger.warning(
                            "[FINAM METADATA] GetAsset %s raised locally: %s",
                            normalized, type(exc).__name__,
                        )
                        asset_payload = None
                if not is_paper and callable(params_method) and account_id:
                    try:
                        params_payload = await call_limited(params_method, normalized)
                    except Exception as exc:
                        risk_logger.warning(
                            "[FINAM METADATA] GetAssetParams %s failed locally: %s",
                            normalized, type(exc).__name__,
                        )
                        params_payload = None

            # Finam REST payloads may expose the asset as a nested object and
            # protobuf scalar wrappers may appear as {"value": "..."} or as
            # attribute objects. Normalize both forms before reading lot_size.
            def field(source, *names):
                if isinstance(source, dict):
                    for name in names:
                        if name in source:
                            return source[name]
                else:
                    for name in names:
                        candidate = getattr(source, name, None)
                        if candidate is not None:
                            return candidate
                return None

            asset_data = field(asset_payload, "asset", "data") or asset_payload
            if not isinstance(asset_data, dict):
                try:
                    asset_data = vars(asset_data)
                except (TypeError, AttributeError):
                    asset_data = None

            def scalar(value):
                # Unwrap common protobuf/JSON scalar wrappers without accepting
                # arbitrary objects or silently coercing booleans.
                for _ in range(3):
                    if isinstance(value, dict):
                        next_value = field(value, "value", "units", "amount")
                        if next_value is None:
                            return None
                        value = next_value
                    elif not isinstance(value, (str, int, float)) and value is not None:
                        next_value = getattr(value, "value", None)
                        if next_value is None:
                            return None
                        value = next_value
                    else:
                        break
                if isinstance(value, bool):
                    return None
                return value

            if isinstance(asset_data, dict):
                try:
                    decimals_raw = scalar(field(asset_data, "decimals"))
                    min_step_raw = scalar(field(asset_data, "min_step", "minStep"))
                    decimals = int(decimals_raw) if decimals_raw is not None else -1
                    min_step = int(min_step_raw) if min_step_raw is not None else 0
                    mic = str(scalar(field(asset_data, "mic", "exchange_mic")) or "").strip().upper()
                    symbol = str(scalar(field(asset_data, "symbol", "ticker")) or "")
                    asset_mic = mic or ("MISX" if symbol.upper().endswith("@MISX") else "UNSPECIFIED")
                    asset_market_ok = (
                        mic == "MISX"
                        or symbol.upper().endswith("@MISX")
                    )

                    price_step_raw = field(asset_data, "price_step", "priceStep")
                    price_step_data = field(price_step_raw, "price_step", "priceStep") or price_step_raw
                    dynamic_rule = None
                    if price_step_data is not None:
                        threshold_raw = scalar(field(price_step_data, "threshold_price", "thresholdPrice"))
                        below_raw = scalar(field(price_step_data, "step"))
                        above_raw = scalar(field(price_step_data, "step_from_threshold", "stepFromThreshold"))
                        try:
                            threshold = float(threshold_raw)
                            below_step = float(below_raw)
                            above_step = float(above_raw)
                            if (
                                math.isfinite(threshold) and threshold > 0.0
                                and math.isfinite(below_step) and below_step > 0.0
                                and math.isfinite(above_step) and above_step > 0.0
                            ):
                                dynamic_rule = {
                                    "threshold_price": threshold,
                                    "step": below_step,
                                    "step_from_threshold": above_step,
                                }
                        except (TypeError, ValueError, OverflowError):
                            dynamic_rule = None

                    fixed_tick = (
                        min_step / (10 ** decimals)
                        if 0 <= decimals <= 10 and min_step > 0 else 0.0
                    )
                    metadata_valid = (
                        0 <= decimals <= 10
                        and asset_market_ok
                        and (fixed_tick > 0.0 or dynamic_rule is not None)
                    )
                    if metadata_valid:
                        ASSET_METADATA[normalized] = {
                            "symbol": symbol or f"{normalized}@MISX",
                            "decimals": decimals,
                            "min_step": min_step,
                            "price_tick": fixed_tick,
                            "price_step": dynamic_rule,
                            "lot_size": int(LOT_SIZES.get(normalized, 0) or 0),
                            "quote_currency": str(scalar(field(asset_data, "quote_currency", "quoteCurrency")) or ""),
                            "metadata_source": metadata_source_label,
                        }
                        updated_metadata += 1
                    else:
                        failed_metadata += 1
                except (TypeError, ValueError, OverflowError, ZeroDivisionError):
                    failed_metadata += 1
            elif callable(asset_method):
                failed_metadata += 1

            if is_paper:
                # LOTSIZE is maintained in the project's static exchange-lot dictionary.
                # It is intentionally independent from Finam metadata and account trade_lot_size.
                try:
                    static_lot = int(LOT_SIZES.get(normalized, 0) or 0)
                except (TypeError, ValueError, OverflowError):
                    static_lot = 0
                if static_lot > 0:
                    # LOT_SIZES is the canonical PAPER exchange-lot dictionary.
                    # Preserve an already confirmed runtime value during refresh.
                    existing_lot = FINAM_TRADE_LOT_SIZES.get(normalized)
                    try:
                        existing_lot = int(existing_lot or 0)
                    except (TypeError, ValueError, OverflowError):
                        existing_lot = 0
                    if existing_lot <= 0:
                        FINAM_TRADE_LOT_SIZES[normalized] = static_lot
                        updated_lots += 1
                else:
                    # A missing static value must not erase a confirmed value
                    # during a transient provider outage.
                    existing_lot = FINAM_TRADE_LOT_SIZES.get(normalized)
                    try:
                        existing_lot = int(existing_lot or 0)
                    except (TypeError, ValueError, OverflowError):
                        existing_lot = 0
                    if existing_lot <= 0:
                        FINAM_TRADE_LOT_SIZES.pop(normalized, None)
                    failed_lots += 1
                    if len(paper_lot_failure_examples) < 8:
                        paper_lot_failure_examples.append(
                            f"{normalized}:LOT_SIZES dictionary has no valid exchange lot"
                        )
            elif isinstance(params_payload, dict):
                raw = params_payload.get("trade_lot_size")
                tradable_raw = params_payload.get("is_tradable")
                if tradable_raw is None:
                    tradable_raw = params_payload.get("tradeable")
                if isinstance(tradable_raw, dict):
                    tradable_raw = tradable_raw.get("value")
                is_tradable = (
                    tradable_raw is True
                    or (isinstance(tradable_raw, str) and tradable_raw.strip().lower() in {"true", "1", "yes"})
                )
                try:
                    value_float = float(raw)
                    value = int(value_float)
                    if not math.isfinite(value_float) or value_float != value:
                        value = 0
                except (TypeError, ValueError, OverflowError):
                    value = 0
                if value > 0 and is_tradable:
                    FINAM_TRADE_LOT_SIZES[normalized] = value
                    updated_lots += 1
                else:
                    # A positive lot size is insufficient if Finam says the instrument
                    # is not tradable (or does not explicitly confirm tradability).
                    FINAM_TRADE_LOT_SIZES.pop(normalized, None)
                    failed_lots += 1
                    if not is_tradable:
                        blocked_not_tradable += 1
            elif callable(params_method):
                FINAM_TRADE_LOT_SIZES.pop(normalized, None)
                failed_lots += 1
            elif not is_paper:
                FINAM_TRADE_LOT_SIZES.pop(normalized, None)
                failed_lots += 1

        symbols_to_refresh = list(dict.fromkeys(list(LOT_SIZES) + list(TICKERS_TO_TRADE)))
        await asyncio.gather(*(refresh_one(ticker) for ticker in symbols_to_refresh))
        risk_logger.info(
            "📦 [FINAM METADATA] mode=%s account_context=%s; "
            "lot multiplier confirmed=%d/%d; unavailable=%d (new entries blocked); "
            "not_tradable_or_unconfirmed=%d; tick-size=%d/%d; metadata unavailable=%d (static tick-size retained); "
            "PAPER lot failure examples=%s.",
            "PAPER" if is_paper else "LIVE",
            (
                (
                    f"metadata_only:{metadata_account_id_source}:{bool(metadata_account_id)}"
                    if is_paper else f"{account_id_source}:{bool(account_id)}"
                )
            ),
            updated_lots, len(symbols_to_refresh), failed_lots, blocked_not_tradable,
            updated_metadata, len(symbols_to_refresh), failed_metadata,
            "; ".join(paper_lot_failure_examples) if paper_lot_failure_examples else "-",
        )
