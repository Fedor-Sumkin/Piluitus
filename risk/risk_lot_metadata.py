"""Finam-confirmed lot metadata refresh and normalization mixin."""
from __future__ import annotations

from risk.risk_shared import *  # noqa: F401,F403


class RiskLotMetadataMixin:
    @classmethod
    async def update_finam_lot_sizes(cls, finam_broker_client=None, paper_mode: bool | None = None):
        """Refresh PAPER exchange lots or LIVE account-specific trade lots from Finam metadata."""
        risk_logger = logging.getLogger("Piluitus.RiskManager")
        is_paper = PAPER_TRADING if paper_mode is None else bool(paper_mode)
        if not LOT_SIZES and not TICKERS_TO_TRADE:
            risk_logger.error("❌ Список инструментов пуст: Finam metadata refresh невозможен.")
            return
        if finam_broker_client is None or not (
            callable(getattr(finam_broker_client, "get_asset", None))
            or callable(getattr(finam_broker_client, "get_moex_asset", None))
        ):
            risk_logger.error(
                "Exchange asset metadata source unavailable: exchange lot cannot be confirmed; new entries remain blocked."
            )
            return
        params_method = getattr(finam_broker_client, "get_asset_params", None)
        account_id = str(getattr(finam_broker_client, "account_id", "") or "").strip()
        account_id_source = str(
            getattr(finam_broker_client, "account_id_source", "") or "authenticated_session"
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
        metadata_account_id = str(
            getattr(finam_broker_client, "metadata_account_id", "") or ""
        ).strip()
        moex_asset_method = getattr(finam_broker_client, "get_moex_asset", None)
        use_moex_iss = is_paper and callable(moex_asset_method)
        if is_paper and not use_moex_iss:
            # Compatibility for test doubles/older clients only. Production PAPER
            # uses public MOEX ISS and never discovers or borrows a Finam account.
            metadata_discover = getattr(finam_broker_client, "discover_metadata_account_id", None)
            if callable(metadata_discover) and not metadata_account_id:
                try:
                    metadata_account_id = str(await metadata_discover() or "").strip()
                except Exception as exc:
                    risk_logger.warning(
                        "[FINAM METADATA] legacy read-only context discovery failed: %s",
                        type(exc).__name__,
                    )
                if not metadata_account_id:
                    risk_logger.warning(
                        "⛔ [FINAM METADATA] no public MOEX ISS client is available and Finam "
                        "GetAsset requires an authorized account context; previously confirmed "
                        "exchange lots are preserved and unknown lots remain blocked."
                    )
                    return
        asset_method = moex_asset_method if use_moex_iss else getattr(finam_broker_client, "get_asset", None)
        metadata_source_label = "MOEX_ISS_TQBR" if use_moex_iss else "FINAM_GET_ASSET"
        if not callable(asset_method):
            risk_logger.warning("📐 [ASSET METADATA] Exchange metadata method unavailable; static tick-size metadata retained.")
        if not is_paper and not callable(params_method):
            risk_logger.error(
                "Finam GetAssetParams unavailable; no account-specific lot values will authorize LIVE orders."
            )

        # Pace external metadata calls and avoid an uncontrolled startup burst.
        # MOEX ISS uses a separate, conservative interval; Finam's documented
        # 200 requests/minute limit applies only to Finam REST endpoints.
        semaphore = asyncio.Semaphore(6)
        request_lock = asyncio.Lock()
        interval_env = (
            "PILUITUS_MOEX_ISS_REQUEST_INTERVAL_SEC"
            if use_moex_iss else "FINAM_METADATA_REQUEST_INTERVAL_SEC"
        )
        interval_default = "0.50" if use_moex_iss else "0.36"
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
            # PAPER has no brokerage account. Never inherit an account ID from
            # the client when requesting public exchange instrument metadata.
            if is_paper:
                return await method(ticker)
            return await method(ticker, account_id=account_id)

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
                if callable(asset_method):
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

            lot_raw = None
            if isinstance(asset_data, dict):
                try:
                    decimals_raw = scalar(field(asset_data, "decimals"))
                    min_step_raw = scalar(field(asset_data, "min_step", "minStep"))
                    decimals = int(decimals_raw) if decimals_raw is not None else -1
                    min_step = int(float(min_step_raw or 0))
                    lot_raw = scalar(field(asset_data, "lot_size", "lotSize"))
                    exchange_lot = float(lot_raw or 0)
                    mic = str(scalar(field(asset_data, "mic", "exchange_mic")) or "").strip().upper()
                    symbol = str(scalar(field(asset_data, "symbol", "ticker")) or "")
                    asset_mic = mic or ("MISX" if symbol.upper().endswith("@MISX") else "UNSPECIFIED")
                    asset_market_ok = mic in {"", "MISX"} and (not symbol or "@" not in symbol or symbol.upper().endswith("@MISX"))
                    if decimals >= 0 and min_step > 0 and asset_market_ok:
                        ASSET_METADATA[normalized] = {
                            "symbol": symbol or f"{normalized}@MISX",
                            "decimals": decimals,
                            "min_step": min_step,
                            "price_tick": min_step / (10 ** decimals),
                            "lot_size": exchange_lot,
                            "quote_currency": str(scalar(field(asset_data, "quote_currency", "quoteCurrency")) or ""),
                            "metadata_source": str(
                                scalar(field(asset_data, "metadata_source")) or metadata_source_label
                            ),
                        }
                        updated_metadata += 1
                    else:
                        failed_metadata += 1
                except (TypeError, ValueError, OverflowError, ZeroDivisionError):
                    failed_metadata += 1
            elif callable(asset_method):
                failed_metadata += 1

            if is_paper:
                # Preserve a previously confirmed exchange lot during transient
                # metadata failures; never replace it with a static guessed value.
                try:
                    lot_value = float(exchange_lot)
                    paper_lot = int(lot_value)
                    if asset_market_ok and math.isfinite(lot_value) and lot_value > 0 and lot_value == paper_lot:
                        FINAM_TRADE_LOT_SIZES[normalized] = paper_lot
                        updated_lots += 1
                    else:
                        failed_lots += 1
                        if len(paper_lot_failure_examples) < 8:
                            payload_type = type(asset_payload).__name__
                            payload_keys = ",".join(sorted(asset_payload.keys())) if isinstance(asset_payload, dict) else "-"
                            if asset_payload is None:
                                reason = f"{metadata_source_label} returned empty response"
                            elif not isinstance(asset_data, dict):
                                reason = f"unsupported payload type={payload_type}"
                            elif not asset_market_ok:
                                reason = f"exchange not confirmed ({asset_mic})"
                            else:
                                reason = f"invalid/missing lot_size={lot_raw!r}; keys={payload_keys}"
                            paper_lot_failure_examples.append(f"{normalized}:{reason}")
                except (TypeError, ValueError, OverflowError):
                    failed_lots += 1
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
                "public_MOEX_ISS_TQBR"
                if use_moex_iss else (
                    f"read_only_metadata:{bool(metadata_account_id)}"
                    if is_paper else f"{account_id_source}:{bool(account_id)}"
                )
            ),
            updated_lots, len(symbols_to_refresh), failed_lots, blocked_not_tradable,
            updated_metadata, len(symbols_to_refresh), failed_metadata,
            "; ".join(paper_lot_failure_examples) if paper_lot_failure_examples else "-",
        )
