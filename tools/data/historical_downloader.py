"""Download historical trade prints and OHLC bars from Finam's website export."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import sys
from datetime import date, datetime, time as datetime_time, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Any
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import httpx
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from config import TICKERS_TO_TRADE  # noqa: E402

LOGGER = logging.getLogger("Piluitus.HistoricalDownloader")
EXPORT_PAGE_URL = "https://www.finam.ru/profile/moex-akcii/gazprom/export/"
EXPORT_BASE_URL = "https://export.finam.ru/table.csv"
MOSCOW_TZ = ZoneInfo("Europe/Moscow")
SHARES_MARKET_ID = 1

BAR_TIMEFRAMES: dict[str, int] = {
    "m1": 2, "m5": 3, "m10": 4, "m15": 5, "m30": 6,
    "h1": 7, "d1": 8, "w1": 9, "mn1": 10,
}
TICK_TIMEFRAME_CODE = 1
INTRADAY_MAX_CHUNK_DAYS = 120
DAILY_MAX_CHUNK_DAYS = 365 * 5

EXPORT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/130.0.0.0 Safari/537.36"
    ),
    "Accept": "text/csv,text/plain,*/*",
    "Referer": EXPORT_PAGE_URL,
}


class FinamExportError(RuntimeError):
    """Finam export returned a response that cannot be used safely."""


def split_date_range(start: date, end: date, max_days: int) -> list[tuple[date, date]]:
    """Split an inclusive date range into chunks no longer than max_days."""
    if end < start:
        raise ValueError("end date must not be earlier than start date")
    if max_days < 1:
        raise ValueError("max_days must be positive")
    chunks: list[tuple[date, date]] = []
    cursor = start
    while cursor <= end:
        chunk_end = min(end, cursor + timedelta(days=max_days - 1))
        chunks.append((cursor, chunk_end))
        cursor = chunk_end + timedelta(days=1)
    return chunks


def _parse_js_array(payload: str) -> list[str]:
    quoted = re.findall(r"""'((?:\\.|[^'])*)'|"((?:\\.|[^"])*)""", payload)
    if quoted:
        return [(single or double).replace("\\'", "'").replace('\\"', '"') for single, double in quoted]
    return [item.strip() for item in payload.split(",") if item.strip()]


def parse_export_catalog(script_text: str) -> dict[str, dict[str, Any]]:
    """Parse Finam icharts.js parallel arrays: id, name, code and market."""
    arrays = re.findall(r"\[([^\]]*)\]", script_text, flags=re.DOTALL)
    if len(arrays) < 4:
        raise FinamExportError(
            "Не удалось распознать icharts.js: ожидаются массивы id/name/code/market."
        )
    parsed = [_parse_js_array(payload) for payload in arrays[:4]]
    lengths = {len(items) for items in parsed}
    if len(lengths) != 1 or not lengths or next(iter(lengths)) == 0:
        raise FinamExportError("Массивы каталога Finam имеют разную длину или пусты.")

    ids, names, codes, markets = parsed
    catalog: dict[str, dict[str, Any]] = {}
    for raw_id, name, raw_code, raw_market in zip(ids, names, codes, markets):
        try:
            market_id = int(raw_market)
            instrument_id = int(raw_id)
        except (TypeError, ValueError):
            continue
        code = str(raw_code).strip().upper()
        if code and market_id == SHARES_MARKET_ID:
            catalog[code] = {
                "id": instrument_id, "name": str(name).strip(),
                "code": code, "market": market_id,
            }
    if not catalog:
        raise FinamExportError("В каталоге Finam не найдены акции рынка MOEX.")
    return catalog


async def load_export_catalog(client: httpx.AsyncClient) -> dict[str, dict[str, Any]]:
    """Resolve current Finam export instrument IDs instead of using stale hardcoded IDs."""
    page_response = await client.get(EXPORT_PAGE_URL, headers=EXPORT_HEADERS, timeout=30)
    page_response.raise_for_status()
    match = re.search(
        r"""<script\b[^>]*\bsrc\s*=\s*["']([^"']*icharts\.js[^"']*)["']""",
        page_response.text, flags=re.IGNORECASE,
    )
    if not match:
        raise FinamExportError(
            "На странице экспорта Finam не найдена ссылка на icharts.js; формат сайта мог измениться."
        )
    script_url = urljoin(str(page_response.url), match.group(1))
    script_response = await client.get(script_url, headers=EXPORT_HEADERS, timeout=30)
    script_response.raise_for_status()
    try:
        script_text = script_response.content.decode("utf-8-sig")
    except UnicodeDecodeError:
        script_text = script_response.content.decode("cp1251", errors="replace")
    return parse_export_catalog(script_text)


def build_export_params(
    instrument: dict[str, Any], start: date, end: date, timeframe: str
) -> dict[str, Any]:
    """Build Finam website-export query parameters for TICKS or OHLC bars."""
    timeframe = timeframe.lower()
    if timeframe == "ticks":
        period_code, data_format = TICK_TIMEFRAME_CODE, 6
    elif timeframe in BAR_TIMEFRAMES:
        period_code, data_format = BAR_TIMEFRAMES[timeframe], 5
    else:
        raise ValueError(f"Unsupported export timeframe: {timeframe}")
    if end < start:
        raise ValueError("end date must not be earlier than start date")
    return {
        "d": "d", "f": "table", "e": ".csv", "dtf": 1, "tmf": 3,
        "MSOR": 0, "mstime": "on", "mstimever": 1, "sep": 3, "sep2": 1, "at": 1,
        "p": period_code, "em": int(instrument["id"]),
        "market": int(instrument.get("market", SHARES_MARKET_ID)),
        "df": start.day, "mf": start.month - 1, "yf": start.year,
        "dt": end.day, "mt": end.month - 1, "yt": end.year,
        "cn": instrument["code"], "code": instrument["code"],
        "datf": data_format, "fsp": 0,
    }


def _normalize_time(value: Any) -> str:
    text = str(value).strip()
    if re.fullmatch(r"\d{1,6}", text):
        digits = text.zfill(6)
        return f"{digits[:2]}:{digits[2:4]}:{digits[4:6]}"
    if re.fullmatch(r"\d{1,2}:\d{1,2}", text):
        return text + ":00"
    return text


def _numeric_column(series: pd.Series) -> pd.Series:
    values = series.astype(str).str.strip()
    values = values.str.replace(" ", "", regex=False).str.replace(",", ".", regex=False)
    return pd.to_numeric(values, errors="coerce")


def parse_export_csv(raw_text: str, dataset: str, ticker: str) -> pd.DataFrame:
    """Parse and normalize Finam TICKS or OHLC export without inventing missing fields."""
    dataset = dataset.lower()
    if dataset not in {"ticks", *BAR_TIMEFRAMES.keys()}:
        raise ValueError(f"Unsupported dataset: {dataset}")
    if not raw_text or not raw_text.strip():
        raw_text = (
            "<TICKER>;<PER>;<DATE>;<TIME>;<LAST>;<VOL>"
            if dataset == "ticks"
            else "<TICKER>;<PER>;<DATE>;<TIME>;<OPEN>;<HIGH>;<LOW>;<CLOSE>;<VOL>"
        )
    try:
        frame = pd.read_csv(StringIO(raw_text), sep=";", dtype=str, keep_default_na=False)
    except pd.errors.EmptyDataError:
        frame = pd.DataFrame()

    headers = {
        str(column).strip().strip("<>").strip().upper(): column
        for column in frame.columns
    }
    required = (
        {"DATE", "TIME", "LAST", "VOL"}
        if dataset == "ticks"
        else {"DATE", "TIME", "OPEN", "HIGH", "LOW", "CLOSE", "VOL"}
    )
    missing = required - set(headers)
    if missing:
        preview = raw_text[:240].replace("\n", " ")
        raise FinamExportError(
            f"Finam вернул неизвестный формат для {ticker}/{dataset}; "
            f"нет полей {sorted(missing)}. Ответ: {preview}"
        )

    def source_column(name: str) -> pd.Series:
        column = headers.get(name)
        if column is None:
            return pd.Series([ticker] * len(frame), index=frame.index, dtype="object")
        return frame[column]

    output_columns = (
        ["ticker", "datetime_msk", "date", "time", "price", "volume", "period"]
        if dataset == "ticks"
        else ["ticker", "datetime_msk", "date", "time", "open", "high", "low", "close", "volume", "period"]
    )
    if frame.empty:
        return pd.DataFrame(columns=output_columns)

    raw_dates = source_column("DATE").astype(str).str.extract(r"(\d{8})", expand=False)
    raw_times = source_column("TIME").map(_normalize_time)
    timestamps = pd.to_datetime(
        raw_dates.fillna("") + " " + raw_times.astype(str), errors="coerce"
    )
    valid_timestamp = timestamps.notna()

    result = pd.DataFrame(index=frame.index)
    ticker_values = source_column("TICKER").astype(str).str.strip()
    result["ticker"] = ticker_values.replace("", ticker).fillna(ticker).str.upper()
    result["datetime_msk"] = timestamps.dt.tz_localize(MOSCOW_TZ).map(
        lambda value: value.isoformat() if pd.notna(value) else ""
    )
    result["date"] = timestamps.dt.strftime("%Y-%m-%d")
    result["time"] = timestamps.dt.strftime("%H:%M:%S")
    result["period"] = source_column("PER").astype(str) if "PER" in headers else ""

    if dataset == "ticks":
        result["price"] = _numeric_column(source_column("LAST"))
    else:
        for field in ("OPEN", "HIGH", "LOW", "CLOSE"):
            result[field.lower()] = _numeric_column(source_column(field))
    result["volume"] = _numeric_column(source_column("VOL"))

    numeric_fields = ["price", "volume"] if dataset == "ticks" else [
        "open", "high", "low", "close", "volume"
    ]
    valid_numeric = result[numeric_fields].notna().all(axis=1)
    result = result.loc[valid_timestamp & valid_numeric, output_columns]
    return result.sort_values("datetime_msk", kind="stable").reset_index(drop=True)


def _atomic_write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        frame.to_csv(temporary, index=False, encoding="utf-8")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _load_manifest(path: Path) -> dict[str, Any]:
    if path.exists():
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(payload, dict) and isinstance(payload.get("datasets"), dict):
                return payload
        except (OSError, json.JSONDecodeError):
            LOGGER.warning("Не удалось прочитать manifest; будет создан новый.")
    return {
        "schema_version": 1, "source": "Finam website export",
        "timezone": "Europe/Moscow", "datasets": {},
    }


def _save_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    try:
        temporary.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


def _latest_completed_weekday(now: datetime | None = None) -> date:
    current = now.astimezone(MOSCOW_TZ) if now else datetime.now(MOSCOW_TZ)
    candidate = current.date()
    if candidate.weekday() >= 5 or current.time().replace(tzinfo=None) < datetime_time(18, 50):
        candidate -= timedelta(days=1)
    while candidate.weekday() >= 5:
        candidate -= timedelta(days=1)
    return candidate


def resolve_date_range(args: argparse.Namespace) -> tuple[date, date]:
    start = date.fromisoformat(args.start_date) if args.start_date else None
    end = date.fromisoformat(args.end_date) if args.end_date else None
    if start is None and end is None:
        end = _latest_completed_weekday()
        start = end - timedelta(days=max(1, args.days) - 1)
    elif start is None:
        start = end - timedelta(days=max(1, args.days) - 1)
    elif end is None:
        end = _latest_completed_weekday()
    if end < start:
        raise ValueError("--end-date не может быть раньше --start-date")
    return start, end


def _load_existing_bars(path: Path) -> pd.DataFrame | None:
    if not path.exists() or path.stat().st_size == 0:
        return None
    try:
        existing = pd.read_csv(path)
        required = {
            "ticker", "datetime_msk", "date", "time",
            "open", "high", "low", "close", "volume",
        }
        return existing if required.issubset(existing.columns) else None
    except (OSError, pd.errors.ParserError, UnicodeDecodeError):
        LOGGER.warning("Существующий файл баров %s не читается; он будет заменён.", path)
        return None


async def _fetch_export_csv(
    client: httpx.AsyncClient,
    instrument: dict[str, Any],
    start: date,
    end: date,
    timeframe: str,
    delay: float,
    max_retries: int,
) -> str:
    params = build_export_params(instrument, start, end, timeframe)
    last_error = ""
    for attempt in range(max_retries + 1):
        try:
            response = await client.get(
                EXPORT_BASE_URL, params=params, headers=EXPORT_HEADERS, timeout=60
            )
        except httpx.HTTPError as exc:
            last_error = str(exc)
            if attempt >= max_retries:
                break
            wait = min(2.0 ** attempt, 30.0)
            LOGGER.warning("Сетевая ошибка Finam для %s: %s; повтор через %.1f сек.", instrument["code"], exc, wait)
            await asyncio.sleep(wait)
            continue

        body = response.text
        lowered = body.lower()
        if "вы запросили данные за слишком" in lowered:
            raise FinamExportError("Finam отклонил слишком большой период запроса.")
        already_processing = "система уже обрабатывает ваш запрос" in lowered
        throttled = response.status_code in {403, 429} or "forbidden: access is denied" in lowered
        if response.status_code >= 400 or already_processing or throttled:
            last_error = f"HTTP {response.status_code}: {body[:180]}"
            if attempt >= max_retries:
                break
            wait = min((5.0 if throttled else 1.0) * (2 ** attempt), 60.0)
            LOGGER.warning(
                "Finam временно отклонил %s/%s (%s); повтор %d/%d через %.1f сек.",
                instrument["code"], timeframe, response.status_code,
                attempt + 1, max_retries, wait,
            )
            await asyncio.sleep(wait)
            continue
        if not body.strip():
            return ""
        if "<TICKER>" not in body.upper() and "<DATE>" not in body.upper():
            raise FinamExportError(
                f"Finam вернул не CSV для {instrument['code']}/{timeframe}: {body[:240]!r}"
            )
        if delay > 0:
            await asyncio.sleep(delay)
        return body
    raise FinamExportError(f"Не удалось скачать {instrument['code']}/{timeframe}: {last_error}")


def _record_manifest(
    manifest: dict[str, Any], key: str, *, ticker: str, dataset: str,
    start: date, end: date, path: Path, rows: int, status: str,
    error: str | None = None,
) -> None:
    record: dict[str, Any] = {
        "ticker": ticker, "dataset": dataset,
        "start_date": start.isoformat(), "end_date": end.isoformat(),
        "file": str(path.relative_to(PROJECT_ROOT)).replace("\\", "/"),
        "rows": int(rows), "status": status,
        "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
        "timestamp_timezone": "Europe/Moscow",
    }
    if error:
        record["error"] = error[:500]
    manifest["datasets"][key] = record


async def download_ticks_for_ticker(
    client: httpx.AsyncClient, instrument: dict[str, Any],
    start: date, end: date, trades_dir: Path,
    manifest: dict[str, Any], manifest_path: Path, *,
    delay: float, max_retries: int, force: bool,
) -> tuple[int, int]:
    completed = failed = 0
    ticker = instrument["code"]
    for day, _ in split_date_range(start, end, 1):
        output_path = trades_dir / ticker / f"{day:%Y%m%d}.csv"
        key = f"ticks|{ticker}|{day:%Y%m%d}"
        if output_path.exists() and output_path.stat().st_size > 20 and not force:
            LOGGER.info("SKIP ticks %s %s: файл уже существует.", ticker, day)
            completed += 1
            continue
        try:
            raw = await _fetch_export_csv(
                client, instrument, day, day, "ticks", delay, max_retries
            )
            frame = parse_export_csv(raw, "ticks", ticker)
            _atomic_write_csv(frame, output_path)
            status = "downloaded" if not frame.empty else "empty"
            _record_manifest(
                manifest, key, ticker=ticker, dataset="ticks", start=day,
                end=day, path=output_path, rows=len(frame), status=status,
            )
            _save_manifest(manifest_path, manifest)
            LOGGER.info("TICKS %-7s %s: %d сделок (%s)", ticker, day, len(frame), status)
            completed += 1
        except Exception as exc:
            LOGGER.error("TICKS %s %s: %s", ticker, day, exc)
            _record_manifest(
                manifest, key, ticker=ticker, dataset="ticks", start=day,
                end=day, path=output_path, rows=0, status="failed", error=str(exc),
            )
            _save_manifest(manifest_path, manifest)
            failed += 1
    return completed, failed


async def download_bars_for_ticker(
    client: httpx.AsyncClient, instrument: dict[str, Any],
    start: date, end: date, timeframe: str, bars_dir: Path,
    manifest: dict[str, Any], manifest_path: Path, *,
    delay: float, max_retries: int, force: bool,
) -> tuple[int, int]:
    ticker = instrument["code"]
    timeframe = timeframe.lower()
    max_chunk = DAILY_MAX_CHUNK_DAYS if timeframe in {"d1", "w1", "mn1"} else INTRADAY_MAX_CHUNK_DAYS
    output_path = bars_dir / timeframe / f"{ticker}.csv"
    key = f"bars|{timeframe}|{ticker}|{start:%Y%m%d}-{end:%Y%m%d}"
    try:
        frames = []
        for chunk_start, chunk_end in split_date_range(start, end, max_chunk):
            raw = await _fetch_export_csv(
                client, instrument, chunk_start, chunk_end, timeframe, delay, max_retries
            )
            frames.append(parse_export_csv(raw, timeframe, ticker))
        downloaded = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
        existing = None if force else _load_existing_bars(output_path)
        if existing is not None and not existing.empty:
            downloaded = pd.concat([existing, downloaded], ignore_index=True)
        if not downloaded.empty:
            downloaded = downloaded.drop_duplicates(subset=["datetime_msk"], keep="last")
            downloaded = downloaded.sort_values("datetime_msk", kind="stable").reset_index(drop=True)
        _atomic_write_csv(downloaded, output_path)
        status = "downloaded" if not downloaded.empty else "empty"
        _record_manifest(
            manifest, key, ticker=ticker, dataset=f"bars_{timeframe}",
            start=start, end=end, path=output_path, rows=len(downloaded), status=status,
        )
        _save_manifest(manifest_path, manifest)
        LOGGER.info("BARS  %-7s %-3s: %d баров (%s)", ticker, timeframe, len(downloaded), status)
        return 1, 0
    except Exception as exc:
        LOGGER.error("BARS %s %s: %s", ticker, timeframe, exc)
        _record_manifest(
            manifest, key, ticker=ticker, dataset=f"bars_{timeframe}",
            start=start, end=end, path=output_path, rows=0, status="failed", error=str(exc),
        )
        _save_manifest(manifest_path, manifest)
        return 0, 1


def _parse_tickers(value: str | None) -> list[str]:
    if not value:
        return list(dict.fromkeys(str(t).strip().upper() for t in TICKERS_TO_TRADE))
    return list(dict.fromkeys(part.strip().upper() for part in value.split(",") if part.strip()))


def _parse_timeframes(value: str) -> list[str]:
    items = [item.strip().lower() for item in value.split(",") if item.strip()]
    unknown = sorted(set(items) - set(BAR_TIMEFRAMES))
    if unknown:
        raise ValueError(f"Неизвестные таймфреймы {unknown}; доступны: {', '.join(BAR_TIMEFRAMES)}")
    return list(dict.fromkeys(items))


async def run_download(args: argparse.Namespace) -> int:
    start, end = resolve_date_range(args)
    tickers = _parse_tickers(args.tickers)
    if not tickers:
        raise ValueError("Список тикеров пуст.")
    timeframes = _parse_timeframes(args.bar_timeframes) if args.mode in {"all", "bars"} else []

    trades_dir = PROJECT_ROOT / "data" / "historical_trades"
    bars_dir = PROJECT_ROOT / "data" / "historical_bars"
    manifest_path = PROJECT_ROOT / "data" / "historical_download_manifest.json"
    trades_dir.mkdir(parents=True, exist_ok=True)
    bars_dir.mkdir(parents=True, exist_ok=True)
    manifest = _load_manifest(manifest_path)

    timeout = httpx.Timeout(60.0, connect=20.0)
    limits = httpx.Limits(max_connections=2, max_keepalive_connections=1)
    async with httpx.AsyncClient(
        timeout=timeout, limits=limits, follow_redirects=True, headers=EXPORT_HEADERS
    ) as client:
        LOGGER.info("Загружаю и обновляю каталог инструментов Finam...")
        catalog = await load_export_catalog(client)
        missing = [ticker for ticker in tickers if ticker not in catalog]
        if missing:
            LOGGER.warning("В каталоге Finam не найдены тикеры: %s", ", ".join(missing))
        resolved = [catalog[ticker] for ticker in tickers if ticker in catalog]
        if not resolved:
            raise FinamExportError("Ни один запрошенный тикер не найден в каталоге MOEX Finam.")

        LOGGER.info(
            "Период: %s — %s; инструментов: %d/%d; режим: %s",
            start, end, len(resolved), len(tickers), args.mode,
        )
        LOGGER.info(
            "TICKS — отдельные сделки; бары: %s; часовой пояс: Europe/Moscow.",
            ", ".join(timeframes) if timeframes else "не запрашиваются",
        )

        completed = failed = 0
        for index, instrument in enumerate(resolved, start=1):
            ticker = instrument["code"]
            LOGGER.info("[%d/%d] %s (%s)", index, len(resolved), ticker, instrument.get("name", ""))
            if args.mode in {"all", "ticks"}:
                ok, errors = await download_ticks_for_ticker(
                    client, instrument, start, end, trades_dir, manifest, manifest_path,
                    delay=args.delay, max_retries=args.max_retries, force=args.force,
                )
                completed += ok
                failed += errors
            if args.mode in {"all", "bars"}:
                for timeframe in timeframes:
                    ok, errors = await download_bars_for_ticker(
                        client, instrument, start, end, timeframe, bars_dir, manifest, manifest_path,
                        delay=args.delay, max_retries=args.max_retries, force=args.force,
                    )
                    completed += ok
                    failed += errors

    LOGGER.info("ЗАВЕРШЕНО: успешно/проверено=%d, ошибок=%d. Manifest: %s", completed, failed, manifest_path)
    LOGGER.info("Исторический стакан не загружен: в этих данных нет Bid/Ask и очереди заявок.")
    return 1 if failed else 0


def build_argument_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Загрузка исторических сделок (TICKS) и свечей Finam для PILUITUS REPLAY."
    )
    parser.add_argument(
        "--mode", choices=("all", "ticks", "bars"), default="all",
        help="all — сделки и свечи; ticks — только сделки; bars — только свечи.",
    )
    parser.add_argument(
        "--days", type=int, default=1,
        help="Количество календарных дней до последней завершённой будничной даты (по умолчанию: 1).",
    )
    parser.add_argument("--start-date", help="Начало периода YYYY-MM-DD.")
    parser.add_argument("--end-date", help="Конец периода YYYY-MM-DD.")
    parser.add_argument(
        "--tickers", help="Список тикеров через запятую; по умолчанию TICKERS_TO_TRADE из config.py.",
    )
    parser.add_argument(
        "--bar-timeframes", default="m1,m5,m15,d1",
        help="Таймфреймы баров через запятую: " + ",".join(BAR_TIMEFRAMES),
    )
    parser.add_argument("--delay", type=float, default=1.0, help="Пауза между успешными запросами, секунды.")
    parser.add_argument("--max-retries", type=int, default=4, help="Количество повторов временных ошибок.")
    parser.add_argument("--force", action="store_true", help="Перекачать существующие файлы сделок/баров.")
    parser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR"))
    return parser


def main() -> int:
    parser = build_argument_parser()
    args = parser.parse_args()
    if args.days < 1:
        parser.error("--days должен быть не меньше 1")
    if args.delay < 0 or args.max_retries < 0:
        parser.error("--delay и --max-retries не могут быть отрицательными")
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    try:
        return asyncio.run(run_download(args))
    except KeyboardInterrupt:
        LOGGER.warning("Загрузка прервана. Готовые файлы сохранены; повторный запуск продолжит пропущенное.")
        return 130
    except Exception as exc:
        LOGGER.exception("Критическая ошибка загрузчика: %s", exc)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
