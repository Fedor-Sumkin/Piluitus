# Historical market-data download for PILUITUS

The downloader uses Finam's public website export, not the Finam Trade REST
Bars endpoint. This distinction is intentional: the documented Trade API Bars
timeframes begin at M1, while the website export offers individual trade
prints (TICKS).

## What is downloaded

- Individual trade prints into data/historical_trades/<TICKER>/<YYYYMMDD>.csv.
  Normalized fields: ticker, datetime_msk, date, time, price, volume, trade_id, operation, period.
- OHLCV bars into data/historical_bars/<TIMEFRAME>/<TICKER>.csv.
  Default timeframes: m1,m5,m15,d1. Optional: m10,m30,h1,w1,mn1.

Timestamps are interpreted as Moscow time (Europe/Moscow). The timezone offset
is kept in datetime_msk. Trade volume is preserved as returned by Finam; the
downloader does not convert shares to lots.

The TICKS export provides transaction time, price and volume. It does not
provide historical Bid/Ask depth. The optional OPER field is preserved as a raw
operation code; its semantics are not assumed to mean BUY/SELL aggressor side.
The downloader therefore does not fabricate bid, ask, queue imbalance or OFI. Historical order-book reconstruction requires a
separate historical market-data source (for example, MOEX order-book archive).

## Usage

Run from the project root:

~~~powershell
# Safe first check: one completed trading date, all project tickers, TICKS + bars
python historical_downloader.py --days 1

# Small format/connectivity check
python historical_downloader.py --days 1 --tickers SBER,GAZP,SMLT

# Download the last 20 calendar days (not 20 trading sessions)
python historical_downloader.py --days 20

# Explicit period
python historical_downloader.py --start-date 2026-10-01 --end-date 2026-10-31

# Only individual trade prints
python historical_downloader.py --mode ticks --days 5

# Only bars, including M1/M5/M15 and daily context
python historical_downloader.py --mode bars --days 90

# Force re-download of already saved files
python historical_downloader.py --mode ticks --days 1 --force
~~~

The root historical_downloader.py remains a compatibility launcher for
tools/data/historical_downloader.py.

## Limits and restart behavior

- Finam website export limits one TICKS request to one calendar day. The
  downloader requests each ticker/day separately.
- Finam website export limits intraday bar requests to four calendar months.
  The downloader uses chunks of at most 120 calendar days. Daily and larger
  intervals are chunked at five years.
- The default period is one last completed weekday. Use --start-date,
  --end-date or --days to select another period.
- Requests are sequential by design to reduce the risk of export throttling.
  Temporary HTTP 403/429 and "request already processing" responses are retried.
- Existing TICKS files are skipped on restart. Bar files are merged with new
  downloads and deduplicated by datetime_msk.
- data/historical_download_manifest.json records requested period, row count,
  file, source, timestamp timezone and download status. It is updated after
  every dataset so an interrupted run can be resumed.
- Large market-data files and the manifest are git-ignored and should remain
  local; do not commit raw exports into the source repository.

Finam's website export is a separate interface from the authenticated Trade
API. The export catalog and URL parameter format are not a stable public API;
if Finam changes the page or response format, the downloader stops with an
explicit parsing error rather than silently saving a malformed dataset.
