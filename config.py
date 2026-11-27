from __future__ import annotations

import os
from pathlib import Path
from dotenv import load_dotenv

# --------------------------------------------------------------
# 1️⃣  Загрузка .env (если файл существует рядом с проектом)
# --------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
load_dotenv(dotenv_path=ENV_PATH)   # .env необязателен – если нет, просто читаем системные переменные

def resolve_paper_book_size_unit(requested_unit: object, evidence_source: object) -> str:
    """Fail closed unless a supported external source accompanies an explicit unit."""
    requested = str(requested_unit or "unknown").strip().lower()
    evidence = str(evidence_source or "").strip().upper()
    if requested in {"shares", "lots"} and evidence in {
        "FINAM_SUPPORT", "BROKER_EXPORT", "CONTROLLED_RECONCILIATION"
    }:
        return requested
    return "unknown"


def _resolve_trade_deposit(
    canonical_raw: str | None,
    legacy_raw: str | None,
    default: float = 40000.0,
) -> tuple[float, str]:
    """Resolve one authoritative start-capital value and reject conflicting aliases."""
    canonical = str(canonical_raw).strip() if canonical_raw is not None else ""
    legacy = str(legacy_raw).strip() if legacy_raw is not None else ""
    if canonical and legacy and abs(float(canonical) - float(legacy)) > 0.01:
        raise ValueError(
            "Конфликт стартового капитала: TRADE_DEPOSIT и TRADING_DEPOSIT "
            "заданы разными значениями. Оставьте одно значение или синхронизируйте оба."
        )
    if canonical:
        return float(canonical), "TRADE_DEPOSIT"
    if legacy:
        return float(legacy), "TRADING_DEPOSIT (legacy)"
    return float(default), "default"



# --------------------------------------------------------------
#  Справочники инструментов проекта (единая точка настройки)
#  Обновляйте эти таблицы здесь, не в бизнес-модулях.
# --------------------------------------------------------------
TICKERS_TO_TRADE: list[str] = [
    "SBER", "SBERP", "GAZP", "LKOH", "NVTK", "YDEX", "ROSN", "VTBR", "AFLT", "MAGN",
    "CHMF", "NLMK", "MOEX", "ALRS", "MTSS", "MGNT", "TATN", "SNGS", "SNGSP", "PLZL",
    "HYDR", "FEES", "CBOM", "IRAO", "PHOR", "PIKK", "TRNFP", "TATNP", "AFKS", "RUAL",
    "GCHE", "LSRG", "SMLT", "MSNG", "BANEP", "BSPB", "RENI", "UPRO", "SOFL", "POSI",
    "RNFT", "UWGN", "VKCO", "HEAD", "OZON", "APRI", "MVID", "LENT", "OKEY", "AGRO",
    "MDMG", "WUSH", "CARM", "LEAS", "DELI", "SVCB", "SFIN", "MTLR", "MTLRP", "RASP",
    "FESH", "NMTP", "FLOT", "GECO", "ABIO", "BLNG", "DVEC", "ELFV", "ENPG", "GTRK",
    "IRKT", "KAZT", "KAZTP", "KBSB", "KROT", "MAGE", "MAGEP", "MGTS", "MGTSP", "NKHP",
    "NKNC", "NKNCP", "TGKA", "TGKB", "TGKBP", "TTLK", "VJGZ", "VJGZP"
]

LOT_SIZES: dict[str, int] = {
    # --- Крупнейшие "голубые фишки" и тяжеловесы индекса ---
    "SBER": 10, "SBERP": 10, "GAZP": 10, "LKOH": 1, "NVTK": 1, 
    "YDEX": 1, "ROSN": 10, "VTBR": 10000, "AFLT": 10, "MAGN": 10,
    "CHMF": 1, "NLMK": 10, "MOEX": 10, "ALRS": 10, "MTSS": 10, 
    "MGNT": 1, "TATN": 1, "TATNP": 1, "SNGS": 100, "SNGSP": 100, 
    "PLZL": 1, "PHOR": 1, "AFKS": 100, "RUAL": 10,

    # --- Энергетика, металлы и ритейл ---
    "HYDR": 1000, "FEES": 10000, "CBOM": 100, "IRAO": 100, 
    "PIKK": 1, "TRNFP": 1, "GCHE": 1, "LSRG": 1, "SMLT": 1, 
    "MSNG": 1000, "BANEP": 1, "BSPB": 10, "RENI": 10, "UPRO": 100, 
    "SOFL": 10, "POSI": 1, "RNFT": 1, "UWGN": 10, "VKCO": 1, 
    "HEAD": 1, "OZON": 1, "APRI": 10, "MVID": 10, "LENT": 1, 
    "OKEY": 10, "AGRO": 1, "MDMG": 1, "WUSH": 1, "CARM": 100, 
    "LEAS": 1, "DELI": 1, "SVCB": 10, "SFIN": 1, "MTLR": 1, 
    "MTLRP": 1, "RASP": 1, "FLOT": 10, "GECO": 10, "ABIO": 10,

    # --- Транспорт, порты и региональные дочки (Эшелоны) ---
    "FESH": 10, "NMTP": 100, "BLNG": 10, "DVEC": 100, "ELFV": 1000, 
    "ENPG": 1, "GTRK": 10, "IRKT": 10, "KAZT": 10, "KAZTP": 10, 
    "KBSB": 1, "KROT": 1, "MAGE": 1, "MAGEP": 1, "MGTS": 1, 
    "MGTSP": 1, "NKHP": 1, "NKNC": 10, "NKNCP": 10, "TGKA": 10000, 
    "TGKB": 100000, "TGKBP": 100000, "TTLK": 10, "VJGZ": 100, "VJGZP": 100
}

# Static MOEX lot-size reference. Retained for legacy/accounting fallback only.
# New order sizing uses the account-specific Finam trade_lot_size map below.
# In PAPER mode this map is populated from public exchange lot_size metadata.
# In LIVE mode it contains account-specific Finam trade_lot_size values.
FINAM_TRADE_LOT_SIZES: dict[str, int] = {}

# Runtime Finam instrument metadata populated during startup.
# price_tick = min_step / 10**decimals; lot_size is shares per exchange lot.
ASSET_METADATA: dict[str, dict[str, object]] = {}

RSS_FEEDS: list[str] = [
    "https://bcs-express.ru",
    "https://www.bfm.ru/news.rss?rubric=28",
    "https://moex.com/export/news.aspx?cat=100",
    "https://moex.com/export/news.aspx?cat=101",
    "https://www.moex.com/export/news.aspx?cat=104",
    "https://bfm.ru",
    "https://www.moex.com/export/news.aspx?cat=122",
    "https://www.finam.ru/analysis/forecasts/rsspoint/"
]


def _env_flag(name: str, default: bool = True) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


# One canonical mode flag shared by execution, metadata refresh and startup.
PAPER_TRADING = _env_flag("PAPER_TRADING", True)


# Локальный корень проекта можно задать в .env. По умолчанию используется
# папка config.py, поэтому настройка не обязательна и проект остаётся переносимым.
_project_root_value = os.getenv("PILUITUS_PROJECT_ROOT", "").strip()
PROJECT_ROOT = (
    Path(_project_root_value).expanduser()
    if _project_root_value else BASE_DIR
)
if not PROJECT_ROOT.is_absolute():
    PROJECT_ROOT = BASE_DIR / PROJECT_ROOT
PROJECT_ROOT = PROJECT_ROOT.resolve()

# Приватные значения фильтруются непосредственно при форматировании логов.
LOG_REDACT_TELEGRAM_ID = _env_flag("PILUITUS_LOG_REDACT_TELEGRAM_ID", True)
LOG_REDACT_PROJECT_PATH = _env_flag("PILUITUS_LOG_REDACT_PROJECT_PATH", True)


# --------------------------------------------------------------
# 2️⃣  Класс‑контейнер с параметрами (все публичные атрибуты!)
# --------------------------------------------------------------
class Settings:
    PAPER_TRADING: bool = PAPER_TRADING

    # ---------- FastAPI / Uvicorn ----------
    APP_HOST: str = os.getenv("APP_HOST", "0.0.0.0")
    APP_PORT: int = int(os.getenv("APP_PORT", "5813"))
    RELOAD: bool = os.getenv("APP_RELOAD", "False").lower() in ("true", "1", "yes")

    # ---------- Redis ----------
    REDIS_HOST: str = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT: int = int(os.getenv("REDIS_PORT", "6379"))
    REDIS_DB: int   = int(os.getenv("REDIS_DB", "0"))
    REDIS_PASSWORD: str | None = os.getenv("REDIS_PASSWORD")

    # ---------- vLLM (LLM‑инференс) ----------
    # URL, по которому доступен HTTP‑API vLLM
    VLLM_ENDPOINT: str = os.getenv(
        "VLLM_ENDPOINT",
        "http://127.0.0.1:8000/v1/chat/completions"
    )
    # Путь (или имя) модели, которую принимает ваш vLLM‑сервер
    # Если модель уже указана в самом vLLM‑контейнере, можно оставить пустой строкой,
    # тогда сервер будет использовать «default model».
    VLLM_MODEL: str = os.getenv(
        "VLLM_MODEL",
        "/home/sumkin/models/deepseek-r1-7b"
    )
    # Таймаут одной попытки vLLM; AIEngine выполняет один ограниченный сетевой повтор.
    VLLM_TIMEOUT: int = int(os.getenv("VLLM_TIMEOUT", "30"))
    # Короткий JSON-контракт: эффективный max_tokens ограничен диапазоном 384..768.
    VLLM_MAX_TOKENS: int = int(os.getenv("VLLM_MAX_TOKENS", "768"))
    # Bound simultaneous inference requests; queueing is measured by AIEngine.
    VLLM_MAX_CONCURRENCY: int = max(
        1, min(16, int(os.getenv("PILUITUS_VLLM_MAX_CONCURRENCY", "4")))
    )
    # Consecutive vLLM connection failures open an exponential backoff circuit.
    VLLM_CONNECT_BACKOFF_BASE_SEC: float = max(
        1.0, float(os.getenv("PILUITUS_VLLM_CONNECT_BACKOFF_BASE_SEC", "5"))
    )
    VLLM_CONNECT_BACKOFF_MAX_SEC: float = max(
        VLLM_CONNECT_BACKOFF_BASE_SEC,
        float(os.getenv("PILUITUS_VLLM_CONNECT_BACKOFF_MAX_SEC", "60")),
    )
    # Maximum acceptable spread as a fraction of M1 ATR; tune from shadow outcomes.
    MAX_SPREAD_TO_ATR_RATIO: float = min(
        2.0, max(0.05, float(os.getenv("PILUITUS_MAX_SPREAD_TO_ATR_RATIO", "0.50")))
    )

    # Finam's public ORDER_BOOK schema omits the buy_size/sell_size unit.
    # Require explicit provenance; never infer shares/lots from trade_lot_size or order.quantity.
    PAPER_BOOK_SIZE_UNIT_EVIDENCE: str = os.getenv(
        "PILUITUS_PAPER_BOOK_SIZE_UNIT_EVIDENCE", ""
    ).strip().upper()
    PAPER_BOOK_SIZE_UNIT: str = resolve_paper_book_size_unit(
        os.getenv("PILUITUS_PAPER_BOOK_SIZE_UNIT", "unknown"),
        PAPER_BOOK_SIZE_UNIT_EVIDENCE,
    )
    PAPER_MAX_BOOK_PARTICIPATION_PCT: float = float(
        os.getenv("PILUITUS_PAPER_MAX_BOOK_PARTICIPATION_PCT", "10.0")
    )
    # Conservative upper bound supplied by the operator: Finam RF securities tariff.
    # Charged on each side of the PAPER round trip.
    PAPER_COMMISSION_PCT: float = min(
        1.0, max(0.0, float(os.getenv("PILUITUS_PAPER_COMMISSION_PCT", "0.0354")))
    )
    # PAPER-only net break-even stop. The arm threshold is measured from the
    # initial ATR stop distance; the active stop is rounded conservatively to MINSTEP.
    PAPER_BREAKEVEN_ENABLED: bool = os.getenv(
        "PILUITUS_PAPER_BREAKEVEN_ENABLED", "true"
    ).strip().lower() not in {"0", "false", "no", "off", "disabled"}
    PAPER_BREAKEVEN_ARM_R: float = min(
        3.0, max(0.25, float(os.getenv("PILUITUS_PAPER_BREAKEVEN_ARM_R", "1.0")))
    )
    # PAPER-only trailing stop: activate after a confirmed BBO move and trail from its high-water mark.
    PAPER_TRAILING_ENABLED: bool = os.getenv(
        "PILUITUS_PAPER_TRAILING_ENABLED", "true"
    ).strip().lower() not in {"0", "false", "no", "off", "disabled"}
    PAPER_TRAILING_ARM_R: float = min(
        5.0, max(0.5, float(os.getenv("PILUITUS_PAPER_TRAILING_ARM_R", "1.5")))
    )
    PAPER_TRAILING_DISTANCE_R: float = min(
        2.0, max(0.25, float(os.getenv("PILUITUS_PAPER_TRAILING_DISTANCE_R", "0.75")))
    )
    # Escalated diagnostics for a latched exit waiting for a fresh executable BBO.
    PAPER_EXIT_STALL_ALERT_SEC: float = min(
        300.0, max(2.0, float(os.getenv("PILUITUS_PAPER_EXIT_STALL_ALERT_SEC", "10.0")))
    )
    PAPER_EXIT_STALL_LOG_INTERVAL_SEC: float = min(
        300.0, max(5.0, float(os.getenv("PILUITUS_PAPER_EXIT_STALL_LOG_INTERVAL_SEC", "30.0")))
    )
    # Signal exits require fresh aggressor-flow and momentum evidence on distinct trade samples.
    PAPER_SIGNAL_EXIT_ENABLED: bool = os.getenv(
        "PILUITUS_PAPER_SIGNAL_EXIT_ENABLED", "true"
    ).strip().lower() not in {"0", "false", "no", "off", "disabled"}
    PAPER_SIGNAL_EXIT_MIN_MOMENTUM_PCT: float = min(
        1.0, max(0.001, float(os.getenv("PILUITUS_PAPER_SIGNAL_EXIT_MIN_MOMENTUM_PCT", "0.02")))
    )
    PAPER_SIGNAL_EXIT_MIN_FLOW_IMBALANCE: float = min(
        0.9, max(0.05, float(os.getenv("PILUITUS_PAPER_SIGNAL_EXIT_MIN_FLOW_IMBALANCE", "0.15")))
    )
    PAPER_SIGNAL_EXIT_MIN_QUEUE_IMBALANCE: float = min(
        0.9, max(0.05, float(os.getenv("PILUITUS_PAPER_SIGNAL_EXIT_MIN_QUEUE_IMBALANCE", "0.10")))
    )
    PAPER_SIGNAL_EXIT_CONFIRMATIONS: int = min(
        5, max(2, int(os.getenv("PILUITUS_PAPER_SIGNAL_EXIT_CONFIRMATIONS", "2")))
    )
    PAPER_EXIT_METRICS_MAX_AGE_SEC: float = min(
        15.0, max(1.0, float(os.getenv("PILUITUS_PAPER_EXIT_METRICS_MAX_AGE_SEC", "5.0")))
    )
    PAPER_PARTIAL_PROFIT_ENABLED: bool = os.getenv(
        "PILUITUS_PAPER_PARTIAL_PROFIT_ENABLED", "true"
    ).strip().lower() not in {"0", "false", "no", "off", "disabled"}
    PAPER_PARTIAL_PROFIT_ARM_R: float = min(
        5.0, max(0.5, float(os.getenv("PILUITUS_PAPER_PARTIAL_PROFIT_ARM_R", "1.0")))
    )
    PAPER_PARTIAL_PROFIT_FRACTION: float = min(
        0.9, max(0.1, float(os.getenv("PILUITUS_PAPER_PARTIAL_PROFIT_FRACTION", "0.50")))
    )
    # Hard Moscow-time cutoff for new entries; open-position exits remain active.
    ENTRY_HARD_CUTOFF_MSK: str = os.getenv("PILUITUS_ENTRY_HARD_CUTOFF_MSK", "22:00").strip()

    # ---------- Finam ----------
    FINAM_TOKEN: str = os.getenv("FINAM_TOKEN", "")
    # Brokerage account identifiers are optional and used only by LIVE execution.
    # PAPER trades on the internal simulator and requires only FINAM_TOKEN.
    FINAM_ACCOUNT_ID: str = os.getenv("FINAM_ACCOUNT_ID", "")
    # Legacy LIVE-only compatibility; ignored by PAPER mode.
    FINAM_CLIENT_ID: str = os.getenv("FINAM_CLIENT_ID", "")
    FINAM_USER: str = os.getenv("FINAM_USER", "")
    FINAM_PASSWORD: str = os.getenv("FINAM_PASSWORD", "")
    FINAM_DEMO: bool = os.getenv("FINAM_DEMO", "True").lower() in ("true", "1", "yes")

    # ---------- Telegram ----------
    TG_BOT_TOKEN: str = os.getenv("TG_BOT_TOKEN", "")
    TG_USER_ID: int = int(os.getenv("TG_USER_ID", "0"))

    # ---------- Бизнес‑правила ----------
    DAILY_LOSS_LIMIT_PCT: float = float(os.getenv("DAILY_LOSS_LIMIT_PCT", "0.015"))   # 1.5 % от депозита
    MAX_RISK_PCT: float = float(os.getenv("MAX_RISK_PCT", "0.005"))                     # 0.5 % от сделки
    # Capital allocation: 10% default overall / 3% per position; hard overall ceiling 15%.
    # Values from older .env files are clamped again in ExecutionLayer.
    MAX_GROSS_EXPOSURE_PCT: float = min(float(os.getenv("PILUITUS_MAX_GROSS_EXPOSURE_PCT", "0.10")), 0.15)
    MAX_SINGLE_POSITION_PCT: float = min(float(os.getenv("PILUITUS_MAX_SINGLE_POSITION_PCT", "0.03")), 0.03)
    MAX_OPEN_STOP_RISK_PCT: float = min(float(os.getenv("PILUITUS_MAX_OPEN_STOP_RISK_PCT", "0.01")), 0.05)
    DRAWDOWN_WARNING_PCT: float = min(float(os.getenv("PILUITUS_DRAWDOWN_WARNING_PCT", "0.03")), 0.50)
    MAX_DRAWDOWN_PCT: float = min(float(os.getenv("PILUITUS_MAX_DRAWDOWN_PCT", "0.05")), 0.50)
    LOSS_STREAK_3_PAUSE_SEC: int = max(0, int(os.getenv("PILUITUS_LOSS_STREAK_3_PAUSE_SEC", "600")))
    LOSS_STREAK_5_PAUSE_SEC: int = max(0, int(os.getenv("PILUITUS_LOSS_STREAK_5_PAUSE_SEC", "1800")))
    # Five consecutive losses stop new entries for the remainder of the Moscow
    # trading date; the next trading date starts with a fresh session streak.
    LOSS_STREAK_HARD_STOP: int = max(3, min(20, int(os.getenv("PILUITUS_LOSS_STREAK_HARD_STOP", "5"))))
    ATR_MULTIPLIER: float = float(os.getenv("ATR_MULTIPLIER", "1.5"))
    # One capital source for every subsystem. TRADE_DEPOSIT is canonical;
    # TRADING_DEPOSIT remains a backward-compatible fallback for existing .env files.
    TRADE_DEPOSIT, TRADE_DEPOSIT_SOURCE = _resolve_trade_deposit(
        os.getenv("TRADE_DEPOSIT"),
        os.getenv("TRADING_DEPOSIT"),
    )

    # ---------- Прочие ----------
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
