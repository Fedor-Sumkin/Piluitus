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


def _env_flag(name: str, default: bool = True) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on"}


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
    # ---------- FastAPI / Uvicorn ----------
    APP_HOST: str = os.getenv("APP_HOST", "0.0.0.0")
    APP_PORT: int = int(os.getenv("APP_PORT", "8080"))
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
    # Общий таймаут (сек) для запросов к vLLM (можно переопределять в отдельных вызовах)
    VLLM_TIMEOUT: int = int(os.getenv("VLLM_TIMEOUT", "30"))
    # Увеличенный бюджет ответа уменьшает риск обрезания JSON; AIEngine ограничивает его диапазоном 768..1024.
    VLLM_MAX_TOKENS: int = int(os.getenv("VLLM_MAX_TOKENS", "1024"))

    # ---------- Finam ----------
    FINAM_TOKEN: str = os.getenv("FINAM_TOKEN", "")
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
    # Capital allocation: 10% default overall / 5% per position; hard overall ceiling 15%.
    # Values from older .env files are clamped again in ExecutionLayer.
    MAX_GROSS_EXPOSURE_PCT: float = min(float(os.getenv("PILUITUS_MAX_GROSS_EXPOSURE_PCT", "0.10")), 0.15)
    MAX_SINGLE_POSITION_PCT: float = min(float(os.getenv("PILUITUS_MAX_SINGLE_POSITION_PCT", "0.05")), 0.05)
    ATR_MULTIPLIER: float = float(os.getenv("ATR_MULTIPLIER", "1.5"))
    TRADE_DEPOSIT: float = float(os.getenv("TRADE_DEPOSIT", "100000.0"))

    # ---------- Прочие ----------
    LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO").upper()
