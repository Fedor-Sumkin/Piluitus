"""vllm_client.py - отказоустойчивый клиент к локальному vLLM (OpenAI-совместимый API).

Ключевые особенности, найденные при диагностике:
  1. vLLM регистрирует модель по её реальному пути (например
     '/home/sumkin/models/deepseek-r1-7b'). Короткое имя 'deepseek-r1-7b'
     даёт HTTP 404 "The model does not exist". Поэтому имя модели ВСЕГДА
     резолвится через GET /v1/models.
  2. DeepSeek-R1 - reasoning-модель: она всегда тратит 500-900 токенов на
     размышления и пишет финальный ответ ПОСЛЕ тега < /think>. Резать
     max_tokens ниже ~512 бессмысленно - JSON никогда не будет сгенерирован.
  3. max_model_len ограничивает сумму prompt+completion, поэтому бюджет
     автоматически урезается под контекстное окно сервера.
"""
import os
import re
import time
import logging
import asyncio
from typing import Any

import httpx
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger("Piluitus.VLLMClient")

VLLM_BASE_URL = os.getenv("VLLM_BASE_URL", "http://127.0.0.1:8000").rstrip("/")
VLLM_API_KEY = os.getenv("VLLM_API_KEY", "")
# Подсказка: какое模型 мы хотим. Реальное имя берётся с сервера.
VLLM_MODEL_HINT = os.getenv("VLLM_MODEL", "deepseek-r1-7b")

THINK_END = "<" + "/think>"
THINK_START = "<" + "think>"

# Кэш резолва модели и её контекстного окна
_MODEL_CACHE: dict[str, Any] = {"id": None, "max_model_len": 2048, "ts": 0.0}
_RESOLVE_TTL = 300.0


def _headers() -> dict[str, str]:
    h = {"Content-Type": "application/json"}
    if VLLM_API_KEY:
        h["Authorization"] = f"Bearer {VLLM_API_KEY}"
    return h


def _norm(s: str) -> str:
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _score(model_id: str, hint: str) -> int:
    """Насколько хорошо id модели на сервере подходит под подсказку."""
    m, h = _norm(model_id), _norm(hint)
    if not m or not h:
        return 0
    score = 0
    if m == h:
        score += 100
    if h in m:
        score += 60
    for tok in filter(None, re.split(r"[^a-z0-9]+", hint.lower())):
        if tok in m:
            score += 15
    # Короткие пути (без '/') чуть предпочтительнее при равном счёте
    if "/" not in model_id:
        score += 1
    return score


async def resolve_model(force: bool = False) -> str:
    """Определяет РЕАЛЬНЫЙ id модели у сервера vLLM. Кэшируется."""
    now = time.time()
    if not force and _MODEL_CACHE["id"] and now - _MODEL_CACHE["ts"] < _RESOLVE_TTL:
        return _MODEL_CACHE["id"]

    try:
        async with httpx.AsyncClient(timeout=6.0) as client:
            resp = await client.get(f"{VLLM_BASE_URL}/v1/models", headers=_headers())
            resp.raise_for_status()
            data = resp.json().get("data", [])
    except Exception as exc:
        logger.warning(
            f"vLLM /v1/models недоступен ({type(exc).__name__}: {exc}). "
            f"Использую имя из VLLM_MODEL как есть: '{VLLM_MODEL_HINT}'"
        )
        _MODEL_CACHE.update(id=VLLM_MODEL_HINT, ts=now)
        return _MODEL_CACHE["id"]

    if not data:
        logger.error("vLLM вернул пустой список моделей")
        _MODEL_CACHE.update(id=VLLM_MODEL_HINT, ts=now)
        return _MODEL_CACHE["id"]

    best = max(data, key=lambda d: _score(str(d.get("id", "")), VLLM_MODEL_HINT))
    resolved = str(best.get("id"))
    _MODEL_CACHE["id"] = resolved
    _MODEL_CACHE["max_model_len"] = int(best.get("max_model_len") or 2048)
    _MODEL_CACHE["ts"] = now
    logger.info(
        f"✅ vLLM резолв: '{VLLM_MODEL_HINT}' -> '{resolved}' "
        f"(max_model_len={_MODEL_CACHE['max_model_len']}, "
        f"доступно моделей: {len(data)})"
    )
    return resolved


def get_max_model_len() -> int:
    return int(_MODEL_CACHE.get("max_model_len") or 2048)


def strip_reasoning(text: str) -> str:
    """Убирает цепочку рассуждений R1, оставляя финальный ответ."""
    if not text:
        return ""
    if THINK_END in text:
        text = text.rsplit(THINK_END, 1)[1]
    text = text.replace(THINK_START, "").replace(THINK_END, "")
    return text.strip()



def _estimate_tokens(text: str) -> int:
    """Grubaya ocenka tokenov dlya smeshannogo RU/EN teksta."""
    if not text:
        return 0
    return max(1, int(len(text) / 2.6))


def _clamp_budget(max_tokens: int, prompt_text: str) -> int:
    """Urezaet max_tokens, chtoby prompt+completion vlezi v okno servera."""
    limit = get_max_model_len()
    reserve = _estimate_tokens(prompt_text) + 48
    allowed = limit - reserve
    if allowed < 128:
        return max(64, limit // 4)
    return max(128, min(int(max_tokens), allowed))


def _messages_to_prompt(messages: list[dict]) -> str:
    """Sobiraet iz messages ploskiy prompt dlya /v1/completions."""
    parts = []
    for msg in messages:
        role = msg.get("role", "user")
        body = msg.get("content", "") or ""
        if role == "system":
            parts.append(f"### Instruction ({role}):\n{body}")
        elif role == "assistant":
            parts.append(f"### Response (assistant):\n{body}")
        else:
            parts.append(f"### Instruction:\n{body}")
    parts.append("### Response:")
    return "\n\n".join(parts) + "\n"


def _extract_text(payload: dict, is_chat: bool) -> str:
    """Dostaet tekst otveta iz oboih endpointov."""
    choices = payload.get("choices") or []
    if not choices:
        return ""
    first = choices[0]
    if is_chat:
        msg = first.get("message") or {}
        content = msg.get("content") or ""
        if not content and msg.get("reasoning"):
            content = msg["reasoning"]
        return content
    return first.get("text") or ""
