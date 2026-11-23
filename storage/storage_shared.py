"""
Unified asynchronous Redis manager for the Piluitus v3.0 project.

* Один singleton‑клиент `RedisClient` (реальный Redis или аварийный in‑memory‑мок с дисковым AOF‑бэкапом).
* Публичный класс `DBManager` сохраняет прежний API, которым пользуются
  остальные модули проекта.
* Добавлен метод get_all_daily_trades для сквозного риск-аудита просадки.
"""

from __future__ import annotations

import json
import logging
import os
import asyncio
import fnmatch
from datetime import datetime
from typing import Any, List, Optional, Union, cast

import redis.asyncio as redis                     # pip install redis‑asyncio
from config import PROJECT_ROOT, Settings       # единый корень проекта и настройки

log = logging.getLogger(__name__)   # под именем "Piluitus.DBManager"
AOF_FILE = str(PROJECT_ROOT / "piluitus_fallback_aof.jsonl")


# ----------------------------------------------------------------------
# 1️⃣  In‑memory fallback‑клиент с персистентным AOF-дисковым бэкапом
# ----------------------------------------------------------------------

__all__ = [
    "json", "logging", "os", "asyncio", "fnmatch", "datetime", "Any", "List",
    "Optional", "Union", "cast", "redis", "PROJECT_ROOT", "Settings", "log", "AOF_FILE",
]
