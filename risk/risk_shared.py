import asyncio
import json
import random
import logging
import math
import os
import time
from datetime import datetime, date, timedelta, timezone
from collections.abc import Callable
import websockets
import httpx
from storage.db_manager import DBManager
from config import ASSET_METADATA, FINAM_TRADE_LOT_SIZES, LOT_SIZES, TICKERS_TO_TRADE, PAPER_TRADING

_MOSCOW_TZ = timezone(timedelta(hours=3), name="MSK")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
)
logger = logging.getLogger("Piluitus.RealMarketSim")

# ======================================================================
# 1️⃣ РЕАЛИСТИЧНЫЙ СИМУЛЯТОР РЫНКА (HFT_STRESS_TESTER)
# ======================================================================

__all__ = [
    "asyncio", "json", "random", "logging", "math", "os", "time",
    "datetime", "date", "timedelta", "timezone", "Callable", "websockets", "httpx",
    "DBManager", "ASSET_METADATA", "FINAM_TRADE_LOT_SIZES", "LOT_SIZES",
    "TICKERS_TO_TRADE", "PAPER_TRADING", "_MOSCOW_TZ", "logger",
]
