"""PAPER position exit monitoring, depth execution and close lifecycle."""
from __future__ import annotations

import asyncio
import logging
import math
import os
import time
from collections.abc import Mapping
from datetime import datetime, timedelta, timezone
from typing import Any, cast

from config import LOT_SIZES
from storage.db_manager import DBManager
from strategy.paper_performance import (
    PAPER_CLOSED_TRADES_STORAGE_KEY,
    PAPER_CLOSED_TRADES_STORAGE_LIMIT,
    paper_performance_tracker,
)
from strategy.paper_exit_diagnostics import build_exit_execution_diagnostics
from execution.execution_primitives import (
    BBOExitQuote,
    TradeRecord,
    _as_string_mapping,
    _finite_or_default,
)


logger = logging.getLogger("Piluitus.ExecutionLayer")
