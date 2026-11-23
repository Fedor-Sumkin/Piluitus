import re
import json
import hashlib
import logging
import math
import asyncio
import time
import os
from httpx import AsyncClient, HTTPStatusError
from config import Settings
import httpx
from ai.rag_prompt_utils import normalise_rag_context, select_prompt_rules
from strategy.signal_guard import classify_direction_evidence, classify_m1_evidence_state

log = logging.getLogger("Piluitus.AIEngine")
thought_log = logging.getLogger("Piluitus.EvolutionThoughts")


class VLLMCircuitOpenError(httpx.ConnectError):
    """Raised while vLLM is in a bounded connection-failure backoff window."""

    def __init__(self, retry_after_sec: float):
        self.retry_after_sec = max(0.0, float(retry_after_sec))
        super().__init__(
            f"vLLM connection circuit is open; retry after {self.retry_after_sec:.1f}s"
        )
MODEL_REASON_CODES = {
    "APPROVED",
    "HOLD_WEAK_DIRECTION",
    "HOLD_M1_CONTRADICTION",
    "HOLD_STALE_BBO",
    "HOLD_WEAK_VOLUME",
    "HOLD_WIDE_SPREAD",
    "HOLD_INSUFFICIENT_SUPPORT",
    "HOLD_RISK_REWARD",
    "HOLD_UNCLEAR",
    "NO_LOCAL_CANDIDATE",
}
