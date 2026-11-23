"""Compatibility facade for deterministic signal evidence and entry guards.

Implementation is split by responsibility. Existing imports keep working while
callers can migrate to the narrower modules when their dependency is clear.
"""
from __future__ import annotations

from strategy.signal_evidence import (
    ALLOWED_BAR_PATTERNS, MAX_BAR_PATTERN_SIGNAL_AGE_SEC,
    MAX_SIZE_SIGNAL_AGE_SEC, MAX_SPREAD_PCT, MIN_SIZE_HISTORY_SAMPLES,
    MIN_TOP_SIZE_DROP_PCT, MIN_VOLUME_SPIKE, _fresh_bar_pattern_direction, _num,
    classify_direction_evidence, classify_m1_evidence_state,
    fresh_bar_pattern_direction, infer_candidate_direction, validate_atr_readiness,
)
from strategy.signal_entry_quality import (
    validate_entry_evidence, validate_paper_market_quality,
    validate_paper_sell_quality, validate_signal_evidence, validate_spread_to_atr,
)
from strategy.signal_text import (
    _complete_short_text, _CORRUPT_MARKERS, _NEGATION, _SIDE_SIZE_CLAIM,
    _STRONG_UNMEASURED_CLAIMS, compact_fallback, opinion_text_is_grounded,
)
