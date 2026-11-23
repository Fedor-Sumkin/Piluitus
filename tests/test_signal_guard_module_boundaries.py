"""Import-boundary contracts for the decomposed signal guard."""
from __future__ import annotations

from pathlib import Path

from strategy import signal_evidence, signal_entry_quality, signal_text
from strategy import signal_guard as facade


def test_compatibility_facade_reexports_the_canonical_implementations():
    assert facade.infer_candidate_direction is signal_evidence.infer_candidate_direction
    assert facade.validate_atr_readiness is signal_evidence.validate_atr_readiness
    assert facade.validate_entry_evidence is signal_entry_quality.validate_entry_evidence
    assert facade.validate_signal_evidence is signal_entry_quality.validate_signal_evidence
    assert facade.opinion_text_is_grounded is signal_text.opinion_text_is_grounded
    assert facade.compact_fallback is signal_text.compact_fallback


def test_extracted_signal_modules_do_not_import_the_compatibility_facade():
    root = Path(__file__).resolve().parents[1] / "strategy"
    for name in ("signal_evidence.py", "signal_entry_quality.py", "signal_text.py"):
        source = (root / name).read_text(encoding="utf-8")
        assert "strategy.signal_guard" not in source


def test_entry_and_text_modules_depend_on_the_narrow_evidence_layer():
    root = Path(__file__).resolve().parents[1] / "strategy"
    assert "from strategy.signal_evidence import" in (
        root / "signal_entry_quality.py"
    ).read_text(encoding="utf-8")
    assert "from strategy.signal_evidence import" in (
        root / "signal_text.py"
    ).read_text(encoding="utf-8")
