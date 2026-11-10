"""Small pure helpers for formatting curated RAG rules in the vLLM prompt."""
from __future__ import annotations

import re


def normalise_rag_context(value: object) -> str:
    """Remove decorative list prefixes while preserving one rule per line."""
    if isinstance(value, str):
        raw_lines = value.splitlines()
    elif isinstance(value, (list, tuple)):
        raw_lines = [str(item) for item in value if item is not None]
    else:
        raw_lines = [str(value)] if value is not None else []

    lines: list[str] = []
    for raw_line in raw_lines:
        line = str(raw_line).strip()
        line = re.sub(r"^[-*]\s*(?:Паттерн:\s*)?", "", line, flags=re.IGNORECASE).strip()
        if line:
            lines.append(line)
    return "\n".join(lines)


def select_prompt_rules(direction: str, rag_context: str, max_chars: int = 900) -> str:
    """Keep the always-on risk rule plus the candidate's BUY/SELL rule."""
    direction = str(direction or "NONE").strip().upper()
    prefixes = ["[RISK /"]
    if direction in {"BUY", "SELL"}:
        prefixes.append(f"[{direction} /")
    matched_lines = [
        line.strip()
        for line in str(rag_context or "").splitlines()
        if any(line.strip().startswith(prefix) for prefix in prefixes)
    ]
    # Preserve complete rule records. Keep mandatory risk first, then the
    # candidate-side rule(s) that fit; never pass a mid-sentence truncation.
    limit = max(0, int(max_chars))
    selected: list[str] = []
    risk = next((line for line in matched_lines if line.startswith("[RISK /")), None)
    directional = [line for line in matched_lines if line.startswith(f"[{direction} /")]
    if risk and len(risk) <= limit:
        selected.append(risk)
    remaining = limit - sum(len(item) for item in selected) - max(0, len(selected) - 1)
    for line in directional:
        extra = len(line) + (1 if selected else 0)
        if extra <= remaining:
            selected.append(line)
            remaining -= extra
    return " ".join(selected)
