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


def select_prompt_rules(direction: str, rag_context: str, max_chars: int = 420) -> str:
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
    rule_text = " ".join(matched_lines)
    limit = max(0, int(max_chars))
    if len(rule_text) > limit:
        if limit <= 3:
            return rule_text[:limit]
        rule_text = rule_text[: limit - 3].rsplit(" ", 1)[0].rstrip() + "..."
    return rule_text
