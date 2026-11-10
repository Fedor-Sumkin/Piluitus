# Bootstrap: allow running this utility from any working directory.
import json
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]


def merge_approved_rules(existing: list[dict[str, Any]], purified: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    """Merge only explicitly approved, enabled, measurable rules; never replace runtime KB."""
    merged = [item for item in existing if isinstance(item, dict) and item.get("enabled") is True]
    known_ids = {str(item.get("rule_id") or "").strip() for item in merged}
    added = 0
    for item in purified:
        if not isinstance(item, dict):
            continue
        rule_id = str(item.get("rule_id") or "").strip()
        if (
            item.get("approved_for_runtime") is not True
            or item.get("enabled") is not True
            or not rule_id
            or rule_id in known_ids
            or not isinstance(item.get("conditions"), list)
            or not item.get("conditions")
            or not str(item.get("rules") or "").strip()
        ):
            continue
        merged.append(item)
        known_ids.add(rule_id)
        added += 1
    return merged, added


def deploy_purified_rules() -> None:
    """Safely merge explicitly approved distilled rules without overwriting active RAG."""
    purified_file = PROJECT_ROOT / "data" / "knowledge" / "purified_book_rules.json"
    target_file = PROJECT_ROOT / "data" / "knowledge" / "ai_market_knowledge.json"
    if not purified_file.exists():
        print(f"Источник правил не найден: {purified_file}")
        return
    existing = json.loads(target_file.read_text(encoding="utf-8")) if target_file.exists() else []
    purified = json.loads(purified_file.read_text(encoding="utf-8"))
    if not isinstance(existing, list) or not isinstance(purified, list):
        print("Ожидались JSON-массивы; активная база знаний не изменена.")
        return
    merged, added = merge_approved_rules(existing, purified)
    if added:
        target_file.write_text(json.dumps(merged, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(
        f"RAG сохранён без перезаписи: активных правил={len(merged)}, "
        f"добавлено явно одобренных={added}. Неодобренные/неизмеримые правила пропущены."
    )


if __name__ == "__main__":
    deploy_purified_rules()
