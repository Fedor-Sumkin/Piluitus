# -*- coding: utf-8 -*-
"""Fail-closed local installer for PILUITUS safety patch v1."""
import argparse
import datetime
import json
import os
from pathlib import Path
import re
import shutil
import tempfile


def atomic_write(path: Path, data: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, temp_name = tempfile.mkstemp(dir=str(path.parent), prefix=path.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(data)
        os.replace(temp_name, path)
    except Exception:
        try:
            os.unlink(temp_name)
        except OSError:
            pass
        raise


def replace_one(source: str, pattern: str, replacement, label: str) -> str:
    updated, count = re.subn(pattern, replacement, source, count=1, flags=re.MULTILINE)
    if count != 1:
        raise RuntimeError(f"{label}: ожидалось одно совпадение, найдено {count}")
    return updated


def patch_requirements(source: str) -> str:
    if re.search(r"(?im)^\s*h2(?:\s*[<=>!~].*)?\s*$", source):
        return source
    return source.rstrip() + "\nh2>=4.1.0,<5\n"


def patch_bot(source: str) -> str:
    if not re.search(r'''os\.getenv\(\s*["']TELEGRAM_BOT_TOKEN["']\s*\)\s*or\s*os\.getenv\(\s*["']TG_BOT_TOKEN["']''', source):
        source = replace_one(
            source,
            r'''^([ \t]*BOT_TOKEN[ \t]*=[ \t]*)os\.getenv\([ \t]*(["'])TELEGRAM_BOT_TOKEN\2[ \t]*(?:,[^\r\n)]*)?\)([ \t]*(?:\#[^\r\n]*)?)$''',
            lambda m: m.group(1) + 'os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TG_BOT_TOKEN")' + m.group(3),
            "BOT_TOKEN в tg_bot.py",
        )
    if not re.search(r'''os\.getenv\(\s*["']TELEGRAM_USER_ID["']\s*\)\s*or\s*os\.getenv\(\s*["']TG_USER_ID["']''', source):
        source = replace_one(
            source,
            r'''^([ \t]*USER_ID[ \t]*=[ \t]*)os\.getenv\([ \t]*(["'])TELEGRAM_USER_ID\2[ \t]*(?:,[^\r\n)]*)?\)([ \t]*(?:\#[^\r\n]*)?)$''',
            lambda m: m.group(1) + 'os.getenv("TELEGRAM_USER_ID") or os.getenv("TG_USER_ID")' + m.group(3),
            "USER_ID в tg_bot.py",
        )
    return source


def ensure_http_exception_import(source: str) -> str:
    if re.search(r"(?m)^\s*from\s+fastapi\s+import\s+.*\bHTTPException\b", source):
        return source
    match = re.search(r"(?m)^([ \t]*)from[ \t]+fastapi[ \t]+import[ \t]+([^\r\n]+)$", source)
    if not match or match.group(2).rstrip().endswith("\\"):
        raise RuntimeError("не найден безопасный однострочный 'from fastapi import ...' для HTTPException")
    replacement = match.group(1) + "from fastapi import " + match.group(2).rstrip() + ", HTTPException"
    return source[:match.start()] + replacement + source[match.end():]


def patch_main(source: str) -> str:
    if "from risk_limits import validate_daily_loss_limit" not in source:
        source = replace_one(
            source,
            r"^([ \t]*from[ \t]+config[ \t]+import[ \t]+Settings[ \t]*)$",
            lambda m: m.group(1) + "\nfrom risk_limits import validate_daily_loss_limit",
            "импорт Settings в main.py",
        )

    if "risk_manager.daily_loss_limit = validate_daily_loss_limit(" not in source:
        source = replace_one(
            source,
            r'''^([ \t]*)risk_manager\.daily_loss_limit[ \t]*=[ \t]*float\([ \t]*event_payload\.get\([ \t]*(["'])value\2[ \t]*,[ \t]*0\.10[ \t]*\)[ \t]*\)[ \t]*$''',
            lambda m: (
                m.group(1) + "try:\n"
                + m.group(1) + "    risk_manager.daily_loss_limit = validate_daily_loss_limit(\n"
                + m.group(1) + '        event_payload.get("value", Settings.DAILY_LOSS_LIMIT_PCT)\n'
                + m.group(1) + "    )\n"
                + m.group(1) + "except ValueError as exc:\n"
                + m.group(1) + '    logger.error("[RISK LIMIT] Некорректное значение в очереди: %s", exc)'
            ),
            "присваивание CMD_SET_RISK_LIMIT в очереди",
        )

    if "new_limit = validate_daily_loss_limit(" not in source:
        source = replace_one(
            source,
            r'''^([ \t]*)new_limit[ \t]*=[ \t]*payload\.get\([ \t]*(["'])value\2[ \t]*,[ \t]*0\.10[ \t]*\)[ \t]*$''',
            lambda m: (
                m.group(1) + "try:\n"
                + m.group(1) + '    new_limit = validate_daily_loss_limit(payload.get("value", Settings.DAILY_LOSS_LIMIT_PCT))\n'
                + m.group(1) + "except ValueError as exc:\n"
                + m.group(1) + "    raise HTTPException(status_code=422, detail=str(exc)) from exc"
            ),
            "HTTP risk_limit endpoint (new_limit)",
        )
    return ensure_http_exception_import(source)


def main() -> int:
    parser = argparse.ArgumentParser(description="Локальный установщик исправлений PILUITUS")
    parser.add_argument("--project", default=".", help="Корень проекта PILUITUS")
    project = Path(parser.parse_args().project).expanduser().resolve()
    required = {name: project / name for name in ("main.py", "tg_bot.py", "requirements.txt")}
    missing = [str(path) for path in required.values() if not path.is_file()]
    if missing:
        print("ОТМЕНА: не найдены обязательные файлы: " + ", ".join(missing))
        return 2

    original = {name: path.read_text(encoding="utf-8-sig") for name, path in required.items()}
    updated = dict(original)
    try:
        updated["requirements.txt"] = patch_requirements(updated["requirements.txt"])
        updated["tg_bot.py"] = patch_bot(updated["tg_bot.py"])
        updated["main.py"] = patch_main(updated["main.py"])
    except Exception as exc:
        print("ОТМЕНА: исходники не совпали с проверенными фрагментами. Ничего не изменено.")
        print("Причина:", exc)
        return 3

    bundle = Path(__file__).resolve().parent
    source_root = bundle.parents[1]
    additions = {
        project / "risk_limits.py": (source_root / "risk_limits.py").read_text(encoding="utf-8"),
        project / "tests" / "test_risk_limits.py": (source_root / "tests" / "test_risk_limits.py").read_text(encoding="utf-8"),
        project / "rollback_local_patch.py": (bundle / "rollback_local_patch.py").read_text(encoding="utf-8"),
    }
    for path, content in additions.items():
        if path.exists() and path.read_text(encoding="utf-8-sig") != content:
            print(f"ОТМЕНА: {path} уже существует и отличается; он не будет перезаписан.")
            return 4

    writes = {required[name]: content for name, content in updated.items() if content != original[name]}
    writes.update({path: content for path, content in additions.items() if not path.exists()})
    if not writes:
        print("Патч уже установлен; изменений нет.")
        return 0

    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = project / ("_backup_local_patch_" + stamp)
    suffix = 1
    while backup.exists():
        backup = project / ("_backup_local_patch_" + stamp + f"_{suffix}")
        suffix += 1
    backup.mkdir()
    manifest = {"backup": backup.name, "files": []}
    try:
        for path in writes:
            relative = path.relative_to(project).as_posix()
            existed = path.exists()
            manifest["files"].append({"path": relative, "existed_before": existed})
            if existed:
                saved = backup / relative
                saved.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(path, saved)
        (backup / "manifest.json").write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
        for path, content in writes.items():
            atomic_write(path, content)
    except Exception as exc:
        for item in manifest["files"]:
            target = project / item["path"]
            saved = backup / item["path"]
            try:
                if item["existed_before"] and saved.exists():
                    atomic_write(target, saved.read_text(encoding="utf-8-sig"))
                elif not item["existed_before"] and target.exists():
                    target.unlink()
            except Exception:
                pass
        print(f"ОШИБКА записи; выполнен автоматический откат. Копия: {backup}\n{exc}")
        return 5

    print("ПАТЧ УСТАНОВЛЕН")
    print("Резервная копия:", backup)
    print("Изменённые/добавленные файлы:")
    for path in writes:
        print(" -", path.relative_to(project).as_posix())
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
