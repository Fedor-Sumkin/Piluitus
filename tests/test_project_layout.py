"""Smoke checks for the reorganized PILUITUS project layout."""
from __future__ import annotations

import subprocess
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def test_templates_are_available_in_interface_package() -> None:
    template_dir = PROJECT_ROOT / "interface" / "templates"
    assert (template_dir / "index.html").is_file()
    assert (template_dir / "maintenance.html").is_file()
    assert not (PROJECT_ROOT / "templates").exists()

    main_source = (PROJECT_ROOT / "main.py").read_text(encoding="utf-8")
    assert 'TEMPLATE_DIR = BASE_DIR / "interface" / "templates"' in main_source
    assert "Jinja2Templates(directory=str(TEMPLATE_DIR))" in main_source
    assert 'page_path = TEMPLATE_DIR / "maintenance.html"' in main_source
    assert 'index_path = TEMPLATE_DIR / "index.html"' in main_source
    assert 'BASE_DIR / "templates"' not in main_source


def test_data_sources_are_in_data_tree() -> None:
    csv_dir = PROJECT_ROOT / "data" / "historical_ticks"
    source_dir = PROJECT_ROOT / "data" / "knowledge" / "sources"
    knowledge_dir = PROJECT_ROOT / "data" / "knowledge"

    assert len(list(csv_dir.glob("*.csv"))) == 87
    assert len(list(source_dir.glob("*.pdf"))) == 6
    assert (source_dir / "imported_OrderBook_and_Risk_Rules.txt").is_file()
    assert (knowledge_dir / "ai_market_knowledge.json").is_file()
    assert (knowledge_dir / "purified_book_rules.json").is_file()
    assert not (PROJECT_ROOT / "historical_ticks").exists()
    assert not (PROJECT_ROOT / "ai_market_knowledge.json").exists()
    assert not (PROJECT_ROOT / "purified_book_rules.json").exists()


def test_patch_implementations_and_legacy_launchers_exist() -> None:
    assert (PROJECT_ROOT / "tools" / "patches" / "install_local_patch.py").is_file()
    assert (PROJECT_ROOT / "tools" / "patches" / "rollback_local_patch.py").is_file()
    assert (PROJECT_ROOT / "install_local_patch.py").is_file()
    assert (PROJECT_ROOT / "rollback_local_patch.py").is_file()


def test_patch_launchers_show_help_from_outside_project(tmp_path: Path) -> None:
    launchers = (
        PROJECT_ROOT / "install_local_patch.py",
        PROJECT_ROOT / "rollback_local_patch.py",
        PROJECT_ROOT / "tools" / "patches" / "install_local_patch.py",
        PROJECT_ROOT / "tools" / "patches" / "rollback_local_patch.py",
    )
    for launcher in launchers:
        result = subprocess.run(
            [sys.executable, str(launcher), "--help"],
            cwd=tmp_path,
            capture_output=True,
            text=True,
            timeout=20,
            check=False,
        )
        assert result.returncode == 0, result.stdout + result.stderr
        assert "usage:" in result.stdout.lower()


def test_gitignore_covers_virtualenv_bytecode_and_runtime_logs() -> None:
    ignore = (PROJECT_ROOT / ".gitignore").read_text(encoding="utf-8")
    for pattern in (".venv/", "**/__pycache__/", "*.py[cod]", "trading_logs/", "live_market_logs/"):
        assert pattern in ignore


def test_fallback_aof_path_is_anchored_to_project_root() -> None:
    db_source = (PROJECT_ROOT / "piluitus" / "storage" / "db_manager.py").read_text(encoding="utf-8")
    assert "AOF_FILE = str(PROJECT_ROOT / \"piluitus_fallback_aof.jsonl\")" in db_source
    assert "def __init__(self, aof_path: str = AOF_FILE)" in db_source
