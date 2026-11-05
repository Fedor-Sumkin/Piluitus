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


def test_data_sources_are_in_data_tree() -> None:
    csv_dir = PROJECT_ROOT / "data" / "historical_ticks"
    source_dir = PROJECT_ROOT / "data" / "knowledge" / "sources"
    assert any(csv_dir.glob("*.csv"))
    assert (source_dir / "imported_OrderBook_and_Risk_Rules.txt").is_file()
    assert any(source_dir.glob("*.pdf"))


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
