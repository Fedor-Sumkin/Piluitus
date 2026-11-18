"""Compatibility launcher for the patch rollback tool in tools/patches/."""
from pathlib import Path
import runpy
import sys


def _configure_console() -> None:
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if callable(reconfigure):
            try:
                reconfigure(encoding="utf-8", errors="replace")
            except (OSError, ValueError):
                pass


_TARGET = Path(__file__).resolve().parent / "tools" / "patches" / "rollback_local_patch.py"

if __name__ == "__main__":
    _configure_console()
    runpy.run_path(str(_TARGET), run_name="__main__")
