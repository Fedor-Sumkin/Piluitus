"""Compatibility launcher for the patch rollback tool in tools/patches/."""
from pathlib import Path
import runpy

_TARGET = Path(__file__).resolve().parent / "tools" / "patches" / "rollback_local_patch.py"

if __name__ == "__main__":
    runpy.run_path(str(_TARGET), run_name="__main__")
