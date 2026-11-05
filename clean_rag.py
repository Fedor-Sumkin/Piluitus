"""Compatibility launcher; implementation moved to tools/maintenance/clean_rag.py."""
from pathlib import Path
import runpy
_TARGET = Path(__file__).resolve().parent / "tools/maintenance/clean_rag.py"
runpy.run_path(str(_TARGET), run_name="__main__")
