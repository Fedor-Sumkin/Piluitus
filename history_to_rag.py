"""Compatibility launcher; implementation moved to tools/data/history_to_rag.py."""
from pathlib import Path
import runpy
_TARGET = Path(__file__).resolve().parent / "tools/data/history_to_rag.py"
runpy.run_path(str(_TARGET), run_name="__main__")
