"""Compatibility launcher; implementation moved to tools/knowledge/rag_ai_distiller.py."""
from pathlib import Path
import runpy
_TARGET = Path(__file__).resolve().parent / "tools/knowledge/rag_ai_distiller.py"
runpy.run_path(str(_TARGET), run_name="__main__")
