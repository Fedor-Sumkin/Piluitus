import ast
from pathlib import Path


def test_paper_session_reset_preserves_unresolved_positions_and_deletes_only_closed():
    source_path = Path(__file__).resolve().parents[1] / "storage" / "db_manager.py"
    module = ast.parse(source_path.read_text(encoding="utf-8"))
    manager = next(
        node for node in ast.walk(module)
        if isinstance(node, ast.ClassDef) and node.name == "DBManager"
    )
    reset = next(
        node for node in manager.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "clear_paper_session"
    )
    source = ast.get_source_segment(source_path.read_text(encoding="utf-8"), reset)

    assert 'status in {"OPEN", "ACTIVE"}' in source
    assert 'status == "STALE_REVIEW"' in source
    assert 'status == "CLOSED"' in source
    assert "preserved_open_trade_ids" in source
    assert "preserved_stale_trade_ids" in source
    assert "preserved_open_paper_positions" in source
    assert "preserved_stale_paper_positions" in source
    assert 'await client.delete("piluitus:paper:closed_trades")' in source
