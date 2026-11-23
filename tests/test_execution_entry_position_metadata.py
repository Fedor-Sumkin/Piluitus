import ast
from pathlib import Path


def test_paper_entry_preserves_original_position_size_without_exit_scope_variables():
    source_path = Path(__file__).resolve().parents[1] / "execution" / "execution_order_runtime.py"
    module = ast.parse(source_path.read_text(encoding="utf-8"))

    trade_log = next(
        node.value
        for node in ast.walk(module)
        if isinstance(node, ast.Assign)
        and any(isinstance(target, ast.Name) and target.id == "trade_log" for target in node.targets)
        and isinstance(node.value, ast.Dict)
    )
    fields = {
        key.value: value
        for key, value in zip(trade_log.keys, trade_log.values)
        if isinstance(key, ast.Constant) and isinstance(key.value, str)
    }

    assert ast.unparse(fields["original_volume"]) == "int(lots * lot_multiplier)"
    assert ast.unparse(fields["original_lots"]) == "int(lots)"

    for field in ("original_volume", "original_lots"):
        assert not any(
            isinstance(node, ast.Name) and node.id in {"trade", "share_qty"}
            for node in ast.walk(fields[field])
        )
