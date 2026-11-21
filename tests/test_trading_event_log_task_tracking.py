"""Regression tests for run-scoped, retained trading event log tasks."""
import ast
from pathlib import Path


def _main_source() -> tuple[str, ast.Module]:
    path = Path(__file__).resolve().parents[1] / "main.py"
    source = path.read_text(encoding="utf-8")
    return source, ast.parse(source)


def test_critical_fire_and_forget_tasks_are_retained_and_drained():
    source, module = _main_source()

    assert "from monitoring.background_tasks import BackgroundOperationTracker" in source
    assert "_background_operations = BackgroundOperationTracker(logger)" in source
    assert "_background_operations.spawn(" in source
    assert "_spawn_tracked_operation" not in source
    assert "_drain_tracked_operations" not in source

    untracked_critical_calls = []
    for node in ast.walk(module):
        if not isinstance(node, ast.Call):
            continue
        if not (
            isinstance(node.func, ast.Attribute)
            and node.func.attr == "create_task"
        ):
            continue
        if not node.args or not isinstance(node.args[0], ast.Call):
            continue
        inner = node.args[0].func
        if isinstance(inner, ast.Name) and inner.id == "log_trading_event_to_file":
            untracked_critical_calls.append(node.lineno)
        if isinstance(inner, ast.Attribute) and inner.attr == "_execute_and_publish":
            untracked_critical_calls.append(node.lineno)

    assert untracked_critical_calls == []


def test_trading_event_log_is_scoped_and_correlated_to_run_candidate_and_verdict():
    source, module = _main_source()
    logger_function = next(
        node for node in module.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "log_trading_event_to_file"
    )
    logger_source = ast.get_source_segment(source, logger_function)

    assert "str(RUN_ID or \"unknown\")" in logger_source
    assert "piluitus_session_{date_str}_{run_id}.txt" in logger_source
    assert "candidate_id={candidate_id or 'NONE'}" in logger_source
    assert "verdict_id={verdict_id or 'NONE'}" in logger_source
    assert "model_confidence" in logger_source
    assert "final_confidence" in logger_source


def test_shutdown_drains_tracked_operations_after_producers_stop():
    _, module = _main_source()
    lifespan = next(
        node for node in module.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "lifespan"
    )
    calls = [
        node for node in ast.walk(lifespan)
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "drain"
        and isinstance(node.func.value, ast.Name)
        and node.func.value.id == "_background_operations"
    ]
    assert len(calls) == 1
    assert any(
        isinstance(node, ast.Await)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Attribute)
        and node.value.func.attr == "drain"
        and isinstance(node.value.func.value, ast.Name)
        and node.value.func.value.id == "_background_operations"
        for node in ast.walk(lifespan)
    )
