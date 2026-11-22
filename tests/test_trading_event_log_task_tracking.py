"""Regression tests for run-scoped, retained trading event log tasks."""
import ast
from pathlib import Path


def _main_source() -> tuple[str, ast.Module]:
    path = Path(__file__).resolve().parents[1] / "main.py"
    source = path.read_text(encoding="utf-8")
    return source, ast.parse(source)


def test_critical_fire_and_forget_tasks_are_retained_and_drained():
    root = Path(__file__).resolve().parents[1]
    source, module = _main_source()
    runtime_paths = [
        root / "runtime" / "market_worker_candidate_admission_runtime.py",
        root / "runtime" / "market_worker_verdict_construction_runtime.py",
        root / "runtime" / "market_worker_verdict_publication_runtime.py",
    ]
    runtime_sources = [path.read_text(encoding="utf-8") for path in runtime_paths]
    runtime_modules = [
        ast.parse(source, filename=str(path))
        for path, source in zip(runtime_paths, runtime_sources)
    ]
    runtime_source = "\\n".join(runtime_sources)

    shared_state_path = root / "runtime" / "application_shared_state_runtime.py"
    shared_state_source = shared_state_path.read_text(encoding="utf-8")
    assert "from monitoring.background_tasks import BackgroundOperationTracker" in shared_state_source
    assert "background_operations = self.background_operations_factory(self.logger)" in shared_state_source
    assert '"_background_operations": shared.background_operations' in (
        root / "runtime" / "application_compatibility_exports.py"
    ).read_text(encoding="utf-8")
    root_path = root / "runtime" / "application_composition_root_runtime.py"
    root_source = root_path.read_text(encoding="utf-8")
    assert '"background_operations": shared.background_operations' in root_source
    assert '"background_operations": shared.background_operations' in root_source
    assert "_background_operations.spawn(" in runtime_source
    assert "_spawn_tracked_operation" not in source
    assert "_drain_tracked_operations" not in source

    untracked_critical_calls = []
    for parsed_module in (module, *runtime_modules):
        for node in ast.walk(parsed_module):
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
    logger_path = Path(__file__).resolve().parents[1] / "monitoring" / "trading_event_logger.py"
    logger_source = logger_path.read_text(encoding="utf-8")
    module = ast.parse(logger_source, filename=str(logger_path))
    logger_function = next(
        node for node in module.body
        if isinstance(node, ast.AsyncFunctionDef)
        and node.name == "log_trading_event_to_file"
    )
    function_source = ast.get_source_segment(logger_source, logger_function)

    assert "str(RUN_ID or \"unknown\")" in function_source
    assert "piluitus_session_{date_str}_{run_id}.txt" in function_source
    assert "candidate_id={candidate_id or 'NONE'}" in function_source
    assert "verdict_id={verdict_id or 'NONE'}" in function_source
    assert "model_confidence" in function_source
    assert "final_confidence" in function_source


def test_shutdown_drains_tracked_operations_after_producers_stop():
    root = Path(__file__).resolve().parents[1]
    main_source, main_module = _main_source()
    runtime_path = root / "runtime" / "application_lifecycle_runtime.py"
    runtime_source = runtime_path.read_text(encoding="utf-8")
    runtime_module = ast.parse(runtime_source, filename=str(runtime_path))

    shared_state_path = root / "runtime" / "application_shared_state_runtime.py"
    shared_state_source = shared_state_path.read_text(encoding="utf-8")
    assert "from monitoring.background_tasks import BackgroundOperationTracker" in shared_state_source
    assert "background_operations = self.background_operations_factory(self.logger)" in shared_state_source
    assert '"_background_operations": shared.background_operations' in (
        root / "runtime" / "application_compatibility_exports.py"
    ).read_text(encoding="utf-8")
    root_path = root / "runtime" / "application_composition_root_runtime.py"
    root_source = root_path.read_text(encoding="utf-8")
    assert '"background_operations": shared.background_operations' in root_source
    assert '"background_operations": shared.background_operations' in root_source
    tracked_runtime_sources = [
        (root / "runtime" / "market_worker_candidate_admission_runtime.py").read_text(encoding="utf-8"),
        (root / "runtime" / "market_worker_verdict_construction_runtime.py").read_text(encoding="utf-8"),
        (root / "runtime" / "market_worker_verdict_publication_runtime.py").read_text(encoding="utf-8"),
    ]
    assert any("self.background_operations.spawn(" in source for source in tracked_runtime_sources)
    assert "_spawn_tracked_operation" not in main_source
    assert "_drain_tracked_operations" not in main_source

    lifecycle_class = next(
        node for node in runtime_module.body
        if isinstance(node, ast.ClassDef)
        and node.name == "ApplicationLifecycleRuntime"
    )
    lifespan = next(
        node for node in lifecycle_class.body
        if isinstance(node, ast.AsyncFunctionDef) and node.name == "lifespan"
    )
    calls = [
        node for node in ast.walk(lifespan)
        if isinstance(node, ast.Await)
        and isinstance(node.value, ast.Call)
        and isinstance(node.value.func, ast.Attribute)
        and node.value.func.attr == "drain"
        and isinstance(node.value.func.value, ast.Attribute)
        and node.value.func.value.attr == "background_operations"
    ]
    assert len(calls) == 1
    shutdown_source = ast.get_source_segment(runtime_source, lifespan) or ""
    assert shutdown_source.index(
        "await background_task_supervisor.cancel_and_drain()"
    ) < shutdown_source.index("await self.background_operations.drain()")
