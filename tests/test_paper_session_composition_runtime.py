"""Behavioral contracts for PAPER session composition."""
from __future__ import annotations

import asyncio

import pytest

from runtime import paper_session_composition_runtime as module


class _ShadowPersistence:
    def __init__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs


class _PaperSession:
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.calls = []
        self.rotation_error = None
        self.next_recorder = object()

    async def persist_shadow_outcomes(self, labels, *, source):
        self.calls.append(("persist", labels, source))

    async def restore_shadow_outcomes(self):
        self.calls.append(("restore_shadow",))

    async def rotate_bbo_replay_session(self):
        self.calls.append(("rotate",))
        if self.rotation_error is not None:
            raise self.rotation_error
        return self.next_recorder

    async def restore_paper_performance(self):
        self.calls.append(("restore_performance",))


def _runtime(monkeypatch):
    monkeypatch.setattr(module, "ShadowOutcomePersistence", _ShadowPersistence)
    monkeypatch.setattr(module, "PaperSessionRuntime", _PaperSession)
    db_manager = object()
    execution_engine = object()
    shadow_tracker = object()
    recorder = object()
    performance_tracker = object()
    labels = []
    logger = object()
    published_recorders = []
    runtime = module.PaperSessionCompositionRuntime(
        db_manager=db_manager,
        execution_engine=execution_engine,
        shadow_tracker=shadow_tracker,
        bbo_replay_recorder=recorder,
        bbo_replay_recorder_factory=lambda: object(),
        paper_performance_tracker=performance_tracker,
        paper_shadow_labels=labels,
        logger=logger,
        closed_trades_storage_key="paper:closed",
        closed_trades_storage_limit=50,
        shadow_outcomes_storage_limit=250,
        set_bbo_replay_recorder=published_recorders.append,
    )
    return runtime, {
        "db_manager": db_manager,
        "execution_engine": execution_engine,
        "shadow_tracker": shadow_tracker,
        "recorder": recorder,
        "performance_tracker": performance_tracker,
        "labels": labels,
        "logger": logger,
        "published_recorders": published_recorders,
    }


def test_composition_builds_shared_shadow_and_session_storage_services(monkeypatch):
    runtime, deps = _runtime(monkeypatch)

    assert runtime.create() is runtime
    persistence = runtime.shadow_outcome_persistence
    session = runtime.paper_session_runtime

    assert isinstance(persistence, _ShadowPersistence)
    assert persistence.args == (
        deps["db_manager"],
        deps["shadow_tracker"],
        deps["labels"],
        deps["logger"],
    )
    assert persistence.kwargs == {"storage_limit": 250}
    assert session.kwargs == {
        "db_manager": deps["db_manager"],
        "execution_engine": deps["execution_engine"],
        "shadow_outcome_persistence": persistence,
        "bbo_replay_recorder": deps["recorder"],
        "bbo_replay_recorder_factory": runtime.bbo_replay_recorder_factory,
        "paper_performance_tracker": deps["performance_tracker"],
        "paper_shadow_labels": deps["labels"],
        "logger": deps["logger"],
        "closed_trades_storage_key": "paper:closed",
        "closed_trades_storage_limit": 50,
    }


def test_composition_preserves_lifecycle_callbacks_and_recorder_publication(monkeypatch):
    runtime, deps = _runtime(monkeypatch)
    runtime.create()
    labels = [{"candidate_id": "candidate-1"}]

    asyncio.run(runtime.persist_shadow_outcomes(labels, source="ORDER_BOOK"))
    asyncio.run(runtime.restore_shadow_outcomes())
    recorder = asyncio.run(runtime.rotate_bbo_replay_session())
    asyncio.run(runtime.restore_paper_performance())

    assert runtime.paper_session_runtime.calls == [
        ("persist", labels, "ORDER_BOOK"),
        ("restore_shadow",),
        ("rotate",),
        ("restore_performance",),
    ]
    assert recorder is runtime.paper_session_runtime.next_recorder
    assert deps["published_recorders"] == [recorder]


def test_failed_recorder_rotation_does_not_publish_replacement(monkeypatch):
    runtime, deps = _runtime(monkeypatch)
    runtime.create()
    runtime.paper_session_runtime.rotation_error = RuntimeError("close failed")

    with pytest.raises(RuntimeError, match="close failed"):
        asyncio.run(runtime.rotate_bbo_replay_session())

    assert deps["published_recorders"] == []
