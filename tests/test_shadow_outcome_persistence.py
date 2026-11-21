from __future__ import annotations

import unittest
from unittest.mock import Mock

from storage.shadow_outcomes import (
    SHADOW_OUTCOMES_STORAGE_KEY,
    ShadowOutcomePersistence,
)


class FakeStorage:
    def __init__(self, rows=None, *, error: Exception | None = None) -> None:
        self.rows = list(rows or [])
        self.error = error
        self.read_calls = []
        self.write_calls = []

    async def get_list(self, key: str, *, start: int, stop: int):
        self.read_calls.append((key, start, stop))
        if self.error:
            raise self.error
        return list(self.rows)

    async def push_many_to_list(self, key: str, rows, *, max_len: int):
        self.write_calls.append((key, list(rows), max_len))
        if self.error:
            raise self.error


class FakeTracker:
    def __init__(self) -> None:
        self.clear_calls = 0
        self.restored_rows = None

    def clear(self) -> int:
        self.clear_calls += 1
        return 3

    def restore_completed_labels(self, rows) -> int:
        self.restored_rows = rows
        return sum(isinstance(row, dict) for row in rows)


class ShadowOutcomePersistenceTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.storage = FakeStorage()
        self.tracker = FakeTracker()
        self.labels = [{"older": True}, {"old": True}]
        self.logger = Mock()
        self.adapter = ShadowOutcomePersistence(
            self.storage,
            self.tracker,
            self.labels,
            self.logger,
            storage_limit=3,
        )

    async def test_persist_adds_session_marker_and_bounds_memory(self) -> None:
        incoming = [{"candidate_id": "a"}, {"candidate_id": "b"}]

        await self.adapter.persist_completed_labels(
            incoming, session_id="paper-42", source="ORDER_BOOK"
        )

        self.assertEqual(
            self.storage.write_calls,
            [(
                SHADOW_OUTCOMES_STORAGE_KEY,
                [
                    {"candidate_id": "a", "paper_session_id": "paper-42"},
                    {"candidate_id": "b", "paper_session_id": "paper-42"},
                ],
                3,
            )],
        )
        self.assertEqual(
            self.labels,
            [
                {"old": True},
                {"candidate_id": "a", "paper_session_id": "paper-42"},
                {"candidate_id": "b", "paper_session_id": "paper-42"},
            ],
        )
        self.assertEqual(incoming, [{"candidate_id": "a"}, {"candidate_id": "b"}])
        self.logger.info.assert_called_once()

    async def test_persist_skips_empty_or_non_dict_batches(self) -> None:
        await self.adapter.persist_completed_labels(
            [], session_id="s", source="QUOTES"
        )
        await self.adapter.persist_completed_labels(
            ["not-a-label"], session_id="s", source="QUOTES"
        )

        self.assertEqual(self.storage.write_calls, [])
        self.assertEqual(self.labels, [{"older": True}, {"old": True}])

    async def test_persist_storage_failure_does_not_update_memory(self) -> None:
        self.storage.error = RuntimeError("storage unavailable")

        await self.adapter.persist_completed_labels(
            [{"candidate_id": "a"}], session_id="s", source="QUOTES"
        )

        self.assertEqual(self.labels, [{"old": True}])
        self.logger.error.assert_called_once()

    async def test_paper_restore_clears_session_state_without_reading_history(self) -> None:
        await self.adapter.restore_completed_labels(
            paper_mode=True, session_id="paper-43"
        )

        self.assertEqual(self.labels, [])
        self.assertEqual(self.tracker.clear_calls, 1)
        self.assertEqual(self.storage.read_calls, [])
        self.logger.info.assert_called_once()

    async def test_non_paper_restore_loads_only_dictionary_labels(self) -> None:
        self.storage.rows = [
            {"candidate_id": "a"},
            "invalid",
            {"candidate_id": "b"},
        ]

        await self.adapter.restore_completed_labels(
            paper_mode=False, session_id="research"
        )

        self.assertEqual(self.storage.read_calls, [(SHADOW_OUTCOMES_STORAGE_KEY, 0, 2)])
        self.assertEqual(self.tracker.restored_rows, self.storage.rows)
        self.assertEqual(
            self.labels,
            [{"candidate_id": "a"}, {"candidate_id": "b"}],
        )
        self.logger.info.assert_called_once()

    async def test_restore_storage_failure_is_logged(self) -> None:
        self.storage.error = RuntimeError("storage unavailable")

        await self.adapter.restore_completed_labels(paper_mode=False)

        self.assertEqual(self.labels, [])
        self.logger.error.assert_called_once()
