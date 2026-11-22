from __future__ import annotations

import unittest

from storage import paper_session_maintenance_runtime as facade
from storage.paper_session_clear import PaperSessionClearMixin
from storage.paper_session_maintenance_base import (
    PAPER_CLEAR_CONFIRMATION,
    _PAPER_MAINTENANCE_KEY,
    PaperSessionMaintenanceBase,
)
from storage.paper_session_resume import PaperSessionResumeMixin
from storage.paper_session_stale_cleanup import PaperSessionStaleCleanupMixin


class PaperSessionMaintenanceModuleTests(unittest.TestCase):
    def test_facade_keeps_public_class_and_confirmation_constant(self):
        self.assertIs(facade.PAPER_CLEAR_CONFIRMATION, PAPER_CLEAR_CONFIRMATION)
        self.assertEqual(
            facade.PAPER_CLEAR_CONFIRMATION,
            "ОЧИСТИТЬ PAPER-СЕССИЮ",
        )
        self.assertIs(
            facade.PaperSessionMaintenanceRuntime.__bases__[0],
            PaperSessionClearMixin,
        )

    def test_facade_declares_async_contracts_and_delegates_to_focused_mixins(self):
        runtime = facade.PaperSessionMaintenanceRuntime
        self.assertEqual(
            runtime.clear_paper_session.__qualname__,
            "PaperSessionMaintenanceRuntime.clear_paper_session",
        )
        self.assertEqual(
            runtime.cleanup_stale_and_resume.__qualname__,
            "PaperSessionMaintenanceRuntime.cleanup_stale_and_resume",
        )
        self.assertEqual(
            runtime.resume_paper_session.__qualname__,
            "PaperSessionMaintenanceRuntime.resume_paper_session",
        )
        self.assertTrue(callable(PaperSessionClearMixin.clear_paper_session))
        self.assertTrue(callable(PaperSessionStaleCleanupMixin.cleanup_stale_and_resume))
        self.assertTrue(callable(PaperSessionResumeMixin.resume_paper_session))

    def test_shared_constructor_and_maintenance_guards_are_in_base(self):
        runtime = facade.PaperSessionMaintenanceRuntime
        self.assertIs(runtime.__init__, PaperSessionMaintenanceBase.__init__)
        self.assertIs(
            runtime._persist_maintenance,
            PaperSessionMaintenanceBase._persist_maintenance,
        )
        self.assertIs(
            runtime._purge_queued_market_events,
            PaperSessionMaintenanceBase._purge_queued_market_events,
        )
        self.assertIs(facade._PAPER_MAINTENANCE_KEY, _PAPER_MAINTENANCE_KEY)

    def test_facade_and_base_share_the_same_time_module(self):
        self.assertIs(facade.time, __import__("storage.paper_session_maintenance_base", fromlist=["time"]).time)


if __name__ == "__main__":
    unittest.main()
