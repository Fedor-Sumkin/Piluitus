from __future__ import annotations

import inspect
import unittest

from runtime import application_lifecycle_runtime as facade
from runtime.application_lifecycle_background import ApplicationLifecycleBackgroundMixin
from runtime.application_lifecycle_base import ApplicationLifecycleBase
from runtime.application_lifecycle_shutdown import ApplicationLifecycleShutdownMixin
from runtime.application_lifecycle_startup import ApplicationLifecycleStartupMixin


class ApplicationLifecycleModuleTests(unittest.TestCase):
    def test_public_runtime_keeps_constructor_from_shared_base(self):
        self.assertIs(
            facade.ApplicationLifecycleRuntime.__init__,
            ApplicationLifecycleBase.__init__,
        )
        self.assertEqual(
            list(inspect.signature(facade.ApplicationLifecycleRuntime).parameters),
            [
                "logger",
                "execution_engine",
                "deposit",
                "db_manager",
                "paper_performance_tracker",
                "shadow_tracker",
                "paper_shadow_labels",
                "risk_manager",
                "paper_bbo_replay_recorder_provider",
                "restore_shadow_outcomes",
                "restore_paper_performance",
                "ai_engine",
                "quarantine_unreconciled_paper_positions",
                "max_cache_size",
                "set_verdict_cache",
                "worker_factory",
                "finam_metadata_refresh_loop",
                "start_news_updater",
                "start_tables_sync_loop",
                "telegram_bot_module",
                "broker",
                "market_broker_init",
                "set_market_task",
                "background_operations",
                "trade_flow_maybe_log",
                "task_supervisor_factory",
            ],
        )

    def test_lifecycle_phases_are_owned_by_focused_mixins(self):
        runtime = facade.ApplicationLifecycleRuntime
        self.assertIs(
            runtime._restore_startup_state,
            ApplicationLifecycleStartupMixin._restore_startup_state,
        )
        self.assertIs(
            runtime._run_market_worker_safely,
            ApplicationLifecycleBackgroundMixin._run_market_worker_safely,
        )
        self.assertIs(
            runtime._spawn_auxiliary_background_tasks,
            ApplicationLifecycleBackgroundMixin._spawn_auxiliary_background_tasks,
        )
        self.assertIs(
            runtime._drain_application_services,
            ApplicationLifecycleShutdownMixin._drain_application_services,
        )

    def test_lifespan_remains_the_public_context_manager_and_owns_supervisor(self):
        runtime = facade.ApplicationLifecycleRuntime
        self.assertTrue(hasattr(runtime.lifespan, "__wrapped__"))
        self.assertIn("lifespan", runtime.__dict__)
        self.assertIn("spawn", inspect.getsource(runtime.lifespan))
        self.assertIn("cancel_and_drain", inspect.getsource(runtime.lifespan))

    def test_legacy_json_module_attribute_is_preserved(self):
        import json

        self.assertIs(facade.json, json)


if __name__ == "__main__":
    unittest.main()
