"""Composition contract for the modular DBManager implementation."""
from __future__ import annotations

import unittest

from storage.db_manager import DBManager, RedisClient, _InMemory
from storage.paper_session_repository import PaperSessionRepositoryMixin
from storage.trade_journal_repository import TradeJournalRepositoryMixin
from storage.storage_utility_repository import StorageUtilityRepositoryMixin


class DBManagerModuleBoundaryTests(unittest.TestCase):
    def test_public_manager_composes_all_repository_responsibilities(self):
        for mixin in (
            PaperSessionRepositoryMixin,
            TradeJournalRepositoryMixin,
            StorageUtilityRepositoryMixin,
        ):
            self.assertTrue(issubclass(DBManager, mixin))

    def test_stable_storage_public_classes_and_methods_remain_available(self):
        self.assertTrue(callable(RedisClient))
        self.assertTrue(callable(_InMemory))
        for name in (
            "get_paper_session_snapshot", "clear_paper_session",
            "clear_unreconciled_paper_positions", "log_trade", "get_active_positions",
            "get_all_daily_trades", "get_json", "set_json", "clear_journal",
        ):
            self.assertTrue(callable(getattr(DBManager, name)))


if __name__ == "__main__":
    unittest.main()
