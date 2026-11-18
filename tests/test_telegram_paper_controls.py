"""Telegram control-plane safety tests for PAPER reconciliation and panic close."""
from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from interface import tg_bot


class TelegramPaperControlTests(unittest.IsolatedAsyncioTestCase):
    async def test_paper_panic_closes_positions_without_erasing_trade_journal(self) -> None:
        execution = SimpleNamespace(
            close_all_positions=AsyncMock(return_value={
                "closed": 2,
                "partial": 0,
                "blocked": 1,
                "message": "Экстренное PAPER-закрытие: закрыто 2, заблокировано 1.",
            })
        )
        database = SimpleNamespace(
            clear_journal=AsyncMock(),
        )
        bot = SimpleNamespace(send_message=AsyncMock())
        callback = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            message=SimpleNamespace(chat=SimpleNamespace(id=123)),
            answer=AsyncMock(),
        )

        with (
            patch.object(tg_bot, "USER_ID", "123"),
            patch.object(tg_bot, "bot", bot),
            patch.object(tg_bot, "global_broker_ref", SimpleNamespace(is_demo=True)),
            patch.object(tg_bot, "global_execution_engine_ref", execution),
            patch.object(tg_bot, "db_manager", database),
        ):
            await tg_bot.process_panic_callback(callback)

        execution.close_all_positions.assert_awaited_once_with(database)
        database.clear_journal.assert_not_awaited()
        bot.send_message.assert_awaited_once()
        self.assertIn("закрыто 2", bot.send_message.await_args.kwargs["text"])

    async def test_unauthorized_user_cannot_trigger_panic_callback(self) -> None:
        execution = SimpleNamespace(close_all_positions=AsyncMock())
        bot = SimpleNamespace(send_message=AsyncMock())
        callback = SimpleNamespace(
            from_user=SimpleNamespace(id=999),
            message=SimpleNamespace(chat=SimpleNamespace(id=123)),
            answer=AsyncMock(),
        )

        with (
            patch.object(tg_bot, "USER_ID", "123"),
            patch.object(tg_bot, "bot", bot),
            patch.object(tg_bot, "global_broker_ref", SimpleNamespace(is_demo=True)),
            patch.object(tg_bot, "global_execution_engine_ref", execution),
        ):
            await tg_bot.process_panic_callback(callback)

        execution.close_all_positions.assert_not_awaited()
        bot.send_message.assert_not_awaited()
        callback.answer.assert_awaited_once_with("Нет доступа.", show_alert=True)

    async def test_risk_reset_command_is_read_only_and_preserves_journal(self) -> None:
        database = SimpleNamespace(
            get_paper_session_snapshot=AsyncMock(return_value={
                "paper_trade_count": 3,
                "paper_open_count": 1,
                "paper_stale_count": 1,
                "paper_closed_count": 1,
                "paper_trades": [
                    {"id": "PAPER_OLD", "ticker": "SBER", "side": "BUY", "status": "STALE_REVIEW"}
                ],
            }),
            clear_journal=AsyncMock(),
        )
        message = SimpleNamespace(
            from_user=SimpleNamespace(id=123),
            answer=AsyncMock(),
        )

        with (
            patch.object(tg_bot, "USER_ID", "123"),
            patch.object(tg_bot, "db_manager", database),
        ):
            await tg_bot.cmd_paper_reconciliation_status(message)

        database.get_paper_session_snapshot.assert_awaited_once()
        database.clear_journal.assert_not_awaited()
        response = message.answer.await_args.args[0]
        self.assertIn("Всего записей", response)
        self.assertIn("STALE_REVIEW", response)
        self.assertIn("журнал и PnL не изменяются", response)


if __name__ == "__main__":
    unittest.main()
