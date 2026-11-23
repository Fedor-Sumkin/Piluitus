"""Behavioral contracts for the dashboard's read-only trading status."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from monitoring.trading_status import build_trading_status_snapshot


MSK = timezone(timedelta(hours=3))


def _runtime(*, allowed=True, connected=True, **risk_overrides):
    attributes = {
        "_loss_streak_count": 0,
        "loss_streak_hard_stop": 5,
        "_cached_trading_allowed": allowed,
        "block_trading": False,
        "_trading_block_reason": None,
        "_loss_streak_pause_until": None,
    }
    attributes.update(risk_overrides)
    risk = SimpleNamespace(**attributes)
    broker = SimpleNamespace(
        is_ws_running=connected,
        _market_stream_connected=connected,
    )
    engine = SimpleNamespace(paper_mode=True, broker=broker)
    return risk, engine


def test_status_reports_active_only_after_risk_approval_and_finam_handshake():
    risk, engine = _runtime(allowed=True, connected=True)

    status = build_trading_status_snapshot(risk_manager=risk, execution_engine=engine)

    assert status["status"] == "ACTIVE"
    assert status["trading_allowed"] is True
    assert status["mode"] == "PAPER"
    assert status["market_connected"] is True
    assert "новые входы" in status["message"]


def test_status_reports_hard_stop_after_configured_loss_streak():
    risk, engine = _runtime(
        allowed=False,
        connected=True,
        _loss_streak_count=5,
        block_trading=True,
        _trading_block_reason="LOSS_STREAK_HARD_STOP",
    )

    status = build_trading_status_snapshot(risk_manager=risk, execution_engine=engine)

    assert status["status"] == "STOPPED"
    assert status["loss_streak"] == 5
    assert status["loss_streak_limit"] == 5
    assert status["block_reason"] == "LOSS_STREAK_HARD_STOP"
    assert "до следующей торговой даты" in status["message"]


def test_status_never_reports_new_entries_allowed_when_a_stop_flag_wins_a_stale_cache_race():
    risk, engine = _runtime(
        allowed=True,
        connected=True,
        block_trading=True,
        _trading_block_reason="LOSS_STREAK_HARD_STOP",
    )

    status = build_trading_status_snapshot(risk_manager=risk, execution_engine=engine)

    assert status["status"] == "STOPPED"
    assert status["trading_allowed"] is False


def test_status_reports_timed_loss_streak_pause_and_countdown():
    pause_until = datetime.now(MSK).replace(tzinfo=None) + timedelta(minutes=5)
    risk, engine = _runtime(
        allowed=False,
        connected=True,
        _loss_streak_count=3,
        _trading_block_reason="LOSS_STREAK_PAUSE",
        _loss_streak_pause_until=pause_until,
    )

    status = build_trading_status_snapshot(risk_manager=risk, execution_engine=engine)

    assert status["status"] == "PAUSED"
    assert 0 < status["pause_remaining_seconds"] <= 300
    assert status["pause_until"] == pause_until.isoformat(timespec="seconds")


def test_expired_pause_waits_for_a_fresh_risk_check_before_reporting_active():
    pause_until = datetime.now(MSK).replace(tzinfo=None) - timedelta(seconds=1)
    risk, engine = _runtime(
        allowed=False,
        connected=True,
        _loss_streak_count=3,
        _trading_block_reason="LOSS_STREAK_PAUSE",
        _loss_streak_pause_until=pause_until,
    )

    status = build_trading_status_snapshot(risk_manager=risk, execution_engine=engine)

    assert status["status"] == "CHECKING"
    assert status["trading_allowed"] is False


def test_status_does_not_confuse_stream_task_with_confirmed_websocket_connection():
    risk, engine = _runtime(allowed=True, connected=False)
    engine.broker.is_ws_running = True
    engine.broker._market_stream_connected = False

    status = build_trading_status_snapshot(risk_manager=risk, execution_engine=engine)

    assert status["status"] == "CONNECTING"
    assert status["market_stream_running"] is True
    assert status["market_connected"] is False
    assert status["trading_allowed"] is False


def test_status_reports_drawdown_guard_as_stopped():
    risk, engine = _runtime(
        allowed=False,
        connected=True,
        _trading_block_reason="EQUITY_DRAWDOWN",
    )

    status = build_trading_status_snapshot(risk_manager=risk, execution_engine=engine)

    assert status["status"] == "STOPPED"
    assert status["block_reason"] == "EQUITY_DRAWDOWN"
