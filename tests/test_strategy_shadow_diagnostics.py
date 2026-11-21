from __future__ import annotations

from unittest.mock import Mock

from monitoring.strategy_shadow_diagnostics import StrategyShadowDiagnostics


def make_diagnostics(result: dict):
    logger = Mock()
    tracker = Mock()
    tracker.mark_llm_not_called.return_value = True
    tracker.record_candidate.return_value = "shadow-123"
    increment = Mock()
    evaluator = Mock(return_value=dict(result))
    bucket = Mock(return_value="size")
    diagnostics = StrategyShadowDiagnostics(
        logger, tracker, increment, bucket, evaluator
    )
    return diagnostics, logger, tracker, increment, evaluator, bucket


def test_mark_llm_not_called_records_reason_bucket_only_for_known_candidate():
    diagnostics, _, tracker, increment, _, bucket = make_diagnostics({})
    diagnostics.mark_llm_not_called("candidate-1", "best ask size reduced")

    tracker.mark_llm_not_called.assert_called_once_with(
        "candidate-1", reason_code="best ask size reduced"
    )
    bucket.assert_called_once_with("best ask size reduced")
    assert increment.call_args_list == [
        (( "shadow_llm_not_called", 1),),
        (( "shadow_llm_not_called_size", 1),),
    ]


def test_mark_llm_not_called_ignores_empty_or_unmatched_candidate():
    diagnostics, _, tracker, increment, _, bucket = make_diagnostics({})
    diagnostics.mark_llm_not_called(" ", "size")
    tracker.mark_llm_not_called.assert_not_called()
    increment.assert_not_called()
    bucket.assert_not_called()

    tracker.mark_llm_not_called.return_value = False
    diagnostics.mark_llm_not_called("candidate-2", "size")
    increment.assert_not_called()
    bucket.assert_not_called()


def test_log_candidate_preserves_shadow_result_counters_and_log_fields():
    result = {
        "eligible": True,
        "score": 91.5,
        "threshold": 70,
        "regime": "TREND_ALIGNED",
        "hard_blocks": ["bbo_stale", "unknown_block"],
        "reasons": ["fresh trend"],
        "components": {"flow": 12.25, "queue": 8, "invalid": float("inf")},
    }
    diagnostics, logger, tracker, increment, evaluator, _ = make_diagnostics(result)
    metrics = {"bid": 100.0, "ask": 100.1}
    observed = diagnostics.log_candidate("SBER", "BUY", metrics)

    evaluator.assert_called_once_with("BUY", metrics)
    tracker.record_candidate.assert_called_once()
    recorded = tracker.record_candidate.call_args.args
    assert recorded[:3] == ("SBER", "BUY", metrics)
    assert recorded[3]["eligible"] is True
    assert recorded[3]["score"] == 91.5
    assert observed["candidate_id"] == "shadow-123"
    assert increment.call_args_list == [
        (("strategy_shadow_evaluated", 1),),
        (("strategy_shadow_pass", 1),),
        (("shadow_block_bbo_stale", 1),),
        (("shadow_block_other", 1),),
    ]
    message = logger.info.call_args.args[0]
    assert "[STRATEGY SHADOW]" in message
    assert "components=%s" in message
    assert logger.info.call_args.args[1] == "SBER"


def test_log_candidate_counts_rejections_and_all_known_block_categories():
    result = {
        "eligible": False,
        "hard_blocks": ["bbo_unavailable", "atr_not_ready", "spread_above_0.50_pct"],
        "components": {},
    }
    diagnostics, _, _, increment, _, _ = make_diagnostics(result)
    observed = diagnostics.log_candidate("GAZP", "SELL", {})

    assert observed["eligible"] is False
    names = [call.args[0] for call in increment.call_args_list]
    assert names == [
        "strategy_shadow_evaluated",
        "strategy_shadow_reject",
        "shadow_block_bbo_unavailable",
        "shadow_block_atr_not_ready",
        "shadow_block_spread",
    ]
