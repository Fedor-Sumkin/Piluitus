"""Tests that the LLM prompt cannot invert the locally selected candidate side."""
from __future__ import annotations

import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

import httpx

from ai_engine import AIEngine


class AIEngineDirectionContractTests(unittest.TestCase):
    def _prompt(self, direction: str) -> str:
        return AIEngine._build_live_prompt(
            ticker="WUSH",
            price=38.0,
            volume=100,
            candidate_direction=direction,
            metrics={},
            rag_context="",
            max_chars=1600,
            include_rule_text=False,
        )

    def test_buy_candidate_requests_approval_not_direction_choice(self) -> None:
        prompt = self._prompt("BUY")
        self.assertIn("exactly two keys: final_decision", prompt)
        self.assertIn("Never select, change, invert, or output BUY/SELL", prompt)
        self.assertIn("APPROVE authorizes only the exact local candidate side", prompt)
        self.assertIn("supporting evidence, not mandatory", prompt)
        self.assertIn("candidate=BUY", prompt)
        self.assertIn("BUY requires either positive signed price_change_pct", prompt)
        self.assertLessEqual(len(prompt), 1600)

    def test_sell_candidate_requests_approval_not_direction_choice(self) -> None:
        prompt = self._prompt("SELL")
        self.assertIn("exactly two keys: final_decision", prompt)
        self.assertIn("Never select, change, invert, or output BUY/SELL", prompt)
        self.assertIn("APPROVE authorizes only the exact local candidate side", prompt)
        self.assertIn("supporting evidence, not mandatory", prompt)
        self.assertIn("candidate=SELL", prompt)
        self.assertIn("SELL requires either negative signed price_change_pct", prompt)
        self.assertLessEqual(len(prompt), 1600)

    def test_missing_candidate_requires_hold(self) -> None:
        prompt = self._prompt("NONE")
        self.assertIn("candidate=NONE => final_decision MUST be HOLD.", prompt)
        self.assertIn("No local direction: MUST HOLD.", prompt)
        self.assertLessEqual(len(prompt), 1600)

    def test_missing_final_decision_is_flagged_as_contract_violation(self) -> None:
        result = object.__new__(AIEngine)._cpu_intensive_processing({
            "choices": [{
                "message": {"content": '{"candidate":"BUY","confidence_score":0.91}'},
                "finish_reason": "stop",
            }]
        })

        self.assertEqual(result["final_decision"], "HOLD")
        self.assertEqual(result["model_decision"], "HOLD")
        self.assertEqual(result["model_raw_decision"], "INVALID_RESPONSE")
        self.assertTrue(result["model_contract_violation"])
        self.assertEqual(result["contract_error_type"], "schema")
        self.assertFalse(result["confidence_valid"])

    def test_truncated_json_is_flagged_as_contract_violation(self) -> None:
        result = object.__new__(AIEngine)._cpu_intensive_processing({
            "choices": [{
                "message": {"content": '{"final_decision":"APPROVE"'},
                "finish_reason": "length",
            }]
        })

        self.assertEqual(result["final_decision"], "HOLD")
        self.assertTrue(result["model_contract_violation"])
        self.assertEqual(result["finish_reason"], "length")

    def test_approval_without_valid_confidence_is_contract_violation(self) -> None:
        engine = object.__new__(AIEngine)
        result = engine._cpu_intensive_processing({
            "choices": [{
                "message": {"content": '{"final_decision":"APPROVE","confidence_score":"unknown"}'},
                "finish_reason": "stop",
            }]
        })

        self.assertEqual(result["final_decision"], "HOLD")
        self.assertEqual(result["model_raw_decision"], "INVALID_RESPONSE")
        self.assertTrue(result["model_contract_violation"])
        self.assertFalse(result["confidence_valid"])

    def test_numeric_confidence_above_one_is_rejected_not_percent_normalized(self) -> None:
        result = object.__new__(AIEngine)._cpu_intensive_processing({
            "choices": [{
                "message": {"content": (
                    '{"final_decision":"APPROVE","confidence_score":89}'
                )},
                "finish_reason": "stop",
            }]
        })
        self.assertEqual(result["final_decision"], "HOLD")
        self.assertTrue(result["model_contract_violation"])
        self.assertEqual(result["contract_error_type"], "invalid_confidence")

    def test_compact_opinion_works_without_constructor_initialization(self) -> None:
        engine = object.__new__(AIEngine)
        self.assertEqual(
            engine._compact_opinion("Импульс недостаточен."),
            "Импульс недостаточен.",
        )

    def test_valid_hold_is_not_a_contract_violation(self) -> None:
        result = object.__new__(AIEngine)._cpu_intensive_processing({
            "choices": [{
                "message": {"content": '{"final_decision":"HOLD","confidence_score":0.8}'},
                "finish_reason": "stop",
            }]
        })

        self.assertEqual(result["final_decision"], "HOLD")
        self.assertFalse(result["model_contract_violation"])
        self.assertTrue(result["confidence_valid"])

    def test_extra_candidate_key_is_rejected_even_with_valid_decision(self) -> None:
        result = object.__new__(AIEngine)._cpu_intensive_processing({
            "choices": [{
                "message": {"content": (
                    '{"final_decision":"APPROVE","confidence_score":0.9,'
                    '"candidate":"BUY"}'
                )},
                "finish_reason": "stop",
            }]
        })
        self.assertEqual(result["final_decision"], "HOLD")
        self.assertTrue(result["model_contract_violation"])
        self.assertEqual(result["contract_error_type"], "schema")
        self.assertIn("candidate", result["exc"])

    def test_json_extractor_prefers_object_with_final_decision(self) -> None:
        parsed = AIEngine._extract_json_object(
            '{"candidate":"BUY","ticker":"SBER"} '
            '{"final_decision":"HOLD","confidence_score":0.8}'
        )
        self.assertEqual(parsed.get("final_decision"), "HOLD")

    def test_nested_payload_cannot_hide_extra_top_level_fields(self) -> None:
        parsed = AIEngine._extract_json_object(
            '{"payload":{"final_decision":"HOLD","confidence_score":0.8},'
            '"unexpected":true}'
        )
        result = object.__new__(AIEngine)._cpu_intensive_processing({
            "choices": [{
                "message": {"content": (
                    '{"payload":{"final_decision":"HOLD","confidence_score":0.8},'
                    '"unexpected":true}'
                )},
                "finish_reason": "stop",
            }]
        })
        self.assertNotIn("final_decision", parsed)
        self.assertTrue(result["model_contract_violation"])
        self.assertEqual(result["contract_error_type"], "schema")

    def test_directional_rag_rule_does_not_override_local_evidence_gate(self) -> None:
        prompt = AIEngine._build_live_prompt(
            ticker="WUSH",
            price=38.0,
            volume=100,
            candidate_direction="BUY",
            metrics={},
            rag_context=(
                "[RISK / RISK_PER_TRADE]: Risk <=0.5% per trade.\n"
                "[BUY / BUY_IMPULSE]: Require a 70% Ask size drop before any entry."
            ),
            max_chars=1600,
            include_rule_text=True,
        )
        self.assertIn("RISK_PER_TRADE", prompt)
        self.assertNotIn("70% Ask size drop", prompt)
        self.assertIn("Ask depletion is supporting", prompt)

    def test_retry_prompt_is_short_and_requires_only_approval_contract(self) -> None:
        prompt = AIEngine._build_contract_retry_prompt(
            "SBER", 100.0, 500, "BUY",
            {"price_change_pct": 0.2, "price_vs_vwap_pct": 0.1, "volume_spike": 2.0},
        )
        self.assertIn("exactly two keys", prompt)
        self.assertIn("APPROVE or HOLD", prompt)
        self.assertIn("Never output BUY/SELL as a decision", prompt)
        self.assertNotIn("RAG", prompt)

    def test_retry_prompt_uses_the_same_m1_alternative_as_primary_prompt(self) -> None:
        prompt = AIEngine._build_contract_retry_prompt(
            "SBER",
            100.0,
            500,
            "SELL",
            {
                "price_change_pct": 0.2,
                "price_vs_vwap_pct": -0.1,
                "volume_spike": 2.0,
                "bar_pattern_confirmed": 1,
                "bar_pattern_direction_code": -1,
            },
        )
        self.assertIn("Directional check: either signed price change agrees with candidate and VWAP, OR a fresh matching", prompt)
        self.assertIn("closed M1 pattern agrees with candidate and price remains on the correct side of VWAP", prompt)
        self.assertNotIn("SELL needs negative price_change_pct and price_vs_vwap_pct", prompt)

    def test_local_opinions_are_built_from_structured_result_and_metrics(self) -> None:
        tactic, critic = AIEngine._build_local_opinions(
            "APPROVE",
            "BUY",
            {
                "price_change_pct": 0.12,
                "price_vs_vwap_pct": 0.08,
                "volume_spike": 2.5,
                "spread_pct": 0.1,
            },
        )
        self.assertIn("одобрила локальный кандидат BUY", tactic)
        self.assertIn("изменение цены +0.120%", critic)
        self.assertIn("всплеск размера сделки 2.500x", critic)
        self.assertIn("ATR и портфельный риск проверяются кодом", critic)

    def test_contract_survives_metric_overflow_at_compact_limit(self) -> None:
        metrics = {
            key: "x" * 200
            for key in (
                "price_change_pct",
                "price_vs_vwap_pct",
                "volume_spike",
                "spread_pct",
                "bid_ask_available",
                "spread_available",
                "bar_pattern_confirmed",
                "bar_pattern_direction_code",
                "bar_breakout_up",
                "bar_pullback_buy",
                "bar_trend_up",
                "ask_size_drop_pct",
                "top_ask_history_samples",
                "ask_size_drop_age_sec",
                "bar_pattern_name",
                "bar_signal_gap_atr",
            )
        }
        prompt = AIEngine._build_live_prompt(
            ticker="WUSH",
            price=38.0,
            volume=100,
            candidate_direction="NONE",
            metrics=metrics,
            rag_context="",
            max_chars=1600,
            include_rule_text=False,
        )
        self.assertIn("candidate=NONE => final_decision MUST be HOLD.", prompt)
        self.assertIn("No local direction: MUST HOLD.", prompt)
        self.assertLessEqual(len(prompt), 1600)


class AIEngineContractRetryTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _response(content: str) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "choices": [{"message": {"content": content}, "finish_reason": "stop"}],
                "usage": {"completion_tokens": 48},
            },
            request=httpx.Request("POST", "http://localhost/v1/chat/completions"),
        )

    async def test_failed_retry_preserves_specific_error_type_without_model_opinion(self) -> None:
        engine = object.__new__(AIEngine)
        engine.api_path = "/v1/chat/completions"
        engine.model_name = "test-model"
        engine.client = SimpleNamespace(post=AsyncMock(side_effect=[
            self._response(
                '{"final_decision":"APPROVE","confidence_score":0.9,"unexpected":true}'
            ),
            self._response(
                '{"final_decision":"APPROVE","confidence_score":1.2}'
            ),
        ]))
        result = await engine.analyze_market_anomaly({
            "ticker": "SBER",
            "price": 100.0,
            "volume": 500,
            "candidate_direction": "BUY",
            "metrics": {
                "price_change_pct": 0.2,
                "price_vs_vwap_pct": 0.1,
                "volume_spike": 2.0,
                "spread_pct": 0.1,
                "spread_available": 1.0,
                "bid_ask_available": 1.0,
            },
        })
        self.assertEqual(result["final_decision"], "HOLD")
        self.assertTrue(result["model_contract_violation"])
        self.assertEqual(result["contract_error_type"], "invalid_confidence")
        self.assertEqual(result["first_contract_error_type"], "schema")
        self.assertTrue(result["contract_retry_failed"])
        self.assertIn("Повторная проверка не восстановила", result["critic_opinion"])
        self.assertNotIn("unexpected", result["critic_opinion"])

    async def test_malformed_first_response_is_retried_and_recovered(self) -> None:
        engine = object.__new__(AIEngine)
        engine.api_path = "/v1/chat/completions"
        engine.model_name = "test-model"
        engine.client = SimpleNamespace(post=AsyncMock(side_effect=[
            self._response('{"candidate":"BUY","ticker":"SBER","price":"100"}'),
            self._response(
                '{"final_decision":"HOLD","confidence_score":0.8}'
            ),
        ]))
        result = await engine.analyze_market_anomaly({
            "ticker": "SBER",
            "price": 100.0,
            "volume": 500,
            "candidate_direction": "BUY",
            "metrics": {
                "price_change_pct": 0.2,
                "price_vs_vwap_pct": 0.1,
                "volume_spike": 2.0,
                "spread_pct": 0.1,
                "spread_available": 1.0,
                "bid_ask_available": 1.0,
            },
        })
        self.assertEqual(engine.client.post.await_count, 2)
        self.assertEqual(result["model_decision"], "HOLD")
        self.assertFalse(result["model_contract_violation"])
        self.assertTrue(result["contract_retry_attempted"])
        self.assertTrue(result["contract_retry_recovered"])
        self.assertFalse(result["contract_retry_failed"])


if __name__ == "__main__":
    unittest.main()
