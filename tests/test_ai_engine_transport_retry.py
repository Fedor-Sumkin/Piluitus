"""Regression tests for bounded vLLM request retries and timeout classification."""
from __future__ import annotations

import asyncio
import unittest
from unittest.mock import AsyncMock, patch

import httpx

from ai_engine import AIEngine
from config import Settings


class AIEngineTransportRetryTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _response(content: str = '{"final_decision":"HOLD","confidence_score":0.8}') -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "choices": [{
                    "message": {"content": content},
                    "finish_reason": "stop",
                }],
                "usage": {"completion_tokens": 32},
            },
            request=httpx.Request("POST", "http://localhost/v1/chat/completions"),
        )

    @staticmethod
    def _engine(side_effect) -> AIEngine:
        engine = object.__new__(AIEngine)
        engine.api_path = "/v1/chat/completions"
        engine.model_name = "test-model"
        engine.client = type("FakeClient", (), {})()
        engine.client.post = AsyncMock(side_effect=side_effect)
        return engine

    async def test_read_timeout_retries_once_and_recovers(self) -> None:
        engine = self._engine([
            httpx.ReadTimeout("first attempt timed out"),
            self._response(),
        ])
        with patch("ai.ai_engine.asyncio.sleep", new_callable=AsyncMock):
            response = await engine._post_with_retry(
                engine.api_path,
                json={"model": "test-model"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(engine.client.post.await_count, 2)

    async def test_timeout_error_retries_once_and_recovers(self) -> None:
        engine = self._engine([
            TimeoutError("first attempt timed out"),
            self._response(),
        ])
        with patch("ai.ai_engine.asyncio.sleep", new_callable=AsyncMock):
            response = await engine._post_with_retry(
                engine.api_path,
                json={"model": "test-model"},
            )

        self.assertEqual(response.status_code, 200)
        self.assertEqual(engine.client.post.await_count, 2)

    async def test_second_timeout_is_request_error_not_contract_violation(self) -> None:
        engine = self._engine([
            TimeoutError("first attempt timed out"),
            TimeoutError("second attempt timed out"),
        ])
        payload = {
            "ticker": "HYDR",
            "price": 0.53,
            "volume": 100,
            "candidate_direction": "SELL",
            "metrics": {
                "price_change_pct": -0.1,
                "price_vs_vwap_pct": -0.1,
                "volume_spike": 2.0,
                "spread_pct": 0.1,
                "spread_available": 1.0,
                "bid_ask_available": 1.0,
            },
        }
        with (
            patch.object(Settings, "VLLM_TIMEOUT", 1),
            patch("ai.ai_engine.asyncio.sleep", new_callable=AsyncMock),
        ):
            result = await engine.analyze_market_anomaly(payload)

        self.assertEqual(engine.client.post.await_count, 2)
        self.assertEqual(result["final_decision"], "HOLD")
        self.assertFalse(result["model_contract_violation"])
        self.assertTrue(result["request_error"])
        self.assertEqual(result["request_error_type"], "timeout")
        self.assertEqual(result["error_type"], "timeout")
        self.assertFalse(result["confidence_valid"])

    async def test_caller_cancellation_is_not_retried_or_swallowed(self) -> None:
        engine = self._engine([asyncio.CancelledError()])
        with self.assertRaises(asyncio.CancelledError):
            await engine._post_with_retry(
                engine.api_path,
                json={"model": "test-model"},
            )
        self.assertEqual(engine.client.post.await_count, 1)

    def test_effective_generation_budget_is_clamped_to_short_contract_range(self) -> None:
        with patch.object(Settings, "VLLM_MAX_TOKENS", 1280):
            self.assertEqual(AIEngine._effective_max_tokens(), 768)
        with patch.object(Settings, "VLLM_MAX_TOKENS", 512):
            self.assertEqual(AIEngine._effective_max_tokens(), 512)
        with patch.object(Settings, "VLLM_MAX_TOKENS", 128):
            self.assertEqual(AIEngine._effective_max_tokens(), 384)


if __name__ == "__main__":
    unittest.main()
