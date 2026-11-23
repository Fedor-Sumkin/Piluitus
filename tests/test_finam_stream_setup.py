"""Tests for extracted Finam WebSocket setup helpers."""
from __future__ import annotations
import unittest
from exchange.finam_stream_setup import build_quote_batches, build_quote_subscription_plan, resolve_websocket_header_argument

class FinamStreamSetupTests(unittest.TestCase):
    def test_resolves_old_and_new_websockets_header_arguments(self):
        def old(uri, *, extra_headers=None): pass
        def new(uri, *, additional_headers=None): pass
        self.assertEqual(resolve_websocket_header_argument(old), "extra_headers")
        self.assertEqual(resolve_websocket_header_argument(new), "additional_headers")

    def test_rejects_unsupported_websockets_api(self):
        def unsupported(uri, *, origin=None): pass
        with self.assertRaisesRegex(RuntimeError, "не поддерживает дополнительные HTTP-заголовки"):
            resolve_websocket_header_argument(unsupported)

    def test_rejects_uninspectable_websockets_api(self):
        class Uninspectable:
            @property
            def __signature__(self): raise ValueError("unknown")
            def __call__(self, uri): pass
        with self.assertRaisesRegex(RuntimeError, "Не удалось определить API"):
            resolve_websocket_header_argument(Uninspectable())

    def test_normalizes_deduplicates_and_batches_symbols_in_order(self):
        symbols, size, batches = build_quote_subscription_plan(
            ["SBER", "SBER@MISX", "", "GAZP", "LKOH@MISX"], batch_size_value="2"
        )
        self.assertEqual(symbols, ["SBER@MISX", "GAZP@MISX", "LKOH@MISX"])
        self.assertEqual(size, 2)
        self.assertEqual(batches, [["SBER@MISX", "GAZP@MISX"], ["LKOH@MISX"]])

    def test_rejects_empty_universe_and_invalid_batch_size(self):
        with self.assertRaisesRegex(ValueError, "Список TICKERS_TO_TRADE пуст"):
            build_quote_subscription_plan(["", " "])
        with self.assertRaisesRegex(ValueError, "должен быть целым числом"):
            build_quote_subscription_plan(["SBER"], batch_size_value="invalid")
        with self.assertRaisesRegex(ValueError, "не меньше 1"):
            build_quote_subscription_plan(["SBER"], batch_size_value="0")
        with self.assertRaises(ValueError):
            build_quote_batches(["SBER"], 0)


    def test_wait_for_handshake_ignores_non_json_and_unrelated_events(self):
        import asyncio
        import json
        from unittest.mock import Mock
        from exchange.finam_stream_session import wait_for_finam_handshake

        class Socket:
            def __init__(self):
                self.messages = [
                    "not-json",
                    json.dumps({"type": "EVENT", "event_info": {"event": "CONNECTED"}}),
                    json.dumps({"type": "EVENT", "event_info": {"event": "HANDSHAKE_SUCCESS"}}),
                ]
            async def recv(self):
                return self.messages.pop(0)

        logger = Mock()
        asyncio.run(wait_for_finam_handshake(Socket(), logger))
        logger.info.assert_called()

    def test_wait_for_handshake_rejects_provider_error_and_disconnect(self):
        import asyncio
        import json
        from unittest.mock import Mock
        from exchange.finam_stream_session import wait_for_finam_handshake

        class Socket:
            def __init__(self, event):
                self.message = json.dumps(event)
            async def recv(self):
                return self.message

        with self.assertRaisesRegex(RuntimeError, "отклонил WebSocket handshake"):
            asyncio.run(wait_for_finam_handshake(
                Socket({"type": "ERROR", "error_info": "denied"}), Mock()
            ))
        with self.assertRaisesRegex(RuntimeError, "закрыл соединение во время handshake"):
            asyncio.run(wait_for_finam_handshake(
                Socket({"type": "EVENT", "event_info": {"event": "CONNECTION_CLOSED"}}), Mock()
            ))

    def test_quote_subscription_helper_sends_batches_in_order(self):
        import asyncio
        import json
        from unittest.mock import Mock, patch
        from exchange.finam_stream_session import send_quote_subscription_batches

        class Socket:
            def __init__(self):
                self.sent = []
            async def send(self, message):
                self.sent.append(json.loads(message))

        socket = Socket()
        with patch("exchange.finam_stream_session.asyncio.sleep", new=lambda _: asyncio.sleep(0)):
            asyncio.run(send_quote_subscription_batches(
                socket, "jwt", [["SBER@MISX", "GAZP@MISX"], ["LKOH@MISX"]], Mock(),
                inter_batch_delay_sec=0,
            ))
        self.assertEqual([item["data"]["symbols"] for item in socket.sent], [
            ["SBER@MISX", "GAZP@MISX"], ["LKOH@MISX"]
        ])

if __name__ == "__main__":
    unittest.main()
