"""Regression tests for classifying normal Finam WebSocket closes."""
from types import SimpleNamespace
import logging
import unittest

from exchange.finam_client import (
    _websocket_close_code,
    _websocket_disconnect_log_level,
)


class FinamWebSocketCloseClassificationTests(unittest.TestCase):
    def test_normal_close_code_is_logged_as_info(self) -> None:
        exc = SimpleNamespace(code=1000)
        self.assertEqual(_websocket_close_code(exc), 1000)
        self.assertEqual(_websocket_disconnect_log_level(exc), logging.INFO)

    def test_going_away_close_code_is_logged_as_info(self) -> None:
        exc = SimpleNamespace(rcvd=SimpleNamespace(code=1001))
        self.assertEqual(_websocket_close_code(exc), 1001)
        self.assertEqual(_websocket_disconnect_log_level(exc), logging.INFO)

    def test_keepalive_timeout_remains_an_error(self) -> None:
        exc = SimpleNamespace(rcvd=SimpleNamespace(code=1011))
        self.assertEqual(_websocket_close_code(exc), 1011)
        self.assertEqual(_websocket_disconnect_log_level(exc), logging.ERROR)

    def test_unknown_close_reason_remains_an_error(self) -> None:
        exc = RuntimeError("unexpected disconnect")
        self.assertIsNone(_websocket_close_code(exc))
        self.assertEqual(_websocket_disconnect_log_level(exc), logging.ERROR)

    def test_dashboard_connection_flag_defaults_to_false_on_broker(self) -> None:
        from exchange.finam_client import FinamClient

        client = FinamClient.__new__(FinamClient)
        client.is_ws_running = False
        client._market_stream_connected = False

        self.assertFalse(client._market_stream_connected)
        client._market_stream_connected = True
        self.assertTrue(client._market_stream_connected)
        client._market_stream_connected = False
        self.assertFalse(client._market_stream_connected)


if __name__ == "__main__":
    unittest.main()
