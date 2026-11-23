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

if __name__ == "__main__":
    unittest.main()
