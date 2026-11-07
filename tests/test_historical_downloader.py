"""Offline unit tests for the Finam historical export downloader."""
from __future__ import annotations

import unittest
from datetime import date

from tools.data.historical_downloader import (
    build_export_params,
    parse_export_catalog,
    parse_export_csv,
    split_date_range,
)


class HistoricalDownloaderTests(unittest.TestCase):
    def test_tick_export_is_parsed_as_individual_trades_without_invented_side(self) -> None:
        raw = (
            "<TICKER>;<PER>;<DATE>;<TIME>;<LAST>;<VOL>\n"
            "SBER;1;20261106;10:00:01;301.25;10\n"
            "SBER;1;20261106;10:00:03;301.30;5\n"
        )
        frame = parse_export_csv(raw, "ticks", "SBER")
        self.assertEqual(len(frame), 2)
        self.assertEqual(frame.iloc[0]["ticker"], "SBER")
        self.assertEqual(frame.iloc[0]["price"], 301.25)
        self.assertEqual(frame.iloc[0]["volume"], 10)
        self.assertEqual(frame.iloc[0]["datetime_msk"], "2026-11-06T10:00:01+03:00")
        self.assertNotIn("aggressor_side", frame.columns)
        self.assertNotIn("bid", frame.columns)
        self.assertNotIn("ask", frame.columns)

    def test_bar_export_is_normalized_to_ohlcv(self) -> None:
        raw = (
            "<TICKER>;<PER>;<DATE>;<TIME>;<OPEN>;<HIGH>;<LOW>;<CLOSE>;<VOL>\n"
            "GAZP;2;20261106;10:01:00;130.0;130.5;129.9;130.2;12000\n"
        )
        frame = parse_export_csv(raw, "m1", "GAZP")
        self.assertEqual(len(frame), 1)
        self.assertEqual(frame.iloc[0]["ticker"], "GAZP")
        self.assertEqual(frame.iloc[0]["open"], 130.0)
        self.assertEqual(frame.iloc[0]["high"], 130.5)
        self.assertEqual(frame.iloc[0]["low"], 129.9)
        self.assertEqual(frame.iloc[0]["close"], 130.2)
        self.assertEqual(frame.iloc[0]["volume"], 12000)

    def test_empty_tick_response_returns_header_compatible_empty_frame(self) -> None:
        frame = parse_export_csv("", "ticks", "SBER")
        self.assertTrue(frame.empty)
        self.assertEqual(
            list(frame.columns),
            ["ticker", "datetime_msk", "date", "time", "price", "volume", "period"],
        )

    def test_export_catalog_resolves_only_moex_share_contracts(self) -> None:
        script = """
        var aIds = [3, 8, 900];
        var aNames = ['Сбербанк', 'Газпром', 'ETF test'];
        var aCodes = ['SBER', 'GAZP', 'TESTETF'];
        var aMarkets = [1, 1, 515];
        """
        catalog = parse_export_catalog(script)
        self.assertEqual(catalog["SBER"]["id"], 3)
        self.assertEqual(catalog["GAZP"]["id"], 8)
        self.assertNotIn("TESTETF", catalog)

    def test_tick_export_parameters_use_finam_tick_format(self) -> None:
        instrument = {"id": 3, "code": "SBER", "market": 1}
        params = build_export_params(instrument, date(2026, 11, 6), date(2026, 11, 6), "ticks")
        self.assertEqual(params["p"], 1)
        self.assertEqual(params["datf"], 6)
        self.assertEqual(params["market"], 1)
        self.assertEqual(params["em"], 3)
        self.assertEqual(params["mstime"], "on")

    def test_bar_export_parameters_use_one_minute_periodicity(self) -> None:
        instrument = {"id": 3, "code": "SBER", "market": 1}
        params = build_export_params(instrument, date(2026, 11, 1), date(2026, 11, 6), "m1")
        self.assertEqual(params["p"], 2)
        self.assertEqual(params["datf"], 5)

    def test_tick_period_is_split_into_single_calendar_days(self) -> None:
        chunks = split_date_range(date(2026, 11, 1), date(2026, 11, 6), 1)
        self.assertEqual(len(chunks), 6)
        self.assertTrue(all(start == end for start, end in chunks))
        self.assertEqual(chunks[0], (date(2026, 11, 1), date(2026, 11, 1)))
        self.assertEqual(chunks[-1], (date(2026, 11, 6), date(2026, 11, 6)))

    def test_intraday_bar_chunks_never_exceed_four_months(self) -> None:
        chunks = split_date_range(date(2026, 1, 1), date(2026, 11, 6), 120)
        self.assertGreater(len(chunks), 1)
        self.assertTrue(all((end - start).days + 1 <= 120 for start, end in chunks))
        self.assertEqual(chunks[0][0], date(2026, 1, 1))
        self.assertEqual(chunks[-1][1], date(2026, 11, 6))

    def test_invalid_or_unknown_export_fields_fail_closed(self) -> None:
        with self.assertRaises(ValueError):
            split_date_range(date(2026, 11, 7), date(2026, 11, 6), 1)
        with self.assertRaisesRegex(Exception, "неизвестный формат"):
            parse_export_csv("unexpected;response\n1;2\n", "ticks", "SBER")


if __name__ == "__main__":
    unittest.main()
