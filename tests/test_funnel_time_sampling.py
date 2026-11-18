"""Cross-sectional funnel price sampling must use fixed, fresh time windows."""
from __future__ import annotations

import unittest
from collections import deque

import numpy as np

from screener_funnel import HFTMemoryScreener


def make_screener(events: list[tuple[float, float]], *, now: float = 1000.0) -> HFTMemoryScreener:
    screener = HFTMemoryScreener(top_n=1, min_samples=3)
    screener.trade_data_mode = True
    screener.set_universe(["TEST"])
    history = screener.history["TEST"]
    history["prices"][:3] = np.array([100.0, 100.1, 100.2], dtype=np.float64)
    history["volumes"][:3] = np.array([10.0, 10.0, 10.0], dtype=np.float64)
    history["count"] = 3

    quote = screener.quotes["TEST"]
    quote["last_seen"] = now - 0.5
    quote["bid"] = 101.0
    quote["ask"] = 101.1
    quote["last_bid_confirmed_seen"] = now - 0.2
    quote["last_ask_confirmed_seen"] = now - 0.2
    quote["day_volume"] = 10000.0
    quote["tick_times"] = deque([now - 1.0, now - 0.5], maxlen=screener.window_size)
    quote["price_events"] = deque(events, maxlen=5000)
    return screener


class FunnelTimeSamplingTests(unittest.TestCase):
    def test_volatility_is_invariant_to_extra_intra_bucket_price_events(self) -> None:
        coarse = [
            (940.0 + index * 5.0, 100.0 + index * 0.1)
            for index in range(13)
        ]
        dense = []
        for index in range(12):
            start_time = 940.0 + index * 5.0
            start_price = 100.0 + index * 0.1
            for second in range(5):
                dense.append((
                    start_time + second,
                    start_price + (0.1 / 5.0) * second,
                ))
        dense.append((1000.0, 101.2))

        coarse_features = make_screener(coarse)._features_for("TEST", 1000.0)
        dense_features = make_screener(dense)._features_for("TEST", 1000.0)

        self.assertIsNotNone(coarse_features)
        self.assertIsNotNone(dense_features)
        self.assertAlmostEqual(
            coarse_features["volatility_pct"],
            dense_features["volatility_pct"],
            places=10,
        )
        self.assertAlmostEqual(
            coarse_features["momentum_pct"],
            dense_features["momentum_pct"],
            places=10,
        )
        self.assertGreater(coarse_features["volatility_pct"], 0.0)
        self.assertGreater(coarse_features["momentum_pct"], 0.0)

    def test_fresh_quote_updates_do_not_make_stale_last_price_fresh(self) -> None:
        events = [
            (940.0 + index * 5.0, 100.0 + index * 0.1)
            for index in range(9)
        ]
        screener = make_screener(events, now=1000.0)
        screener.price_event_max_age_sec = 10.0

        features = screener._features_for("TEST", 1000.0)

        self.assertIsNotNone(features)
        self.assertEqual(features["momentum_pct"], 0.0)
        self.assertEqual(features["volatility_pct"], 0.0)

    def test_sparse_30_second_reference_is_not_stretched_into_a_longer_window(self) -> None:
        events = [
            (940.0, 100.0),
            (955.0, 101.0),
            (970.0, 102.0),
            (985.0, 103.0),
            (1000.0, 104.0),
        ]
        screener = make_screener(events)
        features = screener._features_for("TEST", 1000.0)

        self.assertIsNotNone(features)
        self.assertEqual(features["momentum_pct"], 0.0)


if __name__ == "__main__":
    unittest.main()
