from __future__ import annotations

import unittest

from strategy.signal_diagnostics import classify_guard_reason


class SignalDiagnosticReasonTests(unittest.TestCase):
    def test_classifies_cost_and_spread_separately(self) -> None:
        self.assertEqual(
            classify_guard_reason("PAPER round-trip издержки: cost/ATR=0.72 > 0.50"),
            "cost",
        )
        self.assertEqual(
            classify_guard_reason("Spread/ATR превышает лимит: ratio=0.80"),
            "spread",
        )

    def test_classifies_queue_flow_conflicts_separately_from_other(self) -> None:
        self.assertEqual(
            classify_guard_reason(
                "Устойчивый дисбаланс очереди 5s противоречит агрессивному потоку сделок 30s"
            ),
            "queue_flow",
        )

    def test_preserves_quotes_bucket_for_retryable_bbo_reasons(self) -> None:
        self.assertEqual(classify_guard_reason("Нет актуальных Bid/Ask"), "quotes")
        self.assertEqual(classify_guard_reason("ORDER_BOOK не подтверждён"), "quotes")

    def test_classifies_atr_volume_direction_and_risk_reward(self) -> None:
        self.assertEqual(classify_guard_reason("Недостаточно истории ATR: 2/5"), "atr")
        self.assertEqual(classify_guard_reason("Объём сделок недостаточен"), "volume")
        self.assertEqual(classify_guard_reason("Нет направленного импульса цены"), "direction")
        self.assertEqual(classify_guard_reason("Risk/Reward ниже минимума"), "risk_reward")

    def test_unknown_reasons_remain_in_other(self) -> None:
        self.assertEqual(classify_guard_reason("unclassified condition"), "other")


if __name__ == "__main__":
    unittest.main()
