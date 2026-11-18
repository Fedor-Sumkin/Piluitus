import unittest

from strategy.bbo_replay_simulator import replay_exit_batch, replay_exit_scenarios


def event(sequence, ticker="SBER", *, bid=100.0, ask=100.1, age=0.1,
          epoch=1_800_000_000.0, valid=True, session="session-a"):
    return {
        "session_id": session,
        "sequence": sequence,
        "ticker": ticker,
        "event_time_epoch": epoch,
        "received_monotonic": float(sequence),
        "bid_confirmed_monotonic": float(sequence) - age,
        "ask_confirmed_monotonic": float(sequence) - age,
        "bid_age_sec_at_capture": age,
        "ask_age_sec_at_capture": age,
        "bid": bid,
        "ask": ask,
        "bbo_valid": valid,
    }


def position(side="BUY", **overrides):
    base = {
        "session_id": "session-a",
        "ticker": "SBER",
        "side": side,
        "entry_price": 100.0,
        "entry_time_epoch": 1_800_000_000.0,
        "quantity": 10,
        "stop_loss": 99.0 if side == "BUY" else 101.0,
        "take_profit": 102.0 if side == "BUY" else 98.0,
        "commission_pct_per_side": 0.05,
        "slippage_bps": 10.0,
        "entry_commission_rub": 0.5,
    }
    base.update(overrides)
    return base


class BBOReplaySimulatorTests(unittest.TestCase):
    def test_buy_baseline_exits_on_bid_and_applies_adverse_costs(self):
        rows = [
            event(2, bid=98.9, ask=99.0, epoch=1_800_000_003.0),
            event(1, bid=99.5, ask=99.6, epoch=1_800_000_001.0),
        ]
        result = replay_exit_scenarios(
            rows, position(),
            scenarios=[{"name": "baseline", "kind": "baseline"}],
        )
        exit_row = result["scenarios"][0]["exit"]
        self.assertEqual(exit_row["reason"], "STOP_LOSS")
        self.assertEqual(exit_row["event_sequence"], 2)
        self.assertEqual(exit_row["raw_executable_price"], 98.9)
        self.assertAlmostEqual(exit_row["fill_price_after_slippage"], 98.8011)
        self.assertLess(exit_row["net_pnl_rub"], exit_row["gross_pnl_rub"])
        self.assertEqual(result["engine"], "BBO_ONLY_EXIT_PROXY")

    def test_sell_baseline_uses_ask_to_exit_short(self):
        rows = [event(1, bid=101.0, ask=101.1, epoch=1_800_000_001.0)]
        result = replay_exit_scenarios(
            rows, position("SELL"),
            scenarios=[{"name": "baseline", "kind": "baseline"}],
        )
        exit_row = result["scenarios"][0]["exit"]
        self.assertEqual(exit_row["reason"], "STOP_LOSS")
        self.assertEqual(exit_row["raw_executable_price"], 101.1)
        self.assertGreater(exit_row["fill_price_after_slippage"], 101.1)

    def test_stale_quotes_are_skipped_until_fresh_executable_side_returns(self):
        rows = [
            event(1, bid=98.0, ask=98.1, age=2.5, epoch=1_800_000_001.0),
            event(2, bid=98.8, ask=98.9, age=0.2, epoch=1_800_000_002.0),
        ]
        result = replay_exit_scenarios(
            rows, position(),
            scenarios=[{"name": "baseline", "kind": "baseline"}],
        )
        scenario = result["scenarios"][0]
        self.assertEqual(scenario["exit"]["event_sequence"], 2)
        self.assertEqual(scenario["stale_observations"], 1)
        self.assertEqual(result["coverage"]["stale_events"], 1)

    def test_break_even_arms_at_one_r_and_exits_at_net_break_even_threshold(self):
        rows = [
            event(1, bid=101.0, ask=101.1, epoch=1_800_000_001.0),
            event(2, bid=100.1, ask=100.2, epoch=1_800_000_002.0),
        ]
        result = replay_exit_scenarios(
            rows, position(),
            scenarios=[{"name": "be", "kind": "break_even", "arm_r": 1.0}],
        )
        scenario = result["scenarios"][0]
        self.assertTrue(scenario["armed"])
        self.assertEqual(scenario["exit"]["reason"], "BREAK_EVEN")
        self.assertAlmostEqual(scenario["exit"]["net_pnl_rub"], 0.0, places=3)

    def test_trailing_stop_arms_then_tracks_favorable_extreme(self):
        rows = [
            event(1, bid=101.0, ask=101.1, epoch=1_800_000_001.0),
            event(2, bid=102.0, ask=102.1, epoch=1_800_000_002.0),
            event(3, bid=101.2, ask=101.3, epoch=1_800_000_003.0),
        ]
        result = replay_exit_scenarios(
            rows, position(),
            scenarios=[{"name": "trail", "kind": "trailing", "arm_r": 1.0, "gap_r": 0.75}],
        )
        scenario = result["scenarios"][0]
        self.assertTrue(scenario["armed"])
        self.assertEqual(scenario["exit"]["reason"], "TRAILING_STOP")
        self.assertAlmostEqual(scenario["exit"]["trigger_price"], 101.25)

    def test_time_stop_waits_for_first_fresh_quote_after_timeout(self):
        rows = [
            event(1, bid=100.5, ask=100.6, epoch=1_800_000_100.0),
            event(2, bid=100.4, ask=100.5, age=3.0, epoch=1_800_000_181.0),
            event(3, bid=100.3, ask=100.4, epoch=1_800_000_182.0),
        ]
        result = replay_exit_scenarios(
            rows, position(),
            scenarios=[{"name": "time", "kind": "time_stop", "time_stop_sec": 180.0}],
        )
        exit_row = result["scenarios"][0]["exit"]
        self.assertEqual(exit_row["reason"], "TIME_STOP")
        self.assertEqual(exit_row["event_sequence"], 3)
        self.assertEqual(exit_row["elapsed_sec"], 182.0)

    def test_session_filter_global_sequence_gaps_and_duplicate_handling(self):
        rows = [
            event(5, bid=102.1, ask=102.2, epoch=1_800_000_005.0),
            event(3, ticker="VTBR", epoch=1_800_000_003.0),
            event(5, bid=101.0, ask=101.1, epoch=1_800_000_005.0),
            event(7, bid=100.5, ask=100.6, epoch=1_800_000_007.0, session="other"),
        ]
        result = replay_exit_scenarios(
            rows, position(),
            scenarios=[{"name": "baseline", "kind": "baseline"}],
        )
        self.assertEqual(result["coverage"]["session_events"], 3)
        self.assertEqual(result["coverage"]["duplicate_sequences"], 1)
        self.assertEqual(result["coverage"]["sequence_gaps"], 1)
        self.assertEqual(result["coverage"]["ticker_events"], 1)
        self.assertEqual(result["scenarios"][0]["exit"]["reason"], "TAKE_PROFIT")

    def test_coverage_is_not_multiplied_by_number_of_scenarios(self):
        rows = [
            event(1, bid=100.2, ask=100.3, epoch=1_800_000_001.0),
            event(2, bid=99.0, ask=99.1, age=2.1, epoch=1_800_000_002.0),
        ]
        result = replay_exit_scenarios(rows, position())
        self.assertEqual(result["coverage"]["fresh_executable_observations"], 1)
        self.assertEqual(result["coverage"]["stale_events"], 1)
        self.assertEqual(len(result["scenarios"]), 5)

    def test_actual_latched_trigger_fills_at_trigger_bbo_when_fresh(self):
        rows = [
            event(1, bid=100.0, ask=100.1, epoch=1_800_000_001.0),
            {
                "session_id": "session-a", "sequence": 2, "ticker": "SBER",
                "event_kind": "EXIT_TRIGGER", "event_time_epoch": 1_800_000_002.0,
                "trigger_reason": "STOP_LOSS", "trigger_price": 99.0,
                "trigger_bbo_valid": True, "trigger_bbo_price": 98.8,
                "trigger_bbo_age_sec": 0.2, "trigger_bbo_max_age_sec": 2.0,
            },
        ]
        result = replay_exit_scenarios(
            rows, position(),
            scenarios=[{"name": "baseline", "kind": "baseline"}],
        )
        scenario = result["scenarios"][0]
        self.assertEqual(scenario["trigger_mode"], "ACTUAL_LATCHED_TRIGGER")
        self.assertEqual(scenario["exit"]["reason"], "STOP_LOSS")
        self.assertEqual(scenario["exit"]["event_sequence"], 2)
        self.assertEqual(scenario["exit"]["raw_executable_price"], 98.8)

    def test_actual_trigger_waits_for_next_fresh_executable_quote(self):
        rows = [
            {
                "session_id": "session-a", "sequence": 1, "ticker": "SBER",
                "event_kind": "EXIT_TRIGGER", "event_time_epoch": 1_800_000_001.0,
                "trigger_reason": "TAKE_PROFIT", "trigger_price": 102.0,
                "trigger_bbo_valid": False,
            },
            event(2, bid=101.8, ask=101.9, epoch=1_800_000_003.0),
        ]
        result = replay_exit_scenarios(
            rows, position(),
            scenarios=[{"name": "baseline", "kind": "baseline"}],
        )
        scenario = result["scenarios"][0]
        self.assertEqual(scenario["trigger_mode"], "ACTUAL_LATCHED_TRIGGER")
        self.assertEqual(scenario["exit"]["reason"], "TAKE_PROFIT")
        self.assertEqual(scenario["exit"]["event_sequence"], 2)
        self.assertEqual(scenario["exit"]["raw_executable_price"], 101.8)

    def test_research_variants_ignore_production_trigger_events(self):
        rows = [{
            "session_id": "session-a", "sequence": 1, "ticker": "SBER",
            "event_kind": "EXIT_TRIGGER", "event_time_epoch": 1_800_000_001.0,
            "trigger_reason": "STOP_LOSS", "trigger_price": 99.0,
            "trigger_bbo_valid": True, "trigger_bbo_price": 98.0,
            "trigger_bbo_age_sec": 0.1, "trigger_bbo_max_age_sec": 2.0,
        }, event(2, bid=100.5, ask=100.6, epoch=1_800_000_002.0)]
        result = replay_exit_scenarios(
            rows, position(),
            scenarios=[{"name": "be", "kind": "break_even", "arm_r": 1.0}],
        )
        self.assertEqual(result["scenarios"][0]["status"], "OPEN_AT_END")
        self.assertEqual(result["scenarios"][0]["trigger_mode"], "BBO_THRESHOLD_RESEARCH")

    def test_batch_replay_summarizes_net_expectancy_and_sample_quality(self):
        rows = [
            event(1, bid=102.1, ask=102.2, epoch=1_800_000_001.0),
            event(2, ticker="GAZP", bid=98.8, ask=98.9, epoch=1_800_000_002.0),
        ]
        positions = [
            position(position_id="P1"),
            position(ticker="GAZP", position_id="P2"),
        ]
        result = replay_exit_batch(
            rows, positions,
            scenarios=[{"name": "base", "kind": "baseline"}],
        )
        summary = result["summary_by_scenario"]["base"]["overall"]
        self.assertEqual(result["positions"], 2)
        self.assertEqual(summary["closed"], 2)
        self.assertEqual(summary["sample_quality"], "INSUFFICIENT_LT_30")
        self.assertEqual(summary["wins"], 1)
        self.assertEqual(summary["losses"], 1)
        self.assertIsNotNone(summary["profit_factor"])
        self.assertEqual(set(result["summary_by_scenario"]["base"]["by_ticker"]), {"SBER", "GAZP"})

    def test_batch_replay_accepts_scenario_generators_for_multiple_positions(self):
        rows = [
            event(1, bid=102.1, ask=102.2, epoch=1_800_000_001.0),
            event(2, ticker="GAZP", bid=102.1, ask=102.2, epoch=1_800_000_002.0),
        ]
        scenarios = (item for item in [{"name": "only", "kind": "baseline"}])
        result = replay_exit_batch(
            rows,
            [position(position_id="one"), position(ticker="GAZP", position_id="two")],
            scenarios=scenarios,
        )
        self.assertEqual(result["summary_by_scenario"]["only"]["overall"]["closed"], 2)

    def test_persisted_trade_shape_accepts_naive_legacy_entry_timestamp(self):
        trade = position()
        trade.pop("entry_time_epoch")
        trade["entry_timestamp"] = "2026-11-18T10:00:00"
        trade["volume"] = trade.pop("quantity")
        trade["paper_slippage_bps"] = trade.pop("slippage_bps")
        trade["bbo_replay_session_id"] = trade.pop("session_id")
        result = replay_exit_scenarios(
            [], trade, scenarios=[{"name": "base", "kind": "baseline"}]
        )
        self.assertEqual(result["session_id"], "session-a")
        self.assertGreater(result["entry_time_epoch"], 0.0)

    def test_invalid_position_contract_is_rejected(self):
        with self.assertRaises(ValueError):
            replay_exit_scenarios([], position(stop_loss=100.0))

    def test_no_exit_is_reported_as_open_not_as_zero_profit(self):
        rows = [event(1, bid=100.2, ask=100.3, epoch=1_800_000_001.0)]
        result = replay_exit_scenarios(
            rows, position(),
            scenarios=[{"name": "baseline", "kind": "baseline"}],
        )
        self.assertEqual(result["scenarios"][0]["status"], "OPEN_AT_END")
        self.assertIsNone(result["scenarios"][0]["exit"])


if __name__ == "__main__":
    unittest.main()
