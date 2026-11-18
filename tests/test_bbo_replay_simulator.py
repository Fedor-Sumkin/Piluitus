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
        "id": "PAPER-TEST",
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
        self.assertEqual(result["engine"], "PAPER_EXIT_REPLAY_V1")

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
        net_be_bid = ((100.0 + 0.5 / 10.0) / (1.0 - 0.05 / 100.0)) / (1.0 - 10.0 / 10_000.0)
        rows = [
            event(1, bid=101.0, ask=101.1, epoch=1_800_000_001.0),
            event(2, bid=net_be_bid, ask=net_be_bid + 0.1, epoch=1_800_000_002.0),
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
        self.assertEqual(result["coverage"]["sequence_gaps"], 3)
        self.assertEqual(result["coverage"]["missing_sequence_prefix"], 2)
        self.assertEqual(result["coverage"]["first_sequence"], 3)
        self.assertEqual(result["coverage"]["last_sequence"], 5)
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
                "event_kind": "EXIT_TRIGGER", "trade_id": "PAPER-TEST", "event_time_epoch": 1_800_000_002.0,
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
                "event_kind": "EXIT_TRIGGER", "trade_id": "PAPER-TEST", "event_time_epoch": 1_800_000_001.0,
                "trigger_reason": "TAKE_PROFIT", "trigger_price": 102.0,
                "trigger_source": "LAST_TRADE",
                "trigger_market_time_epoch": 1_800_000_000.9,
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
        self.assertEqual(scenario["exit"]["trigger_event_sequence"], 1)
        self.assertEqual(scenario["exit"]["trigger_source"], "LAST_TRADE")
        self.assertEqual(scenario["exit"]["trigger_market_time_epoch"], 1_800_000_000.9)
        self.assertEqual(scenario["exit"]["raw_executable_price"], 101.8)

    def test_stale_trigger_time_bbo_waits_for_next_fresh_side(self):
        rows = [
            {
                "session_id": "session-a", "sequence": 1, "ticker": "SBER",
                "event_kind": "EXIT_TRIGGER", "trade_id": "PAPER-TEST", "event_time_epoch": 1_800_000_001.0,
                "trigger_reason": "STOP_LOSS", "trigger_price": 99.0,
                "trigger_bbo_valid": True, "trigger_bbo_price": 98.0,
                "trigger_bbo_age_sec": 3.0, "trigger_bbo_max_age_sec": 2.0,
            },
            event(2, bid=98.7, ask=98.8, epoch=1_800_000_003.0),
        ]
        result = replay_exit_scenarios(
            rows, position(),
            scenarios=[{"name": "baseline", "kind": "baseline"}],
        )
        exit_row = result["scenarios"][0]["exit"]
        self.assertEqual(exit_row["event_sequence"], 2)
        self.assertEqual(exit_row["raw_executable_price"], 98.7)
        self.assertEqual(result["scenarios"][0]["trigger_mode"], "ACTUAL_LATCHED_TRIGGER")

    def test_sell_replay_uses_fresh_ask_even_when_bid_is_stale(self):
        row = event(1, bid=101.0, ask=101.1, epoch=1_800_000_001.0)
        row["bid_age_sec_at_capture"] = 4.0
        row["ask_age_sec_at_capture"] = 0.1
        result = replay_exit_scenarios(
            [row], position("SELL"),
            scenarios=[{"name": "baseline", "kind": "baseline"}],
        )
        self.assertEqual(result["scenarios"][0]["exit"]["raw_executable_price"], 101.1)
        self.assertEqual(result["coverage"]["fresh_executable_observations"], 1)

    def test_research_variants_ignore_production_trigger_events(self):
        rows = [{
            "session_id": "session-a", "sequence": 1, "ticker": "SBER",
            "event_kind": "EXIT_TRIGGER", "trade_id": "PAPER-TEST", "event_time_epoch": 1_800_000_001.0,
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

    def test_pre_entry_quotes_are_excluded_from_exit_decisions(self):
        rows = [
            event(1, bid=98.0, ask=98.1, epoch=1_799_999_999.0),
            event(2, bid=100.5, ask=100.6, epoch=1_800_000_001.0),
        ]
        result = replay_exit_scenarios(
            rows, position(),
            scenarios=[{"name": "baseline", "kind": "baseline"}],
        )
        self.assertEqual(result["coverage"]["pre_entry_events"], 1)
        self.assertEqual(result["coverage"]["ticker_events"], 1)
        self.assertEqual(result["scenarios"][0]["status"], "OPEN_AT_END")

    def test_entry_filter_and_holding_time_prefer_local_receive_clock(self):
        row = event(1, bid=98.8, ask=98.9, epoch=1_799_999_990.0)
        row["received_at_utc"] = "2027-01-15T08:00:01+00:00"
        result = replay_exit_scenarios(
            [row], position(),
            scenarios=[{"name": "baseline", "kind": "baseline"}],
        )
        exit_row = result["scenarios"][0]["exit"]
        self.assertEqual(exit_row["reason"], "STOP_LOSS")
        self.assertEqual(exit_row["elapsed_sec"], 1.0)
        self.assertEqual(result["coverage"]["pre_entry_events"], 0)

    def test_exit_trigger_from_another_trade_on_same_ticker_is_ignored(self):
        rows = [{
            "session_id": "session-a", "sequence": 1, "ticker": "SBER",
            "event_kind": "EXIT_TRIGGER", "trade_id": "ANOTHER-PAPER-TRADE",
            "event_time_epoch": 1_800_000_001.0,
            "trigger_reason": "STOP_LOSS", "trigger_price": 99.0,
            "trigger_bbo_valid": True, "trigger_bbo_price": 98.0,
            "trigger_bbo_age_sec": 0.1, "trigger_bbo_max_age_sec": 2.0,
        }, event(2, bid=100.5, ask=100.6, epoch=1_800_000_002.0)]
        result = replay_exit_scenarios(
            rows, position(),
            scenarios=[{"name": "baseline", "kind": "baseline"}],
        )
        self.assertEqual(result["coverage"]["unmatched_exit_trigger_events"], 1)
        self.assertEqual(result["scenarios"][0]["trigger_mode"], "BBO_THRESHOLD_PROXY")
        self.assertEqual(result["scenarios"][0]["status"], "OPEN_AT_END")

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



    def test_batch_reports_paired_comparison_and_separate_cohort_attrition(self):
        rows = [
            event(1, bid=101.0, ask=101.1, epoch=1_800_000_001.0),
            event(2, bid=102.1, ask=102.2, epoch=1_800_000_002.0),
            event(3, bid=100.2, ask=100.3, epoch=1_800_000_003.0),
        ]
        first = position(position_id="P1")
        second = position(
            position_id="P2",
            entry_time_epoch=1_800_000_002.5,
        )
        result = replay_exit_batch(
            rows,
            [first, second],
            scenarios=[
                {"name": "BASELINE", "kind": "baseline"},
                {"name": "TIME_1S", "kind": "time_stop", "time_stop_sec": 1.0},
            ],
        )
        comparison = result["matched_cohort_comparison"]
        self.assertTrue(comparison["available"])
        paired = comparison["comparisons_vs_baseline"]["TIME_1S"]
        self.assertEqual(paired["cohort"]["paired_closed"], 1)
        self.assertEqual(paired["cohort"]["baseline_closed"], 1)
        self.assertEqual(paired["cohort"]["alternative_closed"], 2)
        self.assertEqual(paired["cohort"]["alternative_closed_baseline_open"], 1)
        self.assertEqual(paired["paired_difference"]["alternative_better"], 0)
        self.assertEqual(paired["paired_difference"]["alternative_worse"], 1)
        self.assertEqual(
            comparison["all_scenarios_common_cohort"]["positions_closed_in_every_scenario"],
            1,
        )
        self.assertEqual(len(paired["paired_trades"]), 1)

    def test_batch_matched_comparison_does_not_treat_open_as_zero_pnl(self):
        rows = [event(1, bid=100.2, ask=100.3, epoch=1_800_000_001.0)]
        result = replay_exit_batch(
            rows,
            [position(position_id="P1")],
            scenarios=[
                {"name": "BASELINE", "kind": "baseline"},
                {"name": "TIME_1S", "kind": "time_stop", "time_stop_sec": 1.0},
            ],
        )
        paired = result["matched_cohort_comparison"]["comparisons_vs_baseline"]["TIME_1S"]
        self.assertEqual(paired["cohort"]["paired_closed"], 0)
        self.assertIsNone(paired["paired_difference"]["mean_delta_net_pnl_rub"])
        self.assertEqual(paired["cohort"]["sample_quality"], "INSUFFICIENT_LT_30")

if __name__ == "__main__":
    unittest.main()


    def test_batch_replay_indexes_events_by_session_and_ticker(self):
        rows = [
            event(1, bid=98.8, ask=98.9, epoch=1_800_000_001.0),
            event(2, ticker="GAZP", bid=98.7, ask=98.8, epoch=1_800_000_001.0),
            event(3, bid=102.1, ask=102.2, epoch=1_800_000_001.0, session="other-session"),
        ]
        positions = [
            position(position_id="SBER-1"),
            position(ticker="GAZP", position_id="GAZP-1"),
            position(position_id="SBER-OTHER", session_id="other-session"),
        ]
        result = replay_exit_batch(
            rows, positions,
            scenarios=[{"name": "base", "kind": "baseline"}],
        )
        self.assertEqual(result["positions"], 3)
        by_id = {row["position_id"]: row for row in result["trades"]}
        self.assertEqual(by_id["SBER-1"]["scenarios"][0]["exit"]["event_sequence"], 1)
        self.assertEqual(by_id["GAZP-1"]["scenarios"][0]["exit"]["event_sequence"], 2)
        self.assertEqual(by_id["SBER-OTHER"]["scenarios"][0]["exit"]["event_sequence"], 3)
        self.assertEqual(by_id["SBER-1"]["coverage"]["session_events"], 2)
        self.assertEqual(by_id["SBER-1"]["coverage"]["ticker_events"], 1)


    def test_batch_replay_reports_invalid_positions_without_aborting_valid_ones(self):
        rows = [event(1, bid=98.8, ask=98.9, epoch=1_800_000_001.0)]
        invalid = position(position_id="invalid", stop_loss=100.0)
        valid = position(position_id="valid")
        result = replay_exit_batch(
            rows, [invalid, valid],
            scenarios=[{"name": "base", "kind": "baseline"}],
        )
        self.assertEqual(result["positions"], 1)
        self.assertEqual(len(result["rejected_positions"]), 1)
        self.assertEqual(result["rejected_positions"][0]["position_id"], "invalid")
        self.assertEqual(result["trades"][0]["position_id"], "valid")
