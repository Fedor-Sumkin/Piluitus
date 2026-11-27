import unittest

from strategy.bbo_replay_simulator import (
    _portfolio_capacity_simulation,
    replay_exit_batch,
    replay_exit_scenarios,
    configured_default_scenarios,
)


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
    # Keep the persisted trade id aligned with the readable test position id
    # unless a test explicitly supplies a separate canonical id.
    if "position_id" in overrides and "id" not in overrides:
        base["id"] = overrides["position_id"]
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
        self.assertEqual(len(result["scenarios"]), 7)

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
            event(4, bid=100.2, ask=100.3, epoch=1_800_000_003.2),
        ]
        first = position(position_id="P1")
        second = position(
            position_id="P2",
            entry_time_epoch=1_800_000_002.1,
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

    def test_portfolio_capacity_rejects_overlapping_entries_and_releases_capital(self):
        def trade(position_id, entry, exit_time, pnl):
            return {
                "position_id": position_id,
                "trade_index": int(position_id[-1]),
                "entry_time_epoch": entry,
                "entry_price": 100.0,
                "quantity": 10,
                "scenarios": [{
                    "name": "BASELINE",
                    "exit": {"event_time_epoch": exit_time, "net_pnl_rub": pnl},
                }],
            }

        rows = [
            trade("P1", 1.0, 3.0, 50.0),
            trade("P2", 2.0, 4.0, 20.0),
            trade("P3", 3.5, 5.0, -10.0),
        ]
        result = _portfolio_capacity_simulation(rows, "BASELINE", 10000.0, 15.0)
        self.assertEqual(result["accepted_positions"], 2)
        self.assertEqual(result["rejected_for_capacity"], 1)
        self.assertEqual(result["closed_positions"], 2)
        self.assertEqual(result["open_positions_at_end"], 0)
        self.assertAlmostEqual(result["realized_net_pnl_rub"], 40.0)
        self.assertAlmostEqual(result["peak_reserved_capital_rub"], 1000.0)
        self.assertAlmostEqual(result["peak_allocation_utilization_pct"], 100.0 * 1000.0 / 1500.0)

    def test_portfolio_capacity_keeps_open_position_reserved_at_end(self):
        rows = [{
            "position_id": "P1",
            "entry_time_epoch": 1.0,
            "entry_price": 100.0,
            "quantity": 10,
            "scenarios": [{"name": "BASELINE", "exit": None}],
        }]
        result = _portfolio_capacity_simulation(rows, "BASELINE", 10000.0, 15.0)
        self.assertEqual(result["accepted_positions"], 1)
        self.assertEqual(result["closed_positions"], 0)
        self.assertEqual(result["open_positions_at_end"], 1)
        self.assertEqual(result["ending_reserved_capital_rub"], 1000.0)
        self.assertEqual(result["ending_free_cash_rub"], 9000.0)
        self.assertIsNone(result["expectancy_per_closed_position_rub"])

    def test_portfolio_capacity_validates_capital_and_allocation(self):
        with self.assertRaises(ValueError):
            _portfolio_capacity_simulation([], "BASELINE", 0.0, 15.0)
        with self.assertRaises(ValueError):
            _portfolio_capacity_simulation([], "BASELINE", 10000.0, 0.0)



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
        invalid = position(id="invalid", position_id="invalid", stop_loss=100.0)
        valid = position(id="valid", position_id="valid")
        result = replay_exit_batch(
            rows, [invalid, valid],
            scenarios=[{"name": "base", "kind": "baseline"}],
        )
        self.assertEqual(result["positions"], 1)
        self.assertEqual(len(result["rejected_positions"]), 1)
        self.assertEqual(result["rejected_positions"][0]["position_id"], "invalid")
        self.assertEqual(result["trades"][0]["position_id"], "valid")


    def test_portfolio_mark_to_market_drawdown_includes_open_long_position(self):
        trade = {
            "position_id": "LONG-1", "session_id": "session-a", "ticker": "SBER",
            "side": "BUY", "entry_time_epoch": 1.0, "entry_price": 100.0,
            "quantity": 10,
            "cost_assumptions": {
                "commission_pct_per_side": 0.0, "slippage_bps": 0.0,
                "entry_commission_rub": 0.0,
            },
            "scenarios": [{"name": "BASELINE", "exit": None}],
        }
        rows = [
            event(1, bid=90.0, ask=90.1, epoch=2.0),
            event(2, bid=95.0, ask=95.1, epoch=3.0),
        ]
        result = _portfolio_capacity_simulation(
            [trade], "BASELINE", 10000.0, 15.0, events=rows, max_bbo_age_sec=2.0,
        )
        marked = result["mark_to_market"]
        self.assertEqual(result["open_positions_at_end"], 1)
        self.assertEqual(marked["complete_equity_snapshots"], 2)
        self.assertAlmostEqual(marked["max_drawdown_rub"], 100.0)
        self.assertAlmostEqual(marked["ending_equity_rub"], 9950.0)
        self.assertAlmostEqual(marked["ending_unrealized_net_pnl_rub"], -50.0)
        self.assertTrue(marked["ending_snapshot_complete"])

    def test_portfolio_mark_to_market_short_uses_ask_and_estimated_costs(self):
        trade = {
            "position_id": "SHORT-1", "session_id": "session-a", "ticker": "SBER",
            "side": "SELL", "entry_time_epoch": 1.0, "entry_price": 100.0,
            "quantity": 10,
            "cost_assumptions": {
                "commission_pct_per_side": 0.05, "slippage_bps": 10.0,
                "entry_commission_rub": 0.5,
            },
            "scenarios": [{"name": "BASELINE", "exit": None}],
        }
        rows = [event(1, bid=95.0, ask=105.0, epoch=2.0)]
        result = _portfolio_capacity_simulation(
            [trade], "BASELINE", 10000.0, 15.0, events=rows, max_bbo_age_sec=2.0,
        )
        marked = result["mark_to_market"]
        expected_exit_fill = 105.0 * 1.001
        expected_unrealized = (
            (expected_exit_fill - 100.0) * -10.0
            - 0.5 - expected_exit_fill * 10.0 * 0.05 / 100.0
        )
        self.assertAlmostEqual(marked["ending_unrealized_net_pnl_rub"], expected_unrealized)
        self.assertAlmostEqual(marked["ending_equity_rub"], 10000.0 + expected_unrealized)

    def test_portfolio_mark_to_market_rejects_stale_marks_for_ending_equity(self):
        trade = {
            "position_id": "LONG-1", "session_id": "session-a", "ticker": "SBER",
            "side": "BUY", "entry_time_epoch": 1.0, "entry_price": 100.0,
            "quantity": 10,
            "cost_assumptions": {
                "commission_pct_per_side": 0.0, "slippage_bps": 0.0,
                "entry_commission_rub": 0.0,
            },
            "scenarios": [{"name": "BASELINE", "exit": None}],
        }
        rows = [
            event(1, bid=99.0, ask=99.1, age=1.5, epoch=2.0),
            event(2, ticker="GAZP", bid=100.0, ask=100.1, epoch=3.0),
        ]
        result = _portfolio_capacity_simulation(
            [trade], "BASELINE", 10000.0, 15.0, events=rows, max_bbo_age_sec=2.0,
        )
        marked = result["mark_to_market"]
        self.assertEqual(marked["complete_equity_snapshots"], 1)
        self.assertGreater(marked["incomplete_equity_snapshots"], 0)
        self.assertIsNone(marked["ending_equity_rub"])
        self.assertFalse(marked["ending_snapshot_complete"])
        self.assertEqual(marked["max_positions_with_stale_mark"], 1)



    def test_batch_exposes_mark_to_market_when_portfolio_capacity_is_enabled(self):
        rows = [event(1, bid=100.5, ask=100.6, epoch=1_800_000_001.0)]
        result = replay_exit_batch(
            rows,
            [position(position_id="P1")],
            scenarios=[{"name": "BASELINE", "kind": "baseline"}],
            portfolio_capital_rub=10000.0,
            max_allocation_pct=15.0,
        )
        capacity = result["portfolio_capacity"]["BASELINE"]
        self.assertIn("mark_to_market", capacity)
        self.assertEqual(capacity["mark_to_market"]["complete_equity_snapshots"], 1)
        self.assertTrue(capacity["mark_to_market"]["ending_snapshot_complete"])
        expected_exit_fill = 100.5 * 0.999
        expected_equity = (
            10000.0 + (expected_exit_fill - 100.0) * 10.0
            - 0.5 - expected_exit_fill * 10.0 * 0.05 / 100.0
        )
        self.assertAlmostEqual(capacity["mark_to_market"]["ending_equity_rub"], expected_equity)


    def test_portfolio_mark_to_market_ignores_delayed_out_of_order_quote(self):
        trade = {
            "position_id": "LONG-1", "session_id": "session-a", "ticker": "SBER",
            "side": "BUY", "entry_time_epoch": 1.0, "entry_price": 100.0,
            "quantity": 10,
            "cost_assumptions": {
                "commission_pct_per_side": 0.0, "slippage_bps": 0.0,
                "entry_commission_rub": 0.0,
            },
            "scenarios": [{"name": "BASELINE", "exit": None}],
        }
        rows = [
            event(1, bid=100.0, ask=100.1, age=0.1, epoch=2.0),
            event(2, bid=90.0, ask=90.1, age=1.5, epoch=3.0),
        ]
        result = _portfolio_capacity_simulation(
            [trade], "BASELINE", 10000.0, 15.0, events=rows, max_bbo_age_sec=2.0,
        )
        marked = result["mark_to_market"]
        self.assertEqual(marked["out_of_order_bbo_mark_events"], 1)
        self.assertAlmostEqual(marked["ending_unrealized_net_pnl_rub"], 0.0)
        self.assertAlmostEqual(marked["ending_equity_rub"], 10000.0)



    def test_portfolio_capacity_processes_same_timestamp_exit_after_its_entry(self):
        trade = {
            "position_id": "P1", "entry_time_epoch": 1.0,
            "entry_price": 100.0, "quantity": 10,
            "scenarios": [{
                "name": "BASELINE",
                "exit": {"event_time_epoch": 1.0, "net_pnl_rub": -5.0},
            }],
        }
        result = _portfolio_capacity_simulation([trade], "BASELINE", 10000.0, 15.0)
        self.assertEqual(result["accepted_positions"], 1)
        self.assertEqual(result["closed_positions"], 1)
        self.assertEqual(result["open_positions_at_end"], 0)
        self.assertAlmostEqual(result["realized_net_pnl_rub"], -5.0)


def test_configured_default_scenarios_mirror_paper_exit_settings():
    class Settings:
        PAPER_BREAKEVEN_ENABLED = True
        PAPER_BREAKEVEN_ARM_R = 0.8
        PAPER_TRAILING_ENABLED = True
        PAPER_TRAILING_ARM_R = 1.75
        PAPER_TRAILING_DISTANCE_R = 0.6

    scenarios = configured_default_scenarios(Settings)
    be = next(row for row in scenarios if row.get("production_config_match") and row["kind"] == "break_even")
    trailing = next(row for row in scenarios if row.get("production_config_match") and row["kind"] == "trailing")
    assert be["arm_r"] == 0.8
    assert trailing["arm_r"] == 1.75
    assert trailing["gap_r"] == 0.6


def test_configured_scenarios_skip_disabled_production_exits():
    class Settings:
        PAPER_BREAKEVEN_ENABLED = False
        PAPER_TRAILING_ENABLED = False

    scenarios = configured_default_scenarios(Settings)
    assert not any(row.get("production_config_match") for row in scenarios)


if __name__ == "__main__":
    unittest.main()
