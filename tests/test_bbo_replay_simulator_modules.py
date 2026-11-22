"""Architecture checks for the split BBO replay simulator."""
from __future__ import annotations

import unittest

from strategy import bbo_replay_simulator as facade
from strategy import bbo_replay_simulator_analysis as analysis
from strategy import bbo_replay_simulator_batch as batch
from strategy import bbo_replay_simulator_exit as exit_sim
from strategy import bbo_replay_simulator_portfolio as portfolio
from strategy import bbo_replay_simulator_primitives as primitives


class BBOReplaySimulatorModuleTests(unittest.TestCase):
    def test_legacy_facade_reexports_the_original_callable_surface(self):
        self.assertIs(facade.replay_exit_scenarios, exit_sim.replay_exit_scenarios)
        self.assertIs(facade.replay_exit_batch, batch.replay_exit_batch)
        self.assertIs(
            facade._portfolio_capacity_simulation,
            portfolio._portfolio_capacity_simulation,
        )
        self.assertIs(facade.DEFAULT_SCENARIOS, primitives.DEFAULT_SCENARIOS)

    def test_numeric_and_event_helpers_remain_available_from_facade(self):
        for name in (
            "_number",
            "_epoch",
            "_observation_epoch",
            "_fresh_side",
            "_adverse_exit_fill",
            "_net_pnl",
            "_break_even_trigger",
            "_prepare_events",
        ):
            with self.subTest(name=name):
                self.assertIs(getattr(facade, name), getattr(primitives, name))

    def test_analysis_helpers_remain_available_from_facade(self):
        self.assertIs(facade._sample_quality, analysis._sample_quality)
        self.assertIs(
            facade._summarize_replay_trades,
            analysis._summarize_replay_trades,
        )
        self.assertIs(
            facade._paired_scenario_comparison,
            analysis._paired_scenario_comparison,
        )

    def test_implementation_ownership_is_separated_by_responsibility(self):
        self.assertEqual(
            exit_sim.replay_exit_scenarios.__module__,
            "strategy.bbo_replay_simulator_exit",
        )
        self.assertEqual(
            batch.replay_exit_batch.__module__,
            "strategy.bbo_replay_simulator_batch",
        )
        self.assertEqual(
            analysis._paired_scenario_comparison.__module__,
            "strategy.bbo_replay_simulator_analysis",
        )
        self.assertEqual(
            portfolio._portfolio_capacity_simulation.__module__,
            "strategy.bbo_replay_simulator_portfolio",
        )


if __name__ == "__main__":
    unittest.main()
