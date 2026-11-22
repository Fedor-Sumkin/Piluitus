from __future__ import annotations

import unittest

from strategy import shadow_validation
from strategy.shadow_validation_cost_sensitivity import build_shadow_cost_sensitivity
from strategy.shadow_validation_metrics import _summarize
from strategy.shadow_validation_primitives import _finite
from strategy.shadow_walk_forward_validation import build_shadow_walk_forward_validation


class ShadowValidationModuleTests(unittest.TestCase):
    def test_facade_reexports_public_builders_from_focused_modules(self):
        self.assertIs(
            shadow_validation.build_shadow_walk_forward_validation,
            build_shadow_walk_forward_validation,
        )
        self.assertIs(
            shadow_validation.build_shadow_cost_sensitivity,
            build_shadow_cost_sensitivity,
        )

    def test_legacy_helper_names_remain_available_on_facade(self):
        self.assertIs(shadow_validation._finite, _finite)
        self.assertIs(shadow_validation._summarize, _summarize)

    def test_builders_keep_their_original_public_signatures(self):
        self.assertTrue(callable(build_shadow_walk_forward_validation))
        self.assertTrue(callable(build_shadow_cost_sensitivity))
        self.assertEqual(
            shadow_validation.__all__,
            ["build_shadow_walk_forward_validation", "build_shadow_cost_sensitivity"],
        )


if __name__ == "__main__":
    unittest.main()
