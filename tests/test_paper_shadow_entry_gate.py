from __future__ import annotations

import os
import unittest
from unittest.mock import patch

from signal_guard import validate_paper_shadow_entry


class PaperShadowEntryGateTests(unittest.TestCase):
    def test_live_path_is_never_changed(self) -> None:
        ok, _ = validate_paper_shadow_entry(
            {"eligible": False, "score": 10, "hard_blocks": ["bbo_stale"]},
            paper_trading=False,
        )
        self.assertTrue(ok)

    def test_paper_requires_eligible_candidate_by_default(self) -> None:
        with patch.dict(os.environ, {"PILUITUS_PAPER_SHADOW_ENTRY_GATE": "1"}):
            ok, reason = validate_paper_shadow_entry(
                {"eligible": False, "score": 84, "threshold": 85, "hard_blocks": []},
                paper_trading=True,
            )
        self.assertFalse(ok)
        self.assertIn("84.0/85.0", reason)

    def test_paper_accepts_eligible_candidate(self) -> None:
        ok, _ = validate_paper_shadow_entry(
            {"eligible": True, "score": 87, "threshold": 85, "hard_blocks": []},
            paper_trading=True,
            enabled=True,
        )
        self.assertTrue(ok)

    def test_hard_blocks_reject_even_if_eligible_flag_is_true(self) -> None:
        ok, _ = validate_paper_shadow_entry(
            {"eligible": True, "score": 90, "threshold": 85, "hard_blocks": ["bbo_stale"]},
            paper_trading=True,
            enabled=True,
        )
        self.assertFalse(ok)

    def test_explicit_disable_preserves_baseline_paper_path(self) -> None:
        with patch.dict(os.environ, {"PILUITUS_PAPER_SHADOW_ENTRY_GATE": "0"}):
            ok, _ = validate_paper_shadow_entry(
                {"eligible": False, "score": 1, "hard_blocks": ["bbo_stale"]},
                paper_trading=True,
            )
        self.assertTrue(ok)

    def test_missing_shadow_result_fails_closed_in_paper(self) -> None:
        ok, _ = validate_paper_shadow_entry(None, paper_trading=True, enabled=True)
        self.assertFalse(ok)


if __name__ == "__main__":
    unittest.main()
