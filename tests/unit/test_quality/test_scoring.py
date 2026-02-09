"""Tests for DQ scoring."""

from __future__ import annotations

from g2n.quality.scoring import DEFAULT_WEIGHTS


class TestDQScoring:
    def test_default_weights_sum_to_one(self):
        """Weights must sum to 1.0 for correct score range."""
        assert abs(sum(DEFAULT_WEIGHTS.values()) - 1.0) < 1e-9

    def test_all_dimensions_present(self):
        """All six quality dimensions must have weights."""
        expected = {
            "completeness",
            "uniqueness",
            "consistency",
            "timeliness",
            "accuracy",
            "validity",
        }
        assert set(DEFAULT_WEIGHTS.keys()) == expected
