"""Tests for Silver layer quality gate."""

from __future__ import annotations

import pytest


class TestQualityGate:
    """Quality gate tests — require PySpark."""

    @pytest.mark.slow
    def test_records_above_threshold_pass(self):
        pytest.skip("Requires PySpark SparkSession")

    @pytest.mark.slow
    def test_records_below_threshold_quarantined(self):
        pytest.skip("Requires PySpark SparkSession")

    def test_quality_gate_result_fields(self):
        """Verify QualityGateResult dataclass has expected fields."""
        from g2n.pipelines.silver.quality_gate import QualityGateResult

        fields = {f.name for f in QualityGateResult.__dataclass_fields__.values()}
        assert "passed" in fields
        assert "quarantined" in fields
        assert "mean_dq_score" in fields
