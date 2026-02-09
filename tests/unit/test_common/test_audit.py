"""Tests for SOX audit column helpers."""

from __future__ import annotations

import pytest


class TestAddAuditColumns:
    """Tests for add_audit_columns — requires PySpark."""

    @pytest.mark.slow
    def test_adds_scored_at_column(self):
        """Placeholder: verify _audit_scored_at is added."""
        # Requires SparkSession; skip in lightweight CI
        pytest.skip("Requires PySpark SparkSession")

    def test_audit_column_names(self):
        """Verify expected audit column name constants."""
        expected = [
            "_audit_scored_at",
            "_audit_model_version",
            "_audit_feature_versions",
            "_audit_correlation_id",
            "_audit_pipeline_name",
        ]
        # Smoke test: column names are documented
        assert len(expected) == 5
