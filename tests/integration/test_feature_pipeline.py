"""Integration tests for the feature compute pipeline."""

from __future__ import annotations

import pytest


@pytest.mark.integration
class TestFeaturePipeline:
    """End-to-end feature computation tests — require PySpark and Delta."""

    def test_customer_behavior_features_materialize(self):
        """Test: customer behavior features write to Delta successfully."""
        pytest.skip("Requires PySpark and Delta Lake")

    def test_feature_table_read_back(self):
        """Test: features can be read back from Delta after materialization."""
        pytest.skip("Requires PySpark and Delta Lake")
