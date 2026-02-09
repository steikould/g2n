"""Tests for customer behavior feature domain."""

from __future__ import annotations

import pytest


class TestCustomerBehaviorFeatures:
    @pytest.mark.slow
    def test_purchase_velocity_default_windows(self):
        pytest.skip("Requires PySpark SparkSession")

    @pytest.mark.slow
    def test_ordering_regularity(self):
        pytest.skip("Requires PySpark SparkSession")

    @pytest.mark.slow
    def test_species_mix_entropy(self):
        pytest.skip("Requires PySpark SparkSession")
