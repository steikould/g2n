"""Tests for rebate dynamics feature domain."""

from __future__ import annotations

import pytest


class TestRebateDynamicsFeatures:
    @pytest.mark.slow
    def test_rebate_to_sales_ratio(self):
        pytest.skip("Requires PySpark SparkSession")

    @pytest.mark.slow
    def test_over_under_payment_rate(self):
        pytest.skip("Requires PySpark SparkSession")

    @pytest.mark.slow
    def test_rebate_velocity_default_windows(self):
        pytest.skip("Requires PySpark SparkSession")
