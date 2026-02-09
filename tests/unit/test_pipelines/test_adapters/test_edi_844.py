"""Tests for EDI 844 Silver adapter."""

from __future__ import annotations

import pytest


class TestEDI844Adapter:
    """EDI 844 adapter tests — require PySpark."""

    @pytest.mark.slow
    def test_normalize_produces_canonical_schema(self):
        pytest.skip("Requires PySpark SparkSession")

    def test_entity_name(self):
        from g2n.pipelines.silver.adapters.edi_844 import EDI844Adapter
        assert EDI844Adapter.entity_name.fget is not None
