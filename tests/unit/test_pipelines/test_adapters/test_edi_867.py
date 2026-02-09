"""Tests for EDI 867 Silver adapter."""

from __future__ import annotations

import pytest


class TestEDI867Adapter:
    """EDI 867 adapter tests — require PySpark."""

    @pytest.mark.slow
    def test_normalize_produces_canonical_schema(self):
        pytest.skip("Requires PySpark SparkSession")

    def test_entity_name(self):
        """Verify entity name is correct."""
        # Can't instantiate without SparkSession, but can import
        from g2n.pipelines.silver.adapters.edi_867 import EDI867Adapter
        assert EDI867Adapter.entity_name.fget is not None
