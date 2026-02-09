"""Tests for feature registry."""

from __future__ import annotations

from g2n.features.registry import FeatureDefinition


class TestFeatureDefinition:
    def test_create_feature_definition(self):
        """FeatureDefinition can be constructed with required fields."""
        fd = FeatureDefinition(
            name="purchase_velocity_30d",
            domain="customer_behavior",
            definition="Rolling 30-day purchase total per customer",
            computation_logic="SUM(gross_amount) OVER (PARTITION BY customer_id ORDER BY date ROWS 30 PRECEDING)",
            source_lineage=["silver.edi_867"],
            version="1.0.0",
        )
        assert fd.name == "purchase_velocity_30d"
        assert fd.domain == "customer_behavior"
        assert fd.collibra_asset_id is None
