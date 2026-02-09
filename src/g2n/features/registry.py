"""Feature Registry — registration and discovery via Collibra API.

Features are registered with business definitions, computation logic,
source lineage, version history, and quality expectations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from g2n.common.connectors.collibra import CollibraClient
from g2n.common.logging import get_logger

logger = get_logger(__name__)


@dataclass
class FeatureDefinition:
    """Metadata for a registered feature."""

    name: str
    domain: str
    definition: str
    computation_logic: str
    source_lineage: list[str]
    version: str
    quality_expectations: dict[str, Any] = field(default_factory=dict)
    collibra_asset_id: str | None = None


class FeatureRegistry:
    """Interface to the feature catalog in Collibra."""

    def __init__(self) -> None:
        self._client = CollibraClient()

    def register(self, feature: FeatureDefinition) -> str:
        """Register or update a feature in Collibra.

        Returns:
            The Collibra asset ID for the registered feature.
        """
        result = self._client.register_feature(
            name=feature.name,
            definition=feature.definition,
            computation_logic=feature.computation_logic,
            source_lineage=feature.source_lineage,
            version=feature.version,
            quality_expectations=feature.quality_expectations,
        )
        asset_id = result.get("id", "")
        logger.info(
            "feature_registered",
            feature=feature.name,
            version=feature.version,
            asset_id=asset_id,
        )
        return asset_id

    def search(self, query: str) -> list[dict[str, Any]]:
        """Search the feature catalog by keyword."""
        return self._client.search_features(query)

    def get_by_name(self, name: str) -> dict[str, Any] | None:
        """Look up a feature by exact name."""
        results = self._client.search_features(name)
        for r in results:
            if r.get("name") == name:
                return r
        return None

    def close(self) -> None:
        self._client.close()
