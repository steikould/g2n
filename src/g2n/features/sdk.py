"""Feature Access SDK — discover, request, and pull features by name and version.

This is the primary interface for data scientists to consume features
from the Feature Engineering Layer in their training and inference pipelines.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from g2n.features.registry import FeatureRegistry
from g2n.features.store import read_feature_table

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


class FeatureSDK:
    """SDK for discovering and consuming features from the Feature Engineering Layer."""

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self._registry = FeatureRegistry()

    def list_features(self, query: str = "*") -> list[dict[str, Any]]:
        """Search the feature catalog.

        Args:
            query: Search keyword (or "*" to list all).

        Returns:
            List of feature metadata dicts from Collibra.
        """
        return self._registry.search(query)

    def get_feature(
        self,
        domain: str,
        feature_name: str,
        *,
        version: int | None = None,
    ) -> DataFrame:
        """Load a feature table by domain and name.

        Args:
            domain: Feature domain (e.g. "customer_behavior").
            feature_name: Feature table name.
            version: Optional Delta version for reproducibility.

        Returns:
            Feature DataFrame.
        """
        return read_feature_table(
            self.spark, domain, feature_name, version=version
        )

    def get_feature_metadata(self, name: str) -> dict[str, Any] | None:
        """Get full metadata for a feature from the registry."""
        return self._registry.get_by_name(name)

    def close(self) -> None:
        self._registry.close()
