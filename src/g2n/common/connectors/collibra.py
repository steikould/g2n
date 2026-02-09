"""Collibra REST API client for data governance integration.

Handles DQ rule retrieval, DQ result publishing, feature registration, and lineage tracking.
"""

from __future__ import annotations

from typing import Any

import httpx

from g2n.common.config import CollibraSettings, get_settings
from g2n.common.logging import get_logger

logger = get_logger(__name__)


class CollibraClient:
    """Client for the Collibra REST API v2."""

    def __init__(self, settings: CollibraSettings | None = None) -> None:
        cfg = settings or get_settings().collibra
        self._base_url = cfg.base_url.rstrip("/")
        self._community_id = cfg.community_id
        self._client = httpx.Client(
            base_url=self._base_url,
            headers={"Content-Type": "application/json"},
            timeout=30.0,
        )

    # -- Data Quality ----------------------------------------------------------

    def get_dq_rules(self, domain_id: str) -> list[dict[str, Any]]:
        """Fetch active data quality rules for a given Collibra domain."""
        resp = self._client.get(
            "/dataQualityRules",
            params={"domainId": domain_id, "limit": 500},
        )
        resp.raise_for_status()
        return resp.json().get("results", [])

    def push_dq_results(
        self,
        rule_id: str,
        *,
        passed: bool,
        result_details: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Publish a data quality check result back to Collibra."""
        payload: dict[str, Any] = {
            "dataQualityRuleId": rule_id,
            "passed": passed,
        }
        if result_details:
            payload["details"] = result_details

        resp = self._client.post("/dataQualityResults", json=payload)
        resp.raise_for_status()
        logger.info("dq_result_published", rule_id=rule_id, passed=passed)
        return resp.json()

    # -- Feature Registry ------------------------------------------------------

    def register_feature(
        self,
        *,
        name: str,
        definition: str,
        computation_logic: str,
        source_lineage: list[str],
        version: str,
        quality_expectations: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Register or update a feature in the Collibra catalog."""
        payload = {
            "name": name,
            "communityId": self._community_id,
            "typeId": "Feature",  # mapped via config/collibra/field_mappings.yaml
            "attributes": {
                "definition": definition,
                "computation_logic": computation_logic,
                "version": version,
                "source_lineage": source_lineage,
            },
        }
        if quality_expectations:
            payload["attributes"]["quality_expectations"] = quality_expectations

        resp = self._client.post("/assets", json=payload)
        resp.raise_for_status()
        logger.info("feature_registered", feature=name, version=version)
        return resp.json()

    def search_features(self, query: str) -> list[dict[str, Any]]:
        """Search the feature catalog by keyword."""
        resp = self._client.get(
            "/assets",
            params={"name": query, "typeId": "Feature", "limit": 100},
        )
        resp.raise_for_status()
        return resp.json().get("results", [])

    # -- Lineage ---------------------------------------------------------------

    def register_lineage(
        self,
        *,
        source_asset_id: str,
        target_asset_id: str,
        relation_type: str = "Data Flow",
    ) -> dict[str, Any]:
        """Register a lineage relationship between two Collibra assets."""
        payload = {
            "sourceId": source_asset_id,
            "targetId": target_asset_id,
            "typeId": relation_type,
        }
        resp = self._client.post("/relations", json=payload)
        resp.raise_for_status()
        logger.info(
            "lineage_registered",
            source=source_asset_id,
            target=target_asset_id,
        )
        return resp.json()

    def close(self) -> None:
        self._client.close()
