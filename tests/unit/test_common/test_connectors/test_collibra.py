"""Tests for Collibra REST API client."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from g2n.common.connectors.collibra import CollibraClient


@pytest.fixture
def collibra_client():
    """CollibraClient with mocked HTTP client."""
    with patch("g2n.common.connectors.collibra.get_settings") as mock_settings:
        mock_settings.return_value.collibra.base_url = "https://collibra.test/rest/2.0"
        mock_settings.return_value.collibra.community_id = "test-community"
        client = CollibraClient()
        client._client = MagicMock()
        yield client


class TestGetDQRules:
    def test_returns_rules_list(self, collibra_client):
        collibra_client._client.get.return_value.json.return_value = {
            "results": [{"id": "rule-1"}]
        }
        collibra_client._client.get.return_value.raise_for_status = MagicMock()

        rules = collibra_client.get_dq_rules("domain-1")
        assert len(rules) == 1
        assert rules[0]["id"] == "rule-1"


class TestRegisterFeature:
    def test_returns_asset_id(self, collibra_client):
        collibra_client._client.post.return_value.json.return_value = {
            "id": "feat-001"
        }
        collibra_client._client.post.return_value.raise_for_status = MagicMock()

        result = collibra_client.register_feature(
            name="test_feature",
            definition="A test feature",
            computation_logic="SELECT 1",
            source_lineage=["table_a"],
            version="1.0.0",
        )
        assert result["id"] == "feat-001"
