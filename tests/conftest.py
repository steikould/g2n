"""Shared test fixtures for the G2N test suite."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


@pytest.fixture
def mock_collibra_client():
    """Mock Collibra client for unit tests."""
    client = MagicMock()
    client.get_dq_rules.return_value = [
        {"id": "rule-1", "name": "completeness_check", "threshold": 0.95},
        {"id": "rule-2", "name": "uniqueness_check", "threshold": 1.0},
    ]
    client.push_dq_results.return_value = {"status": "ok"}
    client.register_feature.return_value = {"id": "feat-001"}
    client.search_features.return_value = []
    return client


@pytest.fixture
def sample_transaction_data():
    """Sample transaction data for testing adapters and quality checks."""
    return [
        {
            "ref_id": "TXN-001",
            "dist_id": "DIST-A",
            "ship_to_id": "CUST-001",
            "item_id": "PROD-001",
            "ndc": "12345-678-90",
            "txn_date": "20260115",
            "qty": 100,
            "unit_price": 25.50,
        },
        {
            "ref_id": "TXN-002",
            "dist_id": "DIST-A",
            "ship_to_id": "CUST-002",
            "item_id": "PROD-002",
            "ndc": "12345-678-91",
            "txn_date": "20260116",
            "qty": 50,
            "unit_price": 42.00,
        },
        {
            "ref_id": "TXN-003",
            "dist_id": "DIST-B",
            "ship_to_id": "CUST-001",
            "item_id": "PROD-001",
            "ndc": "12345-678-90",
            "txn_date": "20260117",
            "qty": 200,
            "unit_price": 25.50,
        },
    ]


@pytest.fixture
def sample_chargeback_data():
    """Sample chargeback data for testing."""
    return [
        {
            "claim_ref": "CB-001",
            "dist_id": "DIST-A",
            "end_cust_id": "CUST-001",
            "item_id": "PROD-001",
            "contract_ref": "CTR-001",
            "claim_date": "20260120",
            "orig_txn_ref": "TXN-001",
            "cb_amount": 500.00,
            "status_code": "pending",
            "submission_cnt": 1,
        },
    ]


@pytest.fixture
def sample_contract_data():
    """Sample contract data for testing."""
    return [
        {
            "agreement_id": "CTR-001",
            "account_id": "CUST-001",
            "material_group": "PROD-001",
            "agreement_type": "rebate",
            "start_date": "2026-01-01",
            "end_date": "2026-12-31",
            "num_tiers": 3,
            "base_discount": 0.05,
            "rebate_rate": 0.03,
            "bundle_flag": "N",
        },
    ]
