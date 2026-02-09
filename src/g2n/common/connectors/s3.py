"""S3 path helpers for the medallion architecture layers."""

from __future__ import annotations

from g2n.common.config import get_settings


def bronze_path(entity: str, *, date_partition: str | None = None) -> str:
    """Return the S3 Bronze path for a given entity.

    Args:
        entity: Logical entity name (e.g. "edi_867", "contracts").
        date_partition: Optional date string (YYYY-MM-DD) for partitioning.
    """
    base = get_settings().s3.bronze_path.rstrip("/")
    path = f"{base}/{entity}"
    if date_partition:
        path = f"{path}/date={date_partition}"
    return path


def silver_path(entity: str) -> str:
    """Return the S3 Silver path for a given entity."""
    base = get_settings().s3.silver_path.rstrip("/")
    return f"{base}/{entity}"


def quarantine_path(entity: str) -> str:
    """Return the S3 quarantine path for failed quality records."""
    base = get_settings().s3.quarantine_path.rstrip("/")
    return f"{base}/{entity}"


def features_path(domain: str, feature_name: str | None = None) -> str:
    """Return the S3 path for materialized feature Delta tables.

    Args:
        domain: Feature domain (e.g. "customer_behavior").
        feature_name: Optional specific feature table name.
    """
    base = get_settings().s3.features_path.rstrip("/")
    path = f"{base}/{domain}"
    if feature_name:
        path = f"{path}/{feature_name}"
    return path
