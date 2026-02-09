"""Quarantine zone management for records failing critical quality checks.

Quarantined records are written to a dedicated S3 path and tracked via
Collibra remediation workflows.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from g2n.common.connectors.collibra import CollibraClient
from g2n.common.connectors.s3 import quarantine_path
from g2n.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = get_logger(__name__)


def write_quarantine(
    df: DataFrame,
    entity: str,
    *,
    reason: str = "below_dq_threshold",
) -> int:
    """Write quarantined records to the quarantine zone.

    Args:
        df: DataFrame of records that failed the quality gate.
        entity: Entity name for path resolution.
        reason: Quarantine reason tag.

    Returns:
        Number of records quarantined.
    """
    tagged = df.withColumn("_quarantine_reason", F.lit(reason))
    tagged = tagged.withColumn("_quarantined_at", F.current_timestamp())

    path = quarantine_path(entity)
    tagged.write.mode("append").parquet(path)

    count = tagged.count()
    logger.info("quarantine_written", entity=entity, count=count, reason=reason)
    return count


def notify_collibra_remediation(
    entity: str,
    record_count: int,
    reason: str,
) -> None:
    """Create a remediation workflow entry in Collibra for quarantined data."""
    client = CollibraClient()
    try:
        client.push_dq_results(
            rule_id=f"quarantine-{entity}",
            passed=False,
            result_details={
                "entity": entity,
                "quarantined_count": record_count,
                "reason": reason,
            },
        )
    finally:
        client.close()
