"""Data Quality Gate for the Silver layer.

Runs Great Expectations checks, calculates composite DQ scores per record,
and routes failing records to quarantine while passing records through with
quality scores attached as metadata.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from g2n.common.connectors.s3 import quarantine_path, silver_path
from g2n.common.logging import get_logger
from g2n.quality.scoring import compute_dq_score

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)


@dataclass
class QualityGateResult:
    """Result of running a DataFrame through the quality gate."""

    passed: DataFrame
    quarantined: DataFrame
    total_count: int
    passed_count: int
    quarantined_count: int
    mean_dq_score: float


def run_quality_gate(
    spark: SparkSession,
    df: DataFrame,
    entity: str,
    *,
    critical_threshold: float = 40.0,
    write_outputs: bool = True,
) -> QualityGateResult:
    """Run the data quality gate on a Silver-layer DataFrame.

    Records with a composite DQ score below the critical threshold are routed
    to quarantine. All others pass through with their DQ score attached.

    Args:
        spark: Active SparkSession.
        df: Input DataFrame (post-adapter normalization).
        entity: Entity name for path resolution.
        critical_threshold: Minimum DQ score to pass (0-100).
        write_outputs: Whether to write results to S3.

    Returns:
        QualityGateResult with passed and quarantined DataFrames.
    """
    scored = compute_dq_score(df)

    passed = scored.filter(F.col("dq_score") >= critical_threshold)
    quarantined = scored.filter(F.col("dq_score") < critical_threshold)

    total = scored.count()
    pass_count = passed.count()
    quarantine_count = quarantined.count()
    avg_score = scored.agg(F.avg("dq_score")).collect()[0][0] or 0.0

    logger.info(
        "quality_gate_result",
        entity=entity,
        total=total,
        passed=pass_count,
        quarantined=quarantine_count,
        mean_dq_score=round(avg_score, 2),
    )

    if write_outputs:
        passed.write.mode("overwrite").parquet(silver_path(entity))
        if quarantine_count > 0:
            quarantined.write.mode("append").parquet(quarantine_path(entity))

    return QualityGateResult(
        passed=passed,
        quarantined=quarantined,
        total_count=total,
        passed_count=pass_count,
        quarantined_count=quarantine_count,
        mean_dq_score=round(avg_score, 2),
    )
