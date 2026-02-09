"""Feature Store — read/write materialized Delta tables on S3."""

from __future__ import annotations

from typing import TYPE_CHECKING

from g2n.common.connectors.s3 import features_path
from g2n.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)


def write_feature_table(
    df: DataFrame,
    domain: str,
    feature_name: str,
    *,
    mode: str = "overwrite",
    partition_by: list[str] | None = None,
) -> str:
    """Materialize a feature DataFrame to a Delta table.

    Args:
        df: Computed feature DataFrame.
        domain: Feature domain (e.g. "customer_behavior").
        feature_name: Feature table name.
        mode: Write mode (overwrite, append).
        partition_by: Optional partition columns.

    Returns:
        The S3 path where the feature table was written.
    """
    path = features_path(domain, feature_name)
    writer = df.write.format("delta").mode(mode)
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)

    logger.info(
        "feature_table_written",
        domain=domain,
        feature=feature_name,
        path=path,
        record_count=df.count(),
    )
    return path


def read_feature_table(
    spark: SparkSession,
    domain: str,
    feature_name: str,
    *,
    version: int | None = None,
) -> DataFrame:
    """Read a materialized feature table from Delta.

    Args:
        spark: Active SparkSession.
        domain: Feature domain.
        feature_name: Feature table name.
        version: Optional Delta version for time-travel (reproducibility).

    Returns:
        Feature DataFrame.
    """
    path = features_path(domain, feature_name)
    reader = spark.read.format("delta")
    if version is not None:
        reader = reader.option("versionAsOf", version)
    return reader.load(path)
