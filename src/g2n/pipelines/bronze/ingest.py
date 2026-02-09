"""Raw ingestion to S3 Bronze layer — immutable copies of source data."""

from __future__ import annotations

from datetime import date
from typing import TYPE_CHECKING

from g2n.common.connectors.s3 import bronze_path
from g2n.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)


def ingest_to_bronze(
    spark: SparkSession,
    source_path: str,
    entity: str,
    *,
    file_format: str = "csv",
    date_partition: str | None = None,
    read_options: dict[str, str] | None = None,
) -> DataFrame:
    """Read raw source data and write an immutable copy to Bronze.

    Args:
        spark: Active SparkSession.
        source_path: Path to the source file or directory.
        entity: Logical entity name (e.g. "edi_867", "contracts").
        file_format: Source file format (csv, json, parquet, etc.).
        date_partition: Date partition string (YYYY-MM-DD). Defaults to today.
        read_options: Additional Spark reader options.

    Returns:
        The raw DataFrame as ingested.
    """
    if date_partition is None:
        date_partition = date.today().isoformat()

    reader = spark.read.format(file_format)
    if read_options:
        reader = reader.options(**read_options)
    if file_format == "csv":
        reader = reader.option("header", "true").option("inferSchema", "true")

    df = reader.load(source_path)

    target = bronze_path(entity, date_partition=date_partition)
    df.write.mode("append").parquet(target)

    logger.info(
        "bronze_ingestion_complete",
        entity=entity,
        source=source_path,
        target=target,
        record_count=df.count(),
    )
    return df
