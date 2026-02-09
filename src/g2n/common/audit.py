"""SOX audit column helpers for traceability from model output back to source data."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def add_audit_columns(
    df: DataFrame,
    *,
    model_version: str | None = None,
    feature_versions: str | None = None,
    correlation_id: str | None = None,
    pipeline_name: str | None = None,
) -> DataFrame:
    """Append standard audit columns to a DataFrame for SOX compliance.

    Columns added:
        _audit_scored_at: UTC timestamp of when the record was processed.
        _audit_model_version: MLflow model version used for scoring (if applicable).
        _audit_feature_versions: JSON string of feature name→version mappings.
        _audit_correlation_id: Correlation ID linking to upstream pipeline run.
        _audit_pipeline_name: Name of the pipeline that produced this record.
    """
    now = datetime.now(timezone.utc).isoformat()

    df = df.withColumn("_audit_scored_at", F.lit(now))

    if model_version is not None:
        df = df.withColumn("_audit_model_version", F.lit(model_version))

    if feature_versions is not None:
        df = df.withColumn("_audit_feature_versions", F.lit(feature_versions))

    if correlation_id is not None:
        df = df.withColumn("_audit_correlation_id", F.lit(correlation_id))

    if pipeline_name is not None:
        df = df.withColumn("_audit_pipeline_name", F.lit(pipeline_name))

    return df
