"""Batch inference for leakage detection."""

from __future__ import annotations

from typing import TYPE_CHECKING

import mlflow

from g2n.common.audit import add_audit_columns
from g2n.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

logger = get_logger(__name__)


def batch_predict(
    spark: SparkSession,
    features_df: DataFrame,
    *,
    model_uri: str,
    model_version: str,
    correlation_id: str | None = None,
) -> DataFrame:
    """Run batch leakage detection predictions."""
    model = mlflow.sklearn.load_model(model_uri)
    pdf = features_df.toPandas()

    feature_cols = [c for c in pdf.columns if not c.startswith("_")]
    pdf["anomaly_score"] = model.decision_function(pdf[feature_cols])
    pdf["is_leakage"] = model.predict(pdf[feature_cols]) == -1

    result = spark.createDataFrame(pdf)
    result = add_audit_columns(
        result,
        model_version=model_version,
        correlation_id=correlation_id,
        pipeline_name="leakage_detection_batch",
    )

    logger.info(
        "leakage_batch_complete",
        model_uri=model_uri,
        flagged=int(pdf["is_leakage"].sum()),
    )
    return result
