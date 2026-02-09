"""Batch inference for accrual forecasting."""

from __future__ import annotations

from typing import TYPE_CHECKING

import mlflow
import pandas as pd

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
    """Run batch accrual forecast predictions.

    Args:
        spark: Active SparkSession.
        features_df: Feature DataFrame from the Feature Engineering Layer.
        model_uri: MLflow model URI (e.g. "models:/accrual_forecast/Production").
        model_version: Model version string for audit.
        correlation_id: Pipeline correlation ID.

    Returns:
        DataFrame with predictions and audit columns.
    """
    model = mlflow.sklearn.load_model(model_uri)
    pdf = features_df.toPandas()

    feature_cols = [c for c in pdf.columns if not c.startswith("_")]
    pdf["predicted_accrual"] = model.predict(pdf[feature_cols])

    result = spark.createDataFrame(pdf)
    result = add_audit_columns(
        result,
        model_version=model_version,
        correlation_id=correlation_id,
        pipeline_name="accrual_forecast_batch",
    )

    logger.info(
        "batch_prediction_complete",
        model_uri=model_uri,
        record_count=result.count(),
    )
    return result
