"""Batch and real-time inference for deduction classification."""

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
    df: DataFrame,
    *,
    model_uri: str,
    model_version: str,
    correlation_id: str | None = None,
) -> DataFrame:
    """Run batch deduction classification."""
    model = mlflow.sklearn.load_model(model_uri)
    pdf = df.toPandas()

    pdf["predicted_deduction_type"] = model.predict(pdf["remittance_text"])
    pdf["prediction_confidence"] = model.predict_proba(
        pdf["remittance_text"]
    ).max(axis=1)

    result = spark.createDataFrame(pdf)
    result = add_audit_columns(
        result,
        model_version=model_version,
        correlation_id=correlation_id,
        pipeline_name="deduction_classification_batch",
    )
    return result


def predict_single(model_uri: str, text: str) -> dict[str, str | float]:
    """Real-time classification of a single deduction text.

    Designed for the OpenShift REST API endpoint.
    """
    model = mlflow.sklearn.load_model(model_uri)
    prediction = model.predict([text])[0]
    confidence = model.predict_proba([text]).max()
    return {
        "deduction_type": prediction,
        "confidence": float(confidence),
    }
