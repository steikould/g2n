"""Leakage Detection training pipeline.

Anomaly detection comparing contractual terms to actual payments.
Identifies overpayments, duplicate claims, and tier miscalculations.
"""

from __future__ import annotations

from typing import Any

import mlflow
import pandas as pd
from sklearn.ensemble import IsolationForest

from g2n.models.base import BaseG2NModel


class LeakageDetectionModel(BaseG2NModel):
    """Isolation Forest model for rebate leakage detection."""

    @property
    def use_case(self) -> str:
        return "leakage_detection"

    def train(
        self,
        train_df: pd.DataFrame,
        *,
        params: dict[str, Any] | None = None,
        sample_weights: pd.Series | None = None,
    ) -> dict[str, float]:
        default_params = {
            "n_estimators": 200,
            "contamination": 0.05,
            "max_samples": "auto",
            "random_state": 42,
        }
        if params:
            default_params.update(params)

        feature_cols = [c for c in train_df.columns if not c.startswith("_")]
        X = train_df[feature_cols]

        self._model = IsolationForest(**default_params)
        self._model.fit(X)

        scores = self._model.decision_function(X)
        predictions = self._model.predict(X)
        anomaly_rate = (predictions == -1).mean()

        mlflow.sklearn.log_model(self._model, artifact_path="model")

        return {
            "anomaly_rate": anomaly_rate,
            "mean_anomaly_score": scores.mean(),
            "training_samples": len(X),
        }

    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        if self._model is None:
            raise RuntimeError("Model not trained. Call train() first.")

        feature_cols = [c for c in df.columns if not c.startswith("_")]
        result = df.copy()
        result["anomaly_score"] = self._model.decision_function(df[feature_cols])
        result["is_leakage"] = self._model.predict(df[feature_cols]) == -1
        return result

    def evaluate(
        self,
        test_df: pd.DataFrame,
        predictions: pd.DataFrame,
    ) -> dict[str, float]:
        flagged = predictions["is_leakage"].sum()
        total = len(predictions)
        return {
            "flagged_leakages": int(flagged),
            "leakage_rate": flagged / total if total > 0 else 0.0,
            "total_evaluated": total,
        }
