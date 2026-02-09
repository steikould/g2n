"""Returns Prediction training pipeline.

Predicts return propensity by product/channel/region to tighten return reserves.
Accounts for animal pharma seasonality (parasiticide, vaccine cycles).
"""

from __future__ import annotations

from typing import Any

import mlflow
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import cross_val_score

from g2n.models.base import BaseG2NModel


class ReturnsPredictionModel(BaseG2NModel):
    """Random Forest model for return propensity prediction."""

    @property
    def use_case(self) -> str:
        return "returns_prediction"

    def train(
        self,
        train_df: pd.DataFrame,
        *,
        params: dict[str, Any] | None = None,
        sample_weights: pd.Series | None = None,
    ) -> dict[str, float]:
        default_params = {
            "n_estimators": 300,
            "max_depth": 8,
            "min_samples_split": 10,
            "class_weight": "balanced",
            "random_state": 42,
        }
        if params:
            default_params.update(params)

        target_col = "is_returned"
        feature_cols = [c for c in train_df.columns if c != target_col and not c.startswith("_")]

        X = train_df[feature_cols]
        y = train_df[target_col]

        self._model = RandomForestClassifier(**default_params)
        self._model.fit(X, y, sample_weight=sample_weights)

        cv_scores = cross_val_score(
            RandomForestClassifier(**default_params),
            X, y, cv=5, scoring="roc_auc",
        )

        mlflow.sklearn.log_model(self._model, artifact_path="model")

        return {
            "cv_auc_mean": cv_scores.mean(),
            "cv_auc_std": cv_scores.std(),
            "return_rate": y.mean(),
        }

    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        if self._model is None:
            raise RuntimeError("Model not trained. Call train() first.")

        feature_cols = [c for c in df.columns if not c.startswith("_")]
        result = df.copy()
        result["return_probability"] = self._model.predict_proba(df[feature_cols])[:, 1]
        result["predicted_return"] = self._model.predict(df[feature_cols])
        return result

    def evaluate(
        self,
        test_df: pd.DataFrame,
        predictions: pd.DataFrame,
    ) -> dict[str, float]:
        from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score

        y_true = test_df["is_returned"]
        y_pred = predictions["predicted_return"]
        y_prob = predictions["return_probability"]

        return {
            "auc_roc": roc_auc_score(y_true, y_prob),
            "precision": precision_score(y_true, y_pred),
            "recall": recall_score(y_true, y_pred),
            "f1": f1_score(y_true, y_pred),
        }
