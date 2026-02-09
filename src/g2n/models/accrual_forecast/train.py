"""Accrual Forecast training pipeline.

ML models to forecast rebate, chargeback, and return liabilities
at SKU/channel/customer level to reduce over/under accrual variance.
Supports quality-weighted training samples.
"""

from __future__ import annotations

from typing import Any

import mlflow
import pandas as pd
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import cross_val_score

from g2n.models.base import BaseG2NModel


class AccrualForecastModel(BaseG2NModel):
    """Gradient boosting model for accrual amount forecasting."""

    @property
    def use_case(self) -> str:
        return "accrual_forecast"

    def train(
        self,
        train_df: pd.DataFrame,
        *,
        params: dict[str, Any] | None = None,
        sample_weights: pd.Series | None = None,
    ) -> dict[str, float]:
        default_params = {
            "n_estimators": 200,
            "max_depth": 6,
            "learning_rate": 0.1,
            "subsample": 0.8,
        }
        if params:
            default_params.update(params)

        target_col = "accrual_amount"
        feature_cols = [c for c in train_df.columns if c != target_col and not c.startswith("_")]

        X = train_df[feature_cols]
        y = train_df[target_col]

        self._model = GradientBoostingRegressor(**default_params)
        self._model.fit(X, y, sample_weight=sample_weights)

        cv_scores = cross_val_score(
            GradientBoostingRegressor(**default_params),
            X, y,
            cv=5,
            scoring="neg_mean_absolute_error",
            fit_params={"sample_weight": sample_weights} if sample_weights is not None else None,
        )

        mlflow.sklearn.log_model(self._model, artifact_path="model")

        return {
            "cv_mae_mean": -cv_scores.mean(),
            "cv_mae_std": cv_scores.std(),
        }

    def predict(self, df: pd.DataFrame) -> pd.DataFrame:
        if self._model is None:
            raise RuntimeError("Model not trained. Call train() first.")

        feature_cols = [c for c in df.columns if not c.startswith("_")]
        predictions = self._model.predict(df[feature_cols])
        result = df.copy()
        result["predicted_accrual"] = predictions
        return result

    def evaluate(
        self,
        test_df: pd.DataFrame,
        predictions: pd.DataFrame,
    ) -> dict[str, float]:
        from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score

        y_true = test_df["accrual_amount"]
        y_pred = predictions["predicted_accrual"]

        return {
            "mae": mean_absolute_error(y_true, y_pred),
            "rmse": mean_squared_error(y_true, y_pred, squared=False),
            "r2": r2_score(y_true, y_pred),
        }
