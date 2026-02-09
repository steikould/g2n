"""Evaluation metrics and model card generation for accrual forecasting."""

from __future__ import annotations

import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error, r2_score


def evaluate_accrual_forecast(
    y_true: pd.Series,
    y_pred: pd.Series,
) -> dict[str, float]:
    """Compute evaluation metrics for accrual forecasts.

    Returns:
        Dict with MAE, MAPE, R2, and accrual variance reduction estimate.
    """
    mae = mean_absolute_error(y_true, y_pred)
    mape = mean_absolute_percentage_error(y_true, y_pred)
    r2 = r2_score(y_true, y_pred)

    # Accrual variance = mean absolute variance between predicted and actual
    variance = (y_true - y_pred).abs().mean()

    return {
        "mae": mae,
        "mape": mape,
        "r2": r2,
        "mean_accrual_variance": variance,
    }
