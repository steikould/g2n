"""Evaluation for returns prediction model."""

from __future__ import annotations

import pandas as pd
from sklearn.metrics import f1_score, precision_score, recall_score, roc_auc_score


def evaluate_returns_prediction(
    y_true: pd.Series,
    y_pred: pd.Series,
    y_prob: pd.Series,
) -> dict[str, float]:
    """Compute evaluation metrics for returns prediction."""
    return {
        "auc_roc": roc_auc_score(y_true, y_prob),
        "precision": precision_score(y_true, y_pred),
        "recall": recall_score(y_true, y_pred),
        "f1": f1_score(y_true, y_pred),
        "return_rate_actual": y_true.mean(),
        "return_rate_predicted": y_pred.mean(),
    }
