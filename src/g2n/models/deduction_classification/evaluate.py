"""Evaluation for deduction classification model."""

from __future__ import annotations

import pandas as pd
from sklearn.metrics import accuracy_score, classification_report, f1_score


def evaluate_deduction_classification(
    y_true: pd.Series,
    y_pred: pd.Series,
) -> dict[str, float]:
    """Compute classification metrics."""
    return {
        "accuracy": accuracy_score(y_true, y_pred),
        "f1_macro": f1_score(y_true, y_pred, average="macro"),
        "f1_weighted": f1_score(y_true, y_pred, average="weighted"),
    }


def generate_classification_report(
    y_true: pd.Series,
    y_pred: pd.Series,
) -> str:
    """Generate a full classification report string."""
    return classification_report(y_true, y_pred)
