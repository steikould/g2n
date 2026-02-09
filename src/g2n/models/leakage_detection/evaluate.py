"""Evaluation for leakage detection model."""

from __future__ import annotations

import pandas as pd


def evaluate_leakage_detection(
    predictions: pd.DataFrame,
    *,
    amount_column: str = "claim_amount",
) -> dict[str, float]:
    """Evaluate leakage detection results.

    Returns:
        Dict with leakage count, rate, and estimated dollar impact.
    """
    flagged = predictions[predictions["is_leakage"]]
    total_flagged = len(flagged)
    total_amount = flagged[amount_column].sum() if total_flagged > 0 else 0.0

    return {
        "total_flagged": total_flagged,
        "leakage_rate": total_flagged / len(predictions) if len(predictions) > 0 else 0.0,
        "estimated_leakage_amount": total_amount,
    }
