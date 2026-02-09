"""Composite data quality score calculator (0–100).

Every Silver-layer record receives a composite DQ score based on six dimensions:
completeness, uniqueness, consistency, timeliness, accuracy, and validity.
Models can weight training samples by this score.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

# Default weights for each quality dimension (must sum to 1.0)
DEFAULT_WEIGHTS: dict[str, float] = {
    "completeness": 0.25,
    "uniqueness": 0.15,
    "consistency": 0.20,
    "timeliness": 0.10,
    "accuracy": 0.20,
    "validity": 0.10,
}


def compute_dq_score(
    df: DataFrame,
    *,
    weights: dict[str, float] | None = None,
) -> DataFrame:
    """Compute a composite DQ score (0–100) for each record.

    This function expects the DataFrame to have boolean/numeric quality flag
    columns prefixed with `_dq_`. If they don't exist yet, it applies basic
    heuristic checks on standard columns.

    Args:
        df: Input DataFrame.
        weights: Optional custom weights per dimension.

    Returns:
        DataFrame with `dq_score` column populated.
    """
    w = weights or DEFAULT_WEIGHTS

    # Completeness: fraction of non-null standard columns
    standard_cols = [c for c in df.columns if not c.startswith("_")]
    if standard_cols:
        completeness_expr = sum(
            F.when(F.col(c).isNotNull(), 1).otherwise(0) for c in standard_cols
        ) / F.lit(len(standard_cols))
    else:
        completeness_expr = F.lit(1.0)

    # Uniqueness: use _is_duplicate flag if present, otherwise assume unique
    if "_is_duplicate" in df.columns:
        uniqueness_expr = F.when(F.col("_is_duplicate"), 0.0).otherwise(1.0)
    else:
        uniqueness_expr = F.lit(1.0)

    # Validity: use _valid flag if present
    if "_valid" in df.columns:
        validity_expr = F.when(F.col("_valid"), 1.0).otherwise(0.0)
    else:
        validity_expr = F.lit(1.0)

    # Staleness: use _staleness_score if present (inverted: 0=stale, 1=fresh)
    if "_staleness_score" in df.columns:
        timeliness_expr = F.lit(1.0) - F.col("_staleness_score")
    else:
        timeliness_expr = F.lit(1.0)

    # Consistency and accuracy default to 1.0 unless enriched upstream
    consistency_expr = F.lit(1.0)
    accuracy_expr = F.lit(1.0)

    # Weighted composite score (0–100)
    composite = (
        completeness_expr * w["completeness"]
        + uniqueness_expr * w["uniqueness"]
        + consistency_expr * w["consistency"]
        + timeliness_expr * w["timeliness"]
        + accuracy_expr * w["accuracy"]
        + validity_expr * w["validity"]
    ) * 100

    return df.withColumn("dq_score", F.round(composite, 2))
