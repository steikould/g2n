"""Accuracy checks — range validation, statistical outlier detection."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def check_value_in_range(
    df: DataFrame,
    column: str,
    min_val: float,
    max_val: float,
) -> float:
    """Check fraction of records where column falls within [min_val, max_val].

    Returns:
        In-range rate (0.0–1.0).
    """
    total = df.filter(F.col(column).isNotNull()).count()
    if total == 0:
        return 1.0

    in_range = df.filter(F.col(column).between(min_val, max_val)).count()
    return in_range / total


def flag_statistical_outliers(
    df: DataFrame,
    column: str,
    *,
    z_threshold: float = 3.0,
) -> DataFrame:
    """Flag records where a numeric column is beyond z_threshold standard deviations.

    Adds a boolean _is_outlier_{column} column.
    """
    stats = df.agg(
        F.mean(column).alias("_mean"),
        F.stddev(column).alias("_std"),
    ).collect()[0]

    mean_val = stats["_mean"] or 0.0
    std_val = stats["_std"] or 1.0

    return df.withColumn(
        f"_is_outlier_{column}",
        F.abs((F.col(column) - F.lit(mean_val)) / F.lit(std_val)) > z_threshold,
    )
