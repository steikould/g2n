"""Data Quality Signals feature domain.

Features: source completeness score, cross-reference match confidence,
record staleness days, duplicate probability.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def compute_source_completeness_score(df: DataFrame) -> DataFrame:
    """Compute per-record source completeness as a feature.

    Counts the fraction of non-null, non-empty columns per record.
    """
    data_cols = [c for c in df.columns if not c.startswith("_")]
    n_cols = len(data_cols) or 1

    completeness_expr = sum(
        F.when(
            F.col(c).isNotNull() & (F.col(c).cast("string") != ""),
            1,
        ).otherwise(0)
        for c in data_cols
    ) / F.lit(n_cols)

    return df.withColumn("feat_source_completeness", F.round(completeness_expr, 4))


def compute_staleness_days(
    df: DataFrame,
    date_column: str,
) -> DataFrame:
    """Compute days since the record date as a feature."""
    return df.withColumn(
        "feat_staleness_days",
        F.datediff(F.current_date(), F.col(date_column)),
    )


def compute_match_confidence(
    df: DataFrame,
    key_columns: list[str],
    reference_df: DataFrame,
    ref_key_columns: list[str],
) -> DataFrame:
    """Compute cross-reference match confidence.

    For each record, checks how many key columns match in the reference table.
    Returns a score from 0.0 (no match) to 1.0 (full match).
    """
    if len(key_columns) != len(ref_key_columns):
        raise ValueError("key_columns and ref_key_columns must have the same length")

    join_cond = [
        F.col(f"left.{k}") == F.col(f"right.{rk}")
        for k, rk in zip(key_columns, ref_key_columns)
    ]
    joined = df.alias("left").join(
        reference_df.alias("right"),
        join_cond,
        "left",
    )
    match_expr = sum(
        F.when(F.col(f"right.{rk}").isNotNull(), 1).otherwise(0)
        for rk in ref_key_columns
    ) / F.lit(len(ref_key_columns))

    return joined.withColumn("feat_match_confidence", F.round(match_expr, 4)).select(
        "left.*", "feat_match_confidence"
    )
