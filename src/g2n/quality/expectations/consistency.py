"""Consistency checks — cross-source referential integrity."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def check_referential_integrity(
    df: DataFrame,
    foreign_key: str,
    reference_df: DataFrame,
    primary_key: str,
) -> float:
    """Check what fraction of foreign key values exist in the reference table.

    Returns:
        Match rate (0.0–1.0). 1.0 means full integrity.
    """
    total = df.filter(F.col(foreign_key).isNotNull()).count()
    if total == 0:
        return 1.0

    matched = (
        df.alias("left")
        .join(
            reference_df.select(F.col(primary_key)).distinct().alias("right"),
            F.col(f"left.{foreign_key}") == F.col(f"right.{primary_key}"),
            "inner",
        )
        .count()
    )
    return matched / total


def check_cross_source_alignment(
    df_a: DataFrame,
    df_b: DataFrame,
    join_key: str,
    compare_columns: list[str],
) -> dict[str, float]:
    """Compare values across two sources for shared records.

    Returns:
        Dict mapping column name to agreement rate (0.0–1.0).
    """
    joined = df_a.alias("a").join(df_b.alias("b"), join_key, "inner")
    total = joined.count()
    if total == 0:
        return {col: 0.0 for col in compare_columns}

    results: dict[str, float] = {}
    for col in compare_columns:
        matching = joined.filter(F.col(f"a.{col}") == F.col(f"b.{col}")).count()
        results[col] = matching / total
    return results
