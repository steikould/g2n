"""Uniqueness checks — deduplication, unique transaction IDs."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql import Window

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def check_duplicate_rate(df: DataFrame, key_columns: list[str]) -> float:
    """Calculate the duplicate rate based on key columns.

    Returns:
        Fraction of rows that are duplicates (0.0–1.0).
    """
    total = df.count()
    if total == 0:
        return 0.0
    distinct = df.select(*key_columns).distinct().count()
    return (total - distinct) / total


def flag_duplicates(df: DataFrame, key_columns: list[str]) -> DataFrame:
    """Add a _is_duplicate boolean column identifying duplicate records.

    Keeps the first occurrence as non-duplicate based on row ordering.
    """
    window = Window.partitionBy(*key_columns).orderBy(F.lit(1))
    return df.withColumn(
        "_is_duplicate",
        F.row_number().over(window) > 1,
    )


def deduplicate(df: DataFrame, key_columns: list[str]) -> DataFrame:
    """Remove duplicate records, keeping the first occurrence."""
    return df.dropDuplicates(key_columns)
