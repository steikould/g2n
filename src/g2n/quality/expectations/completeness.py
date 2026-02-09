"""Completeness checks — all expected feeds received, no missing periods."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def check_required_columns_present(
    df: DataFrame,
    required_columns: list[str],
) -> dict[str, float]:
    """Check non-null rates for required columns.

    Returns:
        Dict mapping column name to non-null rate (0.0–1.0).
    """
    total = df.count()
    if total == 0:
        return {col: 0.0 for col in required_columns}

    results: dict[str, float] = {}
    for col in required_columns:
        non_null = df.filter(F.col(col).isNotNull()).count()
        results[col] = non_null / total
    return results


def check_expected_periods(
    df: DataFrame,
    date_column: str,
    expected_periods: list[str],
) -> list[str]:
    """Identify expected date periods that are missing from the data.

    Args:
        df: Input DataFrame.
        date_column: Name of the date column.
        expected_periods: List of expected period strings (e.g. "2026-01").

    Returns:
        List of missing period strings.
    """
    actual = (
        df.select(F.date_format(F.col(date_column), "yyyy-MM").alias("period"))
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    return [p for p in expected_periods if p not in actual]


def check_feed_completeness(
    df: DataFrame,
    source_column: str,
    expected_sources: list[str],
) -> list[str]:
    """Check that all expected source feeds are present.

    Returns:
        List of missing source identifiers.
    """
    actual = (
        df.select(F.col(source_column))
        .distinct()
        .rdd.flatMap(lambda x: x)
        .collect()
    )
    return [s for s in expected_sources if s not in actual]
