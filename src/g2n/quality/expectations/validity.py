"""Validity checks — schema validation, business rule enforcement."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def check_valid_codes(
    df: DataFrame,
    column: str,
    allowed_values: list[str],
) -> float:
    """Check fraction of records with a valid code value.

    Returns:
        Validity rate (0.0–1.0).
    """
    total = df.filter(F.col(column).isNotNull()).count()
    if total == 0:
        return 1.0

    valid = df.filter(F.col(column).isin(allowed_values)).count()
    return valid / total


def check_date_logic(
    df: DataFrame,
    start_col: str,
    end_col: str,
) -> float:
    """Check that start date is before end date where both are present.

    Returns:
        Compliance rate (0.0–1.0).
    """
    both_present = df.filter(
        F.col(start_col).isNotNull() & F.col(end_col).isNotNull()
    )
    total = both_present.count()
    if total == 0:
        return 1.0

    valid = both_present.filter(F.col(start_col) < F.col(end_col)).count()
    return valid / total


def check_positive_values(df: DataFrame, columns: list[str]) -> dict[str, float]:
    """Check that monetary/quantity columns contain positive values.

    Returns:
        Dict mapping column name to positive-value rate (0.0–1.0).
    """
    total = df.count()
    if total == 0:
        return {col: 1.0 for col in columns}

    results: dict[str, float] = {}
    for col in columns:
        positive = df.filter(
            F.col(col).isNotNull() & (F.col(col) > 0)
        ).count()
        non_null = df.filter(F.col(col).isNotNull()).count()
        results[col] = positive / non_null if non_null > 0 else 1.0
    return results
