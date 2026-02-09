"""Timeliness checks — SLA monitoring, staleness scoring."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def check_sla_compliance(
    df: DataFrame,
    event_date_col: str,
    received_date_col: str,
    sla_days: int,
) -> float:
    """Check fraction of records arriving within SLA.

    Returns:
        Compliance rate (0.0–1.0).
    """
    total = df.count()
    if total == 0:
        return 1.0

    within_sla = df.filter(
        F.datediff(F.col(received_date_col), F.col(event_date_col)) <= sla_days
    ).count()
    return within_sla / total


def add_staleness_score(
    df: DataFrame,
    date_column: str,
    *,
    max_staleness_days: int = 90,
) -> DataFrame:
    """Add a _staleness_score column (0.0 = fresh, 1.0 = maximally stale).

    Scores are clamped to [0, 1] based on days since the record's date.
    """
    return df.withColumn(
        "_staleness_score",
        F.least(
            F.datediff(F.current_date(), F.col(date_column)) / F.lit(max_staleness_days),
            F.lit(1.0),
        ),
    )
