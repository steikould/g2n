"""Feature quality monitoring — drift detection, null rates, freshness checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING

from pyspark.sql import functions as F

from g2n.common.logging import get_logger

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = get_logger(__name__)


@dataclass
class FeatureHealthReport:
    """Health metrics for a single feature table."""

    feature_name: str
    record_count: int
    null_rates: dict[str, float]
    mean_values: dict[str, float | None]
    stddev_values: dict[str, float | None]
    freshness_days: float | None


def compute_null_rates(df: DataFrame) -> dict[str, float]:
    """Compute null rate for every column in the DataFrame."""
    total = df.count()
    if total == 0:
        return {c: 1.0 for c in df.columns}

    rates: dict[str, float] = {}
    for col in df.columns:
        null_count = df.filter(F.col(col).isNull()).count()
        rates[col] = null_count / total
    return rates


def check_feature_health(
    df: DataFrame,
    feature_name: str,
    *,
    timestamp_col: str | None = None,
) -> FeatureHealthReport:
    """Generate a health report for a feature table.

    Args:
        df: Feature DataFrame.
        feature_name: Name of the feature.
        timestamp_col: Optional column to compute freshness from.
    """
    total = df.count()
    null_rates = compute_null_rates(df)

    numeric_cols = [
        f.name for f in df.schema.fields
        if str(f.dataType) in ("DoubleType()", "FloatType()", "IntegerType()", "LongType()")
    ]
    means: dict[str, float | None] = {}
    stddevs: dict[str, float | None] = {}
    for col in numeric_cols:
        stats = df.agg(F.mean(col), F.stddev(col)).collect()[0]
        means[col] = stats[0]
        stddevs[col] = stats[1]

    freshness: float | None = None
    if timestamp_col and timestamp_col in df.columns:
        max_ts = df.agg(F.max(timestamp_col)).collect()[0][0]
        if max_ts:
            from datetime import datetime, timezone

            now = datetime.now(timezone.utc)
            delta = now - max_ts.replace(tzinfo=timezone.utc)
            freshness = delta.total_seconds() / 86400

    report = FeatureHealthReport(
        feature_name=feature_name,
        record_count=total,
        null_rates=null_rates,
        mean_values=means,
        stddev_values=stddevs,
        freshness_days=freshness,
    )
    logger.info("feature_health_checked", feature=feature_name, records=total)
    return report
