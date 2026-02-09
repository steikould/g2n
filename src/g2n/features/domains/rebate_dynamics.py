"""Rebate Dynamics feature domain.

Features: rebate-to-sales ratio trend, claim submission lag distribution,
historical over/under payment rate by distributor.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Window, functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def compute_rebate_to_sales_ratio(
    accruals: DataFrame,
    transactions: DataFrame,
) -> DataFrame:
    """Compute rebate-to-sales ratio per customer and period."""
    sales = transactions.groupBy("customer_id", F.date_format("transaction_date", "yyyy-MM").alias("period")).agg(
        F.sum("gross_amount").alias("total_sales"),
    )
    rebates = accruals.groupBy("customer_id", "period").agg(
        F.sum("accrual_amount").alias("total_rebate"),
    )
    joined = sales.join(rebates, ["customer_id", "period"], "left")
    return joined.withColumn(
        "rebate_to_sales_ratio",
        F.coalesce(F.col("total_rebate") / F.col("total_sales"), F.lit(0.0)),
    )


def compute_claim_submission_lag(chargebacks: DataFrame) -> DataFrame:
    """Compute claim-to-transaction time delta statistics per distributor."""
    with_lag = chargebacks.filter(
        F.col("transaction_ref_id").isNotNull()
    ).withColumn(
        "submission_lag_days",
        F.datediff("claim_date", F.lit("1970-01-01")),  # placeholder; join with txn date
    )
    return with_lag.groupBy("distributor_id").agg(
        F.avg("submission_count").alias("avg_submission_count"),
        F.max("submission_count").alias("max_submission_count"),
    )


def compute_over_under_payment_rate(accruals: DataFrame) -> DataFrame:
    """Compute historical over/under payment rates.

    Requires accrual_amount (forecast) and actual_amount columns.
    """
    with_variance = accruals.filter(F.col("actual_amount").isNotNull())
    return with_variance.groupBy("customer_id").agg(
        F.avg("variance").alias("avg_accrual_variance"),
        F.sum(F.when(F.col("variance") > 0, 1).otherwise(0)).alias("over_accrual_count"),
        F.sum(F.when(F.col("variance") < 0, 1).otherwise(0)).alias("under_accrual_count"),
        F.count("*").alias("total_accrual_periods"),
    )


def compute_rebate_velocity(
    accruals: DataFrame,
    windows: list[int] | None = None,
) -> DataFrame:
    """Compute rolling rebate velocity (3/6/12 month windows)."""
    if windows is None:
        windows = [3, 6, 12]

    result = accruals.select("customer_id").distinct()
    for months in windows:
        w = (
            Window.partitionBy("customer_id")
            .orderBy("period")
            .rowsBetween(-months + 1, 0)
        )
        vel = accruals.withColumn(
            f"rebate_velocity_{months}m",
            F.sum("accrual_amount").over(w),
        )
        agg = vel.groupBy("customer_id").agg(
            F.last(f"rebate_velocity_{months}m").alias(f"rebate_velocity_{months}m")
        )
        result = result.join(agg, "customer_id", "left")
    return result
