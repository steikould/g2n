"""Customer Behavior feature domain.

Features: purchase velocity (30/60/90d rolling), ordering pattern regularity,
species mix entropy, buying group loyalty index.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Window, functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def compute_purchase_velocity(
    transactions: DataFrame,
    windows: list[int] | None = None,
) -> DataFrame:
    """Compute rolling purchase velocity per customer.

    Args:
        transactions: Transaction DataFrame with customer_id, transaction_date, gross_amount.
        windows: Rolling window sizes in days. Defaults to [30, 60, 90].

    Returns:
        DataFrame with customer_id and velocity columns.
    """
    if windows is None:
        windows = [30, 60, 90]

    result = transactions.select("customer_id").distinct()

    for days in windows:
        window = (
            Window.partitionBy("customer_id")
            .orderBy(F.col("transaction_date").cast("long"))
            .rangeBetween(-days * 86400, 0)
        )
        velocity = transactions.withColumn(
            f"purchase_velocity_{days}d",
            F.sum("gross_amount").over(window),
        )
        agg = velocity.groupBy("customer_id").agg(
            F.last(f"purchase_velocity_{days}d").alias(f"purchase_velocity_{days}d")
        )
        result = result.join(agg, "customer_id", "left")

    return result


def compute_ordering_regularity(transactions: DataFrame) -> DataFrame:
    """Compute ordering regularity (coefficient of variation of inter-order days)."""
    windowed = transactions.withColumn(
        "_prev_date",
        F.lag("transaction_date").over(
            Window.partitionBy("customer_id").orderBy("transaction_date")
        ),
    )
    gaps = windowed.filter(F.col("_prev_date").isNotNull()).withColumn(
        "_gap_days", F.datediff("transaction_date", "_prev_date")
    )
    return gaps.groupBy("customer_id").agg(
        F.mean("_gap_days").alias("avg_order_gap_days"),
        F.stddev("_gap_days").alias("stddev_order_gap_days"),
        (F.stddev("_gap_days") / F.mean("_gap_days")).alias("ordering_regularity_cv"),
    )


def compute_species_mix_entropy(transactions: DataFrame) -> DataFrame:
    """Compute species-mix entropy from product category distribution.

    Requires a `product_category` column (species-level grouping).
    Higher entropy = more diverse purchasing.
    """
    totals = transactions.groupBy("customer_id").agg(
        F.count("*").alias("_total")
    )
    by_category = transactions.groupBy("customer_id", "product_category").agg(
        F.count("*").alias("_cat_count")
    )
    probs = by_category.join(totals, "customer_id").withColumn(
        "_p", F.col("_cat_count") / F.col("_total")
    )
    entropy = probs.groupBy("customer_id").agg(
        (-F.sum(F.col("_p") * F.log2("_p"))).alias("species_mix_entropy")
    )
    return entropy
