"""Contract Complexity feature domain.

Features: active tier count, multi-product flag, days to tier reset,
tier proximity percentage, contract overlap count.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def compute_contract_complexity(contracts: DataFrame) -> DataFrame:
    """Compute contract complexity features per customer.

    Args:
        contracts: Contract DataFrame with canonical schema.

    Returns:
        DataFrame with customer_id and complexity features.
    """
    active = contracts.filter(
        (F.col("effective_date") <= F.current_date())
        & (
            F.col("expiration_date").isNull()
            | (F.col("expiration_date") >= F.current_date())
        )
    )

    return active.groupBy("customer_id").agg(
        F.count("contract_id").alias("active_contract_count"),
        F.sum("tier_count").alias("total_active_tiers"),
        F.avg("tier_count").alias("avg_tiers_per_contract"),
        F.sum(F.when(F.col("multi_product_flag") == "Y", 1).otherwise(0)).alias(
            "multi_product_contract_count"
        ),
        F.max("discount_pct").alias("max_discount_pct"),
        F.avg("rebate_pct").alias("avg_rebate_pct"),
    )


def compute_days_to_expiration(contracts: DataFrame) -> DataFrame:
    """Compute days until nearest contract expiration per customer."""
    active = contracts.filter(
        F.col("expiration_date").isNotNull()
        & (F.col("expiration_date") >= F.current_date())
    )
    return active.groupBy("customer_id").agg(
        F.min(F.datediff("expiration_date", F.current_date())).alias(
            "days_to_nearest_expiration"
        ),
    )


def compute_contract_overlap(contracts: DataFrame) -> DataFrame:
    """Count overlapping contracts per customer (same product, overlapping dates)."""
    active = contracts.filter(
        (F.col("effective_date") <= F.current_date())
        & (
            F.col("expiration_date").isNull()
            | (F.col("expiration_date") >= F.current_date())
        )
    )
    self_join = (
        active.alias("a")
        .join(active.alias("b"), "customer_id")
        .filter(
            (F.col("a.contract_id") < F.col("b.contract_id"))
            & (F.col("a.product_id") == F.col("b.product_id"))
        )
    )
    return self_join.groupBy("customer_id").agg(
        F.count("*").alias("contract_overlap_count")
    )
