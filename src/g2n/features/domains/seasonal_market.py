"""Seasonal & Market feature domain.

Features: parasiticide seasonal index, vaccine season indicator,
regional livestock cycle, YoY growth rate.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import Window, functions as F

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


def compute_seasonal_index(
    transactions: DataFrame,
    product_category: str = "parasiticide",
) -> DataFrame:
    """Compute monthly seasonal index for a product category.

    The seasonal index is the ratio of a month's average sales to the
    overall monthly average (1.0 = average month).
    """
    filtered = transactions.filter(F.col("product_category") == product_category)
    monthly = filtered.groupBy(
        F.month("transaction_date").alias("month")
    ).agg(
        F.avg("gross_amount").alias("avg_monthly_sales"),
    )
    overall_avg = monthly.agg(F.avg("avg_monthly_sales")).collect()[0][0] or 1.0
    return monthly.withColumn(
        f"{product_category}_seasonal_index",
        F.col("avg_monthly_sales") / F.lit(overall_avg),
    )


def compute_yoy_growth_rate(transactions: DataFrame) -> DataFrame:
    """Compute year-over-year growth rate per customer and product."""
    yearly = transactions.groupBy(
        "customer_id",
        "product_id",
        F.year("transaction_date").alias("year"),
    ).agg(
        F.sum("gross_amount").alias("annual_sales"),
    )
    prev_year = yearly.withColumn("year", F.col("year") + 1).withColumnRenamed(
        "annual_sales", "prev_year_sales"
    )
    joined = yearly.join(prev_year, ["customer_id", "product_id", "year"], "left")
    return joined.withColumn(
        "yoy_growth_rate",
        F.when(
            F.col("prev_year_sales").isNotNull() & (F.col("prev_year_sales") > 0),
            (F.col("annual_sales") - F.col("prev_year_sales")) / F.col("prev_year_sales"),
        ),
    )


def add_vaccine_season_indicator(df: DataFrame, date_col: str) -> DataFrame:
    """Add a binary vaccine season indicator based on month.

    Typical animal vaccine seasons: spring (Mar-May) and fall (Sep-Nov).
    """
    return df.withColumn(
        "is_vaccine_season",
        F.month(date_col).isin([3, 4, 5, 9, 10, 11]),
    )
