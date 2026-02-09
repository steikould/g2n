"""Contract management system Silver adapter.

Normalizes contract terms data to the canonical contract schema.
Handles both system-exported and spreadsheet-based contract data.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from g2n.common.schemas.canonical import CONTRACT_SCHEMA
from g2n.pipelines.silver.base_adapter import BaseSilverAdapter

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class ContractsAdapter(BaseSilverAdapter):
    """Adapter for contract management system extracts."""

    @property
    def entity_name(self) -> str:
        return "contracts"

    @property
    def target_schema(self) -> StructType:
        return CONTRACT_SCHEMA

    def normalize(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("agreement_id").alias("contract_id"),
            F.col("account_id").alias("customer_id"),
            F.col("material_group").alias("product_id"),
            F.col("agreement_type").alias("contract_type"),
            F.to_date(F.col("start_date"), "yyyy-MM-dd").alias("effective_date"),
            F.to_date(F.col("end_date"), "yyyy-MM-dd").alias("expiration_date"),
            F.col("num_tiers").cast("int").alias("tier_count"),
            F.col("base_discount").cast("decimal(8,4)").alias("discount_pct"),
            F.col("rebate_rate").cast("decimal(8,4)").alias("rebate_pct"),
            F.when(F.col("bundle_flag") == "Y", "Y").otherwise("N").alias("multi_product_flag"),
            F.current_timestamp().alias("ingested_at"),
            F.lit(None).cast("double").alias("dq_score"),
        )

    def validate(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "_valid",
            (F.col("contract_id").isNotNull())
            & (F.col("effective_date").isNotNull())
            & (
                F.col("expiration_date").isNull()
                | (F.col("expiration_date") > F.col("effective_date"))
            )
            & (F.col("discount_pct").isNull() | (F.col("discount_pct").between(0, 1))),
        )
