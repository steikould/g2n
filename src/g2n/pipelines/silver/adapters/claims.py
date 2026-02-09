"""Claims/chargeback processor Silver adapter.

Normalizes claims data from chargeback processing systems and buying group portals.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from g2n.common.schemas.canonical import CHARGEBACK_SCHEMA
from g2n.pipelines.silver.base_adapter import BaseSilverAdapter

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class ClaimsAdapter(BaseSilverAdapter):
    """Adapter for claims processor and buying group portal data."""

    @property
    def entity_name(self) -> str:
        return "claims"

    @property
    def target_schema(self) -> StructType:
        return CHARGEBACK_SCHEMA

    def normalize(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("claim_number").alias("claim_id"),
            F.lit("claims_processor").alias("source_system"),
            F.col("wholesaler_id").alias("distributor_id"),
            F.col("member_id").alias("customer_id"),
            F.col("product_code").alias("product_id"),
            F.col("agreement_number").alias("contract_id"),
            F.to_date(F.col("submitted_date"), "yyyy-MM-dd").alias("claim_date"),
            F.col("invoice_ref").alias("transaction_ref_id"),
            F.col("claimed_amount").cast("decimal(18,4)").alias("claim_amount"),
            F.col("processing_status").alias("claim_status"),
            F.col("submit_count").cast("int").alias("submission_count"),
            F.current_timestamp().alias("ingested_at"),
            F.lit(None).cast("double").alias("dq_score"),
        )

    def validate(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "_valid",
            (F.col("claim_id").isNotNull())
            & (F.col("claim_amount") > 0)
            & (F.col("claim_date").isNotNull())
            & (F.col("distributor_id").isNotNull()),
        )
