"""SAP/Oracle ERP Silver adapter.

Normalizes ERP sales transaction data to the canonical transaction schema.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from g2n.common.schemas.canonical import TRANSACTION_SCHEMA
from g2n.pipelines.silver.base_adapter import BaseSilverAdapter

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class ERPAdapter(BaseSilverAdapter):
    """Adapter for SAP/Oracle ERP transaction extracts."""

    @property
    def entity_name(self) -> str:
        return "erp"

    @property
    def target_schema(self) -> StructType:
        return TRANSACTION_SCHEMA

    def normalize(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("doc_number").alias("transaction_id"),
            F.lit("erp").alias("source_system"),
            F.coalesce(F.col("ship_from_plant"), F.lit("DIRECT")).alias("distributor_id"),
            F.col("sold_to_party").alias("customer_id"),
            F.col("material_number").alias("product_id"),
            F.col("ndc_number").alias("ndc_code"),
            F.to_date(F.col("billing_date"), "yyyy-MM-dd").alias("transaction_date"),
            F.col("billed_quantity").cast("int").alias("quantity"),
            F.col("net_price").cast("decimal(18,4)").alias("unit_price"),
            F.col("net_value").cast("decimal(18,4)").alias("gross_amount"),
            F.col("doc_type").alias("transaction_type"),
            F.current_timestamp().alias("ingested_at"),
            F.lit(None).cast("double").alias("dq_score"),
        )

    def validate(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "_valid",
            (F.col("transaction_id").isNotNull())
            & (F.col("customer_id").isNotNull())
            & (F.col("gross_amount").isNotNull()),
        )
