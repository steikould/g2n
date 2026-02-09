"""EDI 867 (Product Transfer and Resale Report) Silver adapter.

Normalizes distributor product transfer data to the canonical transaction schema.
Different distributors may use varying field names, product codes, and date formats.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from g2n.common.schemas.canonical import TRANSACTION_SCHEMA
from g2n.pipelines.silver.base_adapter import BaseSilverAdapter

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class EDI867Adapter(BaseSilverAdapter):
    """Adapter for EDI 867 product transfer and resale reports."""

    @property
    def entity_name(self) -> str:
        return "edi_867"

    @property
    def target_schema(self) -> StructType:
        return TRANSACTION_SCHEMA

    def normalize(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("ref_id").alias("transaction_id"),
            F.lit("edi_867").alias("source_system"),
            F.col("dist_id").alias("distributor_id"),
            F.col("ship_to_id").alias("customer_id"),
            F.col("item_id").alias("product_id"),
            F.col("ndc").alias("ndc_code"),
            F.to_date(F.col("txn_date"), "yyyyMMdd").alias("transaction_date"),
            F.col("qty").cast("int").alias("quantity"),
            F.col("unit_price").cast("decimal(18,4)").alias("unit_price"),
            (F.col("qty") * F.col("unit_price")).cast("decimal(18,4)").alias("gross_amount"),
            F.lit("transfer").alias("transaction_type"),
            F.current_timestamp().alias("ingested_at"),
            F.lit(None).cast("double").alias("dq_score"),
        )

    def validate(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "_valid",
            (F.col("transaction_id").isNotNull())
            & (F.col("quantity") > 0)
            & (F.col("transaction_date").isNotNull()),
        )
