"""EDI 844 (Product Transfer Account Adjustment / Chargeback) Silver adapter.

Normalizes chargeback claim data to the canonical chargeback schema.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from g2n.common.schemas.canonical import CHARGEBACK_SCHEMA
from g2n.pipelines.silver.base_adapter import BaseSilverAdapter

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class EDI844Adapter(BaseSilverAdapter):
    """Adapter for EDI 844 chargeback claims."""

    @property
    def entity_name(self) -> str:
        return "edi_844"

    @property
    def target_schema(self) -> StructType:
        return CHARGEBACK_SCHEMA

    def normalize(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("claim_ref").alias("claim_id"),
            F.lit("edi_844").alias("source_system"),
            F.col("dist_id").alias("distributor_id"),
            F.col("end_cust_id").alias("customer_id"),
            F.col("item_id").alias("product_id"),
            F.col("contract_ref").alias("contract_id"),
            F.to_date(F.col("claim_date"), "yyyyMMdd").alias("claim_date"),
            F.col("orig_txn_ref").alias("transaction_ref_id"),
            F.col("cb_amount").cast("decimal(18,4)").alias("claim_amount"),
            F.col("status_code").alias("claim_status"),
            F.col("submission_cnt").cast("int").alias("submission_count"),
            F.current_timestamp().alias("ingested_at"),
            F.lit(None).cast("double").alias("dq_score"),
        )

    def validate(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "_valid",
            (F.col("claim_id").isNotNull())
            & (F.col("claim_amount") > 0)
            & (F.col("claim_date").isNotNull()),
        )
