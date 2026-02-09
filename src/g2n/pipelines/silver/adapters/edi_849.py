"""EDI 849 (Response to Product Transfer Account Adjustment) Silver adapter.

Normalizes chargeback response data — the distributor's reply to 844 chargebacks.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark.sql import functions as F
from pyspark.sql.types import StructType

from g2n.common.schemas.canonical import CHARGEBACK_SCHEMA
from g2n.pipelines.silver.base_adapter import BaseSilverAdapter

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class EDI849Adapter(BaseSilverAdapter):
    """Adapter for EDI 849 chargeback responses."""

    @property
    def entity_name(self) -> str:
        return "edi_849"

    @property
    def target_schema(self) -> StructType:
        return CHARGEBACK_SCHEMA

    def normalize(self, df: DataFrame) -> DataFrame:
        return df.select(
            F.col("response_ref").alias("claim_id"),
            F.lit("edi_849").alias("source_system"),
            F.col("dist_id").alias("distributor_id"),
            F.col("end_cust_id").alias("customer_id"),
            F.col("item_id").alias("product_id"),
            F.col("contract_ref").alias("contract_id"),
            F.to_date(F.col("response_date"), "yyyyMMdd").alias("claim_date"),
            F.col("orig_claim_ref").alias("transaction_ref_id"),
            F.col("approved_amount").cast("decimal(18,4)").alias("claim_amount"),
            F.col("response_code").alias("claim_status"),
            F.lit(None).cast("int").alias("submission_count"),
            F.current_timestamp().alias("ingested_at"),
            F.lit(None).cast("double").alias("dq_score"),
        )

    def validate(self, df: DataFrame) -> DataFrame:
        return df.withColumn(
            "_valid",
            (F.col("claim_id").isNotNull())
            & (F.col("claim_date").isNotNull()),
        )
