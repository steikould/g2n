"""Canonical Silver-layer schemas for G2N entities.

These StructType definitions represent the standardized format that all
source-specific adapters must normalize to in the Silver layer.
"""

from __future__ import annotations

from pyspark.sql.types import (
    DateType,
    DecimalType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("source_system", StringType(), nullable=False),
    StructField("distributor_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("ndc_code", StringType(), nullable=True),
    StructField("transaction_date", DateType(), nullable=False),
    StructField("quantity", IntegerType(), nullable=False),
    StructField("unit_price", DecimalType(18, 4), nullable=False),
    StructField("gross_amount", DecimalType(18, 4), nullable=False),
    StructField("transaction_type", StringType(), nullable=False),
    StructField("ingested_at", TimestampType(), nullable=False),
    StructField("dq_score", DoubleType(), nullable=True),
])

CHARGEBACK_SCHEMA = StructType([
    StructField("claim_id", StringType(), nullable=False),
    StructField("source_system", StringType(), nullable=False),
    StructField("distributor_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("contract_id", StringType(), nullable=True),
    StructField("claim_date", DateType(), nullable=False),
    StructField("transaction_ref_id", StringType(), nullable=True),
    StructField("claim_amount", DecimalType(18, 4), nullable=False),
    StructField("claim_status", StringType(), nullable=False),
    StructField("submission_count", IntegerType(), nullable=True),
    StructField("ingested_at", TimestampType(), nullable=False),
    StructField("dq_score", DoubleType(), nullable=True),
])

CONTRACT_SCHEMA = StructType([
    StructField("contract_id", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=True),
    StructField("contract_type", StringType(), nullable=False),
    StructField("effective_date", DateType(), nullable=False),
    StructField("expiration_date", DateType(), nullable=True),
    StructField("tier_count", IntegerType(), nullable=True),
    StructField("discount_pct", DecimalType(8, 4), nullable=True),
    StructField("rebate_pct", DecimalType(8, 4), nullable=True),
    StructField("multi_product_flag", StringType(), nullable=True),
    StructField("ingested_at", TimestampType(), nullable=False),
    StructField("dq_score", DoubleType(), nullable=True),
])

REBATE_ACCRUAL_SCHEMA = StructType([
    StructField("accrual_id", StringType(), nullable=False),
    StructField("period", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("product_id", StringType(), nullable=False),
    StructField("accrual_amount", DecimalType(18, 4), nullable=False),
    StructField("actual_amount", DecimalType(18, 4), nullable=True),
    StructField("variance", DecimalType(18, 4), nullable=True),
    StructField("accrual_type", StringType(), nullable=False),
    StructField("ingested_at", TimestampType(), nullable=False),
    StructField("dq_score", DoubleType(), nullable=True),
])
