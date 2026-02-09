# Databricks notebook source
# MAGIC %md
# MAGIC # Silver ETL Pipeline
# MAGIC
# MAGIC Orchestrates source-specific adapters and the data quality gate
# MAGIC to transform Bronze data into canonical Silver format.

# COMMAND ----------

# Parameters (set via Databricks Jobs widget or dbutils.widgets)
dbutils.widgets.text("entity", "edi_867", "Source entity to process")
dbutils.widgets.text("date_partition", "", "Date partition (YYYY-MM-DD), empty for latest")

entity = dbutils.widgets.get("entity")
date_partition = dbutils.widgets.get("date_partition") or None

# COMMAND ----------

from g2n.common.connectors.s3 import bronze_path
from g2n.common.logging import configure_logging, new_correlation_id
from g2n.pipelines.silver.adapters.edi_867 import EDI867Adapter
from g2n.pipelines.silver.adapters.edi_844 import EDI844Adapter
from g2n.pipelines.silver.quality_gate import run_quality_gate

configure_logging(json_format=True)
correlation_id = new_correlation_id()

# COMMAND ----------

# Select adapter based on entity
ADAPTERS = {
    "edi_867": EDI867Adapter,
    "edi_844": EDI844Adapter,
}

if entity not in ADAPTERS:
    raise ValueError(f"Unknown entity: {entity}. Available: {list(ADAPTERS)}")

adapter = ADAPTERS[entity](spark)

# COMMAND ----------

# Load Bronze data
source = bronze_path(entity, date_partition=date_partition)
bronze_df = spark.read.parquet(source)
print(f"Loaded {bronze_df.count()} Bronze records for {entity}")

# COMMAND ----------

# Transform through adapter
silver_df = adapter.transform(bronze_df)

# COMMAND ----------

# Run quality gate
result = run_quality_gate(spark, silver_df, entity)
print(f"Passed: {result.passed_count}, Quarantined: {result.quarantined_count}")
print(f"Mean DQ Score: {result.mean_dq_score}")
