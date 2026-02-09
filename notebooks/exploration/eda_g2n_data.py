# Databricks notebook source
# MAGIC %md
# MAGIC # G2N Data Exploratory Analysis
# MAGIC
# MAGIC Initial profiling of G2N source data across Bronze and Silver layers.
# MAGIC Use this notebook to understand data distributions, quality issues,
# MAGIC and feature engineering opportunities.

# COMMAND ----------

from g2n.common.config import get_settings
from g2n.common.connectors.s3 import bronze_path, silver_path

settings = get_settings()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load Bronze Data

# COMMAND ----------

# Load a sample of Bronze transaction data
bronze_transactions = spark.read.parquet(bronze_path("edi_867"))
print(f"Bronze EDI 867 records: {bronze_transactions.count()}")
bronze_transactions.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Quality Profiling

# COMMAND ----------

# Profile null rates across columns
from g2n.quality.expectations.completeness import check_required_columns_present

null_rates = check_required_columns_present(
    bronze_transactions,
    required_columns=bronze_transactions.columns,
)
for col, rate in sorted(null_rates.items(), key=lambda x: x[1]):
    print(f"  {col}: {rate:.2%} non-null")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Distribution Analysis

# COMMAND ----------

display(bronze_transactions.describe())
