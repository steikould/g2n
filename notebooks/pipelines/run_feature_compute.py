# Databricks notebook source
# MAGIC %md
# MAGIC # Feature Compute Pipeline
# MAGIC
# MAGIC Orchestrates feature materialization across all domains.
# MAGIC Writes computed features to Delta tables in the Feature Engineering Layer.

# COMMAND ----------

dbutils.widgets.text("domain", "all", "Feature domain to compute (or 'all')")
domain = dbutils.widgets.get("domain")

# COMMAND ----------

from g2n.common.connectors.s3 import silver_path
from g2n.common.logging import configure_logging, new_correlation_id
from g2n.features.store import write_feature_table

configure_logging(json_format=True)
correlation_id = new_correlation_id()

# COMMAND ----------

# Load Silver data
transactions = spark.read.parquet(silver_path("edi_867"))
chargebacks = spark.read.parquet(silver_path("edi_844"))
contracts = spark.read.parquet(silver_path("contracts"))

# COMMAND ----------

# Compute features by domain
if domain in ("all", "customer_behavior"):
    from g2n.features.domains.customer_behavior import (
        compute_ordering_regularity,
        compute_purchase_velocity,
    )

    velocity = compute_purchase_velocity(transactions)
    write_feature_table(velocity, "customer_behavior", "purchase_velocity")

    regularity = compute_ordering_regularity(transactions)
    write_feature_table(regularity, "customer_behavior", "ordering_regularity")

# COMMAND ----------

if domain in ("all", "contract_complexity"):
    from g2n.features.domains.contract_complexity import compute_contract_complexity

    complexity = compute_contract_complexity(contracts)
    write_feature_table(complexity, "contract_complexity", "contract_features")

# COMMAND ----------

if domain in ("all", "rebate_dynamics"):
    from g2n.features.domains.rebate_dynamics import compute_claim_submission_lag

    lag = compute_claim_submission_lag(chargebacks)
    write_feature_table(lag, "rebate_dynamics", "claim_submission_lag")

# COMMAND ----------

print("Feature compute pipeline complete.")
