# Databricks notebook source
# MAGIC %md
# MAGIC # Train Leakage Detection Model
# MAGIC
# MAGIC Entry point for the rebate leakage detection model training.

# COMMAND ----------

from g2n.common.logging import configure_logging
from g2n.features.sdk import FeatureSDK
from g2n.models.leakage_detection.train import LeakageDetectionModel

configure_logging(json_format=True)

# COMMAND ----------

# Load features
sdk = FeatureSDK(spark)
rebate_features = sdk.get_feature("rebate_dynamics", "claim_submission_lag")
complexity = sdk.get_feature("contract_complexity", "contract_features")

# Build training dataset
training_data = rebate_features.join(complexity, "customer_id", "left")

# COMMAND ----------

pdf = training_data.toPandas()

# COMMAND ----------

model = LeakageDetectionModel("leakage_detection_v1")
metrics = model.train_with_tracking(
    pdf,
    params={"contamination": 0.05, "n_estimators": 200},
    tags={"phase": "phase_2", "use_case": "leakage_detection"},
)
print(f"Training metrics: {metrics}")
