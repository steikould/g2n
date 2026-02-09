# Databricks notebook source
# MAGIC %md
# MAGIC # Train Accrual Forecast Model
# MAGIC
# MAGIC Entry point for the accrual forecast model training Databricks Job.

# COMMAND ----------

from g2n.common.logging import configure_logging
from g2n.features.sdk import FeatureSDK
from g2n.models.accrual_forecast.train import AccrualForecastModel

configure_logging(json_format=True)

# COMMAND ----------

# Load features
sdk = FeatureSDK(spark)
velocity = sdk.get_feature("customer_behavior", "purchase_velocity")
complexity = sdk.get_feature("contract_complexity", "contract_features")

# Join features into training dataset
training_data = velocity.join(complexity, "customer_id", "left")

# COMMAND ----------

# Convert to pandas for sklearn
pdf = training_data.toPandas()

# Use DQ score as sample weights (quality-weighted training)
sample_weights = pdf.get("dq_score", None)
if sample_weights is not None:
    sample_weights = sample_weights / 100.0  # normalize to 0-1

# COMMAND ----------

# Train model with MLflow tracking
model = AccrualForecastModel("accrual_forecast_v1")
metrics = model.train_with_tracking(
    pdf,
    params={"n_estimators": 200, "max_depth": 6},
    sample_weights=sample_weights,
    tags={"phase": "phase_1", "use_case": "accrual_forecast"},
)
print(f"Training metrics: {metrics}")
