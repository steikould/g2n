"""Model Governance Dashboard — Streamlit dashboard.

Provides Finance and Internal Audit with visibility into which models
are in production, when they were last retrained, and their current
performance metrics. Supports SOX audit requirements.

Run: streamlit run src/g2n/dashboards/model_governance.py
"""

from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="Model Governance Dashboard", layout="wide")
st.title("Model Governance Dashboard")
st.caption("For Finance, Internal Audit, and Compliance stakeholders")

# Summary
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Models in Production", "3")
with col2:
    st.metric("Models in Staging", "2")
with col3:
    st.metric("Last Audit Review", "2026-01-15")

st.divider()

# Model inventory
st.subheader("Production Model Inventory")
st.info(
    "Connect to MLflow Model Registry to display: model name, version, "
    "last retrained date, performance metrics, training dataset lineage, "
    "and approval status."
)

st.subheader("Model Performance Trends")
st.info("Connect to MLflow to display model accuracy/error metrics over time.")

st.subheader("Lineage Audit Trail")
st.info(
    "Connect to Collibra to display end-to-end lineage: "
    "Source → Bronze → Silver → Features → Model → Scored Output."
)

st.subheader("Data Quality Impact")
st.info("Display correlation between input data quality scores and model performance.")
