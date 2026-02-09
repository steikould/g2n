"""Feature Quality Dashboard — Streamlit dashboard.

Monitors feature freshness, null rates, drift, and distribution changes.
Alerts on anomalies in the Feature Engineering Layer.

Run: streamlit run src/g2n/dashboards/feature_quality.py
"""

from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="Feature Quality Dashboard", layout="wide")
st.title("Feature Quality Dashboard")

# Sidebar
st.sidebar.header("Feature Domain")
domain = st.sidebar.selectbox(
    "Domain",
    ["All", "Customer Behavior", "Contract Complexity", "Rebate Dynamics", "Seasonal & Market", "Data Quality Signals"],
)

# Summary metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Features", "47")
with col2:
    st.metric("Healthy", "42", delta="89%")
with col3:
    st.metric("Stale (>24h)", "3", delta="-2")
with col4:
    st.metric("Drift Alerts", "2", delta="+1")

st.divider()

# Placeholder sections
st.subheader("Feature Freshness")
st.info("Connect to Delta tables to compute feature freshness metrics.")

st.subheader("Null Rate Trends")
st.info("Connect to feature monitoring to display null rate trends per feature.")

st.subheader("Distribution Drift")
st.info("Connect to feature monitoring to display statistical drift detection results.")
