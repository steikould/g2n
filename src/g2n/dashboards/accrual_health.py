"""G2N Accrual Health Monitor — Streamlit dashboard.

Displays accrual forecast accuracy, variance trends, and alerting
for over/under accrual conditions across products and customers.

Run: streamlit run src/g2n/dashboards/accrual_health.py
"""

from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="G2N Accrual Health Monitor", layout="wide")
st.title("G2N Accrual Health Monitor")

# Sidebar filters
st.sidebar.header("Filters")
time_range = st.sidebar.selectbox("Time Range", ["Last 30 Days", "Last 90 Days", "YTD", "Custom"])
product_filter = st.sidebar.multiselect("Product Category", ["Parasiticide", "Vaccine", "Antibiotic", "Other"])
customer_segment = st.sidebar.multiselect("Customer Segment", ["Large Vet Chain", "Independent Clinic", "Buying Group", "Distributor"])

# Main content
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Accrual Accuracy", "87.3%", "+2.1%")
with col2:
    st.metric("Mean Variance", "$124K", "-$31K")
with col3:
    st.metric("Over-Accrual Count", "23", "-5")
with col4:
    st.metric("Under-Accrual Count", "8", "+2")

st.divider()

# Placeholder sections for Snowflake-connected data
st.subheader("Accrual Variance Trend")
st.info("Connect to Snowflake to load accrual variance time series data.")

st.subheader("Top Variance Drivers")
st.info("Connect to Snowflake to load customer/product-level variance breakdown.")

st.subheader("Model Performance")
st.info("Connect to MLflow to load model accuracy metrics over time.")
