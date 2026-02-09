"""Leakage Investigation Console — Streamlit dashboard.

Enables analysts to review flagged leakage anomalies, investigate
individual claims, and track remediation progress.

Run: streamlit run src/g2n/dashboards/leakage_console.py
"""

from __future__ import annotations

import streamlit as st

st.set_page_config(page_title="Leakage Investigation Console", layout="wide")
st.title("Leakage Investigation Console")

# Sidebar filters
st.sidebar.header("Filters")
severity = st.sidebar.selectbox("Severity", ["All", "High", "Medium", "Low"])
distributor = st.sidebar.multiselect("Distributor", ["Distributor A", "Distributor B", "Distributor C"])
status_filter = st.sidebar.selectbox("Investigation Status", ["All", "Open", "In Progress", "Resolved"])

# Summary metrics
col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Total Flagged", "156", "+12 this week")
with col2:
    st.metric("Estimated Leakage", "$2.3M", "+$180K")
with col3:
    st.metric("Resolved", "89", "+7")

st.divider()

# Placeholder for investigation table
st.subheader("Flagged Claims")
st.info("Connect to Snowflake to load leakage-flagged claims with anomaly scores.")

st.subheader("Claim Detail View")
st.info("Select a claim above to view full details, contract terms, and transaction history.")

st.subheader("Distributor Error Rates")
st.info("Connect to Snowflake to load distributor-level error rate analysis.")
