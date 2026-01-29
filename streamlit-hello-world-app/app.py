import streamlit as st
import pandas as pd

# ----------------------------
# Page config
# ----------------------------
st.set_page_config(
    page_title="Orders Over Time",
    layout="wide"
)

# ----------------------------
# Header
# ----------------------------
st.title("📊 Orders Over Time (Databricks App Demo)")
st.caption(
    "Demo app using a curated snapshot of production-like order data. "
    "In production, this data would be sourced live from Databricks."
)

# ----------------------------
# Load data (local, permission-free)
# ----------------------------
@st.cache_data
def load_data():
    return pd.read_csv(
        "streamlit-hello-world-app/orders_over_time.csv",
        parse_dates=["order_date"]
    )

df = load_data()

# ----------------------------
# Controls
# ----------------------------
st.subheader("Filters")

min_date, max_date = df["order_date"].min(), df["order_date"].max()
date_range = st.date_input(
    "Date range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date
)

filtered_df = df[
    (df["order_date"] >= pd.to_datetime(date_range[0])) &
    (df["order_date"] <= pd.to_datetime(date_range[1]))
]

# ----------------------------
# KPIs
# ----------------------------
col1, col2, col3 = st.columns(3)

col1.metric(
    "Total Orders",
    int(filtered_df["order_count"].sum())
)

col2.metric(
    "Peak Daily Revenue",
    f"${filtered_df['daily_total'].max():,.0f}"
)

col3.metric(
    "Avg Daily Revenue",
    f"${filtered_df['daily_total'].mean():,.0f}"
)

# ----------------------------
# Chart
# ----------------------------
st.subheader("Revenue Trend")

st.line_chart(
    filtered_df,
    x="order_date",
    y="daily_total",
    height=450
)

# ----------------------------
# 🎉 Celebration logic
# ----------------------------
PEAK_REVENUE_GOAL = 3000

if filtered_df["daily_total"].max() >= PEAK_REVENUE_GOAL:
    st.success("🚀 Revenue goal exceeded!")
    st.balloons()

# ----------------------------
# Footer note
# ----------------------------
with st.expander("ℹ️ About this demo"):
    st.write(
        """
        - This app runs entirely within the Databricks Apps runtime
        - Data is loaded from a version-controlled snapshot for demo purposes
        - Live SQL Warehouse connectivity can be enabled once permissions are granted
        - No Spark or warehouse dependency required for UI development
        """
    )
