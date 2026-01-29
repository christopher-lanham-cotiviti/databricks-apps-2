import streamlit as st
import pandas as pd
from databricks import sql

st.set_page_config(layout="wide")
st.title("📊 Orders Over Time (Databricks App Demo)")

# --- CONFIG: from SQL Warehouse → Connection details ---
SERVER_HOSTNAME = "dbc-0fa270fd-fb38.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/a3008045957bf8cf"

@st.cache_data
def load_data():
    with sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH
    ) as conn:
        query = """
        SELECT
            order_date,
            daily_total
        FROM samples.tpch.orders_by_day
        ORDER BY order_date
        """
        return pd.read_sql(query, conn)

df = load_data()

st.line_chart(
    df,
    x="order_date",
    y="daily_total"
)

# 🎉 Celebrate when crossing a threshold interactively
threshold = st.slider(
    "Celebrate when daily total exceeds:",
    min_value=0,
    max_value=int(df["daily_total"].max()),
    value=int(df["daily_total"].max() * 0.75)
)

if df["daily_total"].max() >= threshold:
    st.success("🚀 Threshold reached!")
    st.balloons()
