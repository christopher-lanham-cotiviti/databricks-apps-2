import streamlit as st
import pandas as pd
from databricks.sdk import WorkspaceClient

# -----------------------------
# Page setup
# -----------------------------
st.set_page_config(layout="wide")
st.title("📊 Orders Over Time (Databricks App Demo)")

# -----------------------------
# Databricks client
# -----------------------------
# Auth is automatic via the App's Service Principal
w = WorkspaceClient()

# -----------------------------
# Choose a SQL Warehouse
# -----------------------------
# Easiest safe approach: pick the first RUNNING warehouse
warehouses = list(w.sql.warehouses.list())

running_wh = next(
    (wh for wh in warehouses if wh.state == "RUNNING"),
    None
)

if not running_wh:
    st.error("❌ No running SQL Warehouse found.")
    st.stop()

warehouse_id = running_wh.id
st.caption(f"Using warehouse: **{running_wh.name}**")

# -----------------------------
# SQL query (Unity Catalog-safe)
# -----------------------------
QUERY = """
SELECT
  date_trunc('day', order_date) AS order_date,
  COUNT(*) AS order_count
FROM samples.tpch.orders
GROUP BY 1
ORDER BY 1
"""

# -----------------------------
# Execute query
# -----------------------------
with st.spinner("Querying Databricks…"):
    result = w.sql.execute_statement(
        warehouse_id=warehouse_id,
        statement=QUERY,
    )

# Convert to Pandas
columns = [c.name for c in result.manifest.schema.columns]
rows = [r.values for r in result.result.data_array]
df = pd.DataFrame(rows, columns=columns)

# -----------------------------
# Display chart
# -----------------------------
st.subheader("Orders per Day")

st.line_chart(
    df,
    x="order_date",
    y="order_count",
    height=500,
)

# -----------------------------
# 🎉 Celebration logic
# -----------------------------
total_orders = df["order_count"].sum()

st.metric("Total Orders", f"{total_orders:,}")

if total_orders > 10_000:
    st.success("🎉 Big milestone reached!")
    st.balloons()
