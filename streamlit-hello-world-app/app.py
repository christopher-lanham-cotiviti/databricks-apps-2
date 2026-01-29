import streamlit as st
import pandas as pd
from databricks.sdk import WorkspaceClient

st.set_page_config(layout="wide")
st.title("📊 Orders Over Time")

# --- CONFIG: SQL Warehouse ID ---
WAREHOUSE_ID = "a3008045957bf8cf"

@st.cache_data
def load_data():
    """Load orders data from Databricks Unity Catalog"""
    w = WorkspaceClient()
    
    query = """
    SELECT
        o_orderdate as order_date,
        SUM(o_totalprice) as daily_total
    FROM samples.tpch.orders
    GROUP BY o_orderdate
    ORDER BY o_orderdate
    LIMIT 1000
    """
    
    result = w.statement_execution.execute_statement(
        warehouse_id=WAREHOUSE_ID,
        statement=query,
        wait_timeout="50s"
    )
    
    if result.result and result.result.data_array:
        columns = [col.name for col in result.manifest.schema.columns]
        data = result.result.data_array
        df = pd.DataFrame(data, columns=columns)
        
        # Convert data types
        df['order_date'] = pd.to_datetime(df['order_date'])
        df['daily_total'] = pd.to_numeric(df['daily_total'])
        
        return df
    else:
        return pd.DataFrame()

# Load the data
with st.spinner("Loading data from Databricks..."):
    df = load_data()

if not df.empty:
    # Slider for threshold
    max_value = int(df["daily_total"].max())
    threshold = st.slider(
        "Set celebration threshold:",
        min_value=0,
        max_value=max_value,
        value=int(max_value * 0.75),
        format="$%d"
    )
    
    # Line chart
    st.line_chart(
        df,
        x="order_date",
        y="daily_total",
        height=400
    )
    
    # Progress bar showing how close we are to threshold
    current_max = df["daily_total"].max()
    progress = min(current_max / threshold, 1.0) if threshold > 0 else 1.0
    
    st.progress(progress)
    st.caption(f"Peak: ${current_max:,.0f} / Goal: ${threshold:,.0f}")
    
    # 🎉 Celebrate when threshold is reached
    if current_max >= threshold:
        st.success("🚀 Threshold reached!")
        st.balloons()
    
else:
    st.error("Unable to load data from Databricks")