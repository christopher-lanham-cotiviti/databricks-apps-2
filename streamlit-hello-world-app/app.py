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

# Initialize session state to track if threshold was previously met
if 'previous_threshold_met' not in st.session_state:
    st.session_state.previous_threshold_met = False

# Load the data
with st.spinner("Loading data from Databricks..."):
    df = load_data()

if not df.empty:
    # Calculate max and round to nearest 10 million for clean intervals
    max_value = df["daily_total"].max()
    max_rounded = int((max_value // 10_000_000 + 1) * 10_000_000)
    
    # Slider for threshold with clean 10 million intervals
    # Start at $200M, balloons fire when reaching $300M
    threshold = st.slider(
        "Set celebration threshold:",
        min_value=0,
        max_value=max_rounded,
        value=200_000_000,  # Start at $200M
        step=10_000_000,  # 10 million increments
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
    
    # Format numbers in millions for readability
    peak_millions = current_max / 1_000_000
    goal_millions = threshold / 1_000_000
    
    st.caption(f"Peak: ${peak_millions:.1f}M / Goal: ${goal_millions:.1f}M")
    
    # Check if threshold is currently met
    threshold_currently_met = current_max >= threshold
    
    # 🎉 Celebrate only when crossing from not met to met
    if threshold_currently_met and not st.session_state.previous_threshold_met:
        st.success("🚀 Threshold reached!")
        st.balloons()
    elif threshold_currently_met:
        st.success("🚀 Threshold reached!")
    
    # Update session state for next interaction
    st.session_state.previous_threshold_met = threshold_currently_met
    
else:
    st.error("Unable to load data from Databricks")