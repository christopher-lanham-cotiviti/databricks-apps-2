import streamlit as st
import pandas as pd
from databricks.sdk import WorkspaceClient

st.set_page_config(layout="wide")
st.title("📊 Orders Over Time")

# --- CONFIG: SQL Warehouse ID ---
WAREHOUSE_ID = "a3008045957bf8cf"

# Set the celebration threshold
CELEBRATION_THRESHOLD = 300_000_000  # $300M

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

# Initialize session state to track previous slider value
if 'previous_slider_value' not in st.session_state:
    st.session_state.previous_slider_value = 200_000_000  # Initial value
if 'balloons_fired' not in st.session_state:
    st.session_state.balloons_fired = False

# Load the data
with st.spinner("Loading data from Databricks..."):
    df = load_data()

if not df.empty:
    # Calculate max and round to nearest 10 million for clean intervals
    max_value = df["daily_total"].max()
    max_rounded = int((max_value // 10_000_000 + 1) * 10_000_000)
    
    # Slider starts at $200M
    slider_value = st.slider(
        "Adjust threshold to celebrate:",
        min_value=0,
        max_value=max_rounded,
        value=200_000_000,  # Slider starts at $200M
        step=10_000_000,
        format="$%d"
    )
    
    # Line chart
    st.line_chart(
        df,
        x="order_date",
        y="daily_total",
        height=400
    )
    
    # Progress bar showing progress toward the threshold
    peak_value = df["daily_total"].max()
    progress = min(peak_value / slider_value, 1.0) if slider_value > 0 else 1.0
    
    st.progress(progress)
    
    # Format numbers in millions for readability
    peak_millions = peak_value / 1_000_000
    slider_millions = slider_value / 1_000_000
    
    st.caption(f"Peak: ${peak_millions:.1f}M / Goal: ${slider_millions:.1f}M")
    
    # Check if slider just crossed the $300M threshold
    crossed_threshold = (
        st.session_state.previous_slider_value < CELEBRATION_THRESHOLD 
        and slider_value >= CELEBRATION_THRESHOLD
    )
    
    # 🎉 Fire balloons when crossing $300M threshold
    if crossed_threshold and not st.session_state.balloons_fired:
        st.success("🚀 Threshold reached!")
        st.balloons()
        st.session_state.balloons_fired = True
    elif slider_value >= CELEBRATION_THRESHOLD and st.session_state.balloons_fired:
        st.success("🚀 Threshold reached!")
    
    # Reset balloons_fired if we go back below threshold
    if slider_value < CELEBRATION_THRESHOLD:
        st.session_state.balloons_fired = False
    
    # Update previous slider value
    st.session_state.previous_slider_value = slider_value
    
else:
    st.error("Unable to load data from Databricks")