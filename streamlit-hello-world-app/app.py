import streamlit as st
import pandas as pd
from databricks.sdk import WorkspaceClient

st.set_page_config(layout="wide")
st.title("📊 Orders Over Time (Databricks App Demo)")

# --- CONFIG: SQL Warehouse ID ---
WAREHOUSE_ID = "a3008045957bf8cf"

@st.cache_data
def load_data():
    """Load orders data from Databricks Unity Catalog"""
    try:
        # Connect using Databricks SDK (handles OAuth automatically)
        w = WorkspaceClient()
        
        # First, let's check what catalogs and tables are available
        st.info("Checking available tables...")
        
        # Try to list available tables in samples catalog
        try:
            catalog_check = w.statement_execution.execute_statement(
                warehouse_id=WAREHOUSE_ID,
                statement="SHOW TABLES IN samples.tpch",
                wait_timeout="30s"
            )
            
            if catalog_check.result and catalog_check.result.data_array:
                st.success("✅ Found tables in samples.tpch")
                st.write("Available tables:", catalog_check.result.data_array[:10])  # Show first 10
        except:
            st.warning("Cannot access samples.tpch catalog")
        
        # Use a table that definitely exists - the orders table (not aggregated)
        query = """
        SELECT
            o_orderdate as order_date,
            SUM(o_totalprice) as daily_total
        FROM samples.tpch.orders
        GROUP BY o_orderdate
        ORDER BY o_orderdate
        LIMIT 1000
        """
        
        st.info(f"Executing query on samples.tpch.orders...")
        
        # Execute the query
        result = w.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout="50s"
        )
        
        st.info(f"Query executed. Status: {result.status.state}")
        
        # Convert results to DataFrame
        if result.result and result.result.data_array:
            columns = [col.name for col in result.manifest.schema.columns]
            data = result.result.data_array
            df = pd.DataFrame(data, columns=columns)
            
            st.success(f"✅ Loaded {len(df)} rows")
            
            # Convert order_date to datetime if it's a string
            if 'order_date' in df.columns:
                df['order_date'] = pd.to_datetime(df['order_date'])
            
            # Convert daily_total to numeric
            if 'daily_total' in df.columns:
                df['daily_total'] = pd.to_numeric(df['daily_total'])
            
            return df
        else:
            st.error("No data returned from query")
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Failed to load data: {str(e)}")
        st.exception(e)
        return pd.DataFrame()

# Load the data
with st.spinner("Loading data from Databricks..."):
    df = load_data()

# Display the chart if we have data
if not df.empty:
    st.success(f"📈 Displaying {len(df)} data points")
    
    st.line_chart(
        df,
        x="order_date",
        y="daily_total"
    )
    
    # Show the data table for debugging
    with st.expander("View raw data (first 20 rows)"):
        st.dataframe(df.head(20))
    
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
else:
    st.warning("⚠️ No data available to display")