import streamlit as st
import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

st.set_page_config(layout="wide")
st.title("🔎 Databricks SQL Debug App")

st.write("Starting app…")
st.write("🔐 Using Databricks App OAuth")

try:
    # Use Databricks SDK - it handles OAuth automatically for Databricks Apps
    w = WorkspaceClient()
    
    st.success("✅ Connected via Databricks SDK")
    
    # Now query your SQL warehouse
    st.write("▶ Running test query…")
    
    # Execute SQL query
    result = w.statement_execution.execute_statement(
        warehouse_id="a3008045957bf8cf",
        statement="SELECT current_user() AS user, current_database() AS db, current_timestamp() AS time",
        wait_timeout="30s"
    )
    
    # Display results
    if result.result and result.result.data_array:
        columns = [col.name for col in result.manifest.schema.columns]
        data = result.result.data_array
        df = pd.DataFrame(data, columns=columns)
        st.dataframe(df)
    else:
        st.write("Query executed but no data returned")
    
except Exception as e:
    st.error("❌ Connection failed")
    st.exception(e)