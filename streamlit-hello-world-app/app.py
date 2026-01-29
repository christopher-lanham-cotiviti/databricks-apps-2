import streamlit as st
import pandas as pd
from databricks import sql
import os

st.set_page_config(layout="wide")
st.title("🔎 Databricks SQL Debug App")

SERVER_HOSTNAME = "dbc-0fa270fd-fb38.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/a3008045957bf8cf"

st.write("Starting app…")
st.write("🔐 Using Databricks App OAuth")

try:
    # Databricks Apps provide authentication automatically
    # Use the environment token or OAuth flow
    access_token = os.environ.get("DATABRICKS_TOKEN")
    
    if not access_token:
        st.warning("No token found, trying OAuth flow...")
        # For Databricks Apps with OAuth enabled
        with sql.connect(
            server_hostname=SERVER_HOSTNAME,
            http_path=HTTP_PATH,
            auth_type="databricks-oauth"
        ) as conn:
            st.success("✅ Connected via OAuth")
            cursor = conn.cursor()
            cursor.execute("SELECT current_user() AS user, current_database() AS db")
            result = cursor.fetchall()
            st.write(result)
    else:
        # Use the provided token
        with sql.connect(
            server_hostname=SERVER_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=access_token
        ) as conn:
            st.success("✅ Connected via App Token")
            cursor = conn.cursor()
            cursor.execute("SELECT current_user() AS user, current_database() AS db")
            result = cursor.fetchall()
            st.write(result)
        
except Exception as e:
    st.error("❌ SQL connection failed")
    st.exception(e)
    st.write("Available environment variables:")
    st.write([key for key in os.environ.keys() if 'DATABRICKS' in key or 'TOKEN' in key])