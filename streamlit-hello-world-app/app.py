import streamlit as st
import pandas as pd
from databricks import sql
import time

st.set_page_config(layout="wide")
st.title("🔎 Databricks SQL Debug App (OAuth)")

SERVER_HOSTNAME = "dbc-0fa270fd-fb38.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/f29bee003b134bcc"
TIMEOUT_SECONDS = 30

st.write("Starting app…")
st.write("🔐 Using Databricks OAuth (App Authorization)")

st.write("⏱ Preparing SQL connection…")

start = time.time()

try:
    with sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        auth_type="databricks-oauth",
        timeout=15,   # 🔥 IMPORTANT
    ) as conn:

        st.success(f"✅ Connected in {round(time.time() - start, 2)}s")

        cursor = conn.cursor()
        st.write("▶ Running test query…")
        cursor.execute("SELECT 1")
        st.write(cursor.fetchall())

except Exception as e:
    st.error("❌ SQL connection failed")
    st.exception(e)
