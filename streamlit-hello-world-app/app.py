import streamlit as st
import pandas as pd
from databricks import sql

st.set_page_config(layout="wide")
st.title("🔎 Databricks SQL Debug App (OAuth)")

SERVER_HOSTNAME = "dbc-0fa270fd-fb38.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/f29bee003b134bcc"
TIMEOUT_SECONDS = 30

st.write("Starting app…")
st.write("🔐 Using Databricks OAuth (App Authorization)")

try:
    st.write("🔌 Attempting SQL connection…")

    with sql.connect(
        server_hostname=SERVER_HOSTNAME,
        http_path=HTTP_PATH,
        auth_type="databricks-oauth",
        timeout=TIMEOUT_SECONDS,
    ) as conn:

        st.success("✅ Connected to SQL Warehouse")

        cursor = conn.cursor()

        st.write("▶ Running SELECT 1")
        cursor.execute("SELECT 1 AS ok")
        st.write(cursor.fetchall())

        st.write("▶ Identity check")
        cursor.execute("""
            SELECT
              current_user(),
              current_catalog(),
              current_schema()
        """)
        st.write(cursor.fetchall())

        st.write("▶ Running sample query")
        df = pd.read_sql("""
            SELECT
              date_trunc('day', o_orderdate) AS order_date,
              COUNT(*) AS order_count
            FROM samples.tpch.orders
            GROUP BY 1
            ORDER BY 1
            LIMIT 30
        """, conn)

        st.success("🎉 Query successful")
        st.dataframe(df)

except Exception as e:
    st.error("🔥 SQL ERROR")
    st.exception(e)
