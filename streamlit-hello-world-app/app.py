import os
import streamlit as st
import pandas as pd
from databricks import sql

st.set_page_config(layout="wide")
st.title("🔎 Databricks SQL Debug App")

st.write("Starting app…")

# ---- CONFIG (EDIT ONLY IF THESE CHANGE) ----
SERVER_HOSTNAME = "dbc-0fa270fd-fb38.cloud.databricks.com"
HTTP_PATH = "/sql/1.0/warehouses/a3008045957bf8cf"
TOKEN_ENV_VAR = "DATABRICKS_TOKEN"
TIMEOUT_SECONDS = 30
# -------------------------------------------


def debug_sql():
    st.write("🔌 Attempting SQL connection...")

    token = os.environ.get(TOKEN_ENV_VAR)
    if not token:
        st.error("❌ DATABRICKS_TOKEN not found in environment variables")
        st.stop()

    try:
        with sql.connect(
            server_hostname=SERVER_HOSTNAME,
            http_path=HTTP_PATH,
            access_token=token,
            timeout=TIMEOUT_SECONDS,  # 🔑 prevents infinite hang
        ) as conn:

            st.success("✅ Connected to SQL Warehouse")

            cursor = conn.cursor()

            st.write("▶ Running sanity query...")
            cursor.execute("SELECT 1 AS ok")
            st.write("Result:", cursor.fetchall())

            st.write("▶ Checking identity & context...")
            cursor.execute(
                """
                SELECT
                  current_user(),
                  current_catalog(),
                  current_schema()
                """
            )
            st.write(cursor.fetchall())

            st.write("▶ Running real sample query...")
            df = pd.read_sql(
                """
                SELECT
                  date_trunc('day', o_orderdate) AS order_date,
                  SUM(o_totalprice) AS daily_total
                FROM samples.tpch.orders
                GROUP BY 1
                ORDER BY 1
                LIMIT 50
                """,
                conn,
            )

            st.success("🎉 Query completed successfully")
            st.dataframe(df)

    except Exception as e:
        st.error("🔥 SQL ERROR")
        st.exception(e)


# ---- RUN DEBUG ----
debug_sql()
