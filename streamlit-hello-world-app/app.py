import streamlit as st
import pandas as pd
from databricks import sql

st.set_page_config(layout="wide")
st.title("📊 Orders Over Time (Databricks App Demo)")

@st.cache_data(ttl=300)
def load_data():
    # Uses the app's service principal automatically
    with sql.connect() as conn:
        df = pd.read_sql("""
            SELECT
                date_trunc('day', order_timestamp) AS order_date,
                COUNT(*) AS order_count
            FROM samples.tpch.orders
            GROUP BY 1
            ORDER BY 1
        """, conn)
    return df

df = load_data()

# ---- UI ----
threshold = st.slider(
    "Celebration threshold (total orders)",
    min_value=100,
    max_value=5000,
    value=1000,
    step=100
)

st.line_chart(df, x="order_date", y="order_count")

total_orders = df["order_count"].sum()

st.metric("Total orders", f"{total_orders:,}")

# ---- Celebration (correctly gated) ----
if "celebrated" not in st.session_state:
    st.session_state.celebrated = False

if total_orders >= threshold and not st.session_state.celebrated:
    st.toast("🚀 Order milestone reached!", icon="🎉")
    st.success("Threshold crossed — nice work!")
    st.session_state.celebrated = True
