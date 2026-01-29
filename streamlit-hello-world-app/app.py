import streamlit as st
import pandas as pd

# -------------------------
# Page setup
# -------------------------
st.set_page_config(layout="wide")
st.title("📊 Orders Over Time (Databricks App Demo)")
st.caption(
    "Demo app using a curated snapshot of production-like order data. "
    "In production, this would be sourced live from Databricks."
)

# -------------------------
# Session state initialization
# -------------------------
if "initialized" not in st.session_state:
    st.session_state.initialized = False
    st.session_state.prev_max = None

# -------------------------
# Load data
# -------------------------
@st.cache_data
def load_data():
    return pd.read_csv("orders_over_time.csv", parse_dates=["order_date"])

df = load_data()

# -------------------------
# Slider to scale data
# -------------------------
threshold = 20_000

scale = st.slider(
    "Scale order volume",
    min_value=1,
    max_value=20,
    value=1,
    help="Simulates growth in order volume"
)

df_scaled = df.copy()
df_scaled["daily_total"] = df_scaled["daily_total"] * scale

# -------------------------
# Chart
# -------------------------
st.line_chart(
    df_scaled,
    x="order_date",
    y="daily_total",
    height=450
)

# -------------------------
# Threshold progress
# -------------------------
current_max = df_scaled["daily_total"].max()

st.progress(
    min(current_max / threshold, 1.0),
    text=f"{current_max:,.0f} / {threshold:,} orders"
)

# -------------------------
# Celebration logic (NO FIRE ON LOAD)
# -------------------------
if not st.session_state.initialized:
    # First render — establish baseline only
    st.session_state.prev_max = current_max
    st.session_state.initialized = True
else:
    # Fire only when crossing upward
    if (
        st.session_state.prev_max < threshold
        and current_max >= threshold
    ):
        # st.success("🚀 Orders crossed 20k!")
        st.toast("🚀 Orders crossed 20k!", icon="🎉")

    st.session_state.prev_max = current_max
