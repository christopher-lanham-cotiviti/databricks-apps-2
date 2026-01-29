import streamlit as st
import pandas as pd

from streamlit.components.v1 import html

def confetti():
    html("""
    <script src="https://cdn.jsdelivr.net/npm/canvas-confetti@1.6.0/dist/confetti.browser.min.js"></script>
    <script>
      confetti({ particleCount: 150, spread: 80, origin: { y: 0.6 } });
    </script>
    """, height=0)

st.set_page_config(layout="wide")
 
st.header("Hello world!!!")

apps = st.slider("Number of apps", max_value=60, value=10)

chart_data = pd.DataFrame({
    'y': [2 ** x for x in range(apps)]
})

st.bar_chart(
    chart_data,
    height=500,
    width=min(100 + 50 * apps, 1000),
    use_container_width=False,
    x_label="Apps",
    y_label="Fun with data"
)

# 🎉 Celebrate when we cross a threshold
if chart_data["y"].max() >= 100_000_000:
    st.success("🚀 Goal reached!")
    confetti()