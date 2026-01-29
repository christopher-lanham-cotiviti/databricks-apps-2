import streamlit as st
from pyspark.sql import functions as F

st.set_page_config(layout="wide")
st.header("Orders over time (real Databricks data)")

# Pull a small, demo-safe slice
df = (
    spark.table("samples.tpch.orders")
    .select("o_orderdate", "o_totalprice")
    .groupBy("o_orderdate")
    .agg(F.sum("o_totalprice").alias("daily_total"))
    .orderBy("o_orderdate")
    .limit(1000)
)

pdf = df.toPandas()

st.line_chart(
    pdf,
    x="o_orderdate",
    y="daily_total"
)
