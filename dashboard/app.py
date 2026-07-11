import time

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine


# =====================================================
# DATABASE
# =====================================================
engine = create_engine(
    "postgresql+psycopg2://postgres:postgres@localhost:5433/retail_streaming"
)


# =====================================================
# PAGE
# =====================================================
st.set_page_config(
    page_title="Retail Live Dashboard",
    page_icon="🛒",
    layout="wide",
)

st.title("🛒 Real-Time Retail Dashboard")

st.caption("Updates every time the page refreshes.")


# =====================================================
# LOAD DATA
# =====================================================
df = pd.read_sql(
    "SELECT * FROM purchase_events",
    engine,
)

if df.empty:
    st.warning("No purchase events found.")
    st.stop()


# =====================================================
# KPI
# =====================================================
total_revenue = df["total_amount"].sum()

total_orders = len(df)

average_order = df["total_amount"].mean()

unique_customers = df["customer_id"].nunique()


col1, col2, col3, col4 = st.columns(4)

col1.metric(
    "Revenue",
    f"RM {total_revenue:,.2f}"
)

col2.metric(
    "Orders",
    total_orders
)

col3.metric(
    "Average Order",
    f"RM {average_order:,.2f}"
)

col4.metric(
    "Customers",
    unique_customers
)


st.divider()


# =====================================================
# TOP PRODUCTS
# =====================================================
product_df = (
    df.groupby("product_name")["total_amount"]
    .sum()
    .reset_index()
    .sort_values(
        "total_amount",
        ascending=False
    )
)

fig = px.bar(
    product_df,
    x="product_name",
    y="total_amount",
    title="Revenue by Product"
)

st.plotly_chart(
    fig,
    use_container_width=True
)


# =====================================================
# CATEGORY
# =====================================================
category_df = (
    df.groupby("category")["total_amount"]
    .sum()
    .reset_index()
)

fig2 = px.pie(
    category_df,
    names="category",
    values="total_amount",
    title="Revenue by Category"
)

st.plotly_chart(
    fig2,
    use_container_width=True
)


# =====================================================
# CITY
# =====================================================
city_df = (
    df.groupby("city")["total_amount"]
    .sum()
    .reset_index()
)

fig3 = px.bar(
    city_df,
    x="city",
    y="total_amount",
    title="Revenue by City"
)

st.plotly_chart(
    fig3,
    use_container_width=True
)