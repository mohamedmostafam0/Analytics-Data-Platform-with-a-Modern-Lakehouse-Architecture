import os
import time
import pandas as pd
import plotly.express as px
import streamlit as st
from clickhouse_connect import get_client

# Page Config
st.set_page_config(
    page_title="Flash Sale Analytics",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Configuration ---
CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_HOST', 'clickhouse')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_PORT', 8123))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_PASSWORD', 'mysecret')

# --- Data Connection ---
def get_db_client():
    try:
        return get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD)
    except Exception as e:
        st.error(f"Failed to connect to ClickHouse: {e}")
        return None

client = get_db_client()

# --- Helper Functions ---
@st.cache_data(ttl=5)
def get_hourly_sales():
    query = """
    SELECT 
        toStartOfHour(created_at) AS hour,
        SUM(quantity) AS total_sales,
        SUM(quantity * purchase_price) as total_revenue
        FROM oneshop.purchases
    WHERE deleted = 0
            AND created_at >= now() - toIntervalHour(24)
    GROUP BY hour
    ORDER BY hour ASC
    """
    if client:
        try:
            return client.query_df(query)
        except Exception as e:
            # st.error(f"Error fetching hourly sales: {e}") # Suppress transient errors in loop
            return pd.DataFrame()
    return pd.DataFrame()

@st.cache_data(ttl=5)
def get_top_products():
    query = """
    SELECT 
        item_id,
        SUM(quantity) AS total_quantity,
        SUM(quantity * purchase_price) AS total_revenue
    FROM oneshop.purchases
    WHERE deleted = 0
    GROUP BY item_id
    ORDER BY total_revenue DESC
    LIMIT 10
    """
    if client:
        try:
            return client.query_df(query)
        except Exception as e:
            return pd.DataFrame()
    return pd.DataFrame()

@st.cache_data(ttl=5)
def get_kpis():
    query = """
    SELECT
        SUM(quantity) as total_items_sold,
        SUM(quantity * purchase_price) as total_revenue,
        count() as total_transactions
    FROM oneshop.purchases
    WHERE deleted = 0
    """
    if client:
        try:
            df = client.query_df(query)
            if not df.empty:
                return df.iloc[0]
        except Exception:
            pass
    return None

# --- UI Layout ---

# Sidebar
with st.sidebar:
    st.header("Settings")
    auto_refresh = st.checkbox("Auto-refresh (5s)", value=True)
    
    if st.button("Refresh Now"):
        st.rerun()

st.title("⚡ Flash Sale Analytics Dashboard")
st.markdown("Real-time insights into sales performance.")

# KPIs
kpis = get_kpis()
if kpis is not None:
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Revenue", f"${kpis['total_revenue']:,.2f}")
    col2.metric("Items Sold", f"{kpis['total_items_sold']:,}")
    col3.metric("Transactions", f"{kpis['total_transactions']:,}")
else:
    st.warning("Connecting to Analytics Database...")

st.divider()

# Charts
col_chart, col_table = st.columns([2, 1])

with col_chart:
    st.subheader("Hourly Sales Trend (Last 24h)")
    hourly_sales_df = get_hourly_sales()
    if not hourly_sales_df.empty and "hour" in hourly_sales_df.columns:
        fig = px.bar(
            hourly_sales_df, 
            x="hour", 
            y="total_sales",
            hover_data=["total_revenue"],
            labels={"hour": "Hour", "total_sales": "Items Sold", "total_revenue": "Revenue"},
            template="plotly_dark",
            color="total_sales",
            color_continuous_scale=px.colors.sequential.Viridis
        )
        fig.update_layout(xaxis_title=None, yaxis_title=None, margin=dict(l=0, r=0, t=0, b=0))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("Waiting for data...")

with col_table:
    st.subheader("Top Products")
    top_products_df = get_top_products()
    if not top_products_df.empty and "item_id" in top_products_df.columns:
        st.dataframe(
            top_products_df.style.format({"total_revenue": "${:,.2f}"}),
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No sales data yet.")

# Auto-refresh logic at the end to ensure full render first
if auto_refresh:
    time.sleep(5)
    st.rerun()