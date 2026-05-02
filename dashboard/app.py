import streamlit as st
from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd
import os
import json

PROJECT_ID = "marketpulse-494919"
DATASET    = "marketpulse_gold"

# Load credentials from Streamlit secrets
credentials = service_account.Credentials.from_service_account_info(
    json.loads(st.secrets["gcp_service_account"])
)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

st.set_page_config(
    page_title="MarketPulse — Anomaly Monitor",
    page_icon="📈",
    layout="wide"
)

st.title("📈 MarketPulse — Real-Time Market Anomaly Monitor")
st.caption("Detecting volume spikes, price deviations, and wash signals across AAPL, NVDA, TSLA, SPY, AMD")

tab1, tab2 = st.tabs(["🚨 Live Anomaly Feed", "📊 Ticker Stats"])

# ── Tab 1: Live Anomaly Feed ──────────────────────────────────────────────────
with tab1:
    st.subheader("Last 50 Anomalies Detected")

    query = f"""
        SELECT
            detected_at,
            ticker,
            signal_type,
            ROUND(confidence, 3) as confidence,
            ROUND(vwap, 2) as vwap,
            total_volume,
            ROUND(baseline_volume, 0) as baseline_volume,
            ROUND(z_score, 3) as z_score,
            window_start,
            window_end
        FROM `{PROJECT_ID}.{DATASET}.anomalies`
        ORDER BY detected_at DESC
        LIMIT 50
    """

    df = client.query(query).to_dataframe()

    if df.empty:
        st.info("No anomalies detected yet. Run the pipeline to generate data.")
    else:
        def color_signal(val):
            colors = {
                "VOLUME_SPIKE":    "background-color: #FF6B6B; color: white",
                "PRICE_DEVIATION": "background-color: #FFD93D; color: black",
                "WASH_SIGNAL":     "background-color: #6BCB77; color: white",
            }
            return colors.get(val, "")

        styled = df.style.map(color_signal, subset=["signal_type"])
        st.dataframe(styled, use_container_width=True, hide_index=True)

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Anomalies", len(df))
        col2.metric("Tickers Affected", df["ticker"].nunique())
        col3.metric("Most Recent", str(df["detected_at"].max())[:19])

# ── Tab 2: Ticker Stats ───────────────────────────────────────────────────────
with tab2:
    st.subheader("7-Day Ticker Health")

    health_query = f"""
        SELECT
            ticker,
            total_anomalies_7d,
            ROUND(avg_confidence, 3) as avg_confidence,
            dominant_signal,
            FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', last_seen) as last_seen
        FROM `{PROJECT_ID}.{DATASET}.ticker_health`
        ORDER BY total_anomalies_7d DESC
    """

    health_df = client.query(health_query).to_dataframe()

    if health_df.empty:
        st.info("No ticker health data yet.")
    else:
        st.dataframe(health_df, use_container_width=True, hide_index=True)
        st.bar_chart(health_df.set_index("ticker")["total_anomalies_7d"])

    st.subheader("Daily Anomaly Summary")

    daily_query = f"""
        SELECT
            anomaly_date,
            ticker,
            signal_type,
            anomaly_count,
            ROUND(avg_confidence, 3) as avg_confidence
        FROM `{PROJECT_ID}.{DATASET}.daily_anomaly_summary`
        ORDER BY anomaly_date DESC, anomaly_count DESC
    """

    daily_df = client.query(daily_query).to_dataframe()

    if not daily_df.empty:
        st.dataframe(daily_df, use_container_width=True, hide_index=True)