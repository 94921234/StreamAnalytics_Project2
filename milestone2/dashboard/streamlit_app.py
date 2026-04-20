"""
streamlit_app.py - Milestone 2.4 Dashboard
Group 04 - Real-Time Food Delivery Analytics

Live dashboard for Spark Structured Streaming pipeline.
Reads Parquet metrics from Azure Blob Storage and displays:
  - Live KPIs + time-series charts
  - Zone-level analytics
  - Health view: surge indicators (UC3A)
  - Courier efficiency alerts (UC2B)
  - Pricing anomaly proxy (UC3B)

Run (from milestone2/dashboard/):
    streamlit run streamlit_app.py

Environment variables required:
    STORAGE_ACCOUNT_KEY   - Azure Blob Storage account key
    STORAGE_ACCOUNT_NAME  - (optional) defaults to iesstsabbadbab
    CONTAINER_NAME        - (optional) defaults to group04
"""

import io
import os
import datetime

import pandas as pd
import plotly.express as px
import streamlit as st
from azure.storage.blob import BlobServiceClient

# ── Config ──────────────────────────────────────────────────────────────
STORAGE_ACCOUNT = os.getenv("STORAGE_ACCOUNT_NAME", "iesstsabbadbab")
CONTAINER = os.getenv("CONTAINER_NAME", "group04")
STORAGE_KEY = os.getenv("STORAGE_ACCOUNT_KEY", "").strip()
ACCOUNT_URL = f"https://{STORAGE_ACCOUNT}.blob.core.windows.net"

METRICS_PATH = {
    "uc1": "metrics/uc1/",
    "uc2": "metrics/uc2/",
    "uc2b": "metrics/uc2b/",
    "uc3": "metrics/uc3/",
    "uc3b": "metrics/uc3b/",
}

REFRESH_INTERVAL = 30  # seconds

# ── Azure Blob helpers ──────────────────────────────────────────────────
@st.cache_resource(show_spinner=False)
def get_blob_client():
    if not STORAGE_KEY:
        st.error("STORAGE_ACCOUNT_KEY not set. Run: export STORAGE_ACCOUNT_KEY='<key>'")
        st.stop()
    return BlobServiceClient(account_url=ACCOUNT_URL, credential=STORAGE_KEY)

def read_parquet_blobs(client, prefix):
    cc = client.get_container_client(CONTAINER)
    blobs = [b.name for b in cc.list_blobs(name_starts_with=prefix) if b.name.endswith(".parquet")]
    if not blobs:
        return pd.DataFrame()

    frames = []
    for name in blobs:
        try:
            data = cc.download_blob(name).readall()
            frames.append(pd.read_parquet(io.BytesIO(data)))
        except Exception as e:
            st.warning(f"Could not read {name}: {e}")

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

@st.cache_data(ttl=REFRESH_INTERVAL, show_spinner="Loading from Azure Blob Storage...")
def load_metrics(metric):
    client = get_blob_client()
    df = read_parquet_blobs(client, METRICS_PATH[metric])

    for col in ["window_start", "window_end", "ws", "we"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True)
    if not df.empty and "window_start" in df.columns:
        df = df.sort_values("window_start")

    return df

# ── Page setup ──────────────────────────────────────────────────────────
st.set_page_config(page_title="Group 04 - Real-Time Delivery Analytics", layout="wide")
st.title("Group 04 - Real-Time Delivery Analytics")
st.caption(
    f"Source: wasbs://{CONTAINER}@{STORAGE_ACCOUNT}.blob.core.windows.net/metrics/ | "
    f"Auto-refresh: {REFRESH_INTERVAL}s"
)

df_uc1 = load_metrics("uc1")
df_uc2 = load_metrics("uc2")
df_uc2b = load_metrics("uc2b")
df_uc3 = load_metrics("uc3")
df_uc3b = load_metrics("uc3b")

if df_uc1.empty:
    st.warning("UC1 not loaded yet")
    st.warning("No metrics data yet. Ensure spark_streaming.py is running.")
    st.stop()

# ── Sidebar ─────────────────────────────────────────────────────────────
st.sidebar.header("Filters")

zone_candidates = []
for df in [df_uc1, df_uc2, df_uc2b, df_uc3, df_uc3b]:
    if not df.empty and "zone_id" in df.columns:
        zone_candidates.extend(df["zone_id"].dropna().astype(str).unique().tolist())

all_zones = sorted(set(zone_candidates))
selected_zones = st.sidebar.multiselect("Zone(s)", options=all_zones, default=all_zones)

time_range = None
time_df = None
for df in [df_uc1, df_uc2, df_uc2b, df_uc3, df_uc3b]:
    if not df.empty and "window_start" in df.columns:
        time_df = df
        break

if time_df is not None:
    min_ts = time_df["window_start"].min().to_pydatetime()
    max_ts = time_df["window_start"].max().to_pydatetime()
    if min_ts < max_ts:
        time_range = st.sidebar.slider(
            "Time window",
            min_value=min_ts,
            max_value=max_ts,
            value=(min_ts, max_ts),
            format="HH:mm",
        )

st.sidebar.markdown("---")
st.sidebar.markdown(f"Last refreshed: {datetime.datetime.utcnow().strftime('%H:%M:%S')} UTC")

def apply_filters(df, zone_col="zone_id"):
    if df.empty:
        return df

    if selected_zones and zone_col in df.columns:
        df = df[df[zone_col].astype(str).isin([str(z) for z in selected_zones])]

    if time_range and "window_start" in df.columns:
        s = pd.Timestamp(time_range[0]).tz_localize("UTC") if pd.Timestamp(time_range[0]).tzinfo is None else pd.Timestamp(time_range[0])
        e = pd.Timestamp(time_range[1]).tz_localize("UTC") if pd.Timestamp(time_range[1]).tzinfo is None else pd.Timestamp(time_range[1])
        df = df[(df["window_start"] >= s) & (df["window_start"] <= e)]

    return df

uc1_f = apply_filters(df_uc1)
uc2_f = apply_filters(df_uc2)
uc2b_f = apply_filters(df_uc2b)
uc3_f = apply_filters(df_uc3)
uc3b_f = apply_filters(df_uc3b)

# ── Tabs ────────────────────────────────────────────────────────────────
tab_kpi, tab_zones, tab_efficiency, tab_health, tab_pricing = st.tabs(
    ["Live KPIs", "Zone Analysis", "Courier Efficiency", "Health and Alerts", "Pricing Anomalies"]
)

# TAB 1 - Live KPIs
with tab_kpi:
    st.subheader("Live KPIs")

    if uc1_f.empty:
        st.info("No UC1 data for current filter selection.")
    else:
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Total Orders", f"{int(uc1_f['order_count'].sum()):,}")
        c2.metric("Total Revenue", f"EUR {float(uc1_f['total_value'].sum()):,.2f}")
        c3.metric("Avg Order Value", f"EUR {float(uc1_f['avg_value'].mean()):.2f}")
        c4.metric(
            "Latest Window",
            uc1_f["window_start"].max().strftime("%H:%M") if "window_start" in uc1_f.columns else "N/A"
        )

        st.markdown("---")

        ts = (
            uc1_f.groupby("window_start", as_index=False)["order_count"]
            .sum()
            .sort_values("window_start")
        )
        st.plotly_chart(
            px.line(
                ts,
                x="window_start",
                y="order_count",
                title="Orders per Minute",
                labels={"window_start": "Time", "order_count": "Orders"},
                markers=True,
                height=320,
            ),
            use_container_width=True,
        )

        tr = (
            uc1_f.groupby("window_start", as_index=False)["total_value"]
            .sum()
            .sort_values("window_start")
        )
        st.plotly_chart(
            px.area(
                tr,
                x="window_start",
                y="total_value",
                title="Revenue per Minute (EUR)",
                color_discrete_sequence=["#2ecc71"],
                height=280,
            ),
            use_container_width=True,
        )

# TAB 2 - Zone Analysis (UC2A)
with tab_zones:
    st.subheader("Zone-Level Demand vs Supply")

    col_l, col_r = st.columns(2)

    with col_l:
        if not uc1_f.empty:
            zo = (
                uc1_f.groupby("zone_id", as_index=False)["order_count"]
                .sum()
                .sort_values("order_count", ascending=False)
            )
            st.plotly_chart(
                px.bar(
                    zo,
                    x="zone_id",
                    y="order_count",
                    title="Orders by Zone",
                    color="order_count",
                    color_continuous_scale="Blues",
                    height=360,
                ),
                use_container_width=True,
            )

    with col_r:
        if not uc2_f.empty:
            latest_uc2 = (
                uc2_f.sort_values("window_start")
                .groupby("zone_id")
                .last()
                .reset_index()
            )
            st.plotly_chart(
                px.scatter(
                    latest_uc2,
                    x="demand",
                    y="supply",
                    text="zone_id",
                    title="Demand vs Supply (latest window)",
                    size="demand",
                    color="imbalance",
                    color_continuous_scale="RdYlGn_r",
                    height=360,
                ),
                use_container_width=True,
            )

    if not uc2_f.empty:
        fig_imb = px.line(
            uc2_f.sort_values("window_start"),
            x="window_start",
            y="imbalance",
            color="zone_id",
            title="Imbalance per Zone over Time",
            height=300,
        )
        fig_imb.add_hline(y=0, line_dash="dash", line_color="gray", annotation_text="Balanced")
        st.plotly_chart(fig_imb, use_container_width=True)

# TAB 3 - Courier Efficiency (UC2B)
with tab_efficiency:
    st.subheader("UC2B — Courier Idle Inefficiency")

    if uc2b_f.empty:
        st.info("No UC2B data for current filter selection.")
    else:
        latest_uc2b = (
            uc2b_f.sort_values("window_start")
            .groupby("zone_id")
            .last()
            .reset_index()
        )

        c1, c2, c3 = st.columns(3)
        c1.metric("Underutilised Zones", int(latest_uc2b["is_underutilised"].sum()))
        c2.metric("Avg Utilization Ratio", f"{latest_uc2b['utilization_ratio'].mean():.2f}")
        c3.metric("Total Idle Inefficiency", f"{latest_uc2b['idle_inefficiency'].sum():.0f}")

        st.markdown("---")

        st.dataframe(
            latest_uc2b[
                [
                    "zone_id",
                    "demand",
                    "available_couriers",
                    "utilization_ratio",
                    "idle_inefficiency",
                    "is_underutilised",
                ]
            ],
            use_container_width=True,
        )

        st.plotly_chart(
            px.bar(
                latest_uc2b.sort_values("idle_inefficiency", ascending=False),
                x="zone_id",
                y="idle_inefficiency",
                color="is_underutilised",
                title="Idle Inefficiency by Zone",
                height=320,
            ),
            use_container_width=True,
        )

        fig_util = px.line(
            uc2b_f.sort_values("window_start"),
            x="window_start",
            y="utilization_ratio",
            color="zone_id",
            title="Utilization Ratio over Time",
            height=300,
        )
        fig_util.add_hline(y=0.5, line_dash="dash", line_color="red", annotation_text="Underutilisation threshold")
        st.plotly_chart(fig_util, use_container_width=True)

# TAB 4 - Health and Alerts (UC3A)
with tab_health:
    st.subheader("Health View — Surge Alerts")

    if uc3_f.empty:
        st.info("No UC3A data for current filter selection.")
    else:
        surges = uc3_f[uc3_f["is_surge"] == True]

        c1, c2, c3 = st.columns(3)
        c1.metric("Surge Events", len(surges))
        c2.metric("Zones in Surge", surges["zone_id"].nunique() if not surges.empty else 0)
        c3.metric("Max Ratio", f"{uc3_f['ratio'].max():.2f}x" if "ratio" in uc3_f.columns else "N/A")

        st.markdown("---")

        if not surges.empty:
            st.error("SURGE ALERT — Zones with demand/supply ratio > 1.5x")
            latest_surges = (
                surges.sort_values("window_start")
                .groupby("zone_id")
                .last()
                .reset_index()
            )
            for _, row in latest_surges.iterrows():
                st.warning(
                    f"Zone {row['zone_id']} | Ratio: {row['ratio']:.2f}x | "
                    f"Demand: {int(row['demand'])} | Supply: {int(row['supply'])} | "
                    f"{row['window_start'].strftime('%H:%M')}"
                )
        else:
            st.success("No active surge zones detected.")

        st.markdown("---")

        pivot = uc3_f.pivot_table(index="window_start", columns="zone_id", values="ratio", aggfunc="mean")
        if not pivot.empty:
            st.plotly_chart(
                px.imshow(
                    pivot.T,
                    color_continuous_scale="RdYlGn_r",
                    aspect="auto",
                    title="Ratio Heatmap (red=surge, green=healthy)",
                    height=350,
                ),
                use_container_width=True,
            )

        fig_r = px.line(
            uc3_f.sort_values("window_start"),
            x="window_start",
            y="ratio",
            color="zone_id",
            title="Demand/Supply Ratio per Zone",
            height=300,
        )
        fig_r.add_hline(y=1.5, line_dash="dash", line_color="red", annotation_text="Surge threshold (1.5x)")
        st.plotly_chart(fig_r, use_container_width=True)

# TAB 5 - Pricing Anomalies (UC3B)
with tab_pricing:
    st.subheader("UC3B — Pricing Anomaly Proxy")

    if uc3b_f.empty:
        st.info("No UC3B data for current filter selection.")
    else:
        latest_uc3b = (
            uc3b_f.sort_values("window_start")
            .groupby("zone_id")
            .last()
            .reset_index()
        )

        anomalies = latest_uc3b[latest_uc3b["is_pricing_anomaly"] == True]

        c1, c2, c3 = st.columns(3)
        c1.metric("Pricing Anomalies", len(anomalies))
        c2.metric("Zones Flagged", anomalies["zone_id"].nunique() if not anomalies.empty else 0)
        c3.metric(
            "Max Deviation Ratio",
            f"{latest_uc3b['price_deviation_ratio'].max():.2f}x"
            if "price_deviation_ratio" in latest_uc3b.columns else "N/A",
        )

        st.markdown("---")

        st.dataframe(
            latest_uc3b[
                [
                    "zone_id",
                    "current_avg_order_value",
                    "baseline_avg_order_value",
                    "price_deviation_ratio",
                    "order_count",
                    "is_pricing_anomaly",
                ]
            ],
            use_container_width=True,
        )

        st.plotly_chart(
            px.bar(
                latest_uc3b.sort_values("price_deviation_ratio", ascending=False),
                x="zone_id",
                y="price_deviation_ratio",
                color="is_pricing_anomaly",
                title="Price Deviation Ratio by Zone",
                height=320,
            ),
            use_container_width=True,
        )

        fig_price = px.line(
            uc3b_f.sort_values("window_start"),
            x="window_start",
            y="price_deviation_ratio",
            color="zone_id",
            title="Pricing Deviation over Time",
            height=300,
        )
        fig_price.add_hline(
            y=1.25,
            line_dash="dash",
            line_color="red",
            annotation_text="Anomaly threshold (1.25x)",
        )
        st.plotly_chart(fig_price, use_container_width=True)

st.markdown("---")
if st.button("Refresh now"):
    st.cache_data.clear()
    st.rerun()
