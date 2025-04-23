# dashboard.py

import os
import io
import streamlit as st
import pandas as pd
import plotly.express as px
from azure.storage.blob import BlobServiceClient, ContainerClient

# ─── 1) PAGE CONFIG & THEME ─────────────────────────────────────────────────────
st.set_page_config(
    page_title="🚖 Enhanced Ride Analytics",
    layout="wide"
)
px.defaults.template = "plotly_dark"

# ─── 2) CREDENTIALS & CLIENT ─────────────────────────────────────────────────────
def _get_azure_credentials():
    acct = os.getenv("AZURE_STORAGE_ACCOUNT")
    sas  = os.getenv("AZURE_SAS_TOKEN")
    cont = os.getenv("AZURE_CONTAINER")
    # fallback to Streamlit secrets if env-vars missing
    if not (acct and sas and cont):
        try:
            secrets = st.secrets
            acct = acct or secrets["AZURE_STORAGE_ACCOUNT"]
            sas  = sas  or secrets["AZURE_SAS_TOKEN"]
            cont = cont or secrets["AZURE_CONTAINER"]
        except Exception:
            pass
    return acct, sas, cont

@st.cache_resource
def get_container_client() -> ContainerClient:
    acct, sas, cont = _get_azure_credentials()
    if not (acct and sas and cont):
        st.error(
            "Missing Azure credentials. "
            "Please set AZURE_STORAGE_ACCOUNT, AZURE_SAS_TOKEN, and AZURE_CONTAINER "
            "as environment variables or in ~/.streamlit/secrets.toml"
        )
        return None
    account_url = f"https://{acct}.blob.core.windows.net"
    service = BlobServiceClient(account_url=account_url, credential=sas)
    return service.get_container_client(cont)

# ─── 3) DATA LOADING ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=600)
def load_data() -> pd.DataFrame:
    client = get_container_client()
    if client is None:
        return pd.DataFrame()

    def load_folder(prefix: str) -> pd.DataFrame:
        parts = []
        for blob_props in client.list_blobs(name_starts_with=prefix):
            # Skip zero-byte blobs
            if getattr(blob_props, "size", 0) == 0:
                st.warning(f"Skipping empty blob: {blob_props.name}")
                continue
            try:
                raw = client.get_blob_client(blob_props.name).download_blob().readall()
                parts.append(pd.read_parquet(io.BytesIO(raw), engine="pyarrow"))
            except Exception as e:
                st.warning(f"Failed to read {blob_props.name}: {e}")
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    rides = load_folder("output_ride/")
    drivers = load_folder("output_driver/")

    if "driver_id" in rides.columns and "driver_id" in drivers.columns:
        df = rides.merge(
            drivers[["driver_id","capacity","status","timestamp"]],
            on="driver_id", how="left", suffixes=("","_drv")
        )
    else:
        df = rides

    # parse timestamps
    df["ts"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    if "pickup_ts" in df.columns:
        df["pickup_ts"] = pd.to_datetime(df["pickup_ts"], unit="s", utc=True)
    if "dropoff_ts" in df.columns:
        df["dropoff_ts"] = pd.to_datetime(df["dropoff_ts"], unit="s", utc=True)

    # flags
    df["is_completed"] = (df["status"] == "COMPLETED").astype(int)
    df["is_active"]    = df["status"].isin(["REQUESTED","ASSIGNED"]).astype(int)

    return df

df = load_data()
if df.empty:
    st.stop()

# ─── 4) SIDEBAR FILTERS ───────────────────────────────────────────────────────────
st.sidebar.header("Filters")
date_range = st.sidebar.date_input("Date range", [])
if len(date_range) == 2:
    start, end = map(pd.to_datetime, date_range)
    df = df[(df.ts >= start) & (df.ts <= end + pd.Timedelta(days=1))]

vehicles = st.sidebar.multiselect(
    "Vehicle type",
    options=df["vehicle_type"].dropna().unique(),
    default=df["vehicle_type"].dropna().unique()
)
df = df[df["vehicle_type"].isin(vehicles)]

statuses = st.sidebar.multiselect(
    "Status",
    options=df["status"].dropna().unique(),
    default=df["status"].dropna().unique()
)
df = df[df["status"].isin(statuses)]

if st.sidebar.checkbox("Show raw data"):
    st.subheader("📋 Raw Data Sample")
    st.dataframe(df.head(200), use_container_width=True)

# ─── 5) KPI METRICS ───────────────────────────────────────────────────────────────
total     = len(df)
completed = int(df["is_completed"].sum())
active    = int(df["is_active"].sum())
cancelled = int((df["status"] == "CANCELLED").sum())

if "driver_assigned_ts" in df.columns:
    df["driver_assigned_ts"] = pd.to_datetime(df["driver_assigned_ts"], unit="s", utc=True)
    avg_resp = (df["driver_assigned_ts"] - df["ts"]).dt.total_seconds().mean()
else:
    avg_resp = None

if "pickup_ts" in df.columns and "dropoff_ts" in df.columns:
    avg_dur = (df["dropoff_ts"] - df["pickup_ts"]).dt.total_seconds().mean()
else:
    avg_dur = None

st.markdown("## 📊 Key Metrics")
c1, c2, c3, c4 = st.columns(4, gap="small")
c1.metric("Total rides", f"{total:,}")
c2.metric("Completed", f"{completed:,}", delta=f"{completed/total:.1%}")
c3.metric("Active", f"{active:,}")
c4.metric("Cancelled", f"{cancelled:,}", delta=f"{cancelled/total:.1%}")
if avg_resp is not None:
    c1.metric("Avg Response (s)", f"{avg_resp:.1f}")
if avg_dur is not None:
    c2.metric("Avg Duration (s)", f"{avg_dur:.1f}")

# ─── 6) DEMAND vs SUPPLY – HOURLY ────────────────────────────────────────────────
st.subheader("⚖️ Demand vs Supply per Hour")
df["hour"] = df.ts.dt.floor("H")
agg = df.groupby("hour").agg(
    demand=("request_id","count"),
    supply=("request_id", lambda x: x[df.loc[x.index,"is_active"]==1].count())
).reset_index()
full_hours = pd.date_range(agg.hour.min(), agg.hour.max(), freq="1H")
agg = (
    agg.set_index("hour")
       .reindex(full_hours, fill_value=0)
       .rename_axis("hour")
       .reset_index()
)
fig1 = px.bar(
    agg, x="hour", y=["demand","supply"],
    barmode="overlay", opacity=0.7,
    labels={"hour":"Time","value":"Count","variable":"Type"}
)
st.plotly_chart(fig1, use_container_width=True)

# ─── 7) RIDE DURATION DISTRIBUTION ───────────────────────────────────────────────
if "pickup_ts" in df.columns and "dropoff_ts" in df.columns:
    st.subheader("⏱️ Ride Duration Distribution")
    df["duration_min"] = (df["dropoff_ts"] - df["pickup_ts"]).dt.total_seconds() / 60
    fig2 = px.histogram(
        df, x="duration_min", nbins=40,
        labels={"duration_min":"Duration (min)","count":"Rides"}
    )
    st.plotly_chart(fig2, use_container_width=True)

# ─── 8) AVERAGE FARE & PASSENGER LOAD ─────────────────────────────────────────────
st.subheader("💰 Avg Fare & 👥 Passenger Load by Vehicle Type")
col_a, col_b = st.columns(2)
with col_a:
    fare = df.groupby("vehicle_type")["estimated_fare"].mean().reset_index()
    fig3 = px.pie(fare, names="vehicle_type", values="estimated_fare",
                  hole=0.3, title="Avg Fare (€)")
    st.plotly_chart(fig3, use_container_width=True)
with col_b:
    load = df.groupby("vehicle_type")["passenger_count"].mean().reset_index()
    fig4 = px.bar(load, x="vehicle_type", y="passenger_count",
                  labels={"vehicle_type":"Type","passenger_count":"Avg Passengers"})
    st.plotly_chart(fig4, use_container_width=True)

# ─── 9) FARE ANOMALIES ───────────────────────────────────────────────────────────
st.subheader("🚨 Fare Anomalies Over Time")
mean_f, std_f = df["estimated_fare"].mean(), df["estimated_fare"].std()
df["anomaly"] = (df["estimated_fare"] - mean_f).abs() > 1.5 * std_f
fig5 = px.scatter(
    df, x="ts", y="estimated_fare",
    color="anomaly", opacity=0.6,
    labels={"ts":"Time","estimated_fare":"Fare (€)"}
)
st.plotly_chart(fig5, use_container_width=True)

# ─── 10) PICKUP LOCATIONS HEATMAP ────────────────────────────────────────────────
if "pickup_lat" in df.columns and "pickup_lon" in df.columns:
    st.subheader("🗺️ Pickup Locations Heatmap")
    fig6 = px.density_mapbox(
        df, lat="pickup_lat", lon="pickup_lon",
        radius=15,
        center={"lat":df["pickup_lat"].mean(),"lon":df["pickup_lon"].mean()},
        zoom=10, mapbox_style="carto-positron"
    )
    st.plotly_chart(fig6, use_container_width=True)

st.markdown("---")
st.caption("🚀 Pro-grade dashboard powered by pandas, Azure Blob & Plotly")
