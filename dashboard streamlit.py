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
        st.error("Missing Azure credentials. Please configure them.")
        return None
    service = BlobServiceClient(
        account_url=f"https://{acct}.blob.core.windows.net", credential=sas
    )
    return service.get_container_client(cont)

# ─── 3) DATA LOADING ─────────────────────────────────────────────────────────────
@st.cache_data(ttl=600)
def load_data() -> pd.DataFrame:
    client = get_container_client()
    if client is None:
        return pd.DataFrame()

    def load_folder(prefix: str) -> pd.DataFrame:
        parts = []
        for blob in client.list_blobs(name_starts_with=prefix):
            if blob.size == 0 or not blob.name.lower().endswith(".parquet"):
                continue
            try:
                raw = client.get_blob_client(blob.name).download_blob().readall()
                parts.append(pd.read_parquet(io.BytesIO(raw), engine="pyarrow"))
            except Exception as e:
                st.warning(f"Failed to read {blob.name}: {e}")
        return pd.concat(parts, ignore_index=True) if parts else pd.DataFrame()

    rides = load_folder("output_ride")
    drivers = load_folder("output_driver")

    if rides.empty:
        st.error("No ride data found. Check your Azure path.")
        return pd.DataFrame()

    if "driver_id" in rides.columns and "driver_id" in drivers.columns:
        df = rides.merge(
            drivers[["driver_id","capacity","status","timestamp"]],
            on="driver_id", how="left", suffixes=("","_drv")
        )
    else:
        df = rides

    if "timestamp" not in df.columns:
        st.error(f"Missing 'timestamp' column. Available: {list(df.columns)}")
        return pd.DataFrame()
    df["ts"] = pd.to_datetime(df["timestamp"], unit="s", utc=True)

    for col in ("pickup_ts","dropoff_ts","driver_assigned_ts"):  # convert optional
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit="s", utc=True)

    df["is_completed"] = (df["status"] == "COMPLETED").astype(int)
    df["is_active"]    = df["status"].isin(["REQUESTED","ASSIGNED"]).astype(int)

    return df

# ─── EXECUTE LOAD ────────────────────────────────────────────────────────────────
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
    options=df.get("vehicle_type", pd.Series()).dropna().unique(),
    default=df.get("vehicle_type", pd.Series()).dropna().unique()
)
if vehicles:
    df = df[df["vehicle_type"].isin(vehicles)]

statuses = st.sidebar.multiselect(
    "Status",
    options=df.get("status", pd.Series()).dropna().unique(),
    default=df.get("status", pd.Series()).dropna().unique()
)
if statuses:
    df = df[df["status"].isin(statuses)]

if st.sidebar.checkbox("Show raw data"):
    st.subheader("📋 Raw Data Sample")
    st.dataframe(df.head(200), use_container_width=True)

# ─── 5) KPI METRICS ───────────────────────────────────────────────────────────────
total     = len(df)
completed = df["is_completed"].sum()
active    = df["is_active"].sum()
cancelled = (df["status"]=="CANCELLED").sum()

st.markdown("## 📊 Key Metrics")
c1,c2,c3,c4 = st.columns(4, gap="large")
c1.metric("Total rides", f"{total:,}")






# ─── 42) REVENUE SHARE BY VEHICLE TYPE ───────────────────────────────────────────
if {'vehicle_type','estimated_fare'}.issubset(df.columns):
    st.subheader("💰 Revenue Share by Vehicle Type")
    revenue = df.groupby('vehicle_type')['estimated_fare'].sum().reset_index()
    fig37 = px.pie(
        revenue, names='vehicle_type', values='estimated_fare', hole=0.4,
        title='Total Revenue Share by Vehicle Type'
    )
    st.plotly_chart(fig37, use_container_width=True)


# ─── 35) FARE PER PASSENGER DISTRIBUTION ────────────────────────────────────────
if {'estimated_fare','passenger_count'}.issubset(df.columns):
    st.subheader("💵 Fare per Passenger Distribution")
    df['fare_per_passenger'] = df['estimated_fare'] / df['passenger_count'].replace(0, pd.NA)
    fig30 = px.histogram(
        df.dropna(subset=['fare_per_passenger']), x='fare_per_passenger', nbins=40,
        labels={'fare_per_passenger':'Fare per Passenger (€)','count':'Rides'},
        title='Distribution of Fare per Passenger'
    )
    st.plotly_chart(fig30, use_container_width=True)














# ─── 30) VIOLIN PLOT OF FARE BY VEHICLE TYPE ────────────────────────────────────
if {'vehicle_type','estimated_fare'}.issubset(df.columns):
    st.subheader("🎻 Fare Distribution by Vehicle Type (Violin)")
    fig25 = px.violin(
        df, x='vehicle_type', y='estimated_fare',
        box=True, points='all',
        labels={'vehicle_type':'Vehicle Type','estimated_fare':'Fare (€)'},
        title='Fare Distribution by Vehicle Type'
    )
    st.plotly_chart(fig25, use_container_width=True)









# ─── 67) HEATMAP VEHICLE TYPE vs PASSENGER COUNT ────────────────────────────────
if {'vehicle_type','passenger_count'}.issubset(df.columns):
    st.subheader("🔥 Heatmap: Vehicle Type vs Passenger Count")
    heat2 = df.groupby(['vehicle_type','passenger_count']).size().reset_index(name='count')
    fig62 = px.density_heatmap(
        heat2, x='vehicle_type', y='passenger_count', z='count',
        labels={'vehicle_type':'Vehicle Type','passenger_count':'Passengers','count':'Rides'},
        title='Heatmap of Rides by Vehicle Type and Passenger Count'
    )
    st.plotly_chart(fig62, use_container_width=True)





# ─── 30) TOP FARES GEOGRAPHIC MAP ────────────────────────────────────────────────
if {"pickup_lat","pickup_lon","estimated_fare"}.issubset(df.columns):
    st.subheader("🌍 Map of Highest Fares")
    # pick the top 50 most expensive rides
    top_fares = df.nlargest(50, "estimated_fare")
    fig25 = px.scatter_mapbox(
        top_fares,
        lat="pickup_lat", lon="pickup_lon",
        size="estimated_fare", color="estimated_fare",
        hover_data=["request_id","estimated_fare"],
        zoom=10, mapbox_style="carto-positron",
        title="Locations of the Top 50 Highest Fares"
    )
    st.plotly_chart(fig25, use_container_width=True)









    # ─── 10) PICKUP LOCATIONS HEATMAP ──────────────────────────────────────────────
if {"pickup_lat","pickup_lon"}.issubset(df.columns):
    st.subheader("🗺️ Pickup Locations Heatmap")
    fig5 = px.density_mapbox(
        df, lat="pickup_lat", lon="pickup_lon", radius=15,
        center={"lat":df["pickup_lat"].mean(),"lon":df["pickup_lon"].mean()},
        zoom=10, mapbox_style="carto-positron",
        title="Geographic Ride Requests Density"
    )
    st.plotly_chart(fig5, use_container_width=True)

st.markdown("---")
st.caption("Dashboard loaded successfully.")
