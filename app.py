import re
import os
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Miami-Dade Animal Services",
    page_icon="🐾",
    layout="wide",
)

RAW_CSV = "animal_services.csv"
CLEAN_CSV = "animal_services_clean.csv"
DATE_COLS = [
    "ticket_created_date_time",
    "ticket_last_update_date_time",
    "ticket_closed_date_time",
]
API_URL = (
    "https://services.arcgis.com/8Pc9XBTAsYuxx9Ny/arcgis/rest"
    "/services/Animal_Services/FeatureServer/0/query"
)
BATCH_SIZE = 1000
VALID_PRIORITIES = ["Emergency", "Urgent", "Standard"]
METHOD_THRESHOLD = 0.005
DAY_ORDER = ["Monday", "Tuesday", "Wednesday",
             "Thursday", "Friday", "Saturday", "Sunday"]
MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May",
               "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]


# ─────────────────────────────────────────────────────────────────────────────
# ETL — from project.ipynb
# ─────────────────────────────────────────────────────────────────────────────

def extract() -> list[dict]:
    all_data, offset = [], 0
    while True:
        params = {
            "where": "1=1",
            "outFields": "*",
            "f": "json",
            "resultRecordCount": BATCH_SIZE,
            "resultOffset": offset,
        }
        try:
            r = requests.get(API_URL, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
        except requests.exceptions.RequestException as e:
            st.error(f"API error: {e}")
            break

        features = data.get("features", [])
        if not features:
            break
        all_data.extend(f["attributes"] for f in features)
        if not data.get("exceededTransferLimit", False):
            break
        offset += len(features)
    return all_data


def drop_columns(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["case_owner", "case_owner_description", "created_year_month",
            "goal_days", "issue_description", "location_city"]
    return df.drop(columns=cols, errors="ignore")


def date_time_fix(df: pd.DataFrame) -> pd.DataFrame:
    for col in ["ticket_created_date_time", "ticket__last_update_date_time", "ticket_closed_date_time"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], unit="ms", errors="coerce")
    return df


def normalize_capitalization(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].str.strip().str.title()
    for col in ["city", "ticket_status"]:
        if col in df.columns:
            df[col] = df[col].str.replace("_", " ")
    return df


def remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    return df.drop_duplicates(subset="ticket_id")


def validate_zipcode(series: pd.Series) -> pd.Series:
    def clean_zip(z):
        if pd.isna(z):
            return None
        digits = re.sub(r"\D", "", str(z).strip())
        return int(digits[:5]) if len(digits) >= 5 else None
    return series.apply(clean_zip).astype("Int64")


def fix_priority_typo(df: pd.DataFrame) -> pd.DataFrame:
    if "sr_priority" in df.columns:
        df["sr_priority"] = df["sr_priority"].str.replace(
            "Emergncy", "Emergency", regex=False)
    return df


def drop_null_required(df: pd.DataFrame) -> pd.DataFrame:
    return df.dropna(subset=["ticket_id"])


def fix_negative_days(df: pd.DataFrame) -> pd.DataFrame:
    if "actual_completed_days" in df.columns:
        df.loc[df["actual_completed_days"] <
               0, "actual_completed_days"] = pd.NA
    return df


def validate_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    if "latitude" in df.columns and "longitude" in df.columns:
        invalid = (
            df["latitude"].notna() & df["longitude"].notna() & (
                (df["latitude"] < 24) | (df["latitude"] > 27) |
                (df["longitude"] < -82) | (df["longitude"] > -79)
            )
        )
        df.loc[invalid, ["latitude", "longitude"]] = pd.NA
    return df


def rename_update_column(df: pd.DataFrame) -> pd.DataFrame:
    return df.rename(columns={"ticket__last_update_date_time": "ticket_last_update_date_time"})


TRANSFORM_STEPS = [
    ("Dropping unused columns…",          0.15, drop_columns),
    ("Parsing timestamps…",               0.25, date_time_fix),
    ("Normalizing text…",                 0.35, normalize_capitalization),
    ("Removing duplicates…",              0.45, remove_duplicates),
    ("Fixing priority typo…",             0.55, fix_priority_typo),
    ("Dropping rows without ticket_id…",  0.60, drop_null_required),
    ("Fixing negative days…",             0.65, fix_negative_days),
    ("Validating coordinates…",           0.70, validate_coordinates),
    ("Renaming update column…",           0.75, rename_update_column),
]


def run_transforms(df: pd.DataFrame, progress_cb) -> pd.DataFrame:
    for msg, pct, fn in TRANSFORM_STEPS:
        progress_cb(msg, pct)
        df = fn(df)
    progress_cb("Validating zip codes…", 0.80)
    df["zip_code"] = validate_zipcode(df["zip_code"])
    return df


@st.cache_data
def load_clean() -> pd.DataFrame:
    df = pd.read_csv(CLEAN_CSV, parse_dates=DATE_COLS, date_format="mixed")
    df["zip_code"] = df["zip_code"].astype("Int64")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Visualizations — from visualizations.ipynb
# ─────────────────────────────────────────────────────────────────────────────

def plot_issue_types(df: pd.DataFrame, top_n: int = 10) -> go.Figure:
    counts = df["issue_type"].value_counts().head(top_n).reset_index()
    counts.columns = ["issue_type", "count"]
    counts = counts.sort_values("count")
    fig = px.bar(counts, x="count", y="issue_type", orientation="h",
                 title=f"Top {top_n} Issue Types",
                 labels={"count": "Tickets", "issue_type": ""},
                 color="count", color_continuous_scale="Blues")
    fig.update_layout(coloraxis_showscale=False)
    return fig


def plot_tickets_over_time(df: pd.DataFrame) -> go.Figure:
    monthly = (
        df.dropna(subset=["ticket_created_date_time"])
        .set_index("ticket_created_date_time")
        .resample("ME").size().reset_index()
    )
    monthly.columns = ["month", "count"]
    fig = px.line(monthly, x="month", y="count",
                  title="Tickets per Month",
                  labels={"month": "Month", "count": "Tickets"})
    fig.update_traces(line_color="#1f77b4")
    return fig


def plot_method_received(df: pd.DataFrame) -> go.Figure:
    counts = df["method_received"].value_counts()
    threshold = len(df) * METHOD_THRESHOLD
    major = counts[counts >= threshold]
    other_total = counts[counts < threshold].sum()
    if other_total > 0:
        major = pd.concat([major, pd.Series({"Other": other_total})])
    major = major.sort_values()
    plot_df = major.reset_index()
    plot_df.columns = ["method", "count"]
    fig = px.bar(plot_df, x="count", y="method", orientation="h",
                 title="How Tickets Were Submitted",
                 labels={"count": "Tickets", "method": ""},
                 color="count", color_continuous_scale="Blues")
    fig.update_layout(coloraxis_showscale=False)
    return fig


def plot_response_time_by_priority(df: pd.DataFrame, cap_days: int = 60) -> go.Figure:
    subset = df[
        df["sr_priority"].isin(VALID_PRIORITIES) &
        df["actual_completed_days"].notna() &
        (df["actual_completed_days"] <= cap_days)
    ].copy()
    subset["sr_priority"] = pd.Categorical(
        subset["sr_priority"], categories=VALID_PRIORITIES, ordered=True
    )
    fig = px.box(subset, x="sr_priority", y="actual_completed_days",
                 title=f"Response Time by Priority (capped at {cap_days} days)",
                 labels={"sr_priority": "Priority",
                         "actual_completed_days": "Days to Close"},
                 color="sr_priority",
                 color_discrete_map={"Emergency": "#d62728", "Urgent": "#ff7f0e", "Standard": "#1f77b4"})
    fig.update_layout(showlegend=False)
    return fig


def plot_tickets_by_district(df: pd.DataFrame) -> go.Figure:
    counts = df["neighborhood_district"].value_counts().reset_index()
    counts.columns = ["district", "count"]
    fig = px.pie(counts, values="count", names="district",
                 title="Tickets by Neighborhood District")
    fig.update_traces(textposition="inside", textinfo="percent+label")
    return fig


def plot_ticket_status(df: pd.DataFrame) -> go.Figure:
    counts = df["ticket_status"].value_counts().reset_index()
    counts.columns = ["status", "count"]
    counts = counts.sort_values("count")
    fig = px.bar(counts, x="count", y="status", orientation="h",
                 title="Ticket Status Breakdown",
                 labels={"count": "Tickets", "status": ""},
                 color="count", color_continuous_scale="Blues")
    fig.update_layout(coloraxis_showscale=False)
    return fig


def plot_tickets_by_day(df: pd.DataFrame) -> go.Figure:
    counts = (
        df["ticket_created_date_time"].dt.day_name()
        .value_counts().reindex(DAY_ORDER).reset_index()
    )
    counts.columns = ["day", "count"]
    fig = px.bar(counts, x="day", y="count",
                 title="Tickets by Day of Week",
                 labels={"day": "", "count": "Tickets"},
                 color="count", color_continuous_scale="Blues")
    fig.update_layout(coloraxis_showscale=False)
    return fig


def plot_tickets_by_month(df: pd.DataFrame) -> go.Figure:
    counts = (
        df["ticket_created_date_time"].dt.month
        .value_counts().sort_index().reset_index()
    )
    counts.columns = ["month_num", "count"]
    counts["month"] = counts["month_num"].apply(lambda m: MONTH_NAMES[m - 1])
    fig = px.bar(counts, x="month", y="count",
                 title="Seasonal Pattern — Tickets by Month of Year",
                 labels={"month": "", "count": "Tickets"},
                 color="count", color_continuous_scale="Blues")
    fig.update_layout(coloraxis_showscale=False)
    return fig


def plot_day_hour_heatmap(df: pd.DataFrame) -> go.Figure:
    heat = (
        df.dropna(subset=["ticket_created_date_time"])
        .assign(
            day=df["ticket_created_date_time"].dt.day_name(),
            hour=df["ticket_created_date_time"].dt.hour,
        )
        .groupby(["day", "hour"]).size().reset_index(name="count")
    )
    pivot = heat.pivot(index="day", columns="hour", values="count").fillna(0)
    pivot = pivot.reindex(DAY_ORDER)
    fig = go.Figure(go.Heatmap(
        z=pivot.values,
        x=[f"{h:02d}:00" for h in pivot.columns],
        y=pivot.index.tolist(),
        colorscale="Blues",
        hovertemplate="Day: %{y}<br>Hour: %{x}<br>Tickets: %{z:,}<extra></extra>",
    ))
    fig.update_layout(
        title="Call Volume by Day & Hour",
        xaxis_title="Hour of Day",
        yaxis_title="",
        yaxis_autorange="reversed",
    )
    return fig


# ─────────────────────────────────────────────────────────────────────────────
# Data loading / pipeline UI
# ─────────────────────────────────────────────────────────────────────────────

def ensure_data() -> pd.DataFrame:
    # Already fully cleaned
    if os.path.exists(CLEAN_CSV):
        return load_clean()

    # Raw CSV exists — run transforms only
    if os.path.exists(RAW_CSV):
        st.info("Raw CSV found — running transform pipeline…")
        status = st.status("Running pipeline…", expanded=True)
        bar = st.progress(0.0)

        def cb(msg, pct):
            status.write(msg)
            bar.progress(pct)

        df = pd.read_csv(RAW_CSV)
        df = run_transforms(df, cb)
        cb("Saving clean CSV…", 0.90)
        df.to_csv(CLEAN_CSV, index=False)
        cb("Done!", 1.0)
        status.update(label="Pipeline complete!", state="complete")
        st.cache_data.clear()
        return load_clean()

    # No data — offer API fetch
    st.warning("No local data found.")
    if st.button("📡 Fetch from Miami-Dade API (may take a few minutes)"):
        status = st.status("Running full ETL pipeline…", expanded=True)
        bar = st.progress(0.0)

        def cb(msg, pct):
            status.write(msg)
            bar.progress(pct)

        cb("Fetching from API…", 0.05)
        raw = extract()
        df = pd.DataFrame(raw)
        df = run_transforms(df, cb)
        cb("Saving clean CSV…", 0.90)
        df.to_csv(CLEAN_CSV, index=False)
        cb("Done!", 1.0)
        status.update(label="Done!", state="complete")
        st.cache_data.clear()
        st.rerun()

    st.stop()


# ─────────────────────────────────────────────────────────────────────────────
# App
# ─────────────────────────────────────────────────────────────────────────────

df_full = ensure_data()

# ── sidebar filters ───────────────────────────────────────────────────────────
st.sidebar.header("🔍 Filters")

year_min = int(df_full["ticket_created_date_time"].dt.year.min())
year_max = int(df_full["ticket_created_date_time"].dt.year.max())
yr_range = st.sidebar.slider(
    "Year range", year_min, year_max, (year_min, year_max))

all_types = sorted(df_full["issue_type"].dropna().unique())
sel_types = st.sidebar.multiselect("Issue type", all_types, default=all_types)

all_priority = sorted(df_full["sr_priority"].dropna().unique())
sel_priority = st.sidebar.multiselect(
    "Priority", all_priority, default=all_priority)

all_districts = sorted(df_full["neighborhood_district"].dropna().unique())
sel_districts = st.sidebar.multiselect(
    "District", all_districts, default=all_districts)

mask = (
    df_full["ticket_created_date_time"].dt.year.between(*yr_range)
    & df_full["issue_type"].isin(sel_types)
    & df_full["sr_priority"].isin(sel_priority)
    & df_full["neighborhood_district"].isin(sel_districts)
)
df = df_full[mask].copy()

# ── header ────────────────────────────────────────────────────────────────────
st.title("🐾 Miami-Dade Animal Services")
st.caption(f"Showing **{len(df):,}** of **{len(df_full):,}** tickets")

# ── metrics ───────────────────────────────────────────────────────────────────
m1, m2, m3, m4 = st.columns(4)
m1.metric("Total Tickets",         f"{len(df):,}")
m2.metric("Avg Resolution (days)", f"{df['actual_completed_days'].mean():.1f}")
m3.metric("Closed",
          f"{(df['ticket_status'].str.lower() == 'closed').sum():,}")
m4.metric("Issue Types",           df["issue_type"].nunique())

st.divider()

# ── map ───────────────────────────────────────────────────────────────────────
st.subheader("📍 Request Locations")
map_df = df.dropna(subset=["latitude", "longitude"])
if not map_df.empty:
    st.pydeck_chart(pdk.Deck(
        layers=[pdk.Layer(
            "ScatterplotLayer",
            data=map_df[["latitude", "longitude", "issue_type"]],
            get_position="[longitude, latitude]",
            get_color=[31, 119, 180, 160],
            get_radius=200,
            pickable=True,
        )],
        initial_view_state=pdk.ViewState(
            latitude=map_df["latitude"].mean(),
            longitude=map_df["longitude"].mean(),
            zoom=10, pitch=0,
        ),
        tooltip={"text": "{issue_type}"},
    ))
else:
    st.info("No geo data for current filters.")

st.divider()

# ── viz 1 & 2: issue types + time series ─────────────────────────────────────
c1, c2 = st.columns(2)
with c1:
    st.plotly_chart(plot_issue_types(df), use_container_width=True)
with c2:
    st.plotly_chart(plot_tickets_over_time(df), use_container_width=True)

# ── viz 3 & 4: method received + response time box ───────────────────────────
c3, c4 = st.columns(2)
with c3:
    st.plotly_chart(plot_method_received(df), use_container_width=True)
with c4:
    cap = st.slider("Response time cap (days)", 10, 365, 60)
    st.plotly_chart(plot_response_time_by_priority(
        df, cap_days=cap), use_container_width=True)

# ── viz 5 & 6: district pie + ticket status ───────────────────────────────────
c5, c6 = st.columns(2)
with c5:
    st.plotly_chart(plot_tickets_by_district(df), use_container_width=True)
with c6:
    st.plotly_chart(plot_ticket_status(df), use_container_width=True)

# ── viz 7 & 8: day of week + month of year ────────────────────────────────────
c7, c8 = st.columns(2)
with c7:
    st.plotly_chart(plot_tickets_by_day(df), use_container_width=True)
with c8:
    st.plotly_chart(plot_tickets_by_month(df), use_container_width=True)

# ── viz 9: day × hour heatmap ─────────────────────────────────────────────────
st.plotly_chart(plot_day_hour_heatmap(df), use_container_width=True)

# ── raw data ──────────────────────────────────────────────────────────────────
with st.expander("🗂 Raw data"):
    st.dataframe(df.reset_index(drop=True),
                 use_container_width=True, height=300)
