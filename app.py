import os
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="Miami-Dade Animal Services",
    page_icon="🐾",
    layout="wide",
)

VALID_PRIORITIES = ["Emergency", "Urgent", "Standard"]
ISSUE_REMAP = {
    "Animal Cruelty Follow-Up":      "Animal Cruelty",
    "Animal Cruelty Investigation":  "Animal Cruelty",
    "Cat Trap Request":              "Trap Request",
    "Dog Trap Request":              "Trap Request",
    "Found Pet":                     "Lost/Found Pet",
    "Lost Pet":                      "Lost/Found Pet",
}
METHOD_THRESHOLD = 0.005
DAY_ORDER = ["Monday", "Tuesday", "Wednesday",
             "Thursday", "Friday", "Saturday", "Sunday"]
MONTH_NAMES = ["Jan", "Feb", "Mar", "Apr", "May",
               "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
DATE_COLS = [
    "ticket_created_date_time",
    "ticket_last_update_date_time",
    "ticket_closed_date_time",
]

SQL = text("""
    SELECT
        t.ticket_id,
        it.name                AS issue_type,
        t.street_address,
        t.city,
        t.state,
        t.zip_code,
        d.name                 AS neighborhood_district,
        t.ticket_created_at    AS ticket_created_date_time,
        t.ticket_updated_at    AS ticket_last_update_date_time,
        t.ticket_closed_at     AS ticket_closed_date_time,
        ts.name                AS ticket_status,
        t.latitude,
        t.longitude,
        sm.name                AS method_received,
        p.name                 AS sr_priority,
        t.actual_completed_days
    FROM tickets t
    LEFT JOIN issue_types        it ON t.issue_type_id = it.id
    LEFT JOIN ticket_statuses    ts ON t.status_id     = ts.id
    LEFT JOIN priorities          p ON t.priority_id   = p.id
    LEFT JOIN submission_methods sm ON t.method_id     = sm.id
    LEFT JOIN districts           d ON t.district_id   = d.id
""")


@st.cache_resource
def get_engine():
    user = os.environ.get("DB_USER",     "postgres")
    pwd  = os.environ.get("DB_PASSWORD", "postgres123")
    host = os.environ.get("DB_HOST",     "localhost")
    port = os.environ.get("DB_PORT",     "5433")
    db   = os.environ.get("DB_NAME",     "animal_db")
    return create_engine(f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}")


@st.cache_data
def load_data() -> pd.DataFrame:
    with get_engine().connect() as conn:
        df = pd.read_sql(SQL, conn, parse_dates=DATE_COLS)
    df["zip_code"] = df["zip_code"].astype("Int64")
    return df


# ─────────────────────────────────────────────────────────────────────────────
# Visualizations
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
    plot_df = df.dropna(subset=["ticket_created_date_time", "issue_type"]).copy()
    plot_df["issue_type"] = plot_df["issue_type"].replace(ISSUE_REMAP)
    top6 = plot_df["issue_type"].value_counts().head(6).index
    plot_df["issue_type"] = plot_df["issue_type"].where(plot_df["issue_type"].isin(top6), "Other")
    monthly = (
        plot_df.groupby([pd.Grouper(key="ticket_created_date_time", freq="ME"), "issue_type"])
        .size().reset_index(name="count")
    )
    fig = px.area(monthly, x="ticket_created_date_time", y="count", color="issue_type",
                  title="Tickets per Month",
                  labels={"ticket_created_date_time": "Month", "count": "Tickets",
                          "issue_type": "Issue Type"})
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
# App
# ─────────────────────────────────────────────────────────────────────────────

try:
    df_full = load_data()
except Exception as e:
    st.error(f"Could not connect to database: {e}")
    st.info("Make sure the ETL pipeline has run and PostgreSQL is available.")
    st.stop()

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
map_color_by = st.selectbox("Color dots by", ["Issue Type", "Priority"], key="map_color")
map_df = df.dropna(subset=["latitude", "longitude"]).copy()
if not map_df.empty:
    color_col = "issue_type" if map_color_by == "Issue Type" else "sr_priority"
    categories = sorted(map_df[color_col].fillna("Unknown").unique())
    palette = px.colors.qualitative.Plotly
    color_lookup = {}
    for i, cat in enumerate(categories):
        h = palette[i % len(palette)].lstrip("#")
        color_lookup[cat] = [int(h[0:2], 16), int(h[2:4], 16), int(h[4:6], 16), 180]
    map_df["_color"] = map_df[color_col].fillna("Unknown").map(color_lookup)
    st.pydeck_chart(pdk.Deck(
        layers=[pdk.Layer(
            "ScatterplotLayer",
            data=map_df[["latitude", "longitude", "issue_type", "sr_priority", "_color"]],
            get_position="[longitude, latitude]",
            get_color="_color",
            get_radius=25,
            pickable=True,
        )],
        initial_view_state=pdk.ViewState(
            latitude=map_df["latitude"].mean(),
            longitude=map_df["longitude"].mean(),
            zoom=10, pitch=0,
        ),
        tooltip={"text": "{issue_type} — {sr_priority}"},
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
