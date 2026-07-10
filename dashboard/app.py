from __future__ import annotations

import os

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

DSN = os.getenv("TAXI_DW_DSN", "postgresql://taxi:taxi@localhost:5432/taxi_dw")

st.set_page_config(page_title="NYC Yellow Taxi Analytics", page_icon="🚕", layout="wide")


@st.cache_resource
def get_engine():
    return create_engine(DSN, pool_pre_ping=True)


@st.cache_data(ttl=300)
def query(sql: str, params: dict | None = None) -> pd.DataFrame:
    with get_engine().connect() as conn:
        return pd.read_sql(text(sql), conn, params=params or {})


st.title("🚕 NYC Yellow Taxi — Analytics")
st.caption("Star schema in PostgreSQL · loaded by the `nyc_taxi_batch_etl` Airflow DAG")

try:
    bounds = query("""
        SELECT MIN(d.full_date) AS min_date, MAX(d.full_date) AS max_date
        FROM taxi.fact_taxi_trips f
        JOIN taxi.dim_date d ON d.date_key = f.pickup_date_key;
    """)
except Exception as exc:
    st.error("Cannot reach the warehouse. Has the DAG run yet?")
    st.exception(exc)
    st.stop()

if bounds.empty or pd.isna(bounds.loc[0, "min_date"]):
    st.warning("The fact table is empty — trigger the Airflow DAG first.")
    st.stop()

min_date, max_date = bounds.loc[0, "min_date"], bounds.loc[0, "max_date"]

with st.sidebar:
    st.header("Filters")
    date_range = st.date_input("Pickup date range", (min_date, max_date),
                               min_value=min_date, max_value=max_date)
    if len(date_range) != 2:
        st.stop()
    st.markdown("---")
    st.caption("Source: NYC TLC Trip Record Data")

PARAMS = {
    "start": int(pd.Timestamp(date_range[0]).strftime("%Y%m%d")),
    "end": int(pd.Timestamp(date_range[1]).strftime("%Y%m%d")),
}
WHERE = "f.pickup_date_key BETWEEN :start AND :end"

# ------------------------------------------------------------------ KPIs ---
kpis = query(f"""
    SELECT COUNT(*)                                                  AS trips,
           SUM(f.total_amount)                                       AS revenue,
           SUM(f.fare_amount) / NULLIF(SUM(f.trip_distance_miles),0) AS fare_per_mile,
           AVG(f.trip_duration_min)                                  AS avg_duration
    FROM taxi.fact_taxi_trips f WHERE {WHERE};
""", PARAMS).iloc[0]

c1, c2, c3, c4 = st.columns(4)
c1.metric("Total trips", f"{int(kpis.trips):,}")
c2.metric("Total revenue", f"${kpis.revenue:,.0f}")
c3.metric("Avg fare / mile", f"${kpis.fare_per_mile:,.2f}")
c4.metric("Avg trip duration", f"{kpis.avg_duration:,.1f} min")

st.markdown("---")

# --------------------------------------------- Q1: avg fare per mile -------
left, right = st.columns(2)

with left:
    st.subheader("Average fare per mile")
    fpm = query(f"""
        SELECT d.full_date,
               SUM(f.fare_amount) / NULLIF(SUM(f.trip_distance_miles),0) AS avg_fare_per_mile
        FROM taxi.fact_taxi_trips f
        JOIN taxi.dim_date d ON d.date_key = f.pickup_date_key
        WHERE {WHERE}
        GROUP BY d.full_date ORDER BY d.full_date;
    """, PARAMS)
    fig = px.line(fpm, x="full_date", y="avg_fare_per_mile",
                  labels={"full_date": "Date", "avg_fare_per_mile": "$ / mile"})
    st.plotly_chart(fig, use_container_width=True)
    st.caption("SUM(fare) ÷ SUM(miles) — not the mean of per-trip ratios, "
               "which short trips would skew.")

# ------------------------------------------------ Q3: revenue by payment ---
with right:
    st.subheader("Total revenue by payment type")
    pay = query(f"""
        SELECT p.payment_desc, SUM(f.total_amount) AS total_revenue, COUNT(*) AS trips
        FROM taxi.fact_taxi_trips f
        JOIN taxi.dim_payment_type p ON p.payment_type_key = f.payment_type_key
        WHERE {WHERE}
        GROUP BY p.payment_desc ORDER BY total_revenue DESC;
    """, PARAMS)
    fig = px.bar(pay, x="payment_desc", y="total_revenue", text_auto=".2s",
                 labels={"payment_desc": "", "total_revenue": "Revenue ($)"})
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(pay, use_container_width=True, hide_index=True)
    st.caption('"Flex Fare trip" = the 71,743 rows/month with payment_type=0. '
               "Conformed, not dropped — worth ~2.5% of revenue.")

# ------------------------------------------------------- Q2: peak hours ----
st.subheader("Peak hours for taxi rides")
peak = query(f"""
    SELECT t.hour_of_day, t.hour_label, t.day_part, COUNT(*) AS trip_count
    FROM taxi.fact_taxi_trips f
    JOIN taxi.dim_time t ON t.time_key = f.pickup_time_key
    WHERE {WHERE}
    GROUP BY t.hour_of_day, t.hour_label, t.day_part
    ORDER BY t.hour_of_day;
""", PARAMS)
fig = px.bar(peak, x="hour_label", y="trip_count", color="day_part",
             category_orders={"hour_label": peak["hour_label"].tolist()},
             labels={"hour_label": "Pickup hour", "trip_count": "Trips", "day_part": ""})
st.plotly_chart(fig, use_container_width=True)
busiest = peak.sort_values("trip_count", ascending=False).head(3)
st.info("Busiest hours: " + ", ".join(
    f"{r.hour_label} ({r.trip_count:,} trips)" for r in busiest.itertuples()))

# ------------------------------------------------------ Bonus: top zones ---
st.subheader("Top 10 pickup zones by revenue")
zones = query(f"""
    SELECT l.borough, l.zone_name, COUNT(*) AS trips, SUM(f.total_amount) AS revenue
    FROM taxi.fact_taxi_trips f
    JOIN taxi.dim_location l ON l.location_key = f.pu_location_key
    WHERE {WHERE}
    GROUP BY l.borough, l.zone_name ORDER BY revenue DESC LIMIT 10;
""", PARAMS)
st.dataframe(zones, use_container_width=True, hide_index=True)