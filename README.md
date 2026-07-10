<<<<<<< HEAD
# NYC Yellow Taxi — Batch ETL

Assignment 1: Airflow + PostgreSQL + Streamlit.
=======
<<<<<<< HEAD
# nyc-taxi-batch-etl
=======
# NYC Yellow Taxi — Batch ETL

Assignment 1: Airflow + PostgreSQL + Streamlit.
>>>>>>> d8c4b27 (feat(infra): postgres 16 via docker compose with healthcheck and named volume)
>>>>>>> 1d2a36d (Initial project setup)


## Cleaning strategy

**Principle:** impute when the measure is missing but the event is real; delete when the event itself is invalid.

**Duplicates:** no natural key exists in this dataset. Exact full-row duplicates are dropped.
Business-key dedup (vendor + pickup_ts +PULocationID) is rejected: zones are areas, not points, and timestamps are second-resolution,
so concurrent legitimate trips would collide.

**Outliers:** filtered on domain plausibility, not statistical distance.
σ(trip_distance) = 249 miles because the outlier inflates the statistic used
to detect it. A 3σ rule would fail.

**Validation:** three layers —
1. Schema: FK constraints reject bad keys at insert time
2. Load: post-load quality gate fails the DAG on empty/orphan/negative rows
3. Observability: reject_rate_pct persisted per run; drift signals upstream change