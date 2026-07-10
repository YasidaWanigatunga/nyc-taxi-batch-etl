# NYC Yellow Taxi — Batch ETL

Automated batch pipeline: downloads NYC TLC Yellow Taxi trip records,
cleans them, loads a star schema into PostgreSQL, and serves business
metrics through a Streamlit dashboard. Orchestrated with Apache Airflow.

## Tech stack
| Layer | Technology |
|---|---|
| Scripting | Bash (`setup.sh`), Python 3.11 |
| Orchestration | Apache Airflow 2.9.3 (LocalExecutor) |
| Storage | PostgreSQL 16 (Docker) |
| Dashboard | Streamlit + Plotly |
| Infra | Docker Compose |

## Quick start
```bash
git clone <repo> && cd nyc-taxi-batch-etl
chmod +x setup.sh && ./setup.sh
```
Then: Airflow http://localhost:8080 (admin/admin) → un-pause
`nyc_taxi_batch_etl` → ▶ Trigger. Then Streamlit http://localhost:8501.

## Windows 11 / WSL2
Run inside WSL2 with Docker Desktop WSL integration enabled. Clone into
`~/projects/`, **not** `/mnt/c/` — bind-mounts across the Windows boundary
are 10–20x slower. Give Docker ≥ 6 GB RAM.

## Star schema
**Grain: one row per completed taxi trip.**

`fact_taxi_trips` + six dimensions:

| Dimension | Key | Why |
|---|---|---|
| `dim_date` | `YYYYMMDD` | day/week/month roll-ups |
| `dim_time` | hour 0–23 | the *peak hours* query |
| `dim_location` | TLC LocationID | **role-playing**: joined twice (pickup + dropoff) |
| `dim_payment_type` | | *revenue by payment type* |
| `dim_vendor` | | vendor comparison |
| `dim_rate_code` | | airport vs standard fares |

`avg fare per mile` is **not** stored — it's a non-additive ratio,
computed at query time as `SUM(fare) / SUM(miles)`.

## Task mapping
| Task | Where |
|---|---|
| 1. Bash & setup | `setup.sh`, `docker-compose.yml`, `docker/` |
| 2. Data modeling | `sql/schema.sql` |
| 3. Python ETL & orchestration | `dags/taxi_etl_dag.py`, `dags/taxi/{extract,transform,load}.py` |
| 4. Monitoring & logging | `dags/taxi/logging_utils.py`, `alert_on_failure()`, `run_quality_checks()` |
| 5. SQL & dashboard | `sql/analytics_queries.sql`, `dashboard/app.py` |

## Design decisions & assumptions
Full data profile: [`notebooks/data_quality_findings.md`](notebooks/data_quality_findings.md)

1. **71,743 rows/month have `payment_type = 0`** — undocumented in the TLC
   dictionary — and the *same* rows carry all five null columns
   (`passenger_count`, `RatecodeID`, `store_and_fwd_flag`,
   `congestion_surcharge`, `airport_fee`). Verified: zero null
   `passenger_count` rows have `payment_type != 0`. Diagnosed as a separate
   source feed. **Conformed, not dropped** — they carry $3.9M, 2.5% of revenue.
2. **Impute when the measure is missing but the event is real; delete when
   the event itself is invalid.** Nulls → 1 passenger, $0 surcharge, rate
   code 99 = Unknown. Negative fares, zero-mile trips, >24 h durations → rejected.
3. **Outliers filtered on domain plausibility, not statistical distance.**
   σ(trip_distance) = 249 miles because the 258,928-mile outlier inflates the
   very statistic used to detect it. A 3σ rule would keep the garbage and
   delete legitimate airport runs. Cap: 200 miles.
4. **Duplicates: exact full-row only.** Measured: 0 exact, 165 business-key.
   Business-key dedup (vendor + pickup_ts + PULocationID) would delete
   legitimate concurrent trips — zones are areas, not points, and timestamps
   are second-resolution.
5. **Idempotency by delete-then-COPY.** `transform.py` filters pickups to
   inside the file's month, so `DELETE WHERE pickup_date_key IN <month>` is an
   exact, complete reload. Verified: reloading January twice leaves the row
   count unchanged at 5,838,956.
6. **`COPY FROM STDIN`, not `INSERT`/`to_sql`** — ~20x faster; streams in
   200k-row chunks so memory stays flat.
7. **`NUMERIC(10,2)` for money, never `FLOAT`.** Floats can't represent 0.10
   exactly; summing 5.8M of them drifts.
8. **`LocalExecutor`, not Celery.** Celery needs Redis and a worker fleet for
   no benefit on one laptop. The DAG code is executor-agnostic.
9. **One Postgres server, two databases** (`airflow`, `taxi_dw`) to keep the
   local footprint small. In production these would be separate instances.

## Cross-month validation
| Month | Extracted | Rejected | Reject % |
|---|---|---|---|
| 2023-01 | 3,066,766 | 73,397 | 2.393 |
| 2023-02 | 2,913,955 | 68,368 | 2.346 |

Reject rates agree to within 0.05% across two independent files: the rules
characterise the source feed, not one month's quirks. A large divergence in a
future month would signal an upstream schema change.

## Monitoring & observability
- **Structured JSON logging** (`JsonFormatter`): `ts`, `level`, `logger`,
  `message`, `context` — ready for Loki/ELK ingestion without regex parsing.
- **Stage timing**: `log_stage()` emits START/SUCCESS/FAILED with
  `duration_seconds`; every month logs `rows_extracted`, `rows_duplicate`,
  `rows_rejected`, `reject_rate_pct`, `rows_loaded`.
- **Failure alerting**: `on_failure_callback` in `default_args` fires for every
  task. Verified by temporarily adding a task that raises — the callback fired
  after the final retry with `try_number: 4`, emitting `dag_id`, `task_id`,
  `log_url`, and the exception.
  Note: Airflow's **"Mark as Failed" does *not* trigger callbacks** — it writes
  metadata state without executing the task. Tested, not assumed.
- **Retries**: 2, with exponential backoff. Safe precisely because the load is
  idempotent — a retry cannot duplicate data.
- **Quality gate**: `run_quality_checks()` fails the DAG on an empty fact
  table, orphan foreign keys, non-positive totals, or null date keys.

## Observed performance regression
| | First load (empty table) | After 5.8M rows + 4 indexes |
|---|---|---|
| `process_month` | 90 s | 476 s |
| `quality_check` | 19 s | 30 s |

Diagnosed from the `duration_seconds` field in the structured logs, not guessed.
Fixes, in order of preference:
1. Partition `fact_taxi_trips` by `RANGE (pickup_date_key)` — each month's COPY
   then touches only its own partition's indexes, and the idempotent DELETE
   becomes an O(1) `DROP PARTITION`.
2. Drop indexes before bulk load, `CREATE INDEX` after.
3. `SET session_replication_role = replica` to defer FK checks during load.

## What I'd do differently with more time
- `pytest` coverage for `clean_trips()` (assert a negative fare is rejected,
  a null `RatecodeID` maps to 99)
- dbt for the transformation layer, with `dbt test` assertions
- Great Expectations instead of hand-rolled quality checks
- Partition the fact table by month (see above)


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