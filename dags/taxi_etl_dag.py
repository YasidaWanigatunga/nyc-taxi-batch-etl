"""
NYC Yellow Taxi — batch ETL DAG
===============================

Flow
----
                      ┌─ download_zone_lookup ─┐
                      │                        ├─ load_static_dimensions ─┐
                      └─ (static seeds) ───────┘                          │
                                                                          ▼
download_trip_month  (mapped over MONTHS)  ──►  process_month (mapped) ──► quality_check ──► report

Airflow features demonstrated
-----------------------------
* TaskFlow API (@dag / @task) with XCom passing
* Dynamic task mapping (`.expand()`) — one task instance per month, run in parallel
* TaskGroup for the dimension build
* Retries with exponential backoff
* `on_failure_callback` at DAG level (Task 4: alert/log on failure)
* Every task writes a row into `taxi.etl_run_audit`
"""
from __future__ import annotations

import pendulum
from airflow.decorators import dag, task, task_group

from taxi import extract, load, transform
from taxi.config import MONTHS, PROJECT_ROOT
from taxi.logging_utils import get_logger, log_stage

logger = get_logger("taxi_etl_dag")


def alert_on_failure(context) -> None:
    """Task 4: alert/log status on failure.

    In production the body becomes a Slack or PagerDuty call. The important
    part is that it fires automatically for *every* task in the DAG.
    """
    ti = context["task_instance"]
    logger.error(
        "AIRFLOW TASK FAILED — ALERT",
        extra={"context": {
            "dag_id": ti.dag_id,
            "task_id": ti.task_id,
            "run_id": context["run_id"],
            "try_number": ti.try_number,
            "log_url": ti.log_url,
            "exception": str(context.get("exception")),
        }},
    )


DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1),
    "retry_exponential_backoff": True,
    "on_failure_callback": alert_on_failure,
}


@dag(
    dag_id="nyc_taxi_batch_etl",
    description="Download → clean → star-schema load of NYC Yellow Taxi trips",
    schedule="0 3 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_tasks=2,
    default_args=DEFAULT_ARGS,
    tags=["assignment-1", "batch", "star-schema"],
)
def nyc_taxi_batch_etl():

    @task
    def create_schema() -> str:
        ddl = (PROJECT_ROOT / "sql" / "schema.sql").read_text()
        with log_stage(logger, "create_schema"):
            with load.get_connection() as conn, conn.cursor() as cur:
                cur.execute(ddl)
        return "schema_ready"

    @task
    def get_months() -> list[str]:
        logger.info("months selected", extra={"context": {"months": MONTHS}})
        return MONTHS

    @task_group(group_id="build_dimensions")
    def build_dimensions(months: list[str]):

        @task
        def load_seed_dimensions() -> None:
            with log_stage(logger, "load_seed_dimensions"):
                load.upsert_dimension(transform.build_dim_vendor(), "dim_vendor", "vendor_key")
                load.upsert_dimension(transform.build_dim_payment_type(), "dim_payment_type", "payment_type_key")
                load.upsert_dimension(transform.build_dim_rate_code(), "dim_rate_code", "rate_code_key")
                load.upsert_dimension(transform.build_dim_time(), "dim_time", "time_key")

        @task
        def load_dim_location() -> None:
            with log_stage(logger, "load_dim_location"):
                csv_path = extract.download_zone_lookup()
                load.upsert_dimension(transform.build_dim_location(csv_path),
                                      "dim_location", "location_key")

        @task
        def load_dim_date(months: list[str]) -> None:
            # Cover month_end + 1: a trip starting 31 Jan 23:56 has a
            # dropoff_date_key of 20230201, and that column has an FK.
            start = f"{min(months)}-01"
            end = (pendulum.parse(f"{max(months)}-01")
                   .add(months=1).add(days=1).to_date_string())
            with log_stage(logger, "load_dim_date", start=start, end=end):
                load.upsert_dimension(transform.build_dim_date(start, end),
                                      "dim_date", "date_key")

        load_seed_dimensions()
        load_dim_location()
        load_dim_date(months)

    @task
    def download_month(month: str) -> dict:
        return {"month": month, "path": extract.download_trip_month(month)}

    @task
    def process_month(payload: dict) -> dict:
        month, path = payload["month"], payload["path"]
        with log_stage(logger, "process_month", month=month):
            fact, metrics = transform.clean_trips(path, month)
            metrics["rows_loaded"] = load.load_fact_month(fact, month)
        logger.info("month metrics", extra={"context": metrics})
        return metrics

    @task
    def quality_check(all_metrics: list[dict]) -> dict:
        with log_stage(logger, "quality_check"):
            results = load.run_quality_checks()
        results["months_processed"] = len(all_metrics)
        results["total_rows_loaded"] = sum(m["rows_loaded"] for m in all_metrics)
        results["total_rows_rejected"] = sum(m["rows_rejected"] for m in all_metrics)
        logger.info("PIPELINE SUMMARY", extra={"context": results})
        return results

    # @task
    # def force_failure() -> None:
    #     raise RuntimeError("Deliberate failure to demonstrate on_failure_callback")


    # ---- wiring ----
    schema_ready = create_schema()
    months = get_months()
    dims = build_dimensions(months)

    downloaded = download_month.expand(month=months)
    processed = process_month.expand(payload=downloaded)

    schema_ready >> months
    dims >> processed          # dimensions must exist before the fact FKs fire

    quality_check(processed)

    # force_failure()   # TEMPORARY catch failiers

nyc_taxi_batch_etl()