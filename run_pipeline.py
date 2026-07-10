from __future__ import annotations
import sys
import pandas as pd
from dags.taxi import extract, load, transform
from dags.taxi.config import MONTHS
from dags.taxi.logging_utils import get_logger, log_stage

logger = get_logger("run_pipeline")


def load_dimensions(months: list[str]) -> None:
    load.upsert_dimension(transform.build_dim_vendor(), "dim_vendor", "vendor_key")
    load.upsert_dimension(transform.build_dim_payment_type(), "dim_payment_type", "payment_type_key")
    load.upsert_dimension(transform.build_dim_rate_code(), "dim_rate_code", "rate_code_key")
    load.upsert_dimension(transform.build_dim_time(), "dim_time", "time_key")

    zone_csv = extract.download_zone_lookup()
    load.upsert_dimension(transform.build_dim_location(zone_csv), "dim_location", "location_key")

    # dim_date must cover month_end + 1 day. A trip that starts 31 Jan 23:56 and
    # ends 1 Feb 00:12 has dropoff_date_key = 20230201, and that column has an FK.
    start = f"{min(months)}-01"
    end = (pd.Timestamp(f"{max(months)}-01") + pd.offsets.MonthBegin(1)).strftime("%Y-%m-%d")
    load.upsert_dimension(transform.build_dim_date(start, end), "dim_date", "date_key")


def main() -> None:
    months = sys.argv[1:] or MONTHS

    with log_stage(logger, "pipeline", months=months):
        with log_stage(logger, "load_dimensions"):
            load_dimensions(months)

        for month in months:
            with log_stage(logger, "process_month", month=month):
                path = extract.download_trip_month(month)
                fact, metrics = transform.clean_trips(path, month)
                metrics["rows_loaded"] = load.load_fact_month(fact, month)
                logger.info("month metrics", extra={"context": metrics})

        with log_stage(logger, "quality_check"):
            load.run_quality_checks()


if __name__ == "__main__":
    main()