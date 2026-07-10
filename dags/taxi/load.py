from __future__ import annotations
import io
from contextlib import contextmanager
import pandas as pd
import psycopg2
from psycopg2 import sql
from .config import DW_DSN, LOAD_CHUNK_SIZE
from .logging_utils import get_logger

logger = get_logger(__name__)

@contextmanager
def get_connection():
    """Yield a connection whose transaction commits on success, rolls back on error."""
    conn = psycopg2.connect(DW_DSN)
    try:
        with conn:# psycopg2 overloads __exit__ to COMMIT / ROLLBACK
            yield conn
    finally:
        conn.close()# __exit__ does not close the socket; we must


def _copy_frame(cursor, frame: pd.DataFrame, table: str) -> int:
    """COPY a DataFrame into `table`, matching its column order."""
    buffer = io.StringIO()
    frame.to_csv(buffer, index=False, header=False, na_rep="\\N")
    buffer.seek(0)

    # sql.Identifier quotes/escapes column names — never f-string them.
    statement = sql.SQL("COPY {} ({}) FROM STDIN WITH (FORMAT csv, NULL '\\N')").format(
        sql.SQL(table),
        sql.SQL(", ").join(sql.Identifier(c) for c in frame.columns),
    )
    cursor.copy_expert(statement.as_string(cursor), buffer)
    return len(frame)


def upsert_dimension(frame: pd.DataFrame, table: str, key_column: str) -> int:
    """Insert new dimension members; update attributes of existing ones."""
    non_key = [c for c in frame.columns if c != key_column]
    update_clause = ", ".join(f"{c} = EXCLUDED.{c}" for c in non_key)

    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(f"CREATE TEMP TABLE stage_dim (LIKE taxi.{table}) ON COMMIT DROP;")
        _copy_frame(cur, frame, "stage_dim")
        cur.execute(f"""
            INSERT INTO taxi.{table} ({", ".join(frame.columns)})
            SELECT {", ".join(frame.columns)} FROM stage_dim
            ON CONFLICT ({key_column}) DO UPDATE SET {update_clause};
        """)

    logger.info(
        "dimension upserted",
        extra={"context": {"table": table, "rows": len(frame)}},
    )
    return len(frame)


def load_fact_month(fact: pd.DataFrame, month: str) -> int:
    """Idempotent month reload: delete that month's rows, then COPY the new ones.

    Correct only because transform.clean_trips() guarantees every row's pickup
    timestamp falls inside `month`. The DELETE therefore removes exactly and
    completely the previous load of that month.
    """
    start_key = int(pd.Timestamp(f"{month}-01").strftime("%Y%m%d"))
    end_key = int(
        (pd.Timestamp(f"{month}-01") + pd.offsets.MonthBegin(1)).strftime("%Y%m%d")
    )

    loaded = 0
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(
            "DELETE FROM taxi.fact_taxi_trips "
            "WHERE pickup_date_key >= %s AND pickup_date_key < %s;",
            (start_key, end_key),
        )
        logger.info(
            "fact month cleared (idempotency)",
            extra={"context": {"month": month, "rows_deleted": cur.rowcount}},
        )

        for offset in range(0, len(fact), LOAD_CHUNK_SIZE):
            chunk = fact.iloc[offset:offset + LOAD_CHUNK_SIZE]
            loaded += _copy_frame(cur, chunk, "taxi.fact_taxi_trips")
            logger.info(
                "fact chunk loaded",
                extra={"context": {"month": month,
                                   "rows_loaded_so_far": loaded,
                                   "rows_total": len(fact)}},
            )
    logger.info(
        "fact month loaded",
        extra={"context": {"month": month, "rows_loaded": loaded}},
    )
    return loaded


def run_quality_checks() -> dict:
    """Post-load gate. Raises if the warehouse is not in a sane state."""
    checks = {
        "fact_row_count":
            "SELECT COUNT(*) FROM taxi.fact_taxi_trips;",
        "orphan_location_fks":
            """SELECT COUNT(*) FROM taxi.fact_taxi_trips f
               LEFT JOIN taxi.dim_location l ON l.location_key = f.pu_location_key
               WHERE l.location_key IS NULL;""",
        "negative_totals":
            "SELECT COUNT(*) FROM taxi.fact_taxi_trips WHERE total_amount <= 0;",
        "null_date_keys":
            "SELECT COUNT(*) FROM taxi.fact_taxi_trips WHERE pickup_date_key IS NULL;",
    }

    results = {}
    with get_connection() as conn, conn.cursor() as cur:
        for name, query in checks.items():
            cur.execute(query)
            results[name] = cur.fetchone()[0]

    logger.info("data quality results", extra={"context": results})

    if results["fact_row_count"] == 0:
        raise ValueError("Quality check failed: fact table is empty.")
    for bad in ("orphan_location_fks", "negative_totals", "null_date_keys"):
        if results[bad] > 0:
            raise ValueError(f"Quality check failed: {bad} = {results[bad]}")

    return results