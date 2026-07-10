import pandas as pd

from .config import (
    MAX_PASSENGERS, MAX_TRIP_DISTANCE, MAX_TRIP_DURATION,
    MIN_TOTAL_AMOUNT, MIN_TRIP_DISTANCE, MIN_TRIP_DURATION,
    PAYMENT_TYPES, RATE_CODES, VENDORS,
)

RAW_COLUMNS = [
    "VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime",
    "passenger_count", "trip_distance", "RatecodeID",
    "PULocationID", "DOLocationID", "payment_type",
    "fare_amount", "tip_amount", "tolls_amount",
    "congestion_surcharge", "total_amount",
]

DAY_PARTS = [(0, 6, "Night"), (6, 12, "Morning"), (12, 17, "Afternoon"),
             (17, 21, "Evening"), (21, 24, "Night")]


def clean_trips(parquet_path: str, month: str) -> tuple[pd.DataFrame, dict]:
    df = pd.read_parquet(parquet_path, columns=RAW_COLUMNS)
    rows_extracted = len(df)

    df = df.rename(columns={
        "VendorID": "vendor_key",
        "tpep_pickup_datetime": "pickup_ts",
        "tpep_dropoff_datetime": "dropoff_ts",
        "RatecodeID": "rate_code_key",
        "PULocationID": "pu_location_key",
        "DOLocationID": "do_location_key",
        "payment_type": "payment_type_key",
        "trip_distance": "trip_distance_miles",
    })

    df["pickup_ts"] = pd.to_datetime(df["pickup_ts"], errors="coerce")
    df["dropoff_ts"] = pd.to_datetime(df["dropoff_ts"], errors="coerce")
    df["trip_duration_min"] = (df["dropoff_ts"] - df["pickup_ts"]).dt.total_seconds() / 60.0

    # --- Conform: fill and map unknowns (findings #1, #6, #7) ---
    df["passenger_count"] = df["passenger_count"].fillna(1).astype("int16")
    df["congestion_surcharge"] = df["congestion_surcharge"].fillna(0.0)

    for col, valid, unknown in [
        ("vendor_key", VENDORS, 0),
        ("rate_code_key", RATE_CODES, 99),
        ("payment_type_key", PAYMENT_TYPES, 5),
    ]:
        df[col] = df[col].fillna(unknown).astype("int16")
        df.loc[~df[col].isin(valid.keys()), col] = unknown

    # --- Reject: impossible rows (findings #2, #3, #4, #8) ---
    month_start = pd.Timestamp(f"{month}-01")
    month_end = month_start + pd.offsets.MonthBegin(1)

    keep = (
        df["pickup_ts"].notna() & df["dropoff_ts"].notna()
        & (df["pickup_ts"] >= month_start) & (df["pickup_ts"] < month_end)
        & df["trip_duration_min"].between(MIN_TRIP_DURATION, MAX_TRIP_DURATION)
        & df["trip_distance_miles"].between(MIN_TRIP_DISTANCE, MAX_TRIP_DISTANCE)
        & df["passenger_count"].between(0, MAX_PASSENGERS)
        & (df["fare_amount"] >= 0)
        & (df["tip_amount"] >= 0)
        & (df["tolls_amount"] >= 0)
        & (df["total_amount"] >= MIN_TOTAL_AMOUNT)
        & df["pu_location_key"].between(1, 265)
        & df["do_location_key"].between(1, 265)
    )
    rejected = int((~keep).sum())
    df = df.loc[keep].copy()

    # --- Conform to the star schema ---
    df["pickup_date_key"] = df["pickup_ts"].dt.strftime("%Y%m%d").astype("int32")
    df["dropoff_date_key"] = df["dropoff_ts"].dt.strftime("%Y%m%d").astype("int32")
    df["pickup_time_key"] = df["pickup_ts"].dt.hour.astype("int16")

    fact_columns = [
        "pickup_date_key", "pickup_time_key", "dropoff_date_key",
        "pu_location_key", "do_location_key", "vendor_key",
        "payment_type_key", "rate_code_key",
        "pickup_ts", "dropoff_ts",
        "passenger_count", "trip_distance_miles", "trip_duration_min",
        "fare_amount", "tip_amount", "tolls_amount",
        "congestion_surcharge", "total_amount",
    ]
    fact = df[fact_columns].round(2)

    metrics = {
        "month": month,
        "rows_extracted": rows_extracted,
        "rows_rejected": rejected,
        "rows_clean": len(fact),
        "reject_rate_pct": round(100 * rejected / max(rows_extracted, 1), 3),
    }
    return fact, metrics

def build_dim_date(start: str, end: str) -> pd.DataFrame:
    dates = pd.date_range(start, end, freq="D")
    return pd.DataFrame({
        "date_key": dates.strftime("%Y%m%d").astype(int),
        "full_date": dates.date,
        "day_of_month": dates.day,
        "day_of_week": dates.dayofweek,
        "day_name": dates.day_name(),
        "week_of_year": dates.isocalendar().week.values,
        "month_number": dates.month,
        "month_name": dates.month_name(),
        "quarter": dates.quarter,
        "year": dates.year,
        "is_weekend": dates.dayofweek >= 5,
    })


def build_dim_time() -> pd.DataFrame:
    rows = []
    for hour in range(24):
        day_part = next(p for lo, hi, p in DAY_PARTS if lo <= hour < hi)
        rows.append({
            "time_key": hour,
            "hour_of_day": hour,
            "hour_label": f"{hour:02d}:00",
            "am_pm": "AM" if hour < 12 else "PM",
            "day_part": day_part,
        })
    return pd.DataFrame(rows)


def build_dim_location(zone_csv_path: str) -> pd.DataFrame:
    df = pd.read_csv(zone_csv_path).rename(columns={
        "LocationID": "location_key", "Borough": "borough", "Zone": "zone_name",
    })
    for col in ("borough", "zone_name", "service_zone"):
        df[col] = df[col].fillna("Unknown")
    return df[["location_key", "borough", "zone_name", "service_zone"]].drop_duplicates("location_key")


def _kv_dim(mapping, key_col, val_col):
    return pd.DataFrame([{key_col: k, val_col: v} for k, v in mapping.items()]).sort_values(key_col)


def build_dim_vendor():
    return _kv_dim(VENDORS, "vendor_key", "vendor_name")

def build_dim_payment_type():
    return _kv_dim(PAYMENT_TYPES, "payment_type_key", "payment_desc")

def build_dim_rate_code():
    return _kv_dim(RATE_CODES, "rate_code_key", "rate_code_desc")
