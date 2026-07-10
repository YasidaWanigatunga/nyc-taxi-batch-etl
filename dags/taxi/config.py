import os
from pathlib import Path

PROJECT_ROOT = Path(os.getenv("TAXI_PROJECT_ROOT", "."))
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
SQL_DIR = PROJECT_ROOT / "sql"

MONTHS = os.getenv("TAXI_MONTHS", "2023-01,2023-02").split(",")

TRIP_DATA_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{month}.parquet"
ZONE_LOOKUP_URL = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

DW_DSN = os.getenv("TAXI_DW_DSN", "postgresql://taxi:taxi@localhost:5432/taxi_dw")

LOAD_CHUNK_SIZE = 200_000

# Guard rails — each one traces to a finding in data_quality_findings.md
MIN_TRIP_DISTANCE = 0.01
MAX_TRIP_DISTANCE = 200.0     # finding #4: max was 258,928 miles
MIN_TRIP_DURATION = 0.5       # minutes
MAX_TRIP_DURATION = 24 * 60
MAX_PASSENGERS = 9
MIN_TOTAL_AMOUNT = 0.01       # finding #2: min was -751.00

VENDORS = {
    0: "Unknown",
    1: "Creative Mobile Technologies",
    2: "VeriFone Inc.",
    6: "Myle Technologies",
    7: "Helix",
}

PAYMENT_TYPES = {
    0: "Flex Fare trip",       # finding #1: 71,743 rows
    1: "Credit card",
    2: "Cash",
    3: "No charge",
    4: "Dispute",
    5: "Unknown",
    6: "Voided trip",
}

RATE_CODES = {
    1: "Standard rate",
    2: "JFK",
    3: "Newark",
    4: "Nassau or Westchester",
    5: "Negotiated fare",
    6: "Group ride",
    99: "Unknown",             # finding #7: 13,106 rows
}