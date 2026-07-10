from __future__ import annotations
from pathlib import Path
import requests
from .config import RAW_DATA_DIR, TRIP_DATA_URL, ZONE_LOOKUP_URL
from .logging_utils import get_logger, log_stage

logger = get_logger(__name__)

CHUNK_BYTES = 1024 * 1024  # 1 MiB

def _download(url: str, destination: Path) -> Path:
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists() and destination.stat().st_size > 0:
        logger.info(
            "download skipped (cached)",
            extra={"context": {"path": str(destination),
                               "size_bytes": destination.stat().st_size}},
        )
        return destination

    tmp = destination.with_suffix(destination.suffix + ".part")
    with log_stage(logger, "download", url=url, destination=str(destination)):
        with requests.get(url, stream=True, timeout=120) as resp:
            resp.raise_for_status()   # a 404 must not become a "parquet" file
            with tmp.open("wb") as fh:
                for chunk in resp.iter_content(chunk_size=CHUNK_BYTES):
                    fh.write(chunk)
        tmp.rename(destination)        # atomic on POSIX

    logger.info(
        "download complete",
        extra={"context": {"path": str(destination),
                           "size_bytes": destination.stat().st_size}},
    )
    return destination


def download_trip_month(month: str) -> str:
    """month is 'YYYY-MM'. Returns the local path (a str, so it's XCom-safe)."""
    dest = RAW_DATA_DIR / f"yellow_tripdata_{month}.parquet"
    return str(_download(TRIP_DATA_URL.format(month=month), dest))


def download_zone_lookup() -> str:
    return str(_download(ZONE_LOOKUP_URL, RAW_DATA_DIR / "taxi_zone_lookup.csv"))